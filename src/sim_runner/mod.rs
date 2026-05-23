//! 端到端仿真主循环
//!
//! 我们没有使用 `Simulator` 中的 handler 机制（那种风格在每个 handler 需要访问
//! 多个全局状态时会受 borrow checker 限制）。这里采用集中式：所有事件先由
//! Simulator 排序，主循环根据事件类型查实体表，直接修改状态。
//!
//! 事件流：
//!   FlowStart    → 向 Protocol 注册流
//!   TxTick       → Protocol.on_tx_tick()，把生成的 packet 转为 PacketDepart
//!   PacketDepart → 链路 serialization+prop_delay 后 → PacketArrive @ switch
//!   PacketArrive @ switch → Switch.ingress；若端口空闲再 PacketDepart 到下一跳
//!   PacketArrive @ host   → 如果是 Data，Protocol.on_rx_data() 生成控制包；
//!                          → 如果是 Control，Protocol.on_tx_control() 触发 CC

use crate::core::{Event, EventKind, Simulator};
use crate::error::SimResult;
use crate::monitor::{FlowFct, SimSummary};
use crate::network::Packet;
use crate::nic::Protocol;
use crate::topology::Topology;
use crate::traffic::FlowDesc;
use crate::viz::TimeSeriesSampler;
use crate::EntityId;

mod host;
mod switch;

/// 轻量 Slab allocator：用 Vec 做密集存储，O(1) insert/remove，缓存友好。
struct PacketSlab {
    slots: Vec<Option<Packet>>,
    free: Vec<u64>,
}

impl PacketSlab {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    /// 插入包，返回分配的索引（同时覆盖 pkt.id 为 slab 索引）
    fn insert(&mut self, pkt: Packet) -> u64 {
        let id = if let Some(id) = self.free.pop() {
            self.slots[id as usize] = Some(pkt);
            id
        } else {
            let id = self.slots.len() as u64;
            self.slots.push(Some(pkt));
            id
        };
        if let Some(Some(ref mut p)) = self.slots.get_mut(id as usize) {
            p.id = id;
        }
        id
    }

    fn remove(&mut self, id: u64) -> Option<Packet> {
        let idx = id as usize;
        if idx < self.slots.len() {
            let val = self.slots[idx].take();
            if val.is_some() {
                self.free.push(id);
            }
            val
        } else {
            None
        }
    }
}

/// 整个仿真实例
pub struct SimRunner {
    pub sim: Simulator,
    pub topo: Topology,
    /// 每个 host 对应的可插拔协议栈（索引 = host_id）
    pub protocols: Vec<Box<dyn Protocol>>,
    /// 链路下一个空闲时刻（避免发包重叠 → 实现链路 serialization）
    pub link_busy_until: Vec<u64>,
    /// 包暂存：Event 只携带 packet_id 时用。Slab allocator 替代 HashMap，O(1) 且缓存友好。
    packet_buf: PacketSlab,
    /// 每条流的 FCT 记录（索引 = flow_id）
    pub fcts: Vec<FlowFct>,
    /// 协议名称（用于摘要输出）
    pub protocol_name: String,
    /// 每条链路的累计传输字节数（索引 = link_id）
    pub link_bytes_sent: Vec<u64>,
    /// 仿真起始 wall clock 时刻（用于估算链路利用率分母）
    pub sim_start_ns: u64,
    /// TxTick 周期（ns）
    pub tx_tick_ns: u64,
    /// 时间序列采样器（默认 disabled）
    pub sampler: TimeSeriesSampler,
    /// switch EntityId → switches 数组索引（索引 = switch_id）
    switch_index: Vec<usize>,
    /// 全局包追踪 ID 计数器：单调递增，与 slab 索引解耦（P1 可观测性）
    next_trace_id: u64,
    /// 事件类型直方图（P1 可观测性）
    event_histogram: crate::monitor::EventHistogram,
    /// 最大 pending queue 长度（P1 可观测性）
    max_pending: usize,
    /// 当前连续同时间戳事件计数（P1 可观测性）
    same_time_count: u64,
    /// 上一个事件的时间戳（P1 可观测性）
    last_event_time: u64,
    /// 同时间戳连续处理的最大事件数（P1 可观测性）
    max_same_time_burst: u64,
    /// 每个 host 已调度但尚未处理的 Timeout 截止时间。
    /// u64::MAX 表示没有待处理的 Timeout。
    /// 用于在 ACK 提前到达时逻辑取消冗余 Timeout（P1 修复）。
    pending_timeout_deadline: Vec<u64>,
    /// P2：TrainingJob 产生的 flow 边界 [start_fid, end_fid)，每个元素对应一个 CollectiveOp
    training_boundaries: Vec<(u32, u32)>,
    /// P2：每个 iteration 包含多少个 collective
    collectives_per_iteration: Vec<usize>,
    /// P2：Training 级别指标（仅在注入 TrainingJob 时填充）
    pub training_metrics: crate::training::TrainingMetrics,
}

impl SimRunner {
    pub fn new(
        topo: Topology,
        protocol_name: String,
        mut make_proto: impl FnMut(EntityId, &Topology) -> Box<dyn Protocol>,
    ) -> SimResult<Self> {
        let mut protocols = Vec::with_capacity(topo.hosts.len());
        for &h in &topo.hosts {
            protocols.push(make_proto(h, &topo));
        }
        let n_hosts = topo.hosts.len();
        let n_links = topo.links.len();
        let max_sw_id = topo.switches.iter().map(|sw| sw.id).max().unwrap_or(0);
        let mut switch_index = vec![usize::MAX; (max_sw_id + 1) as usize];
        for (i, sw) in topo.switches.iter().enumerate() {
            switch_index[sw.id as usize] = i;
        }
        Ok(Self {
            sim: Simulator::new(),
            topo,
            protocols,
            link_busy_until: vec![0; n_links],
            packet_buf: PacketSlab::new(),
            fcts: Vec::new(),
            protocol_name,
            link_bytes_sent: vec![0; n_links],
            sim_start_ns: 0,
            tx_tick_ns: 200,
            sampler: TimeSeriesSampler::disabled(),
            switch_index,
            next_trace_id: 1,
            event_histogram: crate::monitor::EventHistogram::default(),
            max_pending: 0,
            same_time_count: 0,
            last_event_time: u64::MAX,
            max_same_time_burst: 0,
            pending_timeout_deadline: vec![u64::MAX; n_hosts],
            training_boundaries: Vec::new(),
            collectives_per_iteration: Vec::new(),
            training_metrics: crate::training::TrainingMetrics::default(),
        })
    }

    /// 启用时间序列采样（用于3D可视化），每隔 interval_ns 采集一次链路快照
    pub fn with_sampling(mut self, interval_ns: u64) -> Self {
        let n_links = self.topo.links.len();
        self.sampler = TimeSeriesSampler::new(interval_ns, n_links);
        self
    }

    /// 注入流量
    pub fn inject_flows(&mut self, flows: Vec<FlowDesc>) {
        for f in flows {
            self.sim.schedule(Event::new(
                f.start_time_ns,
                EventKind::FlowStart {
                    flow_id: f.flow_id,
                    src: f.src,
                    dst: f.dst,
                    bytes: f.bytes,
                },
                f.src,
            ));
            let idx = f.flow_id as usize;
            if idx >= self.fcts.len() {
                self.fcts.resize(idx + 1, FlowFct::default());
            }
            self.fcts[idx] = FlowFct {
                flow_id: f.flow_id,
                start_ns: f.start_time_ns,
                finish_ns: 0,
                bytes: f.bytes,
            };
        }
    }

    /// P2：注入 TrainingJob，同时记录 collective 边界用于后续指标计算
    pub fn inject_training_job(
        &mut self,
        job: &crate::training::TrainingJob,
    ) {
        let (flows, boundaries, collectives_per_iteration) = job.generate();
        self.training_boundaries = boundaries;
        self.collectives_per_iteration = collectives_per_iteration;
        self.training_metrics.job_name.clone_from(&job.name);
        self.training_metrics.total_iterations = job.iterations.len() as u32;
        self.training_metrics.collective_labels = job
            .iterations
            .iter()
            .flat_map(|iter| {
                iter.collectives.iter().map(|c| {
                    format!(
                        "iter={} {}-{}",
                        iter.iter_id,
                        format!("{:?}", c.kind).to_lowercase(),
                        format!("{:?}", c.algorithm).to_lowercase()
                    )
                })
            })
            .collect();
        self.inject_flows(flows);
    }

    /// 跑到所有事件处理完，或达到 max_time
    pub fn run(&mut self, max_time_ns: u64) {
        while let Some(ev) = self.sim_pop_until(max_time_ns) {
            self.dispatch(ev);
            self.sampler.maybe_sample(self.sim.now(), &self.link_bytes_sent, &self.topo);
        }
        self.sim_start_ns = 0;
    }

    /// 带进度报告的仿真运行（P1 可观测性）
    ///
    /// 每隔 `progress_interval_ns` 仿真时间打印一次进度，包含：
    /// - 当前仿真时间
    /// - 已处理事件数
    /// - pending queue 长度
    /// - 当前最大同时间戳 burst
    pub fn run_with_progress(&mut self, max_time_ns: u64, progress_interval_ns: u64) {
        let mut next_report_ns = progress_interval_ns;
        while let Some(ev) = self.sim_pop_until(max_time_ns) {
            self.dispatch(ev);
            self.sampler.maybe_sample(self.sim.now(), &self.link_bytes_sent, &self.topo);
            let now = self.sim.now();
            if now >= next_report_ns {
                let pending = self.sim.pending();
                println!(
                    "[progress] sim_time={:.3}ms  processed={}  pending={}  max_burst={}",
                    now as f64 / 1e6,
                    self.sim.processed(),
                    pending,
                    self.max_same_time_burst,
                );
                next_report_ns = now + progress_interval_ns;
            }
        }
        self.sim_start_ns = 0;
    }

    fn sim_pop_until(&mut self, max_time_ns: u64) -> Option<Event> {
        if let Some(peek) = self.sim.peek_time() {
            if peek > max_time_ns {
                return None;
            }
        }
        self.sim.pop_event()
    }

    fn dispatch(&mut self, ev: Event) {
        // P1 可观测性：更新事件直方图
        self.event_histogram.record(&ev.kind);

        // P1 可观测性：更新 pending queue 最大长度
        let pending = self.sim.pending();
        if pending > self.max_pending {
            self.max_pending = pending;
        }

        // P1 可观测性：检测同时间戳事件 burst
        if ev.time == self.last_event_time {
            self.same_time_count += 1;
        } else {
            self.same_time_count = 1;
            self.last_event_time = ev.time;
        }
        if self.same_time_count > self.max_same_time_burst {
            self.max_same_time_burst = self.same_time_count;
        }

        match ev.kind {
            EventKind::FlowStart {
                flow_id,
                src,
                dst,
                bytes,
            } => {
                if let Some(proto) = self.protocols.get_mut(src as usize) {
                    proto.start_flow(flow_id, dst, bytes, ev.time);
                }
                self.sim.schedule(Event::new(
                    ev.time,
                    EventKind::TxTick { host: src },
                    src,
                ));
            }
            EventKind::TxTick { host } => {
                self.handle_tx_tick(host, ev.time);
            }
            EventKind::PacketDepart {
                packet_id,
                dst: _,
                port,
            } => {
                self.handle_packet_depart(packet_id, ev.target, port, ev.time);
            }
            EventKind::PacketArrive { packet_id, src: _ } => {
                self.handle_packet_arrive(packet_id, ev.target, ev.time);
            }
            EventKind::Timeout { .. } => {
                // P1 修复：跳过已被逻辑取消的过时 Timeout
                let host_idx = ev.target as usize;
                if host_idx < self.pending_timeout_deadline.len()
                    && ev.time != self.pending_timeout_deadline[host_idx]
                {
                    // 该 Timeout 已被 ACK 触发的 TxTick 覆盖，忽略
                    return;
                }
                self.pending_timeout_deadline[host_idx] = u64::MAX;
                // RTO 检查：触发一次 TxTick 让协议栈处理超时重传
                self.sim.schedule(Event::new(
                    ev.time,
                    EventKind::TxTick { host: ev.target },
                    ev.target,
                ));
            }
            EventKind::Stop => self.sim.stop(),
            _ => {}
        }
    }

    /// 向 packet_buf 插入包，返回分配的 slab 索引（已写入 pkt.id）。
    /// 同时分配全局唯一的 `trace_id`（P1 可观测性）。
    #[inline]
    fn packet_buf_insert(&mut self, mut pkt: Packet) -> u64 {
        let trace_id = self.next_trace_id;
        self.next_trace_id += 1;
        pkt.trace_id = trace_id;
        self.packet_buf.insert(pkt)
    }

    /// 从 packet_buf 取出包
    #[inline]
    fn packet_buf_remove(&mut self, pid: u64) -> Option<Packet> {
        self.packet_buf.remove(pid)
    }

    /// 输出仿真摘要
    pub fn summarize(&mut self) -> SimSummary {
        let mut fct_list: Vec<FlowFct> = self
            .fcts
            .iter()
            .copied()
            .filter(|f| f.bytes > 0)
            .collect();
        let total_flows = fct_list.len() as u64;
        let mut summary = SimSummary::from_fcts(&self.protocol_name, &mut fct_list);
        summary.total_flows = total_flows;
        summary.total_time_ns = self.sim.now();
        for proto in self.protocols.iter() {
            let s = proto.stats();
            summary.total_packets_sent += s.packets_sent;
            summary.total_packets_retransmitted += s.packets_retransmitted;
        }
        for sw in &self.topo.switches {
            summary.total_ecn_marks += sw.ecn_marks;
            summary.total_drops += sw.drops;
            for p in &sw.ports {
                if p.max_queue_depth_seen > summary.max_queue_depth_bytes {
                    summary.max_queue_depth_bytes = p.max_queue_depth_seen;
                }
            }
        }
        if summary.total_time_ns > 0 && !self.topo.links.is_empty() {
            let total_bw_capacity_per_ns: f64 = self
                .topo
                .links
                .iter()
                .map(|l| l.bandwidth_bps as f64 / 1e9 / 8.0)
                .sum();
            let total_capacity_bytes = total_bw_capacity_per_ns * summary.total_time_ns as f64;
            if total_capacity_bytes > 0.0 {
                summary.avg_link_util = (self.link_bytes_sent.iter().sum::<u64>() as f64) / total_capacity_bytes;
            }
        }
        // P1 可观测性：填充运行剖面
        summary.profile.event_histogram = self.event_histogram.clone();
        summary.profile.max_pending_events = self.max_pending;
        summary.profile.max_same_time_burst = self.max_same_time_burst;

        // P2：计算 Training 指标（如果有 training_boundaries）
        self.compute_training_metrics();
        summary
    }

    /// P2：根据 training_boundaries 和 fcts 计算 collective/iteration 完成时间
    fn compute_training_metrics(&mut self) {
        if self.training_boundaries.is_empty() {
            return;
        }

        let mut collective_times = Vec::with_capacity(self.training_boundaries.len());
        let mut iteration_times = Vec::new();

        let mut boundary_idx = 0usize;
        for &num_collectives in &self.collectives_per_iteration {
            let mut iter_start: Option<u64> = None;
            let mut iter_finish: u64 = 0;

            for _ in 0..num_collectives {
                if boundary_idx >= self.training_boundaries.len() {
                    break;
                }
                let (start_fid, end_fid) = self.training_boundaries[boundary_idx];
                boundary_idx += 1;

                let mut collective_start = u64::MAX;
                let mut collective_finish = 0u64;
                let mut has_flow = false;

                for fid in start_fid..end_fid {
                    let idx = fid as usize;
                    if idx < self.fcts.len() && self.fcts[idx].bytes > 0 {
                        has_flow = true;
                        collective_start = collective_start.min(self.fcts[idx].start_ns);
                        collective_finish = collective_finish.max(self.fcts[idx].finish_ns);
                    }
                }

                if has_flow {
                    let completion = collective_finish.saturating_sub(collective_start);
                    collective_times.push(completion);
                    iter_start = Some(iter_start.unwrap_or(collective_start).min(collective_start));
                    iter_finish = iter_finish.max(collective_finish);
                } else {
                    collective_times.push(0);
                }
            }

            if let Some(start) = iter_start {
                iteration_times.push(iter_finish.saturating_sub(start));
            }
        }

        self.training_metrics.completed_iterations = iteration_times.len() as u32;
        self.training_metrics.iteration_times_ns = iteration_times;
        self.training_metrics.collective_completion_ns = collective_times;
    }
}
