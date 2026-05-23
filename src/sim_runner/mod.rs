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

    /// 跑到所有事件处理完，或达到 max_time
    pub fn run(&mut self, max_time_ns: u64) {
        while let Some(ev) = self.sim_pop_until(max_time_ns) {
            self.dispatch(ev);
            self.sampler.maybe_sample(self.sim.now(), &self.link_bytes_sent, &self.topo);
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

    /// 向 packet_buf 插入包，返回分配的 slab 索引（已写入 pkt.id）
    #[inline]
    fn packet_buf_insert(&mut self, pkt: Packet) -> u64 {
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
            .filter(|f| f.start_ns > 0)
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
        summary
    }
}
