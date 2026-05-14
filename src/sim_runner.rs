//! 端到端仿真主循环
//!
//! 我们没有使用 `Simulator` 中的 handler 机制（那种风格在每个 handler 需要访问
//! 多个全局状态时会受 borrow checker 限制）。这里采用集中式：所有事件先由
//! Simulator 排序，主循环根据事件类型查实体表，直接修改状态。
//!
//! 事件流：
//!   FlowStart    → 向 TxNic 注册流
//!   TxTick       → TxNic.try_send()，把生成的 packet 转为 PacketDepart
//!   PacketDepart → 链路 serialization+prop_delay 后 → PacketArrive @ switch
//!   PacketArrive @ switch → Switch.ingress；若端口空闲再 PacketDepart 到下一跳
//!   PacketArrive @ host   → 如果是 Data，喂 RxNic 生成 ACK/NACK；
//!                          → 如果是 Ack/Nack，喂 TxNic 触发 CC

use crate::core::{Event, EventKind, Simulator};
use crate::network::{Packet, PacketKind};
use crate::nic::{TxNic, RxNic, CongestionMode};
use crate::traffic::FlowDesc;
use crate::topology::Topology;
use crate::monitor::{FlowFct, SimSummary};
use crate::EntityId;
use std::collections::HashMap;

/// 整个仿真实例
pub struct SimRunner {
    pub sim: Simulator,
    pub topo: Topology,
    pub tx_nics: HashMap<EntityId, TxNic>,
    pub rx_nics: HashMap<EntityId, RxNic>,
    /// 链路下一个空闲时刻（避免发包重叠 → 实现链路 serialization）
    pub link_busy_until: Vec<u64>,
    /// 包暂存：Event 只携带 packet_id 时用
    pub packet_buf: HashMap<u64, Packet>,
    /// 每条流的 FCT 记录
    pub fcts: HashMap<u32, FlowFct>,
    pub mode: CongestionMode,
    /// 全局 packet id 生成器（避免不同 TxNic 之间冲突）
    pub global_pid: u64,
    /// 链路总传输字节数（用于平均利用率计算）
    pub link_bytes_sent: u64,
    /// 仿真起始 wall clock 时刻（用于估算链路利用率分母）
    pub sim_start_ns: u64,
    /// TxTick 周期（ns）
    pub tx_tick_ns: u64,
}

impl SimRunner {
    pub fn new(topo: Topology, mode: CongestionMode) -> Self {
        let mut tx_nics = HashMap::new();
        let mut rx_nics = HashMap::new();
        // 计算每个 host 有几条 spine 上行路径（用于 STrack）
        // 简化：取 host_uplink 的 edge_switch 的"上行端口数"
        for &h in &topo.hosts {
            // 找到这个 host 所属 edge switch
            let edge_id = topo.host_uplink.iter().find(|u| u.host == h).map(|u| u.edge_switch).unwrap();
            let edge_sw = topo.switches.iter().find(|s| s.id == edge_id).unwrap();
            // 端口数 = 直连主机端口数(=hosts_per_leaf) + 上行端口数 → 上行 = total - hosts_per_leaf
            // 但我们不知道 hosts_per_leaf，简单做法：所有非该 host 出端口都算 "上行候选" 的数量
            // 这里用一个保底：路径数 = 上行端口数 = 该 edge 在路由表里到任意远程 host 的等价路径数
            let mut remote_paths = 0u8;
            for other in &topo.hosts {
                if *other == h { continue; }
                if let Some(p) = edge_sw.routing.ports_for(*other) {
                    remote_paths = remote_paths.max(p.len() as u8);
                }
            }
            let n_paths = remote_paths.max(1);
            tx_nics.insert(h, TxNic::new(h, mode, n_paths));
            rx_nics.insert(h, RxNic::new(h));
        }
        let n_links = topo.links.len();
        Self {
            sim: Simulator::new(),
            topo,
            tx_nics,
            rx_nics,
            link_busy_until: vec![0; n_links],
            packet_buf: HashMap::new(),
            fcts: HashMap::new(),
            mode,
            global_pid: 1,
            link_bytes_sent: 0,
            sim_start_ns: 0,
            tx_tick_ns: 200,
        }
    }

    /// 注入流量
    pub fn inject_flows(&mut self, flows: Vec<FlowDesc>) {
        for f in flows {
            // 启动事件：在 f.start_time_ns 调用 tx_nic.start_flow + 第一次 TxTick
            self.sim.schedule(Event::new(
                f.start_time_ns,
                EventKind::Custom(format!("FlowStart:{}:{}:{}:{}", f.flow_id, f.src, f.dst, f.bytes)),
                f.src,
            ));
            self.fcts.insert(f.flow_id, FlowFct { flow_id: f.flow_id, start_ns: f.start_time_ns, finish_ns: 0, bytes: f.bytes });
        }
    }

    /// 跑到所有事件处理完，或达到 max_time
    pub fn run(&mut self, max_time_ns: u64) {
        while let Some(ev) = self.sim_pop_until(max_time_ns) {
            self.dispatch(ev);
        }
        self.sim_start_ns = 0;
    }

    fn sim_pop_until(&mut self, max_time_ns: u64) -> Option<Event> {
        if let Some(peek) = self.sim.peek_time() {
            if peek > max_time_ns { return None; }
        }
        self.sim.pop_event()
    }

    fn dispatch(&mut self, ev: Event) {
        // 主分发器：根据 kind 调用不同处理逻辑
        match &ev.kind {
            EventKind::Custom(s) if s.starts_with("FlowStart:") => {
                let parts: Vec<&str> = s.split(':').collect();
                let flow_id: u32 = parts[1].parse().unwrap();
                let src: EntityId = parts[2].parse().unwrap();
                let dst: EntityId = parts[3].parse().unwrap();
                let bytes: u64 = parts[4].parse().unwrap();
                if let Some(tx) = self.tx_nics.get_mut(&src) {
                    tx.start_flow(flow_id, dst, bytes, ev.time);
                }
                // 立刻调度一次 TxTick
                self.sim.schedule(Event::new(ev.time, EventKind::Custom(format!("TxTick:{}", src)), src));
            }
            EventKind::Custom(s) if s.starts_with("TxTick:") => {
                let host: EntityId = s.trim_start_matches("TxTick:").parse().unwrap();
                self.handle_tx_tick(host, ev.time);
            }
            EventKind::PacketDepart { packet_id, dst: _ } => {
                self.handle_packet_depart(*packet_id, ev.target, ev.time);
            }
            EventKind::PacketArrive { packet_id, src: _ } => {
                self.handle_packet_arrive(*packet_id, ev.target, ev.time);
            }
            EventKind::Stop => self.sim.stop(),
            _ => {}
        }
    }

    fn handle_tx_tick(&mut self, host: EntityId, now: u64) {
        let mut pkts = if let Some(tx) = self.tx_nics.get_mut(&host) { tx.try_send(now) } else { return; };
        // 重写全局唯一的 packet id，避免不同 TxNic 之间冲突
        for p in pkts.iter_mut() {
            p.id = self.global_pid;
            self.global_pid += 1;
        }
        // 检查是否还有未完成的流——有的话仍然调度下一次 TxTick，以便检查超时重传
        let has_active = self.tx_nics.get(&host).map(|tx| tx.flows.values().any(|f| !f.done)).unwrap_or(false);
        if pkts.is_empty() {
            if has_active {
                // 超时重传检测需要持续 tick；间隔设为 RTO/4
                self.sim.schedule(Event::new(now + 25_000, EventKind::Custom(format!("TxTick:{}", host)), host));
            }
            return;
        }
        // 找到 host 上的上行链路（host → edge_switch）
        let uplink = self.topo.host_uplink.iter().find(|u| u.host == host).unwrap();
        let link_id = uplink.link_to_switch;
        for pkt in pkts {
            let pid = pkt.id;
            let size = pkt.size;
            self.packet_buf.insert(pid, pkt);
            // 链路 serialization：busy_until
            let start = self.link_busy_until[link_id as usize].max(now);
            let link = self.topo.links.get(link_id);
            let depart_done = start + link.serialization_ns(size);
            let arrive = depart_done + link.prop_delay_ns;
            self.link_busy_until[link_id as usize] = depart_done;
            self.link_bytes_sent += size as u64;
            self.sim.schedule(Event::new(arrive, EventKind::PacketArrive { packet_id: pid, src: host }, uplink.edge_switch));
        }
        // 下一次 TxTick
        self.sim.schedule(Event::new(now + self.tx_tick_ns, EventKind::Custom(format!("TxTick:{}", host)), host));
    }

    fn handle_packet_arrive(&mut self, pid: u64, target: EntityId, now: u64) {
        // 目标是 host 还是 switch？
        let pkt = match self.packet_buf.remove(&pid) {
            Some(p) => p,
            None => return,
        };

        // 是 host 吗？
        if (target as usize) < self.topo.hosts.len() {
            self.handle_arrive_at_host(pkt, target, now);
        } else {
            self.handle_arrive_at_switch(pkt, target, now);
        }
    }

    fn handle_arrive_at_host(&mut self, pkt: Packet, host: EntityId, now: u64) {
        match pkt.kind {
            PacketKind::Data => {
                let returns = if let Some(rx) = self.rx_nics.get_mut(&host) { rx.on_data(&pkt, now) } else { return; };
                // 把 ACK/NACK 发回 src
                let uplink = self.topo.host_uplink.iter().find(|u| u.host == host).unwrap();
                let link_id = uplink.link_to_switch;
                for mut ret in returns {
                    ret.id = self.global_pid;
                    self.global_pid += 1;
                    let pid = ret.id;
                    let size = ret.size;
                    let dst_back = ret.dst;
                    self.packet_buf.insert(pid, ret);
                    let start = self.link_busy_until[link_id as usize].max(now);
                    let link = self.topo.links.get(link_id);
                    let depart_done = start + link.serialization_ns(size);
                    let arrive = depart_done + link.prop_delay_ns;
                    self.link_busy_until[link_id as usize] = depart_done;
                    self.link_bytes_sent += size as u64;
                    let _ = dst_back;
                    self.sim.schedule(Event::new(arrive, EventKind::PacketArrive { packet_id: pid, src: host }, uplink.edge_switch));
                }
            }
            PacketKind::Ack => {
                if let Some(tx) = self.tx_nics.get_mut(&host) {
                    tx.on_ack(&pkt, now);
                    // 检查是否有流完成 → 记录 FCT
                    for f in tx.flows.values() {
                        if f.done {
                            if let Some(rec) = self.fcts.get_mut(&f.flow_id) {
                                if rec.finish_ns == 0 { rec.finish_ns = f.finish_time; }
                            }
                        }
                    }
                }
                // 触发一次 TxTick
                self.sim.schedule(Event::new(now, EventKind::Custom(format!("TxTick:{}", host)), host));
            }
            PacketKind::Nack => {
                if let Some(tx) = self.tx_nics.get_mut(&host) { tx.on_nack(&pkt, now); }
                self.sim.schedule(Event::new(now, EventKind::Custom(format!("TxTick:{}", host)), host));
            }
        }
    }

    fn handle_arrive_at_switch(&mut self, pkt: Packet, switch_id: EntityId, now: u64) {
        // 找 switch 索引
        let sw_idx = self.topo.switches.iter().position(|s| s.id == switch_id);
        if sw_idx.is_none() { return; }
        let sw_idx = sw_idx.unwrap();
        let pkt_id = pkt.id;
        let _trace_src = pkt.src; let _trace_dst = pkt.dst; let _trace_seq = pkt.seq;
        let hash_key = pkt.src ^ pkt.dst ^ pkt.flow_id;
        let pkt_size = pkt.size;
        let (port_opt, dropped) = self.topo.switches[sw_idx].ingress(pkt, hash_key);
        if dropped {
            self.packet_buf.remove(&pkt_id);
            return;
        }
        let port = port_opt.unwrap();
        // 出端口空闲时调度出包；这里实现为：每次 ingress 后尝试 dequeue
        self.try_egress(sw_idx, port, now, pkt_id, pkt_size);
    }

    fn try_egress(&mut self, sw_idx: usize, port: u8, now: u64, _just_in_pid: u64, _pkt_size: u32) {
        let _trace_sw = self.topo.switches[sw_idx].id;
        let _trace_q = self.topo.switches[sw_idx].port(port).queue.len();
        let _trace_busy = self.topo.switches[sw_idx].port(port).busy_until;
        // 拿到端口的 link_id 和 busy_until
        let (link_id, busy_until) = {
            let sw = &self.topo.switches[sw_idx];
            let p = sw.port(port);
            (p.link_id, p.busy_until)
        };
        // 如果端口空闲，立即取队首发出
        if busy_until <= now {
            let next_pkt = self.topo.switches[sw_idx].dequeue(port);
            if let Some(pkt) = next_pkt {
                let pid = pkt.id;
                let size = pkt.size;
                let dst = pkt.dst;
                self.packet_buf.insert(pid, pkt);
                let link = self.topo.links.get(link_id);
                let depart_done = now + link.serialization_ns(size);
                let arrive = depart_done + link.prop_delay_ns;
                self.topo.switches[sw_idx].port_mut(port).busy_until = depart_done;
                self.link_busy_until[link_id as usize] = depart_done;
                self.link_bytes_sent += size as u64;
                // 链路的接收端：可能是另一个 switch 或 host
                let next_target = link.to;
                self.sim.schedule(Event::new(arrive, EventKind::PacketArrive { packet_id: pid, src: link.from }, next_target));
                // 如果队列还有，再调度一个 "EgressReady" 事件 → 用 PacketDepart 占位
                let still_queued = self.topo.switches[sw_idx].port(port).queue_bytes > 0;
                if still_queued {
                    self.sim.schedule(Event::new(depart_done, EventKind::PacketDepart { packet_id: 0, dst }, self.topo.switches[sw_idx].id));
                }
                let _ = pid;
            }
        } else {
            // 端口忙：调度一个 EgressReady 事件
            self.sim.schedule(Event::new(busy_until, EventKind::PacketDepart { packet_id: 0, dst: 0 }, self.topo.switches[sw_idx].id));
        }
    }

    fn handle_packet_depart(&mut self, _pid: u64, target: EntityId, now: u64) {
        // packet_id=0 表示"端口空闲尝试发下一个"
        if let Some(sw_idx) = self.topo.switches.iter().position(|s| s.id == target) {
            // 对所有端口尝试一次（简化）
            let ports: Vec<u8> = (0..self.topo.switches[sw_idx].ports.len() as u8).collect();
            for p in ports {
                let has_queue = self.topo.switches[sw_idx].port(p).queue_bytes > 0;
                if has_queue {
                    self.try_egress(sw_idx, p, now, 0, 0);
                }
            }
        }
    }

    /// 输出仿真摘要
    pub fn summarize(&mut self) -> SimSummary {
        let total_flows = self.fcts.len() as u64;
        let mut fct_list: Vec<FlowFct> = self.fcts.values().copied().filter(|f| f.finish_ns > 0).collect();
        let mut summary = SimSummary::from_fcts(
            match self.mode { CongestionMode::Ecmp => "ecmp", CongestionMode::Strack => "strack" },
            &mut fct_list,
        );
        summary.total_flows = total_flows;
        summary.total_time_ns = self.sim.now();
        for tx in self.tx_nics.values() {
            summary.total_packets_sent += tx.stats.packets_sent;
            summary.total_packets_retransmitted += tx.stats.packets_retransmitted;
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
        // 平均链路利用率 ≈ 总传输字节数 * 8 / (sim_time_ns * 总链路带宽和)
        if summary.total_time_ns > 0 && self.topo.links.len() > 0 {
            let total_bw_capacity_per_ns: f64 = self.topo.links.iter()
                .map(|l| l.bandwidth_bps as f64 / 1e9 / 8.0)
                .sum();
            let total_capacity_bytes = total_bw_capacity_per_ns * summary.total_time_ns as f64;
            if total_capacity_bytes > 0.0 {
                summary.avg_link_util = (self.link_bytes_sent as f64) / total_capacity_bytes;
            }
        }
        summary
    }
}
