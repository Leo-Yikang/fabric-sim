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
use crate::error::{SimError, SimResult};
use crate::monitor::{FlowFct, SimSummary};
use crate::network::{Packet, PacketKind};
use crate::nic::Protocol;
use crate::topology::Topology;
use crate::traffic::FlowDesc;
use crate::EntityId;
use std::collections::HashMap;

/// 整个仿真实例
pub struct SimRunner {
    pub sim: Simulator,
    pub topo: Topology,
    /// 每个 host 对应的可插拔协议栈
    pub protocols: HashMap<EntityId, Box<dyn Protocol>>,
    /// 链路下一个空闲时刻（避免发包重叠 → 实现链路 serialization）
    pub link_busy_until: Vec<u64>,
    /// 包暂存：Event 只携带 packet_id 时用
    pub packet_buf: HashMap<u64, Packet>,
    /// 每条流的 FCT 记录
    pub fcts: HashMap<u32, FlowFct>,
    /// 协议名称（用于摘要输出）
    pub protocol_name: String,
    /// 全局 packet id 生成器（避免不同协议实例之间冲突）
    pub global_pid: u64,
    /// 链路总传输字节数（用于平均利用率计算）
    pub link_bytes_sent: u64,
    /// 仿真起始 wall clock 时刻（用于估算链路利用率分母）
    pub sim_start_ns: u64,
    /// TxTick 周期（ns）
    pub tx_tick_ns: u64,
}

impl SimRunner {
    pub fn new(
        topo: Topology,
        protocol_name: String,
        mut make_proto: impl FnMut(EntityId, u8) -> Box<dyn Protocol>,
    ) -> SimResult<Self> {
        let mut protocols = HashMap::new();
        for &h in &topo.hosts {
            let edge_id = topo
                .host_uplink
                .iter()
                .find(|u| u.host == h)
                .map(|u| u.edge_switch)
                .ok_or_else(|| SimError::Topology(format!("主机 {} 缺少上行链路", h)))?;
            let edge_sw = topo
                .switches
                .iter()
                .find(|s| s.id == edge_id)
                .ok_or_else(|| SimError::Topology(format!("交换机 {} 不存在于拓扑中", edge_id)))?;
            let mut remote_paths = 0u8;
            for other in &topo.hosts {
                if *other == h {
                    continue;
                }
                if let Some(p) = edge_sw.routing.ports_for(*other) {
                    remote_paths = remote_paths.max(p.len() as u8);
                }
            }
            let n_paths = remote_paths.max(1);
            protocols.insert(h, make_proto(h, n_paths));
        }
        let n_links = topo.links.len();
        Ok(Self {
            sim: Simulator::new(),
            topo,
            protocols,
            link_busy_until: vec![0; n_links],
            packet_buf: HashMap::new(),
            fcts: HashMap::new(),
            protocol_name,
            global_pid: 1,
            link_bytes_sent: 0,
            sim_start_ns: 0,
            tx_tick_ns: 200,
        })
    }

    /// 注入流量
    pub fn inject_flows(&mut self, flows: Vec<FlowDesc>) {
        for f in flows {
            self.sim.schedule(Event::new(
                f.start_time_ns,
                EventKind::Custom(format!(
                    "FlowStart:{}:{}:{}:{}",
                    f.flow_id, f.src, f.dst, f.bytes
                )),
                f.src,
            ));
            self.fcts.insert(
                f.flow_id,
                FlowFct {
                    flow_id: f.flow_id,
                    start_ns: f.start_time_ns,
                    finish_ns: 0,
                    bytes: f.bytes,
                },
            );
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
            if peek > max_time_ns {
                return None;
            }
        }
        self.sim.pop_event()
    }

    fn dispatch(&mut self, ev: Event) {
        match &ev.kind {
            EventKind::Custom(s) if s.starts_with("FlowStart:") => {
                let parts: Vec<&str> = s.split(':').collect();
                let flow_id: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                let src: EntityId = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(0);
                let dst: EntityId = parts.get(3).and_then(|p| p.parse().ok()).unwrap_or(0);
                let bytes: u64 = parts.get(4).and_then(|p| p.parse().ok()).unwrap_or(0);
                if let Some(proto) = self.protocols.get_mut(&src) {
                    proto.start_flow(flow_id, dst, bytes, ev.time);
                }
                self.sim.schedule(Event::new(
                    ev.time,
                    EventKind::Custom(format!("TxTick:{}", src)),
                    src,
                ));
            }
            EventKind::Custom(s) if s.starts_with("TxTick:") => {
                let host: EntityId = s.trim_start_matches("TxTick:").parse().unwrap_or(0);
                self.handle_tx_tick(host, ev.time);
            }
            EventKind::PacketDepart {
                packet_id,
                dst: _,
                port,
            } => {
                self.handle_packet_depart(*packet_id, ev.target, *port, ev.time);
            }
            EventKind::PacketArrive { packet_id, src: _ } => {
                self.handle_packet_arrive(*packet_id, ev.target, ev.time);
            }
            EventKind::Stop => self.sim.stop(),
            _ => {}
        }
    }

    fn handle_tx_tick(&mut self, host: EntityId, now: u64) {
        let mut pkts = if let Some(proto) = self.protocols.get_mut(&host) {
            proto.on_tx_tick(now)
        } else {
            return;
        };
        // 重写全局唯一的 packet id
        for p in pkts.iter_mut() {
            p.id = self.global_pid;
            self.global_pid += 1;
        }
        let has_active = self
            .protocols
            .get(&host)
            .map(|p| !p.all_flows_done())
            .unwrap_or(false);
        if pkts.is_empty() {
            if has_active {
                self.sim.schedule(Event::new(
                    now + 25_000,
                    EventKind::Custom(format!("TxTick:{}", host)),
                    host,
                ));
            }
            return;
        }
        let Some(uplink) = self.topo.host_uplink.iter().find(|u| u.host == host) else {
            return;
        };
        let link_id = uplink.link_to_switch;
        for pkt in pkts {
            let pid = pkt.id;
            let size = pkt.size;
            self.packet_buf.insert(pid, pkt);
            let start = self.link_busy_until[link_id as usize].max(now);
            let link = self.topo.links.get(link_id);
            let depart_done = start + link.serialization_ns(size);
            let arrive = depart_done + link.prop_delay_ns;
            self.link_busy_until[link_id as usize] = depart_done;
            self.link_bytes_sent += size as u64;
            self.sim.schedule(Event::new(
                arrive,
                EventKind::PacketArrive {
                    packet_id: pid,
                    src: host,
                },
                uplink.edge_switch,
            ));
        }
        self.sim.schedule(Event::new(
            now + self.tx_tick_ns,
            EventKind::Custom(format!("TxTick:{}", host)),
            host,
        ));
    }

    fn handle_packet_arrive(&mut self, pid: u64, target: EntityId, now: u64) {
        let pkt = match self.packet_buf.remove(&pid) {
            Some(p) => p,
            None => return,
        };
        if (target as usize) < self.topo.hosts.len() {
            self.handle_arrive_at_host(pkt, target, now);
        } else {
            self.handle_arrive_at_switch(pkt, target, now);
        }
    }

    fn handle_arrive_at_host(&mut self, pkt: Packet, host: EntityId, now: u64) {
        match pkt.kind {
            PacketKind::Data => {
                let returns = if let Some(proto) = self.protocols.get_mut(&host) {
                    proto.on_rx_data(&pkt, now)
                } else {
                    return;
                };
                let Some(uplink) = self.topo.host_uplink.iter().find(|u| u.host == host) else {
                    return;
                };
                let link_id = uplink.link_to_switch;
                for mut ret in returns {
                    ret.id = self.global_pid;
                    self.global_pid += 1;
                    let pid = ret.id;
                    let size = ret.size;
                    let _dst_back = ret.dst;
                    self.packet_buf.insert(pid, ret);
                    let start = self.link_busy_until[link_id as usize].max(now);
                    let link = self.topo.links.get(link_id);
                    let depart_done = start + link.serialization_ns(size);
                    let arrive = depart_done + link.prop_delay_ns;
                    self.link_busy_until[link_id as usize] = depart_done;
                    self.link_bytes_sent += size as u64;
                    self.sim.schedule(Event::new(
                        arrive,
                        EventKind::PacketArrive {
                            packet_id: pid,
                            src: host,
                        },
                        uplink.edge_switch,
                    ));
                }
                self.sim.schedule(Event::new(
                    now,
                    EventKind::Custom(format!("TxTick:{}", host)),
                    host,
                ));
            }
            PacketKind::Control(_) => {
                if let Some(proto) = self.protocols.get_mut(&host) {
                    proto.on_tx_control(&pkt, now);
                    for (fid, ft) in proto.take_finished_flows() {
                        if let Some(rec) = self.fcts.get_mut(&fid) {
                            if rec.finish_ns == 0 {
                                rec.finish_ns = ft;
                            }
                        }
                    }
                }
                self.sim.schedule(Event::new(
                    now,
                    EventKind::Custom(format!("TxTick:{}", host)),
                    host,
                ));
            }
        }
    }

    fn handle_arrive_at_switch(&mut self, pkt: Packet, switch_id: EntityId, now: u64) {
        let sw_idx = match self.topo.switches.iter().position(|s| s.id == switch_id) {
            Some(idx) => idx,
            None => return,
        };
        let pkt_id = pkt.id;
        let hash_key = pkt.src ^ pkt.dst ^ pkt.flow_id;
        let pkt_size = pkt.size;
        let (port_opt, dropped) = self.topo.switches[sw_idx].ingress(pkt, hash_key);
        if dropped {
            self.packet_buf.remove(&pkt_id);
            return;
        }
        let port = match port_opt {
            Some(p) => p,
            None => return,
        };
        self.try_egress(sw_idx, port, now, pkt_id, pkt_size);
    }

    fn try_egress(&mut self, sw_idx: usize, port: u8, now: u64, _just_in_pid: u64, _pkt_size: u32) {
        let (link_id, busy_until) = {
            let sw = &self.topo.switches[sw_idx];
            let p = sw.port(port);
            (p.link_id, p.busy_until)
        };
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
                {
                    let p = self.topo.switches[sw_idx].port_mut(port);
                    p.busy_until = depart_done;
                }
                self.link_busy_until[link_id as usize] = depart_done;
                self.link_bytes_sent += size as u64;
                let next_target = link.to;
                self.sim.schedule(Event::new(
                    arrive,
                    EventKind::PacketArrive {
                        packet_id: pid,
                        src: link.from,
                    },
                    next_target,
                ));
                let still_queued = self.topo.switches[sw_idx].port(port).queue_bytes > 0;
                if still_queued {
                    let p = self.topo.switches[sw_idx].port_mut(port);
                    if !p.egress_pending {
                        p.egress_pending = true;
                        self.sim.schedule(Event::new(
                            depart_done,
                            EventKind::PacketDepart {
                                packet_id: 0,
                                dst,
                                port,
                            },
                            self.topo.switches[sw_idx].id,
                        ));
                    }
                }
            }
        } else {
            let p = self.topo.switches[sw_idx].port_mut(port);
            if !p.egress_pending {
                p.egress_pending = true;
                self.sim.schedule(Event::new(
                    busy_until,
                    EventKind::PacketDepart {
                        packet_id: 0,
                        dst: 0,
                        port,
                    },
                    self.topo.switches[sw_idx].id,
                ));
            }
        }
    }

    fn handle_packet_depart(&mut self, _pid: u64, target: EntityId, port: u8, now: u64) {
        if let Some(sw_idx) = self.topo.switches.iter().position(|s| s.id == target) {
            self.topo.switches[sw_idx].port_mut(port).egress_pending = false;
            let has_queue = self.topo.switches[sw_idx].port(port).queue_bytes > 0;
            if has_queue {
                self.try_egress(sw_idx, port, now, 0, 0);
            }
        }
    }

    /// 输出仿真摘要
    pub fn summarize(&mut self) -> SimSummary {
        let total_flows = self.fcts.len() as u64;
        let mut fct_list: Vec<FlowFct> = self
            .fcts
            .values()
            .copied()
            .filter(|f| f.finish_ns > 0)
            .collect();
        let mut summary = SimSummary::from_fcts(&self.protocol_name, &mut fct_list);
        summary.total_flows = total_flows;
        summary.total_time_ns = self.sim.now();
        for proto in self.protocols.values() {
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
                summary.avg_link_util = (self.link_bytes_sent as f64) / total_capacity_bytes;
            }
        }
        summary
    }
}
