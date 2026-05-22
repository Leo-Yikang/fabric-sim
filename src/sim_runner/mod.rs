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
use crate::EntityId;
use std::collections::HashMap;

mod host;
mod switch;

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
    /// switch EntityId → switches 数组索引，O(1) 查找
    switch_index: HashMap<EntityId, usize>,
}

impl SimRunner {
    pub fn new(
        topo: Topology,
        protocol_name: String,
        mut make_proto: impl FnMut(EntityId, &Topology) -> Box<dyn Protocol>,
    ) -> SimResult<Self> {
        let mut protocols = HashMap::new();
        for &h in &topo.hosts {
            protocols.insert(h, make_proto(h, &topo));
        }
        let n_links = topo.links.len();
        let switch_index: HashMap<EntityId, usize> = topo
            .switches
            .iter()
            .enumerate()
            .map(|(i, sw)| (sw.id, i))
            .collect();
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
            switch_index,
        })
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
        match ev.kind {
            EventKind::FlowStart {
                flow_id,
                src,
                dst,
                bytes,
            } => {
                if let Some(proto) = self.protocols.get_mut(&src) {
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
            EventKind::Stop => self.sim.stop(),
            _ => {}
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