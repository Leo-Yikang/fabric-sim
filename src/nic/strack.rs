//! STrack 协议实现
//!
//! 将原有的 `TxNic` + `RxNic` + `CongestionMode` 合并为单一的 `StrackProtocol`，
//! 实现 `Protocol` trait。支持两种模式：
//! - `Ecmp`：传统单路径流哈希 + DCQCN 风格降窗
//! - `Strack`：Packet Spraying 多路径 + 先切路再降窗 + SACK Bitmap 选择性重传

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::topology::Topology;
use crate::EntityId;
use std::collections::HashMap;

/// STrack 内部模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum STrackMode {
    /// ECMP baseline：每条流走单一哈希路径，全 ECN 时直接降窗
    Ecmp,
    /// STrack：多路径喷洒，遇 ECN 先尝试黑名单当前路径
    Strack,
}

/// 每条路径（在多路径模式下=每个出端口）的状态
#[derive(Debug, Clone, Copy)]
pub struct PathState {
    pub path_id: u8,
    pub blacklisted_until: u64, // 仿真时间戳：< 此值则不选这条路
    pub ecn_recent: u32,        // 最近窗口内见到的 ECN 数
}

impl PathState {
    pub fn new(id: u8) -> Self {
        Self {
            path_id: id,
            blacklisted_until: 0,
            ecn_recent: 0,
        }
    }
    pub fn is_available(&self, now: u64) -> bool {
        now >= self.blacklisted_until
    }
}

// ------------------------------------------------------------------
// 发送端状态
// ------------------------------------------------------------------

#[derive(Default, Debug, Clone, Copy)]
pub struct TxStats {
    pub packets_sent: u64,
    pub packets_retransmitted: u64,
    pub ecn_ack_received: u64,
    pub nack_received: u64,
    pub flows_completed: u64,
}

pub struct FlowTxState {
    pub flow_id: FlowId,
    pub dst: EntityId,
    pub total_packets: u32,
    pub next_seq: SeqNum,
    pub un_acked_base: SeqNum,
    pub cwnd: u32,
    pub in_flight: u32,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
    pub retransmit_queue: Vec<SeqNum>,
    pub send_times: HashMap<SeqNum, u64>,
    pub last_ack_time: u64,
}

impl FlowTxState {
    pub fn new(
        flow_id: FlowId,
        dst: EntityId,
        total_packets: u32,
        init_cwnd: u32,
        start_time: u64,
    ) -> Self {
        Self {
            flow_id,
            dst,
            total_packets,
            next_seq: 0,
            un_acked_base: 0,
            cwnd: init_cwnd,
            in_flight: 0,
            done: false,
            start_time,
            finish_time: 0,
            retransmit_queue: Vec::new(),
            send_times: HashMap::new(),
            last_ack_time: 0,
        }
    }
}

// ------------------------------------------------------------------
// 接收端状态
// ------------------------------------------------------------------

#[derive(Default, Debug, Clone, Copy)]
pub struct RxStats {
    pub packets_received: u64,
    pub packets_delivered: u64,
    pub duplicates: u64,
    pub out_of_order: u64,
    pub nacks_sent: u64,
}

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: SeqNum,
    pub received_bits: u64,
}

impl FlowRxState {
    pub fn new(flow_id: FlowId) -> Self {
        Self {
            flow_id,
            next_expected: 0,
            received_bits: 0,
        }
    }
}

// ------------------------------------------------------------------
// STrackProtocol
// ------------------------------------------------------------------

pub struct STrackProtocol {
    pub host_id: EntityId,
    pub mode: STrackMode,
    pub paths: Vec<PathState>,
    pub paths_rr_cursor: u8,
    pub tx_flows: HashMap<FlowId, FlowTxState>,
    pub rx_flows: HashMap<FlowId, FlowRxState>,
    pub tx_stats: TxStats,
    pub rx_stats: RxStats,
    pub next_packet_id: u64,
    pub init_cwnd: u32,
    pub max_cwnd: u32,
    pub min_cwnd: u32,
    pub blacklist_duration_ns: u64,
    pub rto_ns: u64,
    finished: Vec<(FlowId, u64)>,
}

impl STrackProtocol {
    pub fn new(host_id: EntityId, mode: STrackMode, topo: &Topology) -> Self {
        let n_paths = topo
            .host_uplink
            .iter()
            .find(|u| u.host == host_id)
            .and_then(|u| topo.switches.iter().find(|s| s.id == u.edge_switch))
            .map(|edge_sw| {
                let mut max_paths = 1u8;
                for other in &topo.hosts {
                    if *other == host_id {
                        continue;
                    }
                    if let Some(p) = edge_sw.routing.ports_for(*other) {
                        max_paths = max_paths.max(p.len() as u8);
                    }
                }
                max_paths
            })
            .unwrap_or(1);
        let paths = (0..n_paths).map(PathState::new).collect();
        Self {
            host_id,
            mode,
            paths,
            paths_rr_cursor: 0,
            tx_flows: HashMap::new(),
            rx_flows: HashMap::new(),
            tx_stats: TxStats::default(),
            rx_stats: RxStats::default(),
            next_packet_id: 1,
            init_cwnd: 16,
            max_cwnd: 256,
            min_cwnd: 1,
            blacklist_duration_ns: 50_000,
            rto_ns: 100_000,
            finished: Vec::new(),
        }
    }

    /// 测试用：直接指定路径数，跳过拓扑解析
    #[cfg(test)]
    pub(crate) fn with_path_count(host_id: EntityId, mode: STrackMode, n_paths: u8) -> Self {
        let paths = (0..n_paths).map(PathState::new).collect();
        Self {
            host_id,
            mode,
            paths,
            paths_rr_cursor: 0,
            tx_flows: HashMap::new(),
            rx_flows: HashMap::new(),
            tx_stats: TxStats::default(),
            rx_stats: RxStats::default(),
            next_packet_id: 1,
            init_cwnd: 16,
            max_cwnd: 256,
            min_cwnd: 1,
            blacklist_duration_ns: 50_000,
            rto_ns: 100_000,
            finished: Vec::new(),
        }
    }

    // ---- 发送端内部方法 ----

    fn start_flow_tx(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64) {
        let total_packets = ((total_bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        self.tx_flows.insert(
            flow_id,
            FlowTxState::new(flow_id, dst, total_packets, self.init_cwnd, now),
        );
    }

    fn try_send(&mut self, now: u64) -> Vec<Packet> {
        let mut to_send = Vec::new();
        let flow_ids: Vec<FlowId> = self
            .tx_flows
            .iter()
            .filter(|(_, f)| !f.done)
            .map(|(k, _)| *k)
            .collect();

        for fid in flow_ids {
            let (cwnd, in_flight, mut next_seq, total, dst, mut retx) = {
                let f = self.tx_flows.get(&fid).expect("invariant: 刚迭代的活跃流必存在于 tx_flows");
                (
                    f.cwnd,
                    f.in_flight,
                    f.next_seq,
                    f.total_packets,
                    f.dst,
                    f.retransmit_queue.clone(),
                )
            };

            // 检查超时重传
            let mut timeout_seqs: Vec<SeqNum> = Vec::new();
            {
                let f = self.tx_flows.get(&fid).expect("invariant: 刚迭代的活跃流必存在于 tx_flows");
                for (seq, send_t) in &f.send_times {
                    if now.saturating_sub(*send_t) >= self.rto_ns && !retx.contains(seq) {
                        timeout_seqs.push(*seq);
                    }
                }
            }
            for s in timeout_seqs {
                retx.push(s);
            }

            let mut new_inflight = in_flight;
            let mut send_records: Vec<(SeqNum, u64)> = Vec::new();

            let mut retx_budget = cwnd as usize;
            while retx_budget > 0 && !retx.is_empty() {
                let seq = retx.remove(0);
                let Some(path) = self.pick_path(now) else {
                    break;
                };
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let mut pkt = Packet::data(pid, pid, fid, seq, self.host_id, dst, now);
                pkt.routing_tag = path + 1;
                to_send.push(pkt);
                self.tx_stats.packets_retransmitted += 1;
                self.tx_stats.packets_sent += 1;
                send_records.push((seq, now));
                retx_budget -= 1;
            }

            while new_inflight < cwnd && next_seq < total {
                let Some(path) = self.pick_path(now) else {
                    break;
                };
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let mut pkt = Packet::data(pid, pid, fid, next_seq, self.host_id, dst, now);
                pkt.routing_tag = path.saturating_add(1);
                to_send.push(pkt);
                self.tx_stats.packets_sent += 1;
                new_inflight += 1;
                send_records.push((next_seq, now));
                next_seq += 1;
            }

            let f = self.tx_flows.get_mut(&fid).expect("invariant: 刚迭代的活跃流必存在于 tx_flows");
            f.in_flight = new_inflight;
            f.next_seq = next_seq;
            f.retransmit_queue = retx;
            for (seq, t) in send_records {
                f.send_times.insert(seq, t);
            }
        }
        to_send
    }

    fn pick_path(&mut self, now: u64) -> Option<u8> {
        if self.paths.is_empty() {
            return None;
        }
        match self.mode {
            STrackMode::Ecmp => Some(0),
            STrackMode::Strack => {
                let n = self.paths.len() as u8;
                for _ in 0..n {
                    let idx = self.paths_rr_cursor;
                    self.paths_rr_cursor = (self.paths_rr_cursor + 1) % n;
                    if self.paths[idx as usize].is_available(now) {
                        return Some(idx);
                    }
                }
                Some(0)
            }
        }
    }

    fn on_ack(&mut self, ack: &Packet, now: u64) {
        let Some(flow) = self.tx_flows.get_mut(&ack.flow_id) else { return };
        let acked = ack.seq;
        if acked > flow.un_acked_base {
            let delta = acked - flow.un_acked_base;
            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;
            flow.in_flight = flow.in_flight.saturating_sub(delta);
            flow.last_ack_time = now;
            if !ack.ecn {
                flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
            }
        }
        if ack.ecn {
            self.tx_stats.ecn_ack_received += 1;
            match self.mode {
                STrackMode::Strack => {
                    let path = (ack.id as u8) % (self.paths.len().max(1) as u8);
                    self.paths[path as usize].blacklisted_until = now + self.blacklist_duration_ns;
                    self.paths[path as usize].ecn_recent += 1;
                    let avail_count = self.paths.iter().filter(|p| p.is_available(now)).count();
                    if avail_count == 0 {
                        flow.cwnd = (flow.cwnd / 2).max(self.min_cwnd);
                    }
                }
                STrackMode::Ecmp => {
                    flow.cwnd = (flow.cwnd / 2).max(self.min_cwnd);
                }
            }
        }
        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.tx_stats.flows_completed += 1;
            self.finished.push((flow.flow_id, now));
        }
    }

    fn on_nack(&mut self, nack: &Packet, _now: u64) {
        self.tx_stats.nack_received += 1;
        let Some(flow) = self.tx_flows.get_mut(&nack.flow_id) else { return };
        let (base, bits) = if nack.payload.len() >= 12 {
            let base = u32::from_le_bytes([
                nack.payload[0], nack.payload[1], nack.payload[2], nack.payload[3],
            ]);
            let bits = u64::from_le_bytes([
                nack.payload[4], nack.payload[5], nack.payload[6], nack.payload[7],
                nack.payload[8], nack.payload[9], nack.payload[10], nack.payload[11],
            ]);
            (base, bits)
        } else {
            (0, 0)
        };
        for i in 0..64u32 {
            let s = base + i;
            if s >= nack.seq {
                break;
            }
            let received = (bits >> i) & 1 == 1;
            if !received && !flow.retransmit_queue.contains(&s) {
                flow.retransmit_queue.push(s);
            }
        }
    }

    // ---- 接收端内部方法 ----

    fn on_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
        self.rx_stats.packets_received += 1;
        let flow = self
            .rx_flows
            .entry(pkt.flow_id)
            .or_insert_with(|| FlowRxState::new(pkt.flow_id));

        let seq = pkt.seq;
        let next = flow.next_expected;
        let mut nack_needed = false;

        if seq < next {
            self.rx_stats.duplicates += 1;
        } else if seq == next {
            flow.next_expected += 1;
            flow.received_bits >>= 1;
            while flow.received_bits & 1 == 1 {
                flow.next_expected += 1;
                flow.received_bits >>= 1;
            }
        } else {
            self.rx_stats.out_of_order += 1;
            let offset = (seq - next) as u32;
            if offset < 64 {
                let bit = 1u64 << offset;
                if flow.received_bits & bit == 0 {
                    flow.received_bits |= bit;
                    nack_needed = true;
                } else {
                    self.rx_stats.duplicates += 1;
                }
            }
        }

        let mut out = Vec::new();
        let pid_ack = self.next_packet_id;
        self.next_packet_id += 1;
        let mut ack_payload = Vec::with_capacity(12);
        ack_payload.extend_from_slice(&flow.next_expected.to_le_bytes());
        ack_payload.extend_from_slice(&flow.received_bits.to_le_bytes());
        let ack = Packet::control(
            pid_ack,
            pid_ack,
            pkt.flow_id,
            flow.next_expected,
            self.host_id,
            pkt.src,
            pkt.ecn,
            0,
            ack_payload,
            now,
        );
        out.push(ack);

        if nack_needed {
            let pid_nack = self.next_packet_id;
            self.next_packet_id += 1;
            let mut nack_payload = Vec::with_capacity(12);
            nack_payload.extend_from_slice(&flow.next_expected.to_le_bytes());
            nack_payload.extend_from_slice(&flow.received_bits.to_le_bytes());
            let nack = Packet::control(
                pid_nack,
                pid_nack,
                pkt.flow_id,
                seq,
                self.host_id,
                pkt.src,
                false,
                1,
                nack_payload,
                now,
            );
            out.push(nack);
            self.rx_stats.nacks_sent += 1;
        }

        self.rx_stats.packets_delivered = self.rx_stats.packets_delivered.max(flow.next_expected as u64);
        out
    }
}

// ------------------------------------------------------------------
// Protocol trait 实现
// ------------------------------------------------------------------

impl Protocol for STrackProtocol {
    fn start_flow(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64) {
        self.start_flow_tx(flow_id, dst, total_bytes, now);
    }

    fn on_tx_tick(&mut self, now: u64) -> Vec<Packet> {
        self.try_send(now)
    }

    fn on_rx_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
        self.on_data(pkt, now)
    }

    fn on_tx_control(&mut self, pkt: &Packet, now: u64) {
        match pkt.kind {
            crate::network::packet::PacketKind::Control(0) => self.on_ack(pkt, now),
            crate::network::packet::PacketKind::Control(1) => self.on_nack(pkt, now),
            _ => {}
        }
    }

    fn all_flows_done(&self) -> bool {
        !self.tx_flows.is_empty() && self.tx_flows.values().all(|f| f.done)
    }

    fn take_finished_flows(&mut self) -> Vec<(FlowId, u64)> {
        std::mem::take(&mut self.finished)
    }

    fn stats(&self) -> ProtocolStats {
        ProtocolStats {
            packets_sent: self.tx_stats.packets_sent,
            packets_retransmitted: self.tx_stats.packets_retransmitted,
            packets_received: self.rx_stats.packets_received,
            ecn_ack_received: self.tx_stats.ecn_ack_received,
            nack_received: self.tx_stats.nack_received,
            nacks_sent: self.rx_stats.nacks_sent,
            flows_completed: self.tx_stats.flows_completed,
            ..Default::default()
        }
    }

    fn has_pending_work(&self) -> bool {
        for f in self.tx_flows.values() {
            if f.done {
                continue;
            }
            if !f.retransmit_queue.is_empty() {
                return true;
            }
            if f.in_flight < f.cwnd && f.next_seq < f.total_packets {
                return true;
            }
        }
        false
    }

    fn next_rto_deadline(&self) -> Option<u64> {
        let mut min_deadline: Option<u64> = None;
        for f in self.tx_flows.values() {
            if f.done {
                continue;
            }
            for &send_t in f.send_times.values() {
                let deadline = send_t.saturating_add(self.rto_ns);
                min_deadline = Some(match min_deadline {
                    Some(current) => current.min(deadline),
                    None => deadline,
                });
            }
        }
        min_deadline
    }

    fn update_send_time(&mut self, flow_id: FlowId, seq: SeqNum, nic_depart_time: u64) {
        if let Some(f) = self.tx_flows.get_mut(&flow_id) {
            if f.send_times.contains_key(&seq) {
                f.send_times.insert(seq, nic_depart_time);
            }
        }
    }
}

// ------------------------------------------------------------------
// 单元测试（从原 tx.rs / rx.rs 迁移，保持行为不变）
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::Packet;

    // ---- 接收端测试 ----

    #[test]
    fn rx_in_order_delivery() {
        let mut proto = STrackProtocol::with_path_count(99, STrackMode::Ecmp, 1);
        for s in 0..10u32 {
            let pkt = Packet::data(s as u64, 0, 0, s, 1, 99, 0);
            let outs = proto.on_data(&pkt, 0);
            assert_eq!(outs.len(), 1); // 只回 ACK
            assert_eq!(outs[0].seq, s + 1); // 累计 ACK
        }
        assert_eq!(proto.rx_flows[&0].next_expected, 10);
    }

    #[test]
    fn rx_out_of_order_then_fill_gap() {
        let mut proto = STrackProtocol::with_path_count(99, STrackMode::Ecmp, 1);
        for s in [0u32, 2, 3, 1] {
            let pkt = Packet::data(s as u64, 0, 0, s, 1, 99, 0);
            proto.on_data(&pkt, 0);
        }
        assert_eq!(proto.rx_flows[&0].next_expected, 4);
        assert!(proto.rx_stats.nacks_sent > 0);
    }

    #[test]
    fn rx_handles_duplicate() {
        let mut proto = STrackProtocol::with_path_count(99, STrackMode::Ecmp, 1);
        for _ in 0..3 {
            let pkt = Packet::data(0, 0, 0, 0, 1, 99, 0);
            proto.on_data(&pkt, 0);
        }
        assert_eq!(proto.rx_flows[&0].next_expected, 1);
        assert!(proto.rx_stats.duplicates >= 2);
    }

    // ---- 发送端测试 ----

    #[test]
    fn tx_sends_packets_up_to_cwnd() {
        let mut proto = STrackProtocol::with_path_count(1, STrackMode::Ecmp, 1);
        proto.start_flow(0, 2, 1024 * 100, 0); // 100 个包
        let pkts = proto.on_tx_tick(0);
        assert_eq!(pkts.len() as u32, proto.init_cwnd);
    }

    #[test]
    fn tx_ack_advances_window() {
        let mut proto = STrackProtocol::with_path_count(1, STrackMode::Ecmp, 1);
        proto.start_flow(0, 2, 1024 * 100, 0);
        let _ = proto.on_tx_tick(0);
        // ACK 前 cwnd 个包
        let ack = Packet::control(1000, 0, 0, 16, 2, 1, false, 0, Vec::new(), 1000);
        proto.on_tx_control(&ack, 1000);
        let flow = &proto.tx_flows[&0];
        assert_eq!(flow.un_acked_base, 16);
        assert_eq!(flow.in_flight, 0);
    }

    #[test]
    fn tx_ecn_reduces_cwnd() {
        let mut proto = STrackProtocol::with_path_count(1, STrackMode::Ecmp, 1);
        proto.start_flow(0, 2, 1024 * 100, 0);
        let _ = proto.on_tx_tick(0);
        let ack = Packet::control(1000, 0, 0, 16, 2, 1, true, 0, Vec::new(), 1000);
        proto.on_tx_control(&ack, 1000);
        let flow = &proto.tx_flows[&0];
        assert_eq!(flow.cwnd, proto.init_cwnd / 2);
    }

    #[test]
    fn tx_nack_triggers_retransmit() {
        let mut proto = STrackProtocol::with_path_count(1, STrackMode::Ecmp, 1);
        proto.start_flow(0, 2, 1024 * 10, 0);
        let _ = proto.on_tx_tick(0);
        // 构造 NACK：缺失 seq 1
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes()); // bits=0 => 所有包都缺失
        let nack = Packet::control(1000, 0, 0, 1, 2, 1, false, 1, payload, 1000);
        proto.on_tx_control(&nack, 1000);
        let flow = &proto.tx_flows[&0];
        assert!(flow.retransmit_queue.contains(&0));
    }

    #[test]
    fn tx_rto_triggers_at_exact_deadline() {
        let mut proto = STrackProtocol::with_path_count(1, STrackMode::Ecmp, 1);
        proto.start_flow(0, 2, 1024 * 10, 0);
        let _ = proto.on_tx_tick(0);

        let pkts = proto.on_tx_tick(proto.rto_ns);

        assert!(!pkts.is_empty(), "RTO 截止时刻应立即触发重传");
        assert!(proto.tx_stats.packets_retransmitted > 0);
    }
}
