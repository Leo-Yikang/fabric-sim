//! TCP Reno 协议实现
//!
//! 在 SimpleTcp 基础上增加完整的 Fast Recovery 阶段：
//! - 慢启动（cwnd < ssthresh）：每 ACK cwnd += delta
//! - 拥塞避免（cwnd >= ssthresh）：每 cwnd 个 ACK cwnd += 1
//! - 快速重传：3 dup ACK 触发重传
//! - 快速恢复：3 dup ACK 后进入，新 ACK 到达时退出
//!   - 进入：ssthresh = cwnd/2, cwnd = ssthresh + 3
//!   - 期间每个 dup ACK：cwnd += 1
//!   - 退出：cwnd = ssthresh
//! - 超时重传：ssthresh = cwnd/2, cwnd = init_cwnd

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::{HashMap, HashSet};

// ------------------------------------------------------------------
// 发送端状态
// ------------------------------------------------------------------

/// TCP Reno 每条流的发送端状态
pub struct FlowTxState {
    pub flow_id: FlowId,
    pub dst: EntityId,
    pub total_packets: u32,
    pub next_seq: SeqNum,
    pub un_acked_base: SeqNum,
    pub cwnd: u32,
    pub ssthresh: u32,
    pub in_flight: u32,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
    pub send_times: HashMap<SeqNum, u64>,
    pub retransmit_queue: Vec<SeqNum>,
    pub dup_ack_count: u32,
    pub ca_ack_count: u32,
    /// Reno 特有：是否处于快速恢复阶段
    pub fast_recovery: bool,
    /// 快速恢复期间累计收到的 dup ACK 导致的 cwnd 膨胀量
    pub recovery_inflation: u32,
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
            ssthresh: 64,
            in_flight: 0,
            done: false,
            start_time,
            finish_time: 0,
            send_times: HashMap::new(),
            retransmit_queue: Vec::new(),
            dup_ack_count: 0,
            ca_ack_count: 0,
            fast_recovery: false,
            recovery_inflation: 0,
        }
    }
}

// ------------------------------------------------------------------
// 接收端状态（与 SimpleTcp 相同）
// ------------------------------------------------------------------

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: SeqNum,
    pub out_of_order: HashSet<SeqNum>,
}

impl FlowRxState {
    pub fn new(flow_id: FlowId) -> Self {
        Self {
            flow_id,
            next_expected: 0,
            out_of_order: HashSet::new(),
        }
    }
}

// ------------------------------------------------------------------
// TcpReno
// ------------------------------------------------------------------

pub struct TcpReno {
    pub host_id: EntityId,
    pub tx_flows: HashMap<FlowId, FlowTxState>,
    pub rx_flows: HashMap<FlowId, FlowRxState>,
    pub stats: ProtocolStats,
    pub next_packet_id: u64,
    pub init_cwnd: u32,
    pub max_cwnd: u32,
    pub min_cwnd: u32,
    pub rto_ns: u64,
    finished: Vec<(FlowId, u64)>,
}

impl TcpReno {
    pub fn new(host_id: EntityId) -> Self {
        Self {
            host_id,
            tx_flows: HashMap::new(),
            rx_flows: HashMap::new(),
            stats: ProtocolStats::default(),
            next_packet_id: 1,
            init_cwnd: 16,
            max_cwnd: 256,
            min_cwnd: 1,
            rto_ns: 100_000,
            finished: Vec::new(),
        }
    }

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
                let f = self.tx_flows.get(&fid).expect("invariant");
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
                let f = self.tx_flows.get(&fid).expect("invariant");
                for (seq, send_t) in &f.send_times {
                    if now.saturating_sub(*send_t) >= self.rto_ns && !retx.contains(seq) {
                        timeout_seqs.push(*seq);
                    }
                }
            }
            if !timeout_seqs.is_empty() {
                let f = self.tx_flows.get_mut(&fid).expect("invariant");
                f.ssthresh = (f.cwnd / 2).max(self.min_cwnd);
                f.cwnd = self.init_cwnd;
                f.dup_ack_count = 0;
                f.ca_ack_count = 0;
                f.fast_recovery = false;
                f.recovery_inflation = 0;
            }
            for s in timeout_seqs {
                retx.push(s);
            }

            let mut new_inflight = in_flight;
            let mut send_records: Vec<(SeqNum, u64)> = Vec::new();

            // 先重传
            let mut retx_budget = cwnd as usize;
            while retx_budget > 0 && !retx.is_empty() {
                let seq = retx.remove(0);
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let pkt = Packet::data(pid, pid, fid, seq, self.host_id, dst, now);
                to_send.push(pkt);
                self.stats.packets_retransmitted += 1;
                self.stats.packets_sent += 1;
                send_records.push((seq, now));
                retx_budget -= 1;
            }

            // 再发新包
            while new_inflight < cwnd && next_seq < total {
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let pkt = Packet::data(pid, pid, fid, next_seq, self.host_id, dst, now);
                to_send.push(pkt);
                self.stats.packets_sent += 1;
                new_inflight += 1;
                send_records.push((next_seq, now));
                next_seq += 1;
            }

            let f = self.tx_flows.get_mut(&fid).expect("invariant");
            f.in_flight = new_inflight;
            f.next_seq = next_seq;
            f.retransmit_queue = retx;
            for (seq, t) in send_records {
                f.send_times.insert(seq, t);
            }
        }
        to_send
    }

    fn on_ack(&mut self, ack: &Packet, now: u64) {
        let Some(flow) = self.tx_flows.get_mut(&ack.flow_id) else {
            return;
        };
        let acked = ack.seq;

        if acked == flow.un_acked_base {
            // 重复 ACK
            flow.dup_ack_count += 1;

            if flow.fast_recovery {
                // 在快速恢复中，每个 dup ACK 膨胀 cwnd
                flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                flow.recovery_inflation += 1;
            } else if flow.dup_ack_count == 3 {
                // 第 3 个 dup ACK：进入快速恢复
                flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                flow.cwnd = (flow.ssthresh + 3).min(self.max_cwnd);
                flow.fast_recovery = true;
                flow.recovery_inflation = 0;
                // 重传丢失的第一个包
                if !flow.retransmit_queue.contains(&flow.un_acked_base) {
                    flow.retransmit_queue.push(flow.un_acked_base);
                }
            }
        } else if acked > flow.un_acked_base {
            // 新 ACK
            let delta = acked - flow.un_acked_base;
            let was_in_recovery = flow.fast_recovery;

            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;
            flow.in_flight = flow.in_flight.saturating_sub(delta);
            flow.dup_ack_count = 0;

            if was_in_recovery {
                // 退出快速恢复
                flow.cwnd = flow.ssthresh;
                flow.fast_recovery = false;
                flow.recovery_inflation = 0;
            }

            if ack.ecn {
                // ECN 标记：乘性降窗
                flow.fast_recovery = false;
                flow.recovery_inflation = 0;
                flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                flow.cwnd = flow.ssthresh.max(self.min_cwnd);
                flow.ca_ack_count = 0;
            } else if !was_in_recovery {
                // 正常 ACK，未在快速恢复中
                if flow.cwnd < flow.ssthresh {
                    // 慢启动
                    flow.cwnd = (flow.cwnd + delta).min(self.max_cwnd);
                } else {
                    // 拥塞避免：AIMD
                    flow.ca_ack_count += delta;
                    if flow.ca_ack_count >= flow.cwnd {
                        flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                        flow.ca_ack_count = 0;
                    }
                }
            }
            // fast_recovery 退出后，cwnd 已重置为 ssthresh，接下来走拥塞避免
        }

        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.stats.flows_completed += 1;
            self.finished.push((flow.flow_id, now));
        }
    }

    fn on_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
        self.stats.packets_received += 1;
        let flow = self
            .rx_flows
            .entry(pkt.flow_id)
            .or_insert_with(|| FlowRxState::new(pkt.flow_id));

        let seq = pkt.seq;
        let next = flow.next_expected;

        if seq < next {
            // 重复包，忽略
        } else if seq == next {
            flow.next_expected += 1;
            while flow.out_of_order.remove(&flow.next_expected) {
                flow.next_expected += 1;
            }
        } else {
            flow.out_of_order.insert(seq);
        }

        let mut out = Vec::new();
        let pid_ack = self.next_packet_id;
        self.next_packet_id += 1;
        let ack = Packet::control(
            pid_ack, pid_ack, pkt.flow_id,
            flow.next_expected,
            self.host_id, pkt.src, pkt.ecn,
            0, Vec::new(), now,
        );
        out.push(ack);
        out
    }
}

impl Protocol for TcpReno {
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
        self.stats
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 Reno 快速恢复：3 dup ACK 后 cwnd 减半+3，新 ACK 后退出恢复
    #[test]
    fn reno_fast_recovery_on_3_dup_acks() {
        let mut reno = TcpReno::new(0);
        reno.start_flow(1, 1, MTU_BYTES as u64 * 100, 0);

        // 发送初始窗口（16 包）
        let pkts = reno.on_tx_tick(0);
        assert_eq!(pkts.len(), 16);
        let flow = reno.tx_flows.get(&1).unwrap();
        assert_eq!(flow.cwnd, 16);

        // ACK seq=0（重复 ACK #1）：cwnd 不变
        let ack1 = Packet::control(100, 100, 1, 0, 1, 0, false, 0, Vec::new(), 1000);
        reno.on_tx_control(&ack1, 1000);
        let f = reno.tx_flows.get(&1).unwrap();
        assert_eq!(f.dup_ack_count, 1);
        assert!(!f.fast_recovery);

        // ACK seq=0（重复 ACK #2）
        let ack2 = Packet::control(101, 101, 1, 0, 1, 0, false, 0, Vec::new(), 2000);
        reno.on_tx_control(&ack2, 2000);
        let f = reno.tx_flows.get(&1).unwrap();
        assert_eq!(f.dup_ack_count, 2);

        // ACK seq=0（重复 ACK #3）：触发快速恢复
        let ack3 = Packet::control(102, 102, 1, 0, 1, 0, false, 0, Vec::new(), 3000);
        reno.on_tx_control(&ack3, 3000);
        let f = reno.tx_flows.get(&1).unwrap();
        assert!(f.fast_recovery);
        assert_eq!(f.ssthresh, 8); // cwnd=16, half=8
        assert_eq!(f.cwnd, 11);    // ssthresh + 3
        assert!(!f.retransmit_queue.is_empty()); // 重传队列非空

        // dup ACK 在快速恢复中：cwnd += 1
        let ack4 = Packet::control(103, 103, 1, 0, 1, 0, false, 0, Vec::new(), 4000);
        reno.on_tx_control(&ack4, 4000);
        let f = reno.tx_flows.get(&1).unwrap();
        assert_eq!(f.cwnd, 12);

        // 新 ACK：退出快速恢复，cwnd = ssthresh
        let ack_new = Packet::control(104, 104, 1, 5, 1, 0, false, 0, Vec::new(), 5000);
        reno.on_tx_control(&ack_new, 5000);
        let f = reno.tx_flows.get(&1).unwrap();
        assert!(!f.fast_recovery);
        assert_eq!(f.cwnd, 8);
    }

    /// 验证超时后正常退出快速恢复
    #[test]
    fn reno_timeout_exits_fast_recovery() {
        let mut reno = TcpReno::new(1);
        reno.start_flow(1, 0, MTU_BYTES as u64 * 100, 0);
        reno.on_tx_tick(0); // 发包

        // 进入快速恢复
        let flow = reno.tx_flows.get_mut(&1).unwrap();
        flow.fast_recovery = true;
        flow.cwnd = 11;
        flow.ssthresh = 8;
        // 模拟一个过期包
        flow.send_times.insert(0, 0);

        // 超时发生：退出快速恢复
        reno.on_tx_tick(200_000); // 超过 RTO
        let f = reno.tx_flows.get(&1).unwrap();
        assert!(!f.fast_recovery);
        assert_eq!(f.cwnd, 16); // 重置为 init_cwnd
        assert_eq!(f.ssthresh, 5); // max(11/2, 1) = 5
    }
}