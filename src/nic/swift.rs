//! Swift（Datacenter Transport for RoCE）简化占位实现
//!
//! Swift 核心思想（SIGCOMM'20）：
//! - 基于 RTT 测量的拥塞控制（类似 TCP BBR，但更轻量）
//! - 发送端根据 RTT 样本区分拥塞和 benign 排队：
//!   new_rate = current_rate * (base_rtt / current_rtt)
//! - 使用 ACK 中的时间戳信息精确计算 RTT
//!
//! 当前占位实现：
//! - 复用 DCQCN 的 rate-based 框架
//! - 简化 RTT 测量：ACK 携带发送时间戳
//! - 调整公式：rate = rate * min_rtt / current_rtt

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, MTU_BYTES};
use crate::EntityId;
use std::collections::HashMap;

const INIT_RATE_BPS: u64 = 1_000_000_000;
const MIN_RATE_BPS: u64 = 1_000_000;
const MAX_RATE_BPS: u64 = 100_000_000_000;
const RTO_NS: u64 = 100_000;
const INIT_CWND: u32 = 16;

pub struct FlowTxState {
    pub flow_id: FlowId,
    pub dst: EntityId,
    pub total_packets: u32,
    pub next_seq: u32,
    pub un_acked_base: u32,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
    pub send_times: HashMap<u32, u64>,
    pub retransmit_queue: Vec<u32>,
    pub current_rate_bps: u64,
    pub next_tx_time_ns: u64,
    /// 观测到的最小 RTT（ns）
    pub min_rtt_ns: u64,
}

impl FlowTxState {
    pub fn new(flow_id: FlowId, dst: EntityId, total_packets: u32, start_time: u64) -> Self {
        Self {
            flow_id,
            dst,
            total_packets,
            next_seq: 0,
            un_acked_base: 0,
            done: false,
            start_time,
            finish_time: 0,
            send_times: HashMap::new(),
            retransmit_queue: Vec::new(),
            current_rate_bps: INIT_RATE_BPS,
            next_tx_time_ns: start_time,
            min_rtt_ns: u64::MAX,
        }
    }

    fn packet_interval_ns(&self) -> u64 {
        if self.current_rate_bps == 0 {
            return u64::MAX;
        }
        (MTU_BYTES as u64 * 8 * 1_000_000_000) / self.current_rate_bps
    }
}

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: u32,
}

pub struct SwiftProtocol {
    pub host_id: EntityId,
    pub tx_flows: HashMap<FlowId, FlowTxState>,
    pub rx_flows: HashMap<FlowId, FlowRxState>,
    pub stats: ProtocolStats,
    pub next_packet_id: u64,
    finished: Vec<(FlowId, u64)>,
}

impl SwiftProtocol {
    pub fn new(host_id: EntityId) -> Self {
        Self {
            host_id,
            tx_flows: HashMap::new(),
            rx_flows: HashMap::new(),
            stats: ProtocolStats::default(),
            next_packet_id: 1,
            finished: Vec::new(),
        }
    }

    fn start_flow_tx(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64) {
        let total_packets = ((total_bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        self.tx_flows.insert(flow_id, FlowTxState::new(flow_id, dst, total_packets, now));
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
            let (total, dst, mut retx, interval_ns) = {
                let f = self.tx_flows.get(&fid).expect("invariant");
                (f.total_packets, f.dst, f.retransmit_queue.clone(), f.packet_interval_ns())
            };

            let mut timeout_seqs: Vec<u32> = Vec::new();
            {
                let f = self.tx_flows.get(&fid).expect("invariant");
                for (seq, send_t) in &f.send_times {
                    if now.saturating_sub(*send_t) >= RTO_NS && !retx.contains(seq) {
                        timeout_seqs.push(*seq);
                    }
                }
            }
            for s in timeout_seqs {
                retx.push(s);
            }

            let mut flow = self.tx_flows.get_mut(&fid).expect("invariant");
            let mut sent_count = 0u32;

            while !retx.is_empty() && sent_count < INIT_CWND {
                if now < flow.next_tx_time_ns { break; }
                let seq = retx.remove(0);
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let pkt = Packet::data(pid, pid, fid, seq, self.host_id, dst, now);
                to_send.push(pkt);
                self.stats.packets_retransmitted += 1;
                self.stats.packets_sent += 1;
                flow.send_times.insert(seq, now);
                flow.next_tx_time_ns = now + interval_ns;
                sent_count += 1;
            }

            while flow.next_seq < total && sent_count < INIT_CWND {
                if now < flow.next_tx_time_ns { break; }
                let seq = flow.next_seq;
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let pkt = Packet::data(pid, pid, fid, seq, self.host_id, dst, now);
                to_send.push(pkt);
                self.stats.packets_sent += 1;
                flow.send_times.insert(seq, now);
                flow.next_seq += 1;
                flow.next_tx_time_ns = now + interval_ns;
                sent_count += 1;
            }

            flow.retransmit_queue = retx;
        }
        to_send
    }

    fn on_ack(&mut self, ack: &Packet, now: u64) {
        let Some(flow) = self.tx_flows.get_mut(&ack.flow_id) else { return };
        let acked = ack.seq;

        if acked > flow.un_acked_base {
            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;

            // Swift：从 ACK payload 中解析发送时间戳，计算 RTT
            // payload 格式：[send_time_ns as u64 LE]
            if ack.payload.len() >= 8 {
                let bytes = [
                    ack.payload[0], ack.payload[1], ack.payload[2], ack.payload[3],
                    ack.payload[4], ack.payload[5], ack.payload[6], ack.payload[7],
                ];
                let send_time = u64::from_le_bytes(bytes);
                let rtt = now.saturating_sub(send_time);

                if rtt > 0 {
                    flow.min_rtt_ns = flow.min_rtt_ns.min(rtt);

                    if flow.min_rtt_ns < u64::MAX {
                        // rate = rate * min_rtt / current_rtt
                        let new_rate = (flow.current_rate_bps as f64
                            * flow.min_rtt_ns as f64
                            / rtt as f64) as u64;
                        flow.current_rate_bps = new_rate.clamp(MIN_RATE_BPS, MAX_RATE_BPS);
                    }
                }
            }

            if ack.ecn {
                self.stats.ecn_ack_received += 1;
            }
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
            .or_insert_with(|| FlowRxState {
                flow_id: pkt.flow_id,
                next_expected: 0,
            });

        let seq = pkt.seq;
        if seq == flow.next_expected {
            flow.next_expected += 1;
        }

        // Swift：ACK payload 携带对应 Data 包的发送时间戳
        let ts_bytes = pkt.depart_time.to_le_bytes().to_vec();
        let pid_ack = self.next_packet_id;
        self.next_packet_id += 1;
        let ack = Packet::control(
            pid_ack,
            pid_ack,
            pkt.flow_id,
            flow.next_expected,
            self.host_id,
            pkt.src,
            pkt.ecn,
            0,
            ts_bytes,
            now,
        );
        vec![ack]
    }
}

impl Protocol for SwiftProtocol {
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
            if f.done { continue; }
            if !f.retransmit_queue.is_empty() { return true; }
            if f.next_seq < f.total_packets { return true; }
        }
        false
    }

    fn next_rto_deadline(&self) -> Option<u64> {
        let mut min_deadline: Option<u64> = None;
        for f in self.tx_flows.values() {
            if f.done { continue; }
            for &send_t in f.send_times.values() {
                let deadline = send_t.saturating_add(RTO_NS);
                min_deadline = Some(match min_deadline {
                    Some(current) => current.min(deadline),
                    None => deadline,
                });
            }
        }
        min_deadline
    }

    fn next_tx_time(&self) -> Option<u64> {
        let mut min_time: Option<u64> = None;
        for f in self.tx_flows.values() {
            if f.done { continue; }
            if f.next_seq < f.total_packets || !f.retransmit_queue.is_empty() {
                min_time = Some(match min_time {
                    Some(t) => t.min(f.next_tx_time_ns),
                    None => f.next_tx_time_ns,
                });
            }
        }
        min_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swift_basic_flow() {
        let mut proto = SwiftProtocol::new(1);
        proto.start_flow(0, 2, 1024 * 100, 0);
        let pkts = proto.on_tx_tick(0);
        assert!(!pkts.is_empty());
    }

    #[test]
    fn swift_rtt_adjusts_rate() {
        let mut proto = SwiftProtocol::new(1);
        proto.start_flow(0, 2, 1024 * 100, 0);
        let init_rate = proto.tx_flows[&0].current_rate_bps;

        let _ = proto.on_tx_tick(0);
        // 模拟发送时间戳 = 0，ACK 在 1000ns 后到达 → RTT = 1000ns
        let ack = Packet::control(
            1000, 1000, 0, 1, 2, 1, false, 0,
            0u64.to_le_bytes().to_vec(), 1000,
        );
        proto.on_tx_control(&ack, 1000);

        let flow = &proto.tx_flows[&0];
        assert_eq!(flow.min_rtt_ns, 1000);
        // RTT = min_rtt，rate 应不变
        assert_eq!(flow.current_rate_bps, init_rate);
    }
}
