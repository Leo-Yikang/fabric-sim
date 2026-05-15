//! 简化 TCP 协议实现
//!
//! 用于验证 `Protocol` trait 的通用性。特性：
//! - 单路径（无 Packet Spraying，无路径黑名单）
//! - 累计 ACK（无 NACK，无 SACK Bitmap）
//! - 慢启动（Slow Start）+ 拥塞避免（Congestion Avoidance）
//! - 快速重传（Fast Retransmit）：3 个重复 ACK 触发
//! - RTO 超时重传
//! - 接收端带简化的乱序缓存

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::{HashMap, HashSet};

// ------------------------------------------------------------------
// 发送端状态
// ------------------------------------------------------------------

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
    /// 重复 ACK 计数（用于快速重传）
    pub dup_ack_count: u32,
    /// 拥塞避免阶段累计 ACK 数
    pub ca_ack_count: u32,
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
            ssthresh: 64, // 初始慢启动阈值
            in_flight: 0,
            done: false,
            start_time,
            finish_time: 0,
            send_times: HashMap::new(),
            retransmit_queue: Vec::new(),
            dup_ack_count: 0,
            ca_ack_count: 0,
        }
    }
}

// ------------------------------------------------------------------
// 接收端状态
// ------------------------------------------------------------------

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: SeqNum,
    /// 已缓存的乱序包序号
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
// SimpleTcp
// ------------------------------------------------------------------

pub struct SimpleTcp {
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

impl SimpleTcp {
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
                let f = self.tx_flows.get(&fid).unwrap();
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
                let f = self.tx_flows.get(&fid).unwrap();
                for (seq, send_t) in &f.send_times {
                    if now.saturating_sub(*send_t) > self.rto_ns && !retx.contains(seq) {
                        timeout_seqs.push(*seq);
                    }
                }
            }
            if !timeout_seqs.is_empty() {
                // RTO 超时：进入超时恢复
                let f = self.tx_flows.get_mut(&fid).unwrap();
                f.ssthresh = (f.cwnd / 2).max(self.min_cwnd);
                f.cwnd = self.init_cwnd;
                f.dup_ack_count = 0;
                f.ca_ack_count = 0;
            }
            for s in timeout_seqs {
                retx.push(s);
            }

            let mut new_inflight = in_flight;
            let mut send_records: Vec<(SeqNum, u64)> = Vec::new();

            // 先重传（限制批量不超过 cwnd）
            let mut retx_budget = cwnd as usize;
            while retx_budget > 0 && !retx.is_empty() {
                let seq = retx.remove(0);
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let pkt = Packet::data(pid, fid, seq, self.host_id, dst, now);
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
                let pkt = Packet::data(pid, fid, next_seq, self.host_id, dst, now);
                to_send.push(pkt);
                self.stats.packets_sent += 1;
                new_inflight += 1;
                send_records.push((next_seq, now));
                next_seq += 1;
            }

            let f = self.tx_flows.get_mut(&fid).unwrap();
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
            // 重复 ACK（没有推进累计确认号）
            flow.dup_ack_count += 1;
            if flow.dup_ack_count >= 3 {
                // 快速重传：第 3 个重复 ACK
                if flow.dup_ack_count == 3 {
                    flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                    flow.cwnd = flow.ssthresh + 3;
                    // 将 un_acked_base 加入重传队列（如果不存在）
                    if !flow.retransmit_queue.contains(&flow.un_acked_base) {
                        flow.retransmit_queue.push(flow.un_acked_base);
                    }
                } else {
                    // 第 4 个及以后的重复 ACK：继续 inflight 膨胀（快速恢复简化）
                    flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                }
            }
        } else if acked > flow.un_acked_base {
            // 新 ACK，推进了累计确认号
            let delta = acked - flow.un_acked_base;
            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;
            flow.in_flight = flow.in_flight.saturating_sub(delta);
            flow.dup_ack_count = 0;

            if ack.ecn {
                // ECN 标记：乘性降窗
                flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                flow.cwnd = flow.ssthresh.max(self.min_cwnd);
                flow.ca_ack_count = 0;
            } else {
                // 正常 ACK：慢启动或拥塞避免
                if flow.cwnd < flow.ssthresh {
                    // 慢启动：每个 ACK cwnd += delta（通常 delta=1）
                    flow.cwnd = (flow.cwnd + delta).min(self.max_cwnd);
                } else {
                    // 拥塞避免：每 cwnd 个 ACK 才加 1
                    flow.ca_ack_count += delta;
                    if flow.ca_ack_count >= flow.cwnd {
                        flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                        flow.ca_ack_count = 0;
                    }
                }
            }
        }

        // 检查流完成
        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.stats.flows_completed += 1;
            self.finished.push((flow.flow_id, now));
        }
    }

    // ---- 接收端内部方法 ----

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
            // 尝试连续交付缓存的乱序包
            while flow.out_of_order.remove(&flow.next_expected) {
                flow.next_expected += 1;
            }
        } else {
            // 乱序包，缓存
            flow.out_of_order.insert(seq);
        }

        // 回 ACK
        let mut out = Vec::new();
        let pid_ack = self.next_packet_id;
        self.next_packet_id += 1;
        let ack = Packet::control(
            pid_ack,
            pkt.flow_id,
            flow.next_expected,
            self.host_id,
            pkt.src,
            pkt.ecn,
            0, // control_type = 0 => ACK
            Vec::new(),
            now,
        );
        out.push(ack);
        out
    }
}

// ------------------------------------------------------------------
// Protocol trait 实现
// ------------------------------------------------------------------

impl Protocol for SimpleTcp {
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
}

// ------------------------------------------------------------------
// 单元测试
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::Packet;

    #[test]
    fn tcp_rx_in_order_delivery() {
        let mut tcp = SimpleTcp::new(99);
        for s in 0..10u32 {
            let pkt = Packet::data(s as u64, 0, s, 1, 99, 0);
            let outs = tcp.on_rx_data(&pkt, 0);
            assert_eq!(outs.len(), 1);
            assert_eq!(outs[0].seq, s + 1);
        }
        assert_eq!(tcp.rx_flows[&0].next_expected, 10);
    }

    #[test]
    fn tcp_rx_out_of_order_then_fill_gap() {
        let mut tcp = SimpleTcp::new(99);
        let pkt0 = Packet::data(0, 0, 0, 1, 99, 0);
        let pkt2 = Packet::data(1, 0, 2, 1, 99, 0);
        let pkt1 = Packet::data(2, 0, 1, 1, 99, 0);

        let out0 = tcp.on_rx_data(&pkt0, 0);
        assert_eq!(out0[0].seq, 1);

        let out2 = tcp.on_rx_data(&pkt2, 0);
        assert_eq!(out2[0].seq, 1);

        let out1 = tcp.on_rx_data(&pkt1, 0);
        assert_eq!(out1[0].seq, 3);

        assert_eq!(tcp.rx_flows[&0].next_expected, 3);
    }

    #[test]
    fn tcp_tx_sends_up_to_cwnd() {
        let mut tcp = SimpleTcp::new(1);
        tcp.start_flow(0, 2, 1024 * 100, 0);
        let pkts = tcp.on_tx_tick(0);
        assert_eq!(pkts.len() as u32, tcp.init_cwnd);
    }

    #[test]
    fn tcp_ack_advances_window() {
        let mut tcp = SimpleTcp::new(1);
        tcp.start_flow(0, 2, 1024 * 100, 0);
        let _ = tcp.on_tx_tick(0);
        let ack = Packet::control(1000, 0, 16, 2, 1, false, 0, Vec::new(), 1000);
        tcp.on_tx_control(&ack, 1000);
        let flow = &tcp.tx_flows[&0];
        assert_eq!(flow.un_acked_base, 16);
        assert_eq!(flow.in_flight, 0);
    }

    #[test]
    fn tcp_ecn_reduces_cwnd() {
        let mut tcp = SimpleTcp::new(1);
        tcp.start_flow(0, 2, 1024 * 100, 0);
        let _ = tcp.on_tx_tick(0);
        let ack = Packet::control(1000, 0, 16, 2, 1, true, 0, Vec::new(), 1000);
        tcp.on_tx_control(&ack, 1000);
        let flow = &tcp.tx_flows[&0];
        assert!(flow.cwnd <= tcp.init_cwnd); // ECN 后 cwnd 应该下降
        assert_eq!(flow.ssthresh, tcp.init_cwnd / 2);
    }

    #[test]
    fn tcp_timeout_retransmit() {
        let mut tcp = SimpleTcp::new(1);
        tcp.start_flow(0, 2, 1024 * 10, 0);
        let pkts = tcp.on_tx_tick(0);
        assert!(!pkts.is_empty());
        let old_cwnd = tcp.tx_flows[&0].cwnd;
        let old_ssthresh = tcp.tx_flows[&0].ssthresh;
        let pkts2 = tcp.on_tx_tick(tcp.rto_ns + 1);
        assert!(!pkts2.is_empty(), "应当触发超时重传");
        let flow = &tcp.tx_flows[&0];
        assert_eq!(flow.cwnd, tcp.init_cwnd, "RTO 后 cwnd 应重置为 init_cwnd");
        assert_eq!(flow.ssthresh, old_cwnd / 2, "RTO 后 ssthresh 应降为 cwnd/2");
        assert_ne!(flow.ssthresh, old_ssthresh);
    }

    #[test]
    fn tcp_fast_retransmit_on_3_dup_ack() {
        let mut tcp = SimpleTcp::new(1);
        tcp.start_flow(0, 2, 1024 * 100, 0);
        // 发 10 个包
        let _ = tcp.on_tx_tick(0);
        let flow = &tcp.tx_flows[&0];
        let sent_cwnd = flow.cwnd;

        // 模拟收到 3 个重复 ACK（都确认到 seq=0，即第一个包没收到）
        for i in 0..3 {
            let ack = Packet::control(1000 + i as u64, 0, 0, 2, 1, false, 0, Vec::new(), 1000);
            tcp.on_tx_control(&ack, 1000);
        }

        let flow = &tcp.tx_flows[&0];
        assert_eq!(flow.dup_ack_count, 3);
        assert_eq!(flow.ssthresh, sent_cwnd / 2, "ssthresh 应降为 cwnd/2");
        assert!(
            flow.retransmit_queue.contains(&0),
            "快速重传应将 seq=0 加入重传队列"
        );
    }

    #[test]
    fn tcp_slow_start_vs_congestion_avoidance() {
        let mut tcp = SimpleTcp::new(1);
        tcp.init_cwnd = 1; // 用 cwnd=1 方便逐包确认
        tcp.start_flow(0, 2, 1024 * 1000, 0);
        // 发 1 个包
        let _ = tcp.on_tx_tick(0);
        assert_eq!(tcp.tx_flows[&0].cwnd, 1);
        assert_eq!(tcp.tx_flows[&0].ssthresh, 64);

        // 慢启动：每个 ACK 确认 1 个包，cwnd += 1
        let mut ack_seq = 1;
        for _ in 0..10 {
            let ack = Packet::control(
                1000 + ack_seq as u64,
                0,
                ack_seq,
                2,
                1,
                false,
                0,
                Vec::new(),
                1000,
            );
            tcp.on_tx_control(&ack, 1000);
            ack_seq = tcp.tx_flows[&0].un_acked_base + 1;
        }
        // 发了 10 个 ACK，cwnd 应该从 1 增长到 11
        assert_eq!(tcp.tx_flows[&0].cwnd, 11, "慢启动：cwnd 应线性增长");

        // 继续 ACK 直到 cwnd >= ssthresh（64）
        while tcp.tx_flows[&0].cwnd < tcp.tx_flows[&0].ssthresh {
            let ack = Packet::control(
                2000 + ack_seq as u64,
                0,
                ack_seq,
                2,
                1,
                false,
                0,
                Vec::new(),
                2000,
            );
            tcp.on_tx_control(&ack, 2000);
            ack_seq = tcp.tx_flows[&0].un_acked_base + 1;
        }

        // 现在进入拥塞避免
        let cwnd_ca = tcp.tx_flows[&0].cwnd;
        let ssthresh = tcp.tx_flows[&0].ssthresh;
        assert!(cwnd_ca >= ssthresh, "应已进入拥塞避免阶段");

        // 拥塞避免：需要 cwnd 个 ACK（逐个确认）才加 1
        for _ in 0..cwnd_ca {
            let ack = Packet::control(
                3000 + ack_seq as u64,
                0,
                ack_seq,
                2,
                1,
                false,
                0,
                Vec::new(),
                3000,
            );
            tcp.on_tx_control(&ack, 3000);
            ack_seq = tcp.tx_flows[&0].un_acked_base + 1;
        }
        assert_eq!(
            tcp.tx_flows[&0].cwnd,
            cwnd_ca + 1,
            "拥塞避免：cwnd 应只加 1"
        );
    }
}
