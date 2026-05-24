//! DCQCN（Data Center Quantized Congestion Notification）协议实现
//!
//! 基于 Microsoft NSDI'15 论文的简化实现，核心机制：
//! - Rate-based 拥塞控制（替代 cwnd-based）
//! - 接收端对 ECN 标记包回送 CNP（Congestion Notification Packet）
//! - 发送端维护 current_rate / target_rate / alpha
//! - 收到 CNP 时：量化 alpha、target_rate = current_rate、current_rate *= (1 - alpha/2)
//! - 每 RAI 周期：current_rate = (current_rate + target_rate) / 2（逐步恢复）
//! - 每 Hyper-Increase 阶段：target_rate += fixed_step
//!
//! 当前简化：
//! - 无显式 QP 概念（单 QP 每条流）
//! - CNP 复用 `PacketKind::Control(2)`
//! - 无 Byte Counter（用包计数器替代）
//! - 定时恢复由 TxTick 驱动（而非硬件定时器）

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::HashMap;

// ------------------------------------------------------------------
// DCQCN 参数（默认值参考 RoCEv2 常见配置）
// ------------------------------------------------------------------

/// 初始速率（字节/秒）。简化：设为 100 Gbps 的 1%
const INIT_RATE_BPS: u64 = 1_000_000_000; // 1 Gbps
/// 最小速率（字节/秒）
const MIN_RATE_BPS: u64 = 1_000_000; // 1 Mbps
/// 最大速率（字节/秒）
const MAX_RATE_BPS: u64 = 100_000_000_000; // 100 Gbps
/// Rate Increase Interval（ns）。每收到 RAI 个 ACK 做一次速率恢复
const RAI_NS: u64 = 55_000; // 55 us
/// Fast Recovery 参数：每收到 CNP 后，连续收到 RATE_DECREASE 个无 ECN ACK 才恢复
const RATE_DECREASE_ACKS: u32 = 50;
/// alpha 更新参数 g（固定点小数，g = 1/256 ≈ 0.0039）
const ALPHA_G_NUMERATOR: u64 = 1;
const ALPHA_G_DENOMINATOR: u64 = 256;
/// Hyper-Increase 步长（字节/秒）
const HYPER_INCREASE_STEP_BPS: u64 = 100_000_000; // 100 Mbps
/// 超时重传时间
const RTO_NS: u64 = 100_000;
/// 初始拥塞窗口（包数，用于启动阶段快速发完第一批）
const INIT_CWND: u32 = 16;

// ------------------------------------------------------------------
// 发送端状态
// ------------------------------------------------------------------

pub struct FlowTxState {
    pub flow_id: FlowId,
    pub dst: EntityId,
    pub total_packets: u32,
    pub next_seq: SeqNum,
    pub un_acked_base: SeqNum,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
    pub send_times: HashMap<SeqNum, u64>,
    pub retransmit_queue: Vec<SeqNum>,

    // DCQCN 核心状态
    /// 当前发送速率（字节/秒）
    pub current_rate_bps: u64,
    /// 目标发送速率（字节/秒）
    pub target_rate_bps: u64,
    /// 拥塞程度（0 ~ 1，用 u64 表示，1.0 = u64::MAX 不现实，用千分比或固定点）
    /// 这里用 0..1000 表示 0.0 ~ 1.0
    pub alpha: u64,
    /// 距离上次 CNP 后收到的无 ECN ACK 计数
    pub acks_since_cnp: u32,
    /// 是否处于 Fast Recovery 阶段（刚收到 CNP）
    pub in_fast_recovery: bool,
    /// 上次速率恢复时间（ns）
    pub last_rate_increase_ns: u64,
    /// 当前在途字节数（用于 pacing）
    pub in_flight_bytes: u64,
    /// 累计发送字节数（用于 pacing）
    pub bytes_sent_total: u64,
    /// 下一个可发送时间（ns，pacing 用）
    pub next_tx_time_ns: u64,
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
            target_rate_bps: INIT_RATE_BPS,
            alpha: 0,
            acks_since_cnp: 0,
            in_fast_recovery: false,
            last_rate_increase_ns: start_time,
            in_flight_bytes: 0,
            bytes_sent_total: 0,
            next_tx_time_ns: start_time,
        }
    }

    /// 根据当前速率计算发送间隔（ns/包）
    fn packet_interval_ns(&self) -> u64 {
        if self.current_rate_bps == 0 {
            return u64::MAX;
        }
        // interval = (MTU_BYTES * 8) / rate_bps * 1e9
        (MTU_BYTES as u64 * 8 * 1_000_000_000) / self.current_rate_bps
    }

    /// DCQCN：收到 CNP 时更新 alpha 和速率
    fn on_cnp(&mut self, now: u64) {
        // alpha = (1 - g) * alpha + g
        // 用整数运算：alpha_new = alpha * (1 - g) + g * 1000
        let g = ALPHA_G_NUMERATOR * 1000 / ALPHA_G_DENOMINATOR;
        self.alpha = (self.alpha * (1000 - g) / 1000) + g;
        self.alpha = self.alpha.min(1000);

        // target_rate = current_rate
        self.target_rate_bps = self.current_rate_bps;

        // current_rate = current_rate * (1 - alpha / 2)
        // = current_rate * (1000 - alpha/2) / 1000
        let reduction = 1000u64.saturating_sub(self.alpha / 2);
        self.current_rate_bps = (self.current_rate_bps * reduction / 1000).max(MIN_RATE_BPS);

        self.in_fast_recovery = true;
        self.acks_since_cnp = 0;
        self.last_rate_increase_ns = now;
    }

    /// DCQCN：收到无 ECN ACK 时尝试速率恢复
    fn on_ack_no_ecn(&mut self, now: u64) {
        self.acks_since_cnp += 1;

        // 检查是否满足 RAI 间隔
        if now.saturating_sub(self.last_rate_increase_ns) < RAI_NS {
            return;
        }
        self.last_rate_increase_ns = now;

        if self.in_fast_recovery {
            // Fast Recovery 阶段：current_rate = (current_rate + target_rate) / 2
            self.current_rate_bps = (self.current_rate_bps + self.target_rate_bps) / 2;
            if self.acks_since_cnp >= RATE_DECREASE_ACKS {
                self.in_fast_recovery = false;
            }
        } else {
            // Active Increase 阶段：target_rate += step，current_rate = (current_rate + target_rate) / 2
            self.target_rate_bps = (self.target_rate_bps + HYPER_INCREASE_STEP_BPS).min(MAX_RATE_BPS);
            self.current_rate_bps = (self.current_rate_bps + self.target_rate_bps) / 2;
        }

        self.current_rate_bps = self.current_rate_bps.min(MAX_RATE_BPS);
    }
}

// ------------------------------------------------------------------
// 接收端状态
// ------------------------------------------------------------------

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: SeqNum,
    /// 记录每个 seq 是否已收到（用于乱序检测和 CNP）
    pub received: HashMap<SeqNum, bool>,
}

impl FlowRxState {
    pub fn new(flow_id: FlowId) -> Self {
        Self {
            flow_id,
            next_expected: 0,
            received: HashMap::new(),
        }
    }
}

// ------------------------------------------------------------------
// DcqcnProtocol
// ------------------------------------------------------------------

pub struct DcqcnProtocol {
    pub host_id: EntityId,
    pub tx_flows: HashMap<FlowId, FlowTxState>,
    pub rx_flows: HashMap<FlowId, FlowRxState>,
    pub stats: ProtocolStats,
    pub next_packet_id: u64,
    finished: Vec<(FlowId, u64)>,
}

impl DcqcnProtocol {
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
        self.tx_flows.insert(
            flow_id,
            FlowTxState::new(flow_id, dst, total_packets, now),
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
            let (total, dst, mut retx, interval_ns) = {
                let f = self.tx_flows.get(&fid).expect("invariant");
                (
                    f.total_packets,
                    f.dst,
                    f.retransmit_queue.clone(),
                    f.packet_interval_ns(),
                )
            };

            // 检查超时重传
            let mut timeout_seqs: Vec<SeqNum> = Vec::new();
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

            // 重传优先
            let mut sent_count = 0u32;
            while !retx.is_empty() && sent_count < INIT_CWND {
                if now < flow.next_tx_time_ns {
                    break;
                }
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

            // 发送新包（受 pacing 和流完成状态限制）
            while flow.next_seq < total && sent_count < INIT_CWND {
                if now < flow.next_tx_time_ns {
                    break;
                }
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

            if ack.ecn {
                // 收到 ECN 标记的 ACK → 触发 DCQCN 降速
                self.stats.ecn_ack_received += 1;
                flow.on_cnp(now);
            } else {
                flow.on_ack_no_ecn(now);
            }
        }

        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.stats.flows_completed += 1;
            self.finished.push((flow.flow_id, now));
        }
    }

    fn on_cnp(&mut self, cnp: &Packet, now: u64) {
        // CNP 直接触发对应流的降速
        if let Some(flow) = self.tx_flows.get_mut(&cnp.flow_id) {
            flow.on_cnp(now);
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
            // 重复包，只回 ACK
        } else if seq == next {
            flow.next_expected += 1;
            // 清理连续已收到的记录
            while flow.received.contains_key(&flow.next_expected) {
                flow.received.remove(&flow.next_expected);
                flow.next_expected += 1;
            }
        } else {
            flow.received.insert(seq, true);
        }

        let mut out = Vec::new();
        let pid_ack = self.next_packet_id;
        self.next_packet_id += 1;
        let ack = Packet::control(
            pid_ack,
            pid_ack,
            pkt.flow_id,
            flow.next_expected,
            self.host_id,
            pkt.src,
            pkt.ecn, // 将 ECN 状态回传给发送端
            0,       // control_type = 0 => ACK
            Vec::new(),
            now,
        );
        out.push(ack);

        // 如果收到 ECN 标记的数据包，立即发送 CNP
        if pkt.ecn {
            let pid_cnp = self.next_packet_id;
            self.next_packet_id += 1;
            let cnp = Packet::control(
                pid_cnp,
                pid_cnp,
                pkt.flow_id,
                seq,
                self.host_id,
                pkt.src,
                false,
                2, // control_type = 2 => CNP
                Vec::new(),
                now,
            );
            out.push(cnp);
        }

        out
    }
}

// ------------------------------------------------------------------
// Protocol trait 实现
// ------------------------------------------------------------------

impl Protocol for DcqcnProtocol {
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
            crate::network::packet::PacketKind::Control(2) => self.on_cnp(pkt, now),
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
            if f.next_seq < f.total_packets {
                return true;
            }
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
        // P3：rate-based pacing，返回所有流中最早的 next_tx_time_ns
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

    #[test]
    fn dcqcn_rate_decrease_on_cnp() {
        let mut proto = DcqcnProtocol::new(1);
        proto.start_flow(0, 2, 1024 * 100, 0);

        // 初始速率
        let flow = &proto.tx_flows[&0];
        let init_rate = flow.current_rate_bps;
        assert!(init_rate > 0);

        // 模拟收到 CNP
        let cnp = Packet::control(1000, 1000, 0, 0, 2, 1, false, 2, Vec::new(), 0);
        proto.on_cnp(&cnp, 0);

        let flow = &proto.tx_flows[&0];
        // current_rate 应该下降
        assert!(flow.current_rate_bps < init_rate, "收到 CNP 后速率应下降");
        assert!(flow.in_fast_recovery);
    }

    #[test]
    fn dcqcn_rate_recovery_after_ack() {
        let mut proto = DcqcnProtocol::new(1);
        proto.start_flow(0, 2, 1024 * 100, 0);

        // 触发一次 CNP
        let cnp = Packet::control(1000, 1000, 0, 0, 2, 1, false, 2, Vec::new(), 0);
        proto.on_cnp(&cnp, 0);
        let rate_after_cnp = proto.tx_flows[&0].current_rate_bps;

        // 模拟大量无 ECN ACK
        for i in 0..RATE_DECREASE_ACKS + 10 {
            let ack = Packet::control(
                2000 + i as u64,
                2000 + i as u64,
                0,
                i + 1,
                2,
                1,
                false,
                0,
                Vec::new(),
                (i as u64 + 1) * RAI_NS,
            );
            proto.on_ack(&ack, (i as u64 + 1) * RAI_NS);
        }

        let flow = &proto.tx_flows[&0];
        assert!(
            flow.current_rate_bps > rate_after_cnp,
            "收到足够 ACK 后速率应恢复"
        );
    }

    #[test]
    fn dcqcn_tx_pacing() {
        let mut proto = DcqcnProtocol::new(1);
        proto.start_flow(0, 2, 1024 * 100, 0);

        let pkts = proto.on_tx_tick(0);
        assert!(!pkts.is_empty());

        // 紧接着再次 tick，由于 pacing 不应该发送新包
        let pkts2 = proto.on_tx_tick(1);
        assert!(pkts2.is_empty() || pkts2.len() < pkts.len(), "pacing 应限制发送速率");
    }

    #[test]
    fn dcqcn_ecn_generates_cnp() {
        let mut proto = DcqcnProtocol::new(2);
        let data = Packet::data(0, 0, 0, 0, 1, 2, 0);
        let mut data_ecn = data.clone();
        data_ecn.ecn = true;

        let outs = proto.on_rx_data(&data_ecn, 0);
        // 应该有 ACK + CNP
        assert_eq!(outs.len(), 2);
        assert!(outs.iter().any(|p| matches!(p.kind, crate::network::packet::PacketKind::Control(2))));
    }

    #[test]
    fn dcqcn_flow_completion() {
        let mut proto = DcqcnProtocol::new(1);
        proto.start_flow(0, 2, MTU_BYTES as u64 * 5, 0);

        let pkts = proto.on_tx_tick(0);
        assert!(!pkts.is_empty());

        // 模拟收到所有 ACK
        for i in 1..=5 {
            let ack = Packet::control(
                1000 + i as u64,
                1000 + i as u64,
                0,
                i,
                2,
                1,
                false,
                0,
                Vec::new(),
                1000,
            );
            proto.on_tx_control(&ack, 1000);
        }

        assert!(proto.all_flows_done());
        let finished = proto.take_finished_flows();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].0, 0);
    }
}
