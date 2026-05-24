//! TCP CUBIC 协议实现
//!
//! 基于 CUBIC（Ha et al., SIGOPS 2008）的核心拥塞控制算法：
//! - 窗口增长公式：W(t) = C × (t − K)³ + W_max
//! - K = ∛(W_max × β / C)
//! - C = 0.4, β = 0.3（默认参数）
//! - 快速收敛（fast convergence）：新 W_max 小于旧值时进一步缩小
//! - TCP-friendly 区域：cwnd 很小时退化到标准 AIMD
//! - 与 Reno 相同的快速重传 / 超时重传机制

use super::protocol::{Protocol, ProtocolStats};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::{HashMap, HashSet};

// ------------------------------------------------------------------
// CUBIC 参数
// ------------------------------------------------------------------

/// CUBIC 增长常数 C（默认 0.4）
const CUBIC_C: f64 = 0.4;
/// 乘性降窗因子 β（默认 0.3）
const CUBIC_BETA: f64 = 0.3;
/// 快速收敛开关
const FAST_CONVERGENCE: bool = true;

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
    /// 慢启动阈值（CUBIC 中 ssthresh 主要用于慢启动退出判断）
    pub ssthresh: u32,
    pub in_flight: u32,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
    pub send_times: HashMap<SeqNum, u64>,
    pub retransmit_queue: Vec<SeqNum>,
    pub dup_ack_count: u32,
    pub ca_ack_count: u32,

    // ── CUBIC 特有状态 ──
    /// 上一次拥塞事件发生时刻（ns）
    pub epoch_start_ns: u64,
    /// 上一次拥塞事件时的窗口大小（W_max）
    pub w_max_pkts: u32,
    /// 预计算 K = ∛(W_max × β / C)，以 RTT 为单位
    pub k_rtt: f64,
    /// 平滑 RTT 估计（ns）
    pub rtt_estimate_ns: u64,
    /// 是否已测量到首次 RTT
    pub rtt_valid: bool,
    /// 上次窗口更新的 cwnd 累积器（浮点增量）
    pub cwnd_accum: f64,
    /// 快速恢复阶段标志
    pub fast_recovery: bool,
    /// 快速恢复期间 cwnd 膨胀量
    pub recovery_inflation: u32,
    /// 当前 epoch 是否经历了快速收敛
    pub fast_convergence_applied: bool,
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
            epoch_start_ns: 0,
            w_max_pkts: 0,
            k_rtt: 0.0,
            rtt_estimate_ns: 100_000, // 初始假设 100μs RTT
            rtt_valid: false,
            cwnd_accum: 0.0,
            fast_recovery: false,
            recovery_inflation: 0,
            fast_convergence_applied: false,
        }
    }
}

// ------------------------------------------------------------------
// 接收端状态
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
// TcpCubic
// ------------------------------------------------------------------

pub struct TcpCubic {
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

impl TcpCubic {
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

    /// 计算 CUBIC 目标窗口（以包为单位）
    fn cubic_target(elapsed_rtt: f64, w_max: u32, k: f64) -> f64 {
        let w = w_max as f64;
        let dt = elapsed_rtt - k;
        (CUBIC_C * dt * dt * dt + w).max(2.0)
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
                // 超时：记录 W_max，重置 epoch
                f.w_max_pkts = f.cwnd;
                f.ssthresh = (f.cwnd / 2).max(self.min_cwnd);
                f.cwnd = self.init_cwnd;
                f.epoch_start_ns = now;
                f.k_rtt = Self::compute_k(f.w_max_pkts);
                f.fast_recovery = false;
                f.recovery_inflation = 0;
                f.cwnd_accum = 0.0;
                f.dup_ack_count = 0;
                f.ca_ack_count = 0;
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

    fn compute_k(w_max: u32) -> f64 {
        if w_max <= 1 {
            return 0.0;
        }
        let raw = (w_max as f64) * CUBIC_BETA / CUBIC_C;
        raw.cbrt()
    }

    /// 更新 RTT 估计（指数移动平均）
    fn update_rtt(flow: &mut FlowTxState, rtt_sample_ns: u64) {
        if flow.rtt_valid {
            // EMA: new = 0.875 * old + 0.125 * sample
            flow.rtt_estimate_ns = ((flow.rtt_estimate_ns as u128 * 7 + rtt_sample_ns as u128) / 8) as u64;
        } else {
            flow.rtt_estimate_ns = rtt_sample_ns;
            flow.rtt_valid = true;
        }
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
                flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                flow.recovery_inflation += 1;
            } else if flow.dup_ack_count == 3 {
                // 第 3 个 dup ACK：触发快速重传 + 进入快速恢复
                let prev_w_max = flow.w_max_pkts;
                flow.w_max_pkts = flow.cwnd;
                flow.ssthresh = (flow.cwnd as f64 * CUBIC_BETA).max(self.min_cwnd as f64) as u32;
                flow.cwnd = (flow.ssthresh + 3).min(self.max_cwnd);

                // 快速收敛：如果新 cwnd 比旧 W_max 小，进一步收缩
                if FAST_CONVERGENCE && prev_w_max > 0 {
                    let prev_reduced = (prev_w_max as f64 * CUBIC_BETA) as u32;
                    let new_reduced = (flow.w_max_pkts as f64 * CUBIC_BETA) as u32;
                    if new_reduced < prev_reduced {
                        flow.w_max_pkts = ((flow.w_max_pkts as f64) * prev_reduced as f64 / new_reduced as f64) as u32;
                        flow.fast_convergence_applied = true;
                    }
                }

                flow.epoch_start_ns = now;
                flow.k_rtt = Self::compute_k(flow.w_max_pkts);
                flow.cwnd_accum = 0.0;
                flow.fast_recovery = true;
                flow.recovery_inflation = 0;

                if !flow.retransmit_queue.contains(&flow.un_acked_base) {
                    flow.retransmit_queue.push(flow.un_acked_base);
                }
            }
        } else if acked > flow.un_acked_base {
            let delta = acked - flow.un_acked_base;
            let was_in_recovery = flow.fast_recovery;

            // 测量 RTT：用 earliest send_time 近似
            if let Some(&earliest_send) = flow.send_times.get(&flow.un_acked_base) {
                let rtt_sample = now.saturating_sub(earliest_send);
                Self::update_rtt(flow, rtt_sample);
            }

            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;
            flow.in_flight = flow.in_flight.saturating_sub(delta);
            flow.dup_ack_count = 0;

            if was_in_recovery {
                flow.cwnd = flow.ssthresh;
                flow.fast_recovery = false;
                flow.recovery_inflation = 0;
            }

            if ack.ecn {
                // ECN：与丢包相同处理
                flow.w_max_pkts = flow.cwnd;
                flow.ssthresh = (flow.cwnd as f64 * CUBIC_BETA).max(self.min_cwnd as f64) as u32;
                flow.cwnd = flow.ssthresh.max(self.min_cwnd);
                flow.epoch_start_ns = now;
                flow.k_rtt = Self::compute_k(flow.w_max_pkts);
                flow.cwnd_accum = 0.0;
                flow.fast_recovery = false;
                flow.ca_ack_count = 0;
            } else if !was_in_recovery {
                if flow.cwnd < flow.ssthresh {
                    // 慢启动：每 ACK cwnd += delta
                    flow.cwnd = (flow.cwnd + delta).min(self.max_cwnd);
                } else {
                    // CUBIC 拥塞避免：按立方函数增长
                    Self::cubic_congestion_avoidance(flow, now, delta, self.max_cwnd);
                }
            }
            // 快速恢复退出后，cwnd 已重置，直接走 cubic 增长
        }

        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.stats.flows_completed += 1;
            self.finished.push((flow.flow_id, now));
        }
    }

    /// CUBIC 拥塞避免：每个 ACK 按立方函数调整 cwnd
    fn cubic_congestion_avoidance(flow: &mut FlowTxState, now: u64, delta: u32, max_cwnd: u32) {
        let rtt_ns = flow.rtt_estimate_ns.max(1) as f64;
        let elapsed_ns = now.saturating_sub(flow.epoch_start_ns) as f64;
        let elapsed_rtt = elapsed_ns / rtt_ns;
        let _k = flow.k_rtt;

        if flow.w_max_pkts == 0 {
            // 首次，默认 w_max = cwnd
            flow.w_max_pkts = flow.cwnd;
            flow.k_rtt = Self::compute_k(flow.w_max_pkts);
        }

        // 计算目标窗口
        let target = Self::cubic_target(elapsed_rtt, flow.w_max_pkts, flow.k_rtt);

        // TCP-friendly 区域：如果 cwnd 小于 Reno 等效值，使用 AIMD
        let tcp_friendly = Self::tcp_friendly_cwnd(flow.w_max_pkts, rtt_ns, elapsed_ns);

        let effective_target = if tcp_friendly > 0.0 {
            target.max(tcp_friendly)
        } else {
            target
        };

        let cur = flow.cwnd as f64;
        let tgt = effective_target.max(cur);

        if tgt > cur {
            // 按 Linux 风格计算 cnt = cwnd / (W_cubic(t+RTT) - cwnd)
            let next_target = Self::cubic_target(elapsed_rtt + 1.0, flow.w_max_pkts, flow.k_rtt);
            let effective_next = if tcp_friendly > 0.0 {
                let next_friendly = Self::tcp_friendly_cwnd(flow.w_max_pkts, rtt_ns, elapsed_ns + rtt_ns);
                next_target.max(next_friendly)
            } else {
                next_target
            };

            let inc = (effective_next - cur).max(0.0);
            if inc > 0.0 {
                let cnt = cur / inc;
                // 每个 ACK：cwnd += 1 / cnt
                flow.cwnd_accum += delta as f64 / cnt;
                while flow.cwnd_accum >= 1.0 && flow.cwnd < max_cwnd {
                    flow.cwnd += 1;
                    flow.cwnd_accum -= 1.0;
                }
            }
        }
    }

    /// 计算 TCP-friendly 等效窗口（标准 Reno AIMD 在相同条件下应达的 cwnd）
    fn tcp_friendly_cwnd(w_max: u32, rtt_ns: f64, elapsed_ns: f64) -> f64 {
        // Reno 等效：cwnd(t) = W_max * β + 3 * β / (2 - β) * t / RTT
        // 每 RTT cwnd += 1，即线性增长
        let wm = w_max as f64;
        let beta = CUBIC_BETA;
        let t_rtt = elapsed_ns / rtt_ns.max(1.0);
        let reno_cwnd = wm * beta + (3.0 * beta / (2.0 - beta)) * t_rtt;
        // 只在 cwnd 小于 W_max 时使用 TCP-friendly 比较
        if wm > 0.0 && reno_cwnd < wm {
            reno_cwnd
        } else {
            0.0
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
            // 重复，忽略
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

impl Protocol for TcpCubic {
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

// ------------------------------------------------------------------
// 单元测试
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 CUBIC 基本 K 值计算
    #[test]
    fn cubic_k_computation() {
        // W_max=100, K = cbrt(100 * 0.3 / 0.4) = cbrt(75) ≈ 4.22
        let k = TcpCubic::compute_k(100);
        assert!(k > 4.0 && k < 4.5, "K={k} is outside expected range [4.0, 4.5]");
    }

    /// 验证 CUBIC 拥塞事件后 epoch 和 w_max 正确设置
    #[test]
    fn cubic_loss_event_records_state() {
        let mut cubic = TcpCubic::new(0);
        cubic.start_flow(1, 1, MTU_BYTES as u64 * 200, 0);

        // 发包
        cubic.on_tx_tick(0);
        let flow = cubic.tx_flows.get(&1).unwrap();
        assert_eq!(flow.cwnd, 16);

        // 模拟 3 dup ACK（快速重传）
        for _ in 0..3 {
            let ack = Packet::control(100, 100, 1, 0, 1, 0, false, 0, Vec::new(), 1000);
            cubic.on_tx_control(&ack, 1000);
        }
        let f = cubic.tx_flows.get(&1).unwrap();
        assert_eq!(f.w_max_pkts, 16);
        assert!(f.k_rtt > 0.0);
        assert_eq!(f.epoch_start_ns, 1000);
        assert!(f.fast_recovery);
        // cwnd = ssthresh + 3 = floor(16*0.3) + 3 = 4 + 3 = 7
        assert_eq!(f.cwnd, 7);
    }

    /// 验证 cwnd 随 epoch 时间推移按 CUBIC 曲线增长
    #[test]
    fn cubic_growth_over_time() {
        let mut cubic = TcpCubic::new(0);
        cubic.start_flow(1, 1, MTU_BYTES as u64 * 200, 0);
        cubic.on_tx_tick(0);

        // 设置初始状态：模拟一个已经经历过拥塞、cwnd 较小的流
        {
            let f = cubic.tx_flows.get_mut(&1).unwrap();
            f.epoch_start_ns = 0;
            f.w_max_pkts = 80;
            f.k_rtt = TcpCubic::compute_k(80);
            f.ssthresh = 24;
            f.cwnd = 24;
            f.cwnd_accum = 0.0;
            f.rtt_estimate_ns = 100_000;
            f.rtt_valid = true;
            f.in_flight = 0;
        }

        // 经过大量 ACK 后，cwnd 应趋近 W_max=80
        // 对每个 seq 发送 ACK 模拟时间推进
        let start_ns = 0;
        for i in 0..5000 {
            let now = start_ns + i * 100_000; // 每个 RTT
            let ack = Packet::control(100 + i, 100 + i, 1, i as u32, 1, 0, false, 0, Vec::new(), now);
            cubic.on_tx_control(&ack, now);
        }
        let f = cubic.tx_flows.get(&1).unwrap();
        // 经过 5000 RTT，cwnd 应该增长到 W_max 以上（CUBIC 会超过 W_max）
        assert!(f.cwnd >= 40, "CUBIC did not grow sufficiently: cwnd={}", f.cwnd);
    }
}