//! RDMA 协议栈实现
//!
//! 基于 InfiniBand/RoCEv2 语义的完整 RDMA 传输协议：
//! - QP 生命周期管理（创建 → INIT → RTR → RTS）
//! - 消息分段：将 message 拆分为 MTU 大小的 RDMA 包，打上 First/Middle/Last/Solo 标记
//! - 消息重组：接收端按 PSN 累积包，最后一包到达后提交 CQE
//! - WQE/CQE：post_send → doorbell → 分段发包 → 全部 ACK → completion
//! - RNR：接收端无 recv WQE 时回 RNR NAK
//! - 拥塞控制：继承现有的 cwnd + ECN 降窗机制
//!
//! 与 STrack/TCP 协议兼容同一个 SimRunner 框架。

use super::protocol::{Protocol, ProtocolStats};
use super::rdma::{MsgBoundary, Psn, QpState, Qpn, QueuePair, RdmaOpcode, Wqe};
use crate::network::packet::{FlowId, Packet, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::HashMap;

// ── 发送端流状态 ──

struct FlowTxState {
    flow_id: FlowId,
    dst: EntityId,
    qpn: Qpn,
    total_packets: u32,
    cwnd: u32,
    ssthresh: u32,
    in_flight: u32,
    next_seq: SeqNum,
    un_acked_base: SeqNum,
    pub send_times: HashMap<SeqNum, u64>,
    pub retransmit_queue: Vec<SeqNum>,
    pub dup_ack_count: u32,
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,
}

impl FlowTxState {
    fn new(flow_id: FlowId, dst: EntityId, qpn: Qpn, total_packets: u32, start_time: u64) -> Self {
        Self {
            flow_id,
            dst,
            qpn,
            total_packets,
            cwnd: 16,
            ssthresh: 64,
            in_flight: 0,
            next_seq: 0,
            un_acked_base: 0,
            send_times: HashMap::new(),
            retransmit_queue: Vec::new(),
            dup_ack_count: 0,
            done: false,
            start_time,
            finish_time: 0,
        }
    }
}

// ── 接收端重组状态 ──

struct RxReassembly {
    /// 已累积的包（PSN → packet payload）
    packets: HashMap<Psn, Vec<u8>>,
    /// 期望的 first PSN
    first_psn: Option<Psn>,
    /// 总共期望多少个包（从 first 到 last）
    expected_count: u32,
    /// 已接收数
    received_count: u32,
    /// 消息总字节数
    total_bytes: u64,
}

impl RxReassembly {
    fn new() -> Self {
        Self {
            packets: HashMap::new(),
            first_psn: None,
            expected_count: 0,
            received_count: 0,
            total_bytes: 0,
        }
    }
}

// ── 待发送消息 ──

struct PendingMessage {
    msg_id: u64,
    qpn: Qpn,
    dst: EntityId,
    opcode: RdmaOpcode,
    total_bytes: u64,
    total_packets: u32,
    next_packet_idx: u32,
    base_psn: Psn,
    posted_ns: u64,
    /// RNR 重试：下次允许重试的时刻（ns）
    pub rnr_retry_at_ns: u64,
    /// RNR 重试次数
    pub rnr_retries: u32,
}

// ── RdmaProtocol ──

pub struct RdmaProtocol {
    pub host_id: EntityId,
    /// QP 表（本地 QPN → QueuePair）
    pub qps: HashMap<Qpn, QueuePair>,
    /// 活跃的发送流（flow_id → FlowTxState，复用现有的 CC 框架）
    pub tx_flows: HashMap<FlowId, FlowTxState>,
    /// 接收端重组（qpn → RxReassembly）
    pub rx_reassembly: HashMap<Qpn, RxReassembly>,
    /// 待发送的消息队列
    pub pending_messages: Vec<PendingMessage>,
    pub stats: ProtocolStats,
    pub next_packet_id: u64,
    pub next_qpn: Qpn,
    pub next_msg_id: u64,
    pub init_cwnd: u32,
    pub max_cwnd: u32,
    pub min_cwnd: u32,
    pub rto_ns: u64,
    finished_flows: Vec<(FlowId, u64)>,
    finished_messages: Vec<(u64, Qpn, u64)>,
}

impl RdmaProtocol {
    pub fn new(host_id: EntityId) -> Self {
        Self {
            host_id,
            qps: HashMap::new(),
            tx_flows: HashMap::new(),
            rx_reassembly: HashMap::new(),
            pending_messages: Vec::new(),
            stats: ProtocolStats::default(),
            next_packet_id: 1,
            next_qpn: 1,
            next_msg_id: 0,
            init_cwnd: 16,
            max_cwnd: 256,
            min_cwnd: 1,
            rto_ns: 100_000,
            finished_flows: Vec::new(),
            finished_messages: Vec::new(),
        }
    }

    /// 创建 QP 并设置到 RTS 状态
    pub fn create_qp(&mut self, remote_id: EntityId, now: u64) -> Qpn {
        let qpn = self.next_qpn;
        self.next_qpn += 1;
        let remote_qpn = qpn; // 简化：本地和远端 QPN 相同
        let mut qp = QueuePair::new(qpn, remote_qpn, remote_id, now);
        qp.transition(QpState::Init);
        qp.transition(QpState::Rtr);
        qp.transition(QpState::Rts);
        self.qps.insert(qpn, qp);
        qpn
    }

    // ── 消息分段（关联函数，避免 self 借用冲突）──
    fn segment_message(msg: &PendingMessage) -> Vec<(SeqNum, u8)> {
        let total = msg.total_packets;
        let mut segs = Vec::with_capacity(total as usize);

        for i in 0..total {
            let flags = if total == 1 {
                MsgBoundary::Solo
            } else if i == 0 {
                MsgBoundary::First
            } else if i == total - 1 {
                MsgBoundary::Last
            } else {
                MsgBoundary::Middle
            };
            segs.push((msg.base_psn + i, flags.to_flags()));
        }
        segs
    }

    // ── 收包重组 ──

    fn handle_rdma_data(&mut self, pkt: &Packet, now: u64) {
        let qpn = pkt.qpn;
        let psn = pkt.psn;
        let flags = MsgBoundary::from_flags(pkt.msg_flags);
        let _opcode = pkt.rdma_opcode;

        let reassembly = self
            .rx_reassembly
            .entry(qpn)
            .or_insert_with(RxReassembly::new);

        match flags {
            MsgBoundary::Solo => {
                // 单包消息：直接完成
                reassembly.total_bytes = pkt.size as u64;
                self.finished_messages.push((self.next_msg_id, qpn, now));
                self.next_msg_id += 1;
                self.stats.messages_completed += 1;
                self.stats.packets_received += 1;
            }
            MsgBoundary::First => {
                reassembly.first_psn = Some(psn);
                reassembly.packets.clear();
                reassembly.received_count = 1;
                reassembly.total_bytes = pkt.size as u64;
                reassembly.packets.insert(psn, pkt.payload.clone());
                self.stats.packets_received += 1;
            }
            MsgBoundary::Middle => {
                reassembly.packets.insert(psn, pkt.payload.clone());
                reassembly.received_count += 1;
                reassembly.total_bytes += pkt.size as u64;
                self.stats.packets_received += 1;
            }
            MsgBoundary::Last => {
                reassembly.packets.insert(psn, pkt.payload.clone());
                reassembly.received_count += 1;
                reassembly.total_bytes += pkt.size as u64;
                self.stats.packets_received += 1;

                // 消息完成
                self.finished_messages.push((self.next_msg_id, qpn, now));
                self.next_msg_id += 1;
                self.stats.messages_completed += 1;
                self.rx_reassembly.remove(&qpn);
            }
        }
    }

    // ── 发包 ──

    fn try_send(&mut self, now: u64) -> Vec<Packet> {
        let mut to_send = Vec::new();

        // 先处理已有活跃流（cwnd-based 持续发送）
        let flow_ids: Vec<FlowId> = self
            .tx_flows
            .iter()
            .filter(|(_, f)| !f.done)
            .map(|(k, _)| *k)
            .collect();

        for fid in flow_ids {
            let (cwnd, in_flight, mut next_seq, dst, qpn, mut retx) = {
                let f = self.tx_flows.get(&fid).expect("invariant");
                (
                    f.cwnd,
                    f.in_flight,
                    f.next_seq,
                    f.dst,
                    f.qpn,
                    f.retransmit_queue.clone(),
                )
            };

            // 超时检查
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
            }
            for s in timeout_seqs {
                retx.push(s);
            }

            let mut new_inflight = in_flight;
            let mut send_records: Vec<(SeqNum, u64)> = Vec::new();

            // 重传
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
                if let Some(qp) = self.qps.get_mut(&qpn) {
                    qp.retransmissions += 1;
                }
            }

            // 新包
            while new_inflight < cwnd {
                // 找到下一个消息（跳过 RNR 退避中的消息）
                let msg_idx = self
                    .pending_messages
                    .iter()
                    .position(|m| m.qpn == qpn && !m.done() && now >= m.rnr_retry_at_ns);

                let Some(mi) = msg_idx else {
                    break;
                };

                let msg = &mut self.pending_messages[mi];
                let segs = Self::segment_message(msg);
                if next_seq >= segs.len() as u32 {
                    // 当前消息已发完
                    continue;
                }
                // 发当前消息的下一个包
                let (psn_val, flags) = segs[next_seq as usize];
                let pid = self.next_packet_id;
                self.next_packet_id += 1;
                let mut pkt = Packet::rdma_data(
                    pid,
                    pid,
                    qpn,
                    psn_val,
                    flags,
                    msg.opcode as u8,
                    self.host_id,
                    dst,
                    now,
                );
                pkt.flow_id = fid;
                pkt.seq = next_seq;
                to_send.push(pkt);
                self.stats.packets_sent += 1;
                new_inflight += 1;
                send_records.push((next_seq, now));
                msg.next_packet_idx += 1;
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
        let qpn = {
            let Some(flow) = self.tx_flows.get_mut(&ack.flow_id) else {
                return;
            };
            let acked = ack.seq;

            if acked == flow.un_acked_base {
                flow.dup_ack_count += 1;
                if flow.dup_ack_count == 3 {
                    flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                    flow.cwnd = (flow.ssthresh + 3).min(self.max_cwnd);
                    if !flow.retransmit_queue.contains(&flow.un_acked_base) {
                        flow.retransmit_queue.push(flow.un_acked_base);
                    }
                } else if flow.dup_ack_count > 3 {
                    flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
                }
            } else if acked > flow.un_acked_base {
                let delta = acked - flow.un_acked_base;
                for s in flow.un_acked_base..acked {
                    flow.send_times.remove(&s);
                }
                flow.un_acked_base = acked;
                flow.in_flight = flow.in_flight.saturating_sub(delta);
                flow.dup_ack_count = 0;

                if ack.ecn {
                    flow.ssthresh = (flow.cwnd / 2).max(self.min_cwnd);
                    flow.cwnd = flow.ssthresh.max(self.min_cwnd);
                } else if flow.cwnd < flow.ssthresh {
                    flow.cwnd = (flow.cwnd + delta).min(self.max_cwnd);
                } else {
                    if flow.cwnd < self.max_cwnd {
                        flow.cwnd += 1;
                    }
                }
            }
            flow.qpn
        }; // flow borrow released here

        // 检查 flow 是否所有包都已 ACK
        let flow_finished = if let Some(flow) = self.tx_flows.get_mut(&ack.flow_id) {
            if flow.un_acked_base >= flow.total_packets && !flow.done {
                flow.done = true;
                flow.finish_time = now;
                self.stats.flows_completed += 1;
                self.finished_flows.push((flow.flow_id, now));
                true
            } else {
                false
            }
        } else {
            false
        };

        if flow_finished {
            self.check_message_completion(qpn, now);
        }
    }

    fn check_message_completion(&mut self, qpn: Qpn, now: u64) {
        let qp_flows: Vec<FlowId> = self
            .tx_flows
            .iter()
            .filter(|(_, f)| f.qpn == qpn && !f.done)
            .map(|(k, _)| *k)
            .collect();

        if qp_flows.is_empty() {
            // 该 QP 所有流都已完成 → 检查消息是否也全部完成
            let mut completed_msg_ids = Vec::new();
            for (i, msg) in self.pending_messages.iter().enumerate() {
                if msg.qpn == qpn && msg.done() {
                    completed_msg_ids.push((i, msg.msg_id));
                }
            }
            for (idx, msg_id) in completed_msg_ids.iter().rev() {
                self.finished_messages.push((*msg_id, qpn, now));
                self.stats.messages_completed += 1;
                self.pending_messages.remove(*idx);
            }
        }

        // 标记已完成的流
        for fid in qp_flows {
            if let Some(f) = self.tx_flows.get(&fid) {
                if !f.done {
                    return;
                } // 还有活跃流，等全部完成
            }
        }
    }
}

impl PendingMessage {
    fn done(&self) -> bool {
        self.next_packet_idx >= self.total_packets
    }

    /// RNR 退避时间（ns）：指数退避，初始 100μs，最大 100ms
    fn rnr_backoff_ns(retries: u32) -> u64 {
        let base = 100_000u64; // 100μs
        let max = 100_000_000; // 100ms
        base.saturating_mul(1u64 << retries.min(10)).min(max)
    }
}

impl Protocol for RdmaProtocol {
    fn start_flow(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64) {
        let qpn = self.create_qp(dst, now);
        let total_packets = ((total_bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        // 默认使用 post_write 以避免 RNR（Send/RNR 语义由单元测试覆盖）
        self.post_write(qpn, total_bytes, now);
        self.tx_flows.insert(
            flow_id,
            FlowTxState::new(flow_id, dst, qpn, total_packets, now),
        );
    }

    fn on_tx_tick(&mut self, now: u64) -> Vec<Packet> {
        self.try_send(now)
    }

    fn on_rx_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
        self.stats.packets_received += 1;

        let mut out = Vec::new();

        if pkt.qpn > 0 {
            // RDMA 包：检查是否需要 RNR NAK
            let flags = MsgBoundary::from_flags(pkt.msg_flags);
            let needs_rnr = matches!(flags, MsgBoundary::Solo | MsgBoundary::Last)
                && pkt.rdma_opcode == RdmaOpcode::Send as u8;

            if needs_rnr {
                let has_recv = self
                    .qps
                    .get(&pkt.qpn)
                    .map(|q| q.has_recv_wqe())
                    .unwrap_or(true);
                if !has_recv {
                    // 回 RNR NAK（设置 QPN 以便发送端匹配到正确的 pending message）
                    let pid = self.next_packet_id;
                    self.next_packet_id += 1;
                    let mut rnr_pkt = Packet::control(
                        pid,
                        pid,
                        pkt.flow_id,
                        pkt.seq,
                        self.host_id,
                        pkt.src,
                        false,
                        1, // control_type=1 = RNR NAK
                        Vec::new(),
                        now,
                    );
                    rnr_pkt.qpn = pkt.qpn;
                    out.push(rnr_pkt);
                    self.stats.rnr_naks_sent += 1;
                    if let Some(qp) = self.qps.get_mut(&pkt.qpn) {
                        qp.rnr_naks_sent += 1;
                    }
                    // 丢弃重组中的消息
                    self.rx_reassembly.remove(&pkt.qpn);
                    return out;
                }
            }

            // 正常处理 RDMA 数据
            self.handle_rdma_data(pkt, now);
        }

        // 回 ACK
        let pid = self.next_packet_id;
        self.next_packet_id += 1;
        let ack = Packet::control(
            pid,
            pid,
            pkt.flow_id,
            pkt.seq + 1,
            self.host_id,
            pkt.src,
            pkt.ecn,
            0,
            Vec::new(),
            now,
        );
        out.push(ack);
        out
    }

    fn on_tx_control(&mut self, pkt: &Packet, now: u64) {
        match pkt.kind {
            crate::network::packet::PacketKind::Control(0) => self.on_ack(pkt, now),
            crate::network::packet::PacketKind::Control(1) => {
                // RNR NAK：找到对应 QP 的 pending message，设置退避重试
                self.stats.rnr_naks_received += 1;
                if let Some(qp) = self.qps.get_mut(&pkt.qpn) {
                    qp.rnr_naks_received += 1;
                }
                // 标记所有该 QP 的 pending message 需要重试
                for msg in &mut self.pending_messages {
                    if msg.qpn == pkt.qpn && !msg.done() {
                        let backoff = PendingMessage::rnr_backoff_ns(msg.rnr_retries);
                        msg.rnr_retries += 1;
                        msg.rnr_retry_at_ns = now.saturating_add(backoff);
                        // 重置发包指针，从头重发
                        msg.next_packet_idx = 0;
                    }
                }
            }
            _ => {}
        }
    }

    fn all_flows_done(&self) -> bool {
        !self.tx_flows.is_empty()
            && self.tx_flows.values().all(|f| f.done)
            && self.pending_messages.iter().all(|m| m.done())
    }

    fn take_finished_flows(&mut self) -> Vec<(FlowId, u64)> {
        std::mem::take(&mut self.finished_flows)
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
            if f.in_flight < f.cwnd && self.pending_messages.iter().any(|m| !m.done()) {
                return true;
            }
        }
        false
    }

    fn next_rto_deadline(&self) -> Option<u64> {
        let mut min_d: Option<u64> = None;
        for f in self.tx_flows.values() {
            if f.done {
                continue;
            }
            for &send_t in f.send_times.values() {
                let d = send_t.saturating_add(self.rto_ns);
                min_d = Some(match min_d {
                    Some(c) => c.min(d),
                    None => d,
                });
            }
        }
        min_d
    }

    // ── RDMA 扩展 ──

    fn post_send(&mut self, qpn: Qpn, bytes: u64, now: u64) {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        let Some(qp) = self.qps.get_mut(&qpn) else {
            return;
        };
        let total_packets = ((bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        let base_psn = qp.alloc_psn();
        // skip base_psn by total_packets
        for _ in 1..total_packets {
            qp.alloc_psn();
        }
        self.pending_messages.push(PendingMessage {
            msg_id,
            qpn,
            dst: qp.remote_id,
            opcode: RdmaOpcode::Send,
            total_bytes: bytes,
            total_packets,
            next_packet_idx: 0,
            base_psn,
            posted_ns: now,
            rnr_retry_at_ns: 0,
            rnr_retries: 0,
        });
    }

    fn post_write(&mut self, qpn: Qpn, bytes: u64, now: u64) {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        let Some(qp) = self.qps.get_mut(&qpn) else {
            return;
        };
        let total_packets = ((bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        let base_psn = qp.alloc_psn();
        for _ in 1..total_packets {
            qp.alloc_psn();
        }
        self.pending_messages.push(PendingMessage {
            msg_id,
            qpn,
            dst: qp.remote_id,
            opcode: RdmaOpcode::Write,
            total_bytes: bytes,
            total_packets,
            next_packet_idx: 0,
            base_psn,
            posted_ns: now,
            rnr_retry_at_ns: 0,
            rnr_retries: 0,
        });
    }

    fn post_recv(&mut self, qpn: Qpn, bytes: u64, _now: u64) {
        if let Some(qp) = self.qps.get_mut(&qpn) {
            qp.recv_queue.push(Wqe {
                qpn,
                opcode: RdmaOpcode::Send,
                remote_id: qp.remote_id,
                remote_addr: 0,
                local_addr: 0,
                length: bytes,
                posted_ns: _now,
                doorbell_rung: false,
            });
        }
    }

    fn qp_state(&self, qpn: Qpn) -> Option<QpState> {
        self.qps.get(&qpn).map(|q| q.state)
    }

    fn take_finished_messages(&mut self) -> Vec<(u64, Qpn, u64)> {
        std::mem::take(&mut self.finished_messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::PacketKind;

    #[test]
    fn rdma_qp_lifecycle() {
        let mut proto = RdmaProtocol::new(0);
        let qpn = proto.create_qp(1, 0);
        assert_eq!(proto.qp_state(qpn), Some(QpState::Rts));
        assert!(proto.qps[&qpn].state.can_send());
    }

    #[test]
    fn rdma_post_send_creates_pending_message() {
        let mut proto = RdmaProtocol::new(0);
        let qpn = proto.create_qp(1, 0);
        proto.post_send(qpn, MTU_BYTES as u64 * 4, 0);
        assert_eq!(proto.pending_messages.len(), 1);
        let msg = &proto.pending_messages[0];
        assert_eq!(msg.total_packets, 4);
    }

    #[test]
    fn rdma_segmentation_solo_message() {
        let proto = RdmaProtocol::new(0);
        let msg = PendingMessage {
            msg_id: 0,
            qpn: 0,
            dst: 1,
            opcode: RdmaOpcode::Send,
            total_bytes: MTU_BYTES as u64,
            total_packets: 1,
            next_packet_idx: 0,
            base_psn: 0,
            posted_ns: 0,
            rnr_retry_at_ns: 0,
            rnr_retries: 0,
        };
        let segs = RdmaProtocol::segment_message(&msg);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].1, MsgBoundary::Solo.to_flags());
    }

    #[test]
    fn rdma_segmentation_multi_packet() {
        let proto = RdmaProtocol::new(0);
        let msg = PendingMessage {
            msg_id: 0,
            qpn: 0,
            dst: 1,
            opcode: RdmaOpcode::Send,
            total_bytes: MTU_BYTES as u64 * 3,
            total_packets: 3,
            next_packet_idx: 0,
            base_psn: 10,
            posted_ns: 0,
            rnr_retry_at_ns: 0,
            rnr_retries: 0,
        };
        let segs = RdmaProtocol::segment_message(&msg);
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0].1, MsgBoundary::First.to_flags());
        assert_eq!(segs[0].0, 10);
        assert_eq!(segs[1].1, MsgBoundary::Middle.to_flags());
        assert_eq!(segs[1].0, 11);
        assert_eq!(segs[2].1, MsgBoundary::Last.to_flags());
        assert_eq!(segs[2].0, 12);
    }

    #[test]
    fn rdma_reassembly_single_message() {
        let mut proto = RdmaProtocol::new(0);
        let qpn = proto.create_qp(1, 0);
        let pkt = Packet::rdma_data(
            1,
            1,
            qpn,
            0,
            MsgBoundary::Solo.to_flags(),
            RdmaOpcode::Send as u8,
            0,
            1,
            1000,
        );
        proto.handle_rdma_data(&pkt, 1000);
        assert_eq!(proto.stats.messages_completed, 1);
    }

    #[test]
    fn qpn_starts_at_one() {
        // QPN = 0 保留为非 RDMA sentinel，第一个 QP 从 1 开始
        let mut proto = RdmaProtocol::new(0);
        let qpn = proto.create_qp(1, 0);
        assert_eq!(qpn, 1, "第一条 QP 应从 1 开始，0 保留为非 RDMA sentinel");
    }

    #[test]
    fn qpn_one_rdma_packet_handled_as_rdma() {
        // QPN=1 的 RDMA 包应走 RDMA 分支（而非被 pkt.qpn > 0 检查跳过）
        let mut proto = RdmaProtocol::new(0);
        let qpn = proto.create_qp(1, 0);
        // QPN 从 1 开始，create_qp 后 qpn == 1
        let pkt = Packet::rdma_data(
            1,
            1,
            qpn,
            0,
            MsgBoundary::Solo.to_flags(),
            RdmaOpcode::Write as u8,
            0,
            1,
            1000,
        );
        // on_rx_data 路由到 handle_rdma_data，message 应被完成
        let acks = proto.on_rx_data(&pkt, 1000);
        assert!(!acks.is_empty(), "应回 ACK");
        assert_eq!(
            proto.stats.messages_completed, 1,
            "QPN=1 的 Solo 消息应被完成"
        );
    }

    #[test]
    fn rnr_nak_on_send_without_recv_wqe() {
        // 接收端没有 recv WQE 时，RDMA Send 的 Last 包应触发 RNR NAK
        let mut rx = RdmaProtocol::new(1);
        let qpn = rx.create_qp(0, 0); // receiver 创建 QP（但未 post_recv）
        assert!(qpn >= 1);

        // 模拟收到一个多包 Send 的 Last 包（前两包 First/Middle 已到）
        // 先收 First
        let pkt_first = Packet::rdma_data(
            1,
            1,
            qpn,
            10,
            MsgBoundary::First.to_flags(),
            RdmaOpcode::Send as u8,
            0,
            1,
            1000,
        );
        rx.on_rx_data(&pkt_first, 1000);

        // 再收 Last — 应触发 RNR NAK
        let pkt_last = Packet::rdma_data(
            2,
            2,
            qpn,
            11,
            MsgBoundary::Last.to_flags(),
            RdmaOpcode::Send as u8,
            0,
            1,
            2000,
        );
        let ret = rx.on_rx_data(&pkt_last, 2000);
        assert_eq!(ret.len(), 1, "应返回 1 个 RNR NAK");
        assert_eq!(
            ret[0].kind,
            PacketKind::Control(1),
            "control_type=1 是 RNR NAK"
        );
        assert!(rx.stats.rnr_naks_sent > 0);
        assert!(rx.rx_reassembly.get(&qpn).is_none(), "RNR 后重组状态被清除");
    }

    #[test]
    fn rnr_nak_sets_retry_backoff() {
        // 发送端收到 RNR NAK 后，对应的 pending message 应设置退避时间
        let mut tx = RdmaProtocol::new(0);
        let qpn = tx.create_qp(1, 0);
        tx.post_send(qpn, MTU_BYTES as u64 * 4, 0);
        assert_eq!(tx.pending_messages.len(), 1);

        // 模拟收到一个 RNR NAK（control_type=1），设置正确的 QPN
        let mut rnr_pkt = Packet::control(100, 100, qpn, 0, 1, 0, false, 1, Vec::new(), 5000);
        rnr_pkt.qpn = qpn;
        tx.on_tx_control(&rnr_pkt, 5000);

        let msg = &tx.pending_messages[0];
        assert!(msg.rnr_retries > 0, "RNR 重试计数应增加");
        assert!(msg.rnr_retry_at_ns > 5000, "退避时间应在当前时刻之后");
        assert_eq!(msg.next_packet_idx, 0, "包指针应重置");
        assert!(tx.stats.rnr_naks_received > 0);
    }

    #[test]
    fn pending_message_done_after_all_packets_sent() {
        let mut msg = PendingMessage {
            msg_id: 0,
            qpn: 1,
            dst: 2,
            opcode: RdmaOpcode::Write,
            total_bytes: MTU_BYTES as u64 * 3,
            total_packets: 3,
            next_packet_idx: 0,
            base_psn: 0,
            posted_ns: 0,
            rnr_retry_at_ns: 0,
            rnr_retries: 0,
        };
        assert!(!msg.done());
        msg.next_packet_idx = 2;
        assert!(!msg.done());
        msg.next_packet_idx = 3;
        assert!(msg.done());
    }

    #[test]
    fn flow_completed_on_all_acked() {
        // 通过 on_ack 推进 un_acked_base，验证 flow 完成
        let mut proto = RdmaProtocol::new(0);
        proto.start_flow(0, 1, MTU_BYTES as u64 * 2, 0);
        assert_eq!(proto.tx_flows.len(), 1);
        assert!(!proto.tx_flows[&0].done);

        // 模拟所有包被 ACK（seq=2 表示 seq 0 和 1 都已确认）
        let ack = Packet::control(100, 100, 0, 2, 1, 0, false, 0, Vec::new(), 5000);
        proto.on_tx_control(&ack, 5000);

        let flow = &proto.tx_flows[&0];
        assert!(flow.done, "所有包 ACK 后 flow 应完成");
        assert!(flow.finish_time > 0);

        let finished = proto.take_finished_flows();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].0, 0);
    }
}
