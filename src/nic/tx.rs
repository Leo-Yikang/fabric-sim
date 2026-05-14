//! 发送端 NIC
//!
//! 维护：
//! - 流的当前状态（已发送、已 ACK、CWND、可用路径）
//! - Packet Spraying：从可用路径列表中轮询/随机选择
//! - CC：解析 ACK/NACK，处理 ECN（先切路，再降窗）
//! - SACK：根据 NACK 里的位图选择性重传

use super::cc::{CongestionMode, PathState};
use crate::network::packet::{Packet, FlowId, SeqNum, MTU_BYTES};
use crate::EntityId;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Copy)]
pub struct TxStats {
    pub packets_sent: u64,
    pub packets_retransmitted: u64,
    pub ecn_ack_received: u64,
    pub nack_received: u64,
    pub flows_completed: u64,
}

/// 单条流的发送状态
pub struct FlowTxState {
    pub flow_id: FlowId,
    pub dst: EntityId,
    pub total_packets: u32,
    pub next_seq: SeqNum,         // 下一个要发的 seq
    pub un_acked_base: SeqNum,    // 最小的未 cumulative ACK 的 seq
    pub cwnd: u32,
    pub in_flight: u32,           // 当前已发出未 ACK 的包数
    pub done: bool,
    pub start_time: u64,
    pub finish_time: u64,         // 0 表示未完成
    /// 已知丢失的 seq 列表（来自 NACK）→ 优先重传
    pub retransmit_queue: Vec<SeqNum>,
    /// 每个 in_flight 包的发出时间 (seq -> send_time)
    pub send_times: std::collections::HashMap<SeqNum, u64>,
    /// 上次 ACK 收到的时刻
    pub last_ack_time: u64,
}

impl FlowTxState {
    pub fn new(flow_id: FlowId, dst: EntityId, total_packets: u32, init_cwnd: u32, start_time: u64) -> Self {
        Self {
            flow_id, dst, total_packets,
            next_seq: 0, un_acked_base: 0, cwnd: init_cwnd, in_flight: 0,
            done: false, start_time, finish_time: 0, retransmit_queue: Vec::new(),
            send_times: std::collections::HashMap::new(),
            last_ack_time: 0,
        }
    }
}

pub struct TxNic {
    pub host_id: EntityId,
    pub mode: CongestionMode,
    pub paths: Vec<PathState>,    // 在 STrack 模式下为所有可用上行端口
    pub paths_rr_cursor: u8,
    pub flows: HashMap<FlowId, FlowTxState>,
    pub stats: TxStats,
    pub next_packet_id: u64,
    pub init_cwnd: u32,
    pub max_cwnd: u32,
    pub min_cwnd: u32,
    pub blacklist_duration_ns: u64,
    /// 重传超时（ns）——超过该时间未 ACK 的包加入重传队列
    pub rto_ns: u64,
}

impl TxNic {
    pub fn new(host_id: EntityId, mode: CongestionMode, n_paths: u8) -> Self {
        let paths = (0..n_paths).map(PathState::new).collect();
        Self {
            host_id, mode, paths, paths_rr_cursor: 0,
            flows: HashMap::new(),
            stats: TxStats::default(),
            next_packet_id: 1,
            init_cwnd: 16,
            max_cwnd: 256,
            min_cwnd: 1,
            blacklist_duration_ns: 50_000, // 50us
            rto_ns: 100_000,                // 100us 超时
        }
    }

    /// 启动一条流
    pub fn start_flow(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64) {
        let total_packets = ((total_bytes + MTU_BYTES as u64 - 1) / MTU_BYTES as u64) as u32;
        self.flows.insert(flow_id, FlowTxState::new(flow_id, dst, total_packets, self.init_cwnd, now));
    }

    /// 在 now 时刻尝试发送：返回需要被注入网络的数据包列表
    /// 上层（仿真主循环）负责把这些包变为 PacketDepart 事件
    pub fn try_send(&mut self, now: u64) -> Vec<Packet> {
        let mut to_send = Vec::new();
        // 收集所有未完成流的 ID（避免借用冲突）
        let flow_ids: Vec<FlowId> = self.flows.iter().filter(|(_, f)| !f.done).map(|(k, _)| *k).collect();
        for fid in flow_ids {
            // 取出 flow 的副本字段做计算
            let (cwnd, in_flight, mut next_seq, total, dst, mut retx) = {
                let f = self.flows.get(&fid).unwrap();
                (f.cwnd, f.in_flight, f.next_seq, f.total_packets, f.dst, f.retransmit_queue.clone())
            };

            // 检查超时重传：in_flight 中 send_time 超过 rto_ns 的包
            let mut timeout_seqs: Vec<SeqNum> = Vec::new();
            {
                let f = self.flows.get(&fid).unwrap();
                for (seq, send_t) in &f.send_times {
                    if now.saturating_sub(*send_t) > self.rto_ns && !retx.contains(seq) {
                        timeout_seqs.push(*seq);
                    }
                }
            }
            for s in timeout_seqs { retx.push(s); }

            let mut new_inflight = in_flight;
            let mut send_records: Vec<(SeqNum, u64)> = Vec::new();
            // 先重传（不受 cwnd 限制，因为重传是存量补发）
            // 限制重传批量不超过 cwnd，避免雪崩
            let mut retx_budget = cwnd as usize;
            while retx_budget > 0 && !retx.is_empty() {
                let seq = retx.remove(0);
                let path = self.pick_path(now);
                if path.is_none() { break; }
                let pid = self.next_packet_id; self.next_packet_id += 1;
                let mut pkt = Packet::data(pid, fid, seq, self.host_id, dst, now);
                pkt.path_hint = path.unwrap() + 1;
                to_send.push(pkt);
                self.stats.packets_retransmitted += 1;
                self.stats.packets_sent += 1;
                send_records.push((seq, now));
                retx_budget -= 1;
            }
            // 再发新包
            while new_inflight < cwnd && next_seq < total {
                let path = self.pick_path(now);
                if path.is_none() { break; }
                let pid = self.next_packet_id; self.next_packet_id += 1;
                let mut pkt = Packet::data(pid, fid, next_seq, self.host_id, dst, now);
                pkt.path_hint = path.unwrap() + 1;
                to_send.push(pkt);
                self.stats.packets_sent += 1;
                new_inflight += 1;
                send_records.push((next_seq, now));
                next_seq += 1;
            }
            // 写回
            let f = self.flows.get_mut(&fid).unwrap();
            f.in_flight = new_inflight;
            f.next_seq = next_seq;
            f.retransmit_queue = retx;
            for (seq, t) in send_records {
                f.send_times.insert(seq, t);
            }
        }
        to_send
    }

    /// 选择下一条可用路径
    fn pick_path(&mut self, now: u64) -> Option<u8> {
        if self.paths.is_empty() { return None; }
        match self.mode {
            CongestionMode::Ecmp => {
                // ECMP 模式下，所有流共享单一"流哈希"，这里简化为始终走 path 0
                // （真实交换机的 ECMP 哈希依据 5-tuple；我们在 switch 里靠 hash_key 已经体现）
                Some(0)
            }
            CongestionMode::Strack => {
                // 轮询找下一条可用路径
                let n = self.paths.len() as u8;
                for _ in 0..n {
                    let idx = self.paths_rr_cursor;
                    self.paths_rr_cursor = (self.paths_rr_cursor + 1) % n;
                    if self.paths[idx as usize].is_available(now) {
                        return Some(idx);
                    }
                }
                // 全部黑名单：取第一个（强制走）
                Some(0)
            }
        }
    }

    /// 处理收到的 ACK
    pub fn on_ack(&mut self, ack: &Packet, now: u64) -> Option<()> {
        let flow = self.flows.get_mut(&ack.flow_id)?;
        // 累计 ACK：ack.seq 表示"已经累计收到的最高 seq + 1"
        let acked = ack.seq;
        if acked > flow.un_acked_base {
            let delta = acked - flow.un_acked_base;
            // 从 send_times 中移除已 ACK 的 seq
            for s in flow.un_acked_base..acked {
                flow.send_times.remove(&s);
            }
            flow.un_acked_base = acked;
            flow.in_flight = flow.in_flight.saturating_sub(delta);
            flow.last_ack_time = now;
            // AIMD 加性增加
            if !ack.ecn {
                flow.cwnd = (flow.cwnd + 1).min(self.max_cwnd);
            }
        }
        if ack.ecn {
            self.stats.ecn_ack_received += 1;
            match self.mode {
                CongestionMode::Strack => {
                    // 先切路：把"刚才走的路径"加入短暂黑名单
                    // 由于 ACK 中没有显式带路径信息，简化策略：
                    // 在 ECN 比例较高时降窗；否则只切路
                    let path = (ack.id as u8) % (self.paths.len().max(1) as u8);
                    self.paths[path as usize].blacklisted_until = now + self.blacklist_duration_ns;
                    self.paths[path as usize].ecn_recent += 1;
                    let avail_count = self.paths.iter().filter(|p| p.is_available(now)).count();
                    if avail_count == 0 {
                        // 全线拥塞，降窗
                        flow.cwnd = (flow.cwnd / 2).max(self.min_cwnd);
                    }
                }
                CongestionMode::Ecmp => {
                    // 单路径：直接降窗
                    flow.cwnd = (flow.cwnd / 2).max(self.min_cwnd);
                }
            }
        }
        // 检查流完成
        if flow.un_acked_base >= flow.total_packets && !flow.done {
            flow.done = true;
            flow.finish_time = now;
            self.stats.flows_completed += 1;
        }
        Some(())
    }

    /// 处理收到的 NACK：把 bitmap 中缺失的 seq 加入重传队列
    pub fn on_nack(&mut self, nack: &Packet, _now: u64) {
        self.stats.nack_received += 1;
        let flow = match self.flows.get_mut(&nack.flow_id) {
            Some(f) => f,
            None => return,
        };
        // sack_bits 的第 i 位 = 1 表示 sack_base + i 已经被接收；为 0 表示缺失
        // 但只关心 [sack_base, sack_base + 64) 范围内 < nack.seq 的 0 位
        let base = nack.sack_base;
        let bits = nack.sack_bits;
        for i in 0..64u32 {
            let s = base + i;
            if s >= nack.seq { break; }
            let received = (bits >> i) & 1 == 1;
            if !received && !flow.retransmit_queue.contains(&s) {
                flow.retransmit_queue.push(s);
            }
        }
    }

    pub fn all_flows_done(&self) -> bool {
        !self.flows.is_empty() && self.flows.values().all(|f| f.done)
    }
}
