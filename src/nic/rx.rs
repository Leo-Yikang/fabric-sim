//! 接收端 NIC
//!
//! 维护：
//! - 每流的 Reorder Buffer
//! - 期望的下一个累计序号 `next_expected`
//! - SACK Bitmap：[next_expected, next_expected+64) 内已收到的包
//!
//! 收到一个数据包时返回：(可选的 ACK 包, 可选的 NACK 包)
//! - 始终回 ACK（携带 cumulative + SACK bitmap）
//! - 检测到 gap 时附加 NACK（实际上 ACK 已经能传递缺失信息；为了模拟 STrack 论文里
//!   的"快速 NACK"行为，我们这里在 gap 出现时也单独发一个 NACK）

use crate::network::packet::{Packet, FlowId, SeqNum};
use crate::EntityId;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Copy)]
pub struct RxStats {
    pub packets_received: u64,
    pub packets_delivered: u64,    // 顺序提交给上层
    pub duplicates: u64,
    pub out_of_order: u64,
    pub nacks_sent: u64,
}

pub struct FlowRxState {
    pub flow_id: FlowId,
    pub next_expected: SeqNum,
    pub received_bits: u64,        // 位 i = (next_expected + i) 是否已到
}

impl FlowRxState {
    pub fn new(flow_id: FlowId) -> Self { Self { flow_id, next_expected: 0, received_bits: 0 } }
}

pub struct RxNic {
    pub host_id: EntityId,
    pub flows: HashMap<FlowId, FlowRxState>,
    pub stats: RxStats,
    pub next_packet_id: u64,
}

impl RxNic {
    pub fn new(host_id: EntityId) -> Self {
        Self { host_id, flows: HashMap::new(), stats: RxStats::default(), next_packet_id: 10_000_000_000 }
    }

    /// 处理一个收到的数据包，返回要回发的控制包列表
    pub fn on_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
        self.stats.packets_received += 1;
        let flow = self.flows.entry(pkt.flow_id).or_insert_with(|| FlowRxState::new(pkt.flow_id));

        let seq = pkt.seq;
        let next = flow.next_expected;
        let mut nack_needed = false;

        if seq < next {
            // 早就 ACK 过：丢弃，但仍回 ACK
            self.stats.duplicates += 1;
        } else if seq == next {
            // 完美按序到达
            flow.next_expected += 1;
            // 把 bitmap 向右移
            flow.received_bits >>= 1;
            // 然后再把 bitmap 中已经连续的部分继续吃掉
            while flow.received_bits & 1 == 1 {
                flow.next_expected += 1;
                flow.received_bits >>= 1;
            }
        } else {
            // 乱序到达
            self.stats.out_of_order += 1;
            let offset = (seq - next) as u32;
            if offset < 64 {
                let bit = 1u64 << offset;
                if flow.received_bits & bit == 0 {
                    flow.received_bits |= bit;
                    nack_needed = true;
                } else {
                    self.stats.duplicates += 1;
                }
            } // 超出 64 窗口的包先丢
        }

        // 构造 ACK：累计 ACK 到 next_expected，带 bitmap
        let mut out = Vec::new();
        let pid_ack = self.next_packet_id; self.next_packet_id += 1;
        let ack = Packet::ack(pid_ack, pkt.flow_id, flow.next_expected, self.host_id, pkt.src, pkt.ecn,
                              flow.next_expected, flow.received_bits, now);
        out.push(ack);

        if nack_needed {
            let pid_nack = self.next_packet_id; self.next_packet_id += 1;
            let nack = Packet::nack(pid_nack, pkt.flow_id, seq, self.host_id, pkt.src,
                                     flow.next_expected, flow.received_bits, now);
            out.push(nack);
            self.stats.nacks_sent += 1;
        }

        // 提交给上层的累计数
        self.stats.packets_delivered = self.stats.packets_delivered.max(flow.next_expected as u64);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::Packet;

    #[test]
    fn rx_in_order_delivery() {
        let mut rx = RxNic::new(99);
        for s in 0..10u32 {
            let pkt = Packet::data(s as u64, 0, s, 1, 99, 0);
            let outs = rx.on_data(&pkt, 0);
            assert_eq!(outs.len(), 1); // 只回 ACK
            assert_eq!(outs[0].seq, s + 1); // 累计 ACK
        }
        assert_eq!(rx.flows[&0].next_expected, 10);
    }

    #[test]
    fn rx_out_of_order_then_fill_gap() {
        let mut rx = RxNic::new(99);
        // 收到 0, 2, 3, 1（乱序）
        for s in [0u32, 2, 3, 1] {
            let pkt = Packet::data(s as u64, 0, s, 1, 99, 0);
            rx.on_data(&pkt, 0);
        }
        // 收完 1 之后，next_expected 应该一路推到 4
        assert_eq!(rx.flows[&0].next_expected, 4);
        assert!(rx.stats.nacks_sent > 0);
    }

    #[test]
    fn rx_handles_duplicate() {
        let mut rx = RxNic::new(99);
        for _ in 0..3 {
            let pkt = Packet::data(0, 0, 0, 1, 99, 0);
            rx.on_data(&pkt, 0);
        }
        assert_eq!(rx.flows[&0].next_expected, 1);
        assert!(rx.stats.duplicates >= 2);
    }
}
