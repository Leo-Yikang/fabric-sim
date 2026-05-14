//! Packet 数据结构
//!
//! 抽象一个网络数据包。第一字段始终是 `id`，便于在事件中只携带 `packet_id`
//! 而不复制整个 Packet（包内容由实体注册表持有）。

use crate::EntityId;

/// MTU 大小（字节）。STrack 在 AI 集群常用 1KB 或 4KB；这里默认 1KB。
pub const MTU_BYTES: u32 = 1024;

pub type PacketId = u64;
pub type FlowId = u32;
pub type SeqNum = u32;

/// 包的种类
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketKind {
    /// 数据包
    Data,
    /// 累计 ACK（带 SACK Bitmap）
    Ack,
    /// NACK（请求选择性重传）
    Nack,
}

/// 网络包
#[derive(Debug, Clone)]
pub struct Packet {
    pub id: PacketId,
    pub kind: PacketKind,
    pub flow_id: FlowId,
    pub seq: SeqNum,             // 流内序号（用于 Reorder 与 SACK）
    pub size: u32,                // 字节数
    pub src: EntityId,            // 源主机 ID
    pub dst: EntityId,            // 目的主机 ID
    pub ecn: bool,                // 拥塞标记
    pub path_hint: u8,            // STrack: 期望走的发送端口（spraying）
    /// 仅 ACK/NACK 有效：基于 SACK Bitmap 的位图
    pub sack_base: SeqNum,
    pub sack_bits: u64,           // 64 个包窗口足够覆盖一个 RTT
    /// 用于 FCT 统计：包出发时刻（ns）
    pub depart_time: u64,
}

impl Packet {
    pub fn data(id: PacketId, flow: FlowId, seq: SeqNum, src: EntityId, dst: EntityId, depart_time: u64) -> Self {
        Self {
            id,
            kind: PacketKind::Data,
            flow_id: flow,
            seq,
            size: MTU_BYTES,
            src,
            dst,
            ecn: false,
            path_hint: 0,
            sack_base: 0,
            sack_bits: 0,
            depart_time,
        }
    }

    pub fn ack(id: PacketId, flow: FlowId, ack_seq: SeqNum, src: EntityId, dst: EntityId, ecn: bool, sack_base: SeqNum, sack_bits: u64, now: u64) -> Self {
        Self {
            id,
            kind: PacketKind::Ack,
            flow_id: flow,
            seq: ack_seq,
            size: 64,
            src,
            dst,
            ecn,
            path_hint: 0,
            sack_base,
            sack_bits,
            depart_time: now,
        }
    }

    pub fn nack(id: PacketId, flow: FlowId, expected_seq: SeqNum, src: EntityId, dst: EntityId, sack_base: SeqNum, sack_bits: u64, now: u64) -> Self {
        Self {
            id,
            kind: PacketKind::Nack,
            flow_id: flow,
            seq: expected_seq,
            size: 64,
            src,
            dst,
            ecn: false,
            path_hint: 0,
            sack_base,
            sack_bits,
            depart_time: now,
        }
    }
}
