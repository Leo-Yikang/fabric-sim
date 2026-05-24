//! Packet 数据结构
//!
//! 抽象一个网络数据包。第一字段始终是 `id`，便于在事件中只携带 `packet_id`
//! 而不复制整个 Packet（包内容由实体注册表持有）。
//!
//! 为了支持可插拔协议栈，`Packet` 只保留所有协议共有的最小字段；
//! 协议特有的扩展信息（如 SACK bitmap、TCP window 等）通过 `payload` 编码。

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
    /// 控制包。`u8` 由具体协议解释（如 0=ACK, 1=NACK, 2=SACK…）
    Control(u8),
}

/// 网络包
#[derive(Debug, Clone)]
pub struct Packet {
    pub id: PacketId,
    pub kind: PacketKind,
    pub flow_id: FlowId,
    pub seq: SeqNum,
    pub size: u32,
    pub src: EntityId,
    pub dst: EntityId,
    pub ecn: bool,
    pub routing_tag: u8,
    pub payload: Vec<u8>,
    pub depart_time: u64,
    pub trace_id: PacketId,
    /// RDMA QP 编号（0 表示非 RDMA 包，向下兼容）
    pub qpn: u32,
    /// RDMA PSN — 每 QP 独立的包序号（24-bit 有效）
    pub psn: u32,
    /// 消息边界标志（MsgBoundary 枚举编码）
    pub msg_flags: u8,
    /// RDMA 操作码（RdmaOpcode 枚举编码，0 表示非 RDMA）
    pub rdma_opcode: u8,
}

impl Packet {
    pub fn data(id: PacketId, trace_id: PacketId, flow: FlowId, seq: SeqNum, src: EntityId, dst: EntityId, depart_time: u64) -> Self {
        Self {
            id, trace_id, kind: PacketKind::Data,
            flow_id: flow, seq, size: MTU_BYTES,
            src, dst, ecn: false, routing_tag: 0,
            payload: Vec::new(), depart_time,
            qpn: 0, psn: 0, msg_flags: 0, rdma_opcode: 0,
        }
    }

    pub fn control(
        id: PacketId, trace_id: PacketId, flow: FlowId, seq: SeqNum,
        src: EntityId, dst: EntityId, ecn: bool, control_type: u8,
        payload: Vec<u8>, now: u64,
    ) -> Self {
        Self {
            id, trace_id, kind: PacketKind::Control(control_type),
            flow_id: flow, seq, size: 64,
            src, dst, ecn, routing_tag: 0,
            payload, depart_time: now,
            qpn: 0, psn: 0, msg_flags: 0, rdma_opcode: 0,
        }
    }

    /// 构造 RDMA 数据包
    pub fn rdma_data(
        id: PacketId, trace_id: PacketId,
        qpn: u32, psn: u32, msg_flags: u8, opcode: u8,
        src: EntityId, dst: EntityId, depart_time: u64,
    ) -> Self {
        Self {
            id, trace_id, kind: PacketKind::Data,
            flow_id: qpn, seq: psn, size: MTU_BYTES,
            src, dst, ecn: false, routing_tag: 0,
            payload: Vec::new(), depart_time,
            qpn, psn, msg_flags, rdma_opcode: opcode,
        }
    }
}
