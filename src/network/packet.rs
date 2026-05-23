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
    pub seq: SeqNum,             // 流内序号（用于 Reorder 与 ACK）
    pub size: u32,                // 字节数
    pub src: EntityId,            // 源主机 ID
    pub dst: EntityId,            // 目的主机 ID
    pub ecn: bool,                // 拥塞标记
    /// 路由标签：非 0 时交换机优先按该值选端口（1-indexed）；0 表示由交换机自行哈希。
    pub routing_tag: u8,
    /// 协议自定义负载。控制包常用；数据包通常为空。
    pub payload: Vec<u8>,
    /// 用于 FCT 统计：包出发时刻（ns）
    pub depart_time: u64,
    /// 全局唯一、单调递增的追踪 ID，不随 slab 复用而重置。
    /// 用于逐包调试和路径追踪，与 `id`（slab 索引）不同。
    pub trace_id: PacketId,
}

impl Packet {
    pub fn data(id: PacketId, trace_id: PacketId, flow: FlowId, seq: SeqNum, src: EntityId, dst: EntityId, depart_time: u64) -> Self {
        Self {
            id,
            trace_id,
            kind: PacketKind::Data,
            flow_id: flow,
            seq,
            size: MTU_BYTES,
            src,
            dst,
            ecn: false,
            routing_tag: 0,
            payload: Vec::new(),
            depart_time,
        }
    }

    pub fn control(
        id: PacketId,
        trace_id: PacketId,
        flow: FlowId,
        seq: SeqNum,
        src: EntityId,
        dst: EntityId,
        ecn: bool,
        control_type: u8,
        payload: Vec<u8>,
        now: u64,
    ) -> Self {
        Self {
            id,
            trace_id,
            kind: PacketKind::Control(control_type),
            flow_id: flow,
            seq,
            size: 64,
            src,
            dst,
            ecn,
            routing_tag: 0,
            payload,
            depart_time: now,
        }
    }
}
