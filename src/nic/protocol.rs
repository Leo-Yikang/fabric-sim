//! 可插拔传输协议 trait
//!
//! 为了支持 TCP、BBR、QUIC 等不同传输逻辑，NIC 层不再硬编码 STrack，
//! 而是抽象为 `Protocol` trait。每个 host 持有一个 `Box<dyn Protocol>`，
//! 由 `SimRunner` 在事件发生时调用对应的生命周期方法。

use crate::network::packet::{FlowId, Packet};
use crate::EntityId;

/// 协议层通用统计信息
#[derive(Default, Debug, Clone, Copy)]
pub struct ProtocolStats {
    pub packets_sent: u64,
    pub packets_retransmitted: u64,
    pub packets_received: u64,
    pub ecn_ack_received: u64,
    pub nack_received: u64,
    pub nacks_sent: u64,
    pub flows_completed: u64,
}

/// 传输协议接口
///
/// 实现者需要同时维护发送端和接收端状态（或仅维护自己关心的那一端）。
/// `SimRunner` 负责事件排序和链路 serialization，协议实现只关心数据包语义。
pub trait Protocol {
    /// 注册一条新流（发送端）
    fn start_flow(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64);

    /// 发送端定时 tick（由 TxTick 事件触发），返回要注入网络的数据包
    fn on_tx_tick(&mut self, now: u64) -> Vec<Packet>;

    /// 接收端收到 Data 包，返回要回发的控制包（ACK / NACK 等）
    fn on_rx_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet>;

    /// 发送端收到控制包（ACK / NACK / 自定义）
    fn on_tx_control(&mut self, pkt: &Packet, now: u64);

    /// 所有流是否都已完成
    fn all_flows_done(&self) -> bool;

    /// 提取已完成的流，返回 (flow_id, finish_time_ns)。
    /// 调用后实现内部应清空已提取的记录，避免重复上报。
    fn take_finished_flows(&mut self) -> Vec<(FlowId, u64)>;

    /// 获取当前统计信息
    fn stats(&self) -> ProtocolStats;

    /// 当前是否有待发送的工作（cwnd 有空间可发新数据，或重传队列非空）。
    /// 返回 false 表示协议栈暂时不需要 TxTick，可完全由外部事件（ACK/NACK/FlowStart）驱动。
    fn has_pending_work(&self) -> bool;

    /// 返回最早的未确认包的 RTO 截止时间（send_time + rto_ns）。
    /// None 表示当前没有未确认的包，不需要 RTO 检查。
    fn next_rto_deadline(&self) -> Option<u64>;
}
