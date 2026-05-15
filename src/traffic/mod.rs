//! AI 流量生成器
//!
//! - `Incast`：N-1 个发送端同时向 1 个接收端发数据（最容易触发拥塞）
//! - `AllReduce`：Ring AllReduce 流量模式（每节点既发又收）
//! - `AllToAll`：全员两两交换
//! - `Synthetic`：通用合成流量（支持流大小分布、到达过程、通信对模式）
//! - `Permutation`：排列流量（无热点）
//! - `Mix`：混合流量（多组件组合）

pub mod incast;
pub mod all_reduce;
pub mod all_to_all;
pub mod synthetic;
pub mod permute;
pub mod mix;

pub use incast::Incast;
pub use all_reduce::RingAllReduce;
pub use all_to_all::AllToAll;
pub use synthetic::{Synthetic, FlowSizeDist, ArrivalProcess, PairPattern};
pub use permute::{Permutation, PermuteKind};
pub use mix::{Mix, MixComponent};

use crate::EntityId;
use crate::network::packet::FlowId;

/// 一次流量描述
#[derive(Debug, Clone, Copy)]
pub struct FlowDesc {
    pub flow_id: FlowId,
    pub src: EntityId,
    pub dst: EntityId,
    pub bytes: u64,
    pub start_time_ns: u64,
}
