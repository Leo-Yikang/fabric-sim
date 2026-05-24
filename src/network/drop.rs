//! 丢包原因与统计
//!
//! 记录每个被丢弃包的原因、位置和元信息，支持分原因、逐端口、逐流的统计。

use crate::EntityId;
use super::packet::{FlowId, SeqNum};
use super::switch::PortId;
use std::collections::HashMap;

/// 丢包原因
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DropReason {
    /// 路由表中找不到目的地址
    NoRoute,
    /// 出端口 Buffer 已满
    BufferFull,
    /// （预留）TTL 超时
    TtlExceeded,
    /// （预留）其他原因
    Other,
}

impl DropReason {
    /// 人类可读的名称
    pub fn as_str(&self) -> &'static str {
        match self {
            DropReason::NoRoute => "NoRoute",
            DropReason::BufferFull => "BufferFull",
            DropReason::TtlExceeded => "TtlExceeded",
            DropReason::Other => "Other",
        }
    }
}

/// 单次丢包事件的完整记录
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DropEvent {
    /// 丢包发生的时间（ns）
    pub time_ns: u64,
    /// 发生丢包的交换机 ID
    pub switch_id: EntityId,
    /// 目标出端口（None 表示无路由，不知道出端口）
    pub port: Option<PortId>,
    /// 丢包原因
    pub reason: DropReason,
    /// 所属流 ID
    pub flow_id: FlowId,
    /// 包的序列号
    pub seq: SeqNum,
    /// 包大小（bytes）
    pub size: u32,
}

/// 单条流的丢包统计（分原因）
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct PerFlowDrops {
    /// Buffer 满丢包
    pub buffer_full: u64,
    /// 无路由丢包
    pub no_route: u64,
    /// 总丢包
    pub total: u64,
}

/// 分原因 + 逐端口丢包计数器
///
/// 替代原来的 `Switch.drops: u64`，支持多维度的丢包统计。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DropCounters {
    /// 按原因统计的总丢包数
    pub by_reason: [u64; 4],
    /// 逐端口的丢包数（索引 = PortId）
    pub by_port: Vec<u64>,
    /// 总丢包数（兼容旧接口）
    pub total: u64,
    /// 逐流的丢包统计
    pub per_flow: HashMap<FlowId, PerFlowDrops>,
    /// 可选：记录每次丢包事件的详细日志（默认关闭以节省内存）
    #[serde(skip)]
    pub events: Vec<DropEvent>,
}

impl DropCounters {
    /// 创建 DropCounters，预配 `num_ports` 个端口
    pub fn new(num_ports: usize) -> Self {
        Self {
            by_reason: [0; 4],
            by_port: vec![0; num_ports],
            total: 0,
            per_flow: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// 记录一次丢包
    #[inline]
    pub fn record(
        &mut self,
        reason: DropReason,
        port: Option<PortId>,
        flow_id: FlowId,
        seq: SeqNum,
        size: u32,
        switch_id: EntityId,
        time_ns: u64,
    ) {
        let idx = reason_to_index(reason);
        self.by_reason[idx] += 1;
        if let Some(p) = port {
            let pi = p as usize;
            if pi >= self.by_port.len() {
                self.by_port.resize(pi + 1, 0);
            }
            self.by_port[pi] += 1;
        }
        self.total += 1;

        // 逐流统计
        let pf = self.per_flow.entry(flow_id).or_default();
        pf.total += 1;
        match reason {
            DropReason::BufferFull => pf.buffer_full += 1,
            DropReason::NoRoute => pf.no_route += 1,
            _ => {}
        }

        // 记录事件（如果开启了详细日志）
        if self.events.capacity() > 0 {
            self.events.push(DropEvent {
                time_ns,
                switch_id,
                port,
                reason,
                flow_id,
                seq,
                size,
            });
        }
    }

    /// 启用详细事件记录（预分配 capacity 个槽位）
    pub fn enable_event_log(&mut self, capacity: usize) {
        self.events = Vec::with_capacity(capacity);
    }

    /// 获取某原因的丢包数
    #[inline]
    pub fn drops_by_reason(&self, reason: DropReason) -> u64 {
        self.by_reason[reason_to_index(reason)]
    }

    /// 获取 buffer 满引起的丢包数
    #[inline]
    pub fn buffer_full_drops(&self) -> u64 {
        self.drops_by_reason(DropReason::BufferFull)
    }

    /// 获取无路由引起的丢包数
    #[inline]
    pub fn no_route_drops(&self) -> u64 {
        self.drops_by_reason(DropReason::NoRoute)
    }
}

#[inline]
fn reason_to_index(r: DropReason) -> usize {
    match r {
        DropReason::NoRoute => 0,
        DropReason::BufferFull => 1,
        DropReason::TtlExceeded => 2,
        DropReason::Other => 3,
    }
}