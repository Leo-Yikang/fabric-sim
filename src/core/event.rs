//! 事件类型定义
//!
//! 事件是离散事件仿真中的基本调度单元。每个事件携带一个时间戳、一个种类标签
//! 以及一个目标实体 ID，由模拟器在合适的时刻派发到对应的处理函数。

use crate::{EntityId, SimTime};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// 全局事件计数器：用于在时间戳相同时维持插入顺序（FIFO 稳定性）
static EVENT_SEQ: AtomicU64 = AtomicU64::new(0);

/// 事件种类
///
/// 第一阶段仅定义引擎所需的最小集合 + `Custom` 占位。
/// 第二、三阶段会在 `network`、`nic` 模块中扩展真正的网络事件。
#[derive(Debug, Clone)]
pub enum EventKind {
    /// 包到达（接收端网卡）
    PacketArrive { packet_id: u64, src: EntityId },
    /// 包发送完成（发送端网卡 / 出端口）
    PacketDepart { packet_id: u64, dst: EntityId, port: u8 },
    /// 超时（CC / 重传定时器）
    Timeout { timer_id: u64 },
    /// 仿真停止
    Stop,
    /// 用户自定义占位事件（用于引擎单元测试）
    Custom(String),
}

/// 仿真事件
#[derive(Debug, Clone)]
pub struct Event {
    /// 事件触发时刻（纳秒）
    pub time: SimTime,
    /// 事件种类
    pub kind: EventKind,
    /// 目标实体 ID（哪个节点 / NIC / 交换机要处理它）
    pub target: EntityId,
    /// 全局序号：时间戳相同的情况下用它打破平局，保证 FIFO 稳定性
    pub seq: u64,
}

impl Event {
    /// 新建事件，自动分配全局序号
    pub fn new(time: SimTime, kind: EventKind, target: EntityId) -> Self {
        let seq = EVENT_SEQ.fetch_add(1, AtomicOrdering::Relaxed);
        Self { time, kind, target, seq }
    }
}

// ---- 排序逻辑：BinaryHeap 是最大堆，所以我们反转 Ord 实现最小堆 ----

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        // 注意：反向比较，让 BinaryHeap 表现为最小堆
        other
            .time
            .cmp(&self.time)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BinaryHeap;

    #[test]
    fn event_ordering_min_heap_by_time() {
        let mut heap = BinaryHeap::new();
        heap.push(Event::new(300, EventKind::Custom("c".into()), 0));
        heap.push(Event::new(100, EventKind::Custom("a".into()), 0));
        heap.push(Event::new(200, EventKind::Custom("b".into()), 0));

        let first = heap.pop().unwrap();
        let second = heap.pop().unwrap();
        let third = heap.pop().unwrap();
        assert_eq!(first.time, 100);
        assert_eq!(second.time, 200);
        assert_eq!(third.time, 300);
    }

    #[test]
    fn event_ordering_fifo_when_same_time() {
        let mut heap = BinaryHeap::new();
        let e1 = Event::new(100, EventKind::Custom("first".into()), 0);
        let e2 = Event::new(100, EventKind::Custom("second".into()), 0);
        let e3 = Event::new(100, EventKind::Custom("third".into()), 0);
        heap.push(e2.clone());
        heap.push(e3.clone());
        heap.push(e1.clone());

        // 同时间戳下，按插入顺序（seq）弹出
        let a = heap.pop().unwrap();
        let b = heap.pop().unwrap();
        let c = heap.pop().unwrap();
        assert_eq!(a.seq, e1.seq);
        assert_eq!(b.seq, e2.seq);
        assert_eq!(c.seq, e3.seq);
    }
}
