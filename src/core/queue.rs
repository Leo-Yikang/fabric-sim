//! 事件队列：基于二叉堆的最小优先队列封装
//!
//! 直接使用标准库 `BinaryHeap` + 反向比较，实现最小堆语义。
//! `Event` 自身已通过 `seq` 字段保证 FIFO 稳定性。

use super::event::Event;
use std::collections::BinaryHeap;

/// 事件队列
pub struct EventQueue {
    heap: BinaryHeap<Event>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self { heap: BinaryHeap::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { heap: BinaryHeap::with_capacity(cap) }
    }

    /// 插入事件
    #[inline]
    pub fn push(&mut self, ev: Event) {
        self.heap.push(ev);
    }

    /// 弹出最早的事件
    #[inline]
    pub fn pop(&mut self) -> Option<Event> {
        self.heap.pop()
    }

    /// 查看最早的事件但不弹出
    #[inline]
    pub fn peek(&self) -> Option<&Event> {
        self.heap.peek()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl Default for EventQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EventKind;

    #[test]
    fn push_pop_ordered() {
        let mut q = EventQueue::new();
        for t in [500u64, 100, 300, 200, 400] {
            q.push(Event::new(t, EventKind::Custom("x".into()), 0));
        }
        let mut last = 0u64;
        while let Some(ev) = q.pop() {
            assert!(ev.time >= last);
            last = ev.time;
        }
    }
}
