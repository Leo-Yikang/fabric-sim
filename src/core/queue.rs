//! 事件队列：基于 4-ary heap（四叉堆）的最小优先队列
//!
//! 4-ary heap 的树高约为同规模 BinaryHeap 的一半（log₄ N vs log₂ N），
//! push/pop 时的 sift 操作内存访问次数更少，在百万级事件队列下吞吐更高。
//! `Event` 自身已通过 `seq` 字段保证 FIFO 稳定性。

use super::event::Event;

/// 事件队列（4-ary min-heap）
pub struct EventQueue {
    heap: Vec<Event>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self { heap: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            heap: Vec::with_capacity(cap),
        }
    }

    /// 插入事件
    #[inline]
    pub fn push(&mut self, ev: Event) {
        self.heap.push(ev);
        self.sift_up(self.heap.len() - 1);
    }

    /// 弹出最早的事件
    #[inline]
    pub fn pop(&mut self) -> Option<Event> {
        if self.heap.is_empty() {
            return None;
        }
        let last = self.heap.len() - 1;
        self.heap.swap(0, last);
        let ev = self.heap.pop();
        if !self.heap.is_empty() {
            self.sift_down(0);
        }
        ev
    }

    /// 查看最早的事件但不弹出
    #[inline]
    pub fn peek(&self) -> Option<&Event> {
        self.heap.first()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// 移除所有匹配 predicate 的事件。
    /// 内部需要重建堆，复杂度 O(n)。
    pub fn cancel_where(&mut self, predicate: impl Fn(&Event) -> bool) -> usize {
        let before = self.heap.len();
        self.heap.retain(|ev| !predicate(ev));
        let removed = before - self.heap.len();
        if removed > 0 {
            self.heapify();
        }
        removed
    }

    /// 批量重建堆（O(n)）
    fn heapify(&mut self) {
        let len = self.heap.len();
        if len <= 1 {
            return;
        }
        let last_parent = (len - 2) / 4;
        for i in (0..=last_parent).rev() {
            self.sift_down(i);
        }
    }

    /// Event 的 Ord 是为 BinaryHeap（最大堆）反向实现的；
    /// 4-ary heap 自己维护最小堆语义，需要直接比较 time + seq。
    #[inline]
    fn ev_less(a: &Event, b: &Event) -> bool {
        a.time < b.time || (a.time == b.time && a.seq < b.seq)
    }

    /// 将新加入的末尾元素上浮到正确位置
    #[inline]
    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 4;
            if !Self::ev_less(&self.heap[idx], &self.heap[parent]) {
                break;
            }
            self.heap.swap(idx, parent);
            idx = parent;
        }
    }

    /// 将根元素下沉到正确位置
    #[inline]
    fn sift_down(&mut self, mut idx: usize) {
        let len = self.heap.len();
        loop {
            let first_child = idx * 4 + 1;
            if first_child >= len {
                break;
            }
            let last_child = (first_child + 3).min(len - 1);
            let mut min = idx;
            for child in first_child..=last_child {
                if Self::ev_less(&self.heap[child], &self.heap[min]) {
                    min = child;
                }
            }
            if min == idx {
                break;
            }
            self.heap.swap(idx, min);
            idx = min;
        }
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

    #[test]
    fn fifo_stability_same_time() {
        let mut q = EventQueue::new();
        let e1 = Event::new(100, EventKind::Custom("first".into()), 0);
        let e2 = Event::new(100, EventKind::Custom("second".into()), 0);
        let e3 = Event::new(100, EventKind::Custom("third".into()), 0);
        q.push(e2.clone());
        q.push(e3.clone());
        q.push(e1.clone());

        let a = q.pop().unwrap();
        let b = q.pop().unwrap();
        let c = q.pop().unwrap();
        assert_eq!(a.seq, e1.seq);
        assert_eq!(b.seq, e2.seq);
        assert_eq!(c.seq, e3.seq);
    }

    #[test]
    fn cancel_where_removes_matching() {
        let mut q = EventQueue::new();
        for t in [100u64, 200, 300, 400, 500] {
            q.push(Event::new(t, EventKind::Custom("x".into()), 0));
        }
        let removed = q.cancel_where(|ev| ev.time >= 300);
        assert_eq!(removed, 3);
        assert_eq!(q.len(), 2);
        let a = q.pop().unwrap();
        let b = q.pop().unwrap();
        assert_eq!(a.time, 100);
        assert_eq!(b.time, 200);
    }

    #[test]
    fn cancel_where_preserves_order() {
        let mut q = EventQueue::new();
        for t in [500u64, 100, 400, 200, 300] {
            q.push(Event::new(t, EventKind::Custom("x".into()), 0));
        }
        // 移除时间 > 300 的
        q.cancel_where(|ev| ev.time > 300);
        let mut prev = 0u64;
        while let Some(ev) = q.pop() {
            assert!(ev.time >= prev);
            prev = ev.time;
        }
        assert_eq!(prev, 300);
    }

    #[test]
    fn cancel_where_none_matching_preserves_all() {
        let mut q = EventQueue::new();
        for t in [100u64, 200, 300] {
            q.push(Event::new(t, EventKind::Custom("x".into()), 0));
        }
        let removed = q.cancel_where(|ev| ev.time > 999);
        assert_eq!(removed, 0);
        assert_eq!(q.len(), 3);
    }

    #[test]
    fn cancel_where_removes_all() {
        let mut q = EventQueue::new();
        for t in [100u64, 200, 300] {
            q.push(Event::new(t, EventKind::Custom("x".into()), 0));
        }
        let removed = q.cancel_where(|_| true);
        assert_eq!(removed, 3);
        assert!(q.is_empty());
        assert!(q.pop().is_none());
    }

    #[test]
    fn cancel_where_preserves_fifo_for_same_time() {
        let mut q = EventQueue::new();
        let e1 = Event::new(100, EventKind::Custom("keep1".into()), 0);
        let e2 = Event::new(100, EventKind::Custom("drop".into()), 0);
        let e3 = Event::new(100, EventKind::Custom("keep2".into()), 0);
        q.push(e3.clone());
        q.push(e2.clone());
        q.push(e1.clone());

        let removed = q.cancel_where(|ev| matches!(&ev.kind, EventKind::Custom(s) if s == "drop"));
        assert_eq!(removed, 1);

        let a = q.pop().unwrap();
        let b = q.pop().unwrap();
        assert_eq!(a.seq, e1.seq);
        assert_eq!(b.seq, e3.seq);
        assert!(q.pop().is_none());
    }
}
