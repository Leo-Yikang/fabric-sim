//! 模拟器主循环
//!
//! `Simulator` 维护全局时钟与事件队列，按时间顺序逐个派发事件。
//! 第一阶段使用最小化接口：通过 `step` / `run_until` 驱动；
//! 真实的事件处理逻辑由上层（network/nic 模块）通过 handler 注入。

use super::{Event, EventQueue};
use crate::{EntityId, SimTime};
use std::collections::HashMap;

/// 事件处理回调：(当前事件, 模拟器引用) → 可能产生若干新事件
///
/// 为了避免 self-referential 借用问题，handler 返回新事件 Vec，由模拟器统一压入队列。
pub type Handler = Box<dyn FnMut(&Event, SimTime) -> Vec<Event>>;

/// 模拟器
pub struct Simulator {
    clock: SimTime,
    queue: EventQueue,
    handlers: HashMap<EntityId, Handler>,
    /// 统计：已处理事件总数
    processed: u64,
    /// 是否被显式停止
    stopped: bool,
}

impl Simulator {
    pub fn new() -> Self {
        Self {
            clock: 0,
            queue: EventQueue::new(),
            handlers: HashMap::new(),
            processed: 0,
            stopped: false,
        }
    }

    /// 当前仿真时钟（纳秒）
    pub fn now(&self) -> SimTime {
        self.clock
    }

    /// 已处理事件数
    pub fn processed(&self) -> u64 {
        self.processed
    }

    /// 队列中剩余事件数
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// 直接弹出下一个事件（外部仿真循环用）
    pub fn pop_event(&mut self) -> Option<Event> {
        let ev = self.queue.pop()?;
        self.clock = ev.time.max(self.clock);
        self.processed += 1;
        Some(ev)
    }

    /// 查看下一个事件的时间戳
    pub fn peek_time(&self) -> Option<SimTime> {
        self.queue.peek().map(|e| e.time)
    }

    /// 调度一个事件
    pub fn schedule(&mut self, ev: Event) {
        // 允许 ev.time == self.clock（同 tick 内事件）；
        // 对于落后于 clock 的事件，做软提升而非崩溃，便于 SimRunner 内部排序
        let ev = if ev.time < self.clock {
            let mut e = ev;
            e.time = self.clock;
            e
        } else { ev };
        self.queue.push(ev);
    }

    /// 注册某个实体的事件处理回调
    pub fn register_handler(&mut self, entity: EntityId, handler: Handler) {
        self.handlers.insert(entity, handler);
    }

    /// 执行单步：弹出并处理一个事件
    pub fn step(&mut self) -> Option<SimTime> {
        let ev = self.queue.pop()?;
        self.clock = ev.time;
        self.processed += 1;

        // 若注册了 handler，调用之；否则丢弃（第一阶段单元测试场景）
        let new_events = if let Some(h) = self.handlers.get_mut(&ev.target) {
            h(&ev, self.clock)
        } else {
            Vec::new()
        };
        for nev in new_events {
            self.queue.push(nev);
        }
        Some(self.clock)
    }

    /// 跑到指定时刻（含）为止 —— 处理所有 time ≤ until 的事件
    pub fn run_until(&mut self, until: SimTime) {
        while !self.stopped {
            match self.queue.peek() {
                Some(ev) if ev.time <= until => {
                    self.step();
                }
                _ => break,
            }
        }
    }

    /// 跑到队列空为止
    pub fn run(&mut self) {
        while !self.stopped && !self.queue.is_empty() {
            self.step();
        }
    }

    /// 停止仿真（在 handler 内调用，让主循环退出）
    pub fn stop(&mut self) {
        self.stopped = true;
    }
}

impl Default for Simulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EventKind;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn empty_simulator_run_terminates() {
        let mut sim = Simulator::new();
        sim.run();
        assert_eq!(sim.now(), 0);
        assert_eq!(sim.processed(), 0);
    }

    #[test]
    fn schedule_and_run_processes_events_in_order() {
        let mut sim = Simulator::new();
        let log: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
        let log_clone = Rc::clone(&log);

        sim.register_handler(
            1,
            Box::new(move |ev, _now| {
                log_clone.borrow_mut().push(ev.time);
                Vec::new()
            }),
        );

        sim.schedule(Event::new(300, EventKind::Custom("c".into()), 1));
        sim.schedule(Event::new(100, EventKind::Custom("a".into()), 1));
        sim.schedule(Event::new(200, EventKind::Custom("b".into()), 1));
        sim.run();

        assert_eq!(*log.borrow(), vec![100, 200, 300]);
        assert_eq!(sim.now(), 300);
        assert_eq!(sim.processed(), 3);
    }

    #[test]
    fn run_until_stops_at_boundary() {
        let mut sim = Simulator::new();
        sim.schedule(Event::new(100, EventKind::Custom("a".into()), 0));
        sim.schedule(Event::new(500, EventKind::Custom("b".into()), 0));
        sim.schedule(Event::new(1000, EventKind::Custom("c".into()), 0));
        sim.run_until(500);
        assert_eq!(sim.now(), 500);
        assert_eq!(sim.processed(), 2);
        assert_eq!(sim.pending(), 1);
    }

    #[test]
    fn handler_can_spawn_new_events() {
        let mut sim = Simulator::new();
        let counter: Rc<RefCell<u32>> = Rc::new(RefCell::new(0));
        let counter_clone = Rc::clone(&counter);

        sim.register_handler(
            7,
            Box::new(move |ev, now| {
                *counter_clone.borrow_mut() += 1;
                // 每次都再调度一个 100ns 后的后续事件，但限制总次数
                if *counter_clone.borrow() < 5 {
                    vec![Event::new(now + 100, ev.kind.clone(), ev.target)]
                } else {
                    Vec::new()
                }
            }),
        );
        sim.schedule(Event::new(0, EventKind::Custom("tick".into()), 7));
        sim.run();
        assert_eq!(*counter.borrow(), 5);
        assert_eq!(sim.now(), 400);
    }
}
