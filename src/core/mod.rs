//! 离散事件仿真核心引擎
//!
//! 提供事件 (`Event`)、事件队列 (`EventQueue`) 与模拟器 (`Simulator`) 三大组件。
//! 引擎本身与网络概念解耦，可独立测试调度逻辑的正确性与性能。

mod event;
mod queue;
mod simulator;

pub use event::{Event, EventKind};
pub use queue::EventQueue;
pub use simulator::Simulator;
