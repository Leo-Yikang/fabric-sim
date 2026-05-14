//! # STrack 多路径 RDMA 网络模拟器
//!
//! 基于离散事件仿真 (Discrete Event Simulation, DES) 的网络模拟器，
//! 用于研究 STrack 协议在 AI/ML 集群环境下的性能表现。
//!
//! ## 模块组织
//! - [`core`]:     离散事件引擎（事件、事件队列、时钟、调度器）
//! - [`network`]:  网络拓扑与物理层抽象（节点、链路、交换机）
//! - [`nic`]:      网卡模型与 STrack 协议栈（CC、SACK、Reorder Buffer）
//! - [`topology`]: 拓扑生成器（Fat-Tree, Leaf-Spine）
//! - [`traffic`]:  流量生成器（AllReduce, AllToAll, Incast）
//! - [`monitor`]:  指标采集与日志输出
//!
//! ## 快速开始
//! ```no_run
//! use strack_sim::core::{Simulator, Event, EventKind};
//!
//! let mut sim = Simulator::new();
//! sim.schedule(Event::new(100, EventKind::Custom("hello".into()), 0));
//! sim.run_until(1000);
//! ```

pub mod core;
pub mod network;
pub mod nic;
pub mod topology;
pub mod traffic;
pub mod monitor;
pub mod sim_runner;

pub use sim_runner::SimRunner;

/// 仿真时间单位：纳秒（u64 可表示约 584 年的纳秒级仿真）
pub type SimTime = u64;

/// 实体 ID（节点、链路、交换机等通用句柄）
pub type EntityId = u32;
