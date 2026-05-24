//! 网卡模型与可插拔协议栈（阶段三重构后）
//!
//! - `protocol`: 通用 `Protocol` trait + `ProtocolStats`
//! - `strack`: STrack 协议实现（含 ECMP / STrack 两种模式）
//! - `tcp`: 简化 TCP 实现（验证 trait 通用性）

pub mod protocol;
pub mod rdma;
pub mod strack;
pub mod tcp;
pub mod dcqcn;
pub mod hpcc;
pub mod swift;
pub mod reno;
pub mod cubic;

pub use protocol::{Protocol, ProtocolStats};
pub use rdma::{QueuePair, QpState, Qpn, Psn, RdmaOpcode, Wqe, Cqe, MsgBoundary};
pub use strack::{STrackProtocol, STrackMode, PathState};
pub use tcp::SimpleTcp;
pub use dcqcn::DcqcnProtocol;
pub use hpcc::HpccProtocol;
pub use swift::SwiftProtocol;
pub use reno::TcpReno;
pub use cubic::TcpCubic;
