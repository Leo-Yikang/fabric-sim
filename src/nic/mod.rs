//! 网卡模型与 STrack 协议栈（阶段三）
//!
//! - `tx`: 发送端 NIC，含 Packet Spraying + 拥塞窗口 + 路径黑名单
//! - `rx`: 接收端 NIC，含 Reorder Buffer + SACK Bitmap
//! - `cc`: 拥塞控制策略 trait（默认实现 STrack 策略 / ECMP-only baseline）

pub mod tx;
pub mod rx;
pub mod cc;

pub use tx::{TxNic, TxStats};
pub use rx::{RxNic, RxStats};
pub use cc::{CongestionMode, PathState};
