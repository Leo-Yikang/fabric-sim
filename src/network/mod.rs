//! 网络拓扑与物理层抽象（阶段二）
//!
//! - `packet`: Packet 数据结构（含 ECN、SACK 标志）
//! - `link`:   双向链路（带宽 + 传播延迟）
//! - `switch`: 多端口交换机，Ingress/Egress FIFO + ECN 标记 + 路由

pub mod packet;
pub mod link;
pub mod switch;
pub mod host_delay;
pub mod drop;

pub use packet::{Packet, PacketKind, FlowId, SeqNum, PacketId, MTU_BYTES};
pub use link::{Link, LinkId, LinkRegistry};
pub use switch::{Switch, SwitchPort, PortId, RoutingTable};
pub use host_delay::{NodeTopology, GpuLink};
pub use drop::{DropReason, DropEvent, DropCounters, PerFlowDrops};
