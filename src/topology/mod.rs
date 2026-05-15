//! 拓扑生成器
//!
//! - `Dumbell`：Dumbbell 拓扑（两台交换机 + 瓶颈链路，经典拥塞控制实验拓扑）
//! - `LeafSpine`：两层 Leaf-Spine（典型 AI 集群拓扑）
//! - `FatTree`：k-ary Fat-Tree（教科书拓扑）
//!
//! 两者都返回 `Topology` 结构，包含：主机列表、交换机列表、链路列表，
//! 以及每个交换机已配置好的路由表。

pub mod leaf_spine;
pub mod fat_tree;
pub mod dumbell;

pub use leaf_spine::LeafSpine;
pub use fat_tree::FatTree;
pub use dumbell::Dumbell;

use crate::network::{LinkRegistry, Switch};
use crate::EntityId;

/// 拓扑的最终产物
pub struct Topology {
    /// 主机 EntityId（从 0 开始连续编号）
    pub hosts: Vec<EntityId>,
    /// 交换机 EntityId（接在主机之后编号）
    pub switches: Vec<Switch>,
    /// 全部链路
    pub links: LinkRegistry,
    /// host -> 它连接的 (switch_entity, link_to_switch, link_back_to_host)
    pub host_uplink: Vec<HostUplink>,
}

#[derive(Clone, Copy, Debug)]
pub struct HostUplink {
    pub host: EntityId,
    pub edge_switch: EntityId,
    pub link_to_switch: u32,
    pub link_to_host: u32,//？多余？
}

impl Topology {
    pub fn num_hosts(&self) -> usize { self.hosts.len() }
    pub fn num_switches(&self) -> usize { self.switches.len() }
    pub fn num_links(&self) -> usize { self.links.len() }
}
