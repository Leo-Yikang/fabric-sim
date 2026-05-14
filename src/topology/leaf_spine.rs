//! Leaf-Spine 拓扑生成器
//!
//! 结构：
//! - `n_leaf` 个 Leaf 交换机，每个 Leaf 下挂 `hosts_per_leaf` 个主机
//! - `n_spine` 个 Spine 交换机
//! - 每个 Leaf 与每个 Spine 全互联（共 `n_leaf * n_spine` 条上行链路）
//!
//! Entity ID 编号规则：
//! - hosts:    0 .. n_leaf*hosts_per_leaf
//! - leaves:   n_leaf*hosts_per_leaf .. n_leaf*hosts_per_leaf + n_leaf
//! - spines:   leaves_end .. leaves_end + n_spine

use super::{HostUplink, Topology};
use crate::network::{Link, LinkRegistry, Switch};
use crate::EntityId;

pub struct LeafSpine {
    pub n_leaf: u32,
    pub n_spine: u32,
    pub hosts_per_leaf: u32,
    pub host_link_bps: u64,
    pub fabric_link_bps: u64,
    pub prop_delay_ns: u64,
    pub ecn_threshold_bytes: u32,
    pub buffer_bytes: u32,
}

impl LeafSpine {
    pub fn build(self) -> Topology {
        let LeafSpine { n_leaf, n_spine, hosts_per_leaf, host_link_bps, fabric_link_bps, prop_delay_ns, ecn_threshold_bytes, buffer_bytes } = self;

        let n_hosts = (n_leaf * hosts_per_leaf) as usize;
        let hosts: Vec<EntityId> = (0..n_hosts as u32).collect();
        let leaf_start = n_hosts as u32;
        let spine_start = leaf_start + n_leaf;

        let mut links = LinkRegistry::new();
        let mut switches: Vec<Switch> = Vec::with_capacity((n_leaf + n_spine) as usize);
        let mut host_uplinks: Vec<HostUplink> = Vec::with_capacity(n_hosts);

        // 创建所有交换机
        for i in 0..n_leaf {
            switches.push(Switch::new(leaf_start + i, ecn_threshold_bytes, buffer_bytes));
        }
        for j in 0..n_spine {
            switches.push(Switch::new(spine_start + j, ecn_threshold_bytes, buffer_bytes));
        }

        // 主机 ↔ Leaf
        for leaf_i in 0..n_leaf {
            for h in 0..hosts_per_leaf {
                let host_id = leaf_i * hosts_per_leaf + h;
                let leaf_id = leaf_start + leaf_i;
                let l_h2s = links.add(Link { id: 0, from: host_id, to: leaf_id, bandwidth_bps: host_link_bps, prop_delay_ns });
                let l_s2h = links.add(Link { id: 0, from: leaf_id, to: host_id, bandwidth_bps: host_link_bps, prop_delay_ns });
                // 在 leaf 上加一个出端口，直连这个 host
                let leaf_sw = &mut switches[leaf_i as usize];
                let port = leaf_sw.add_port(l_s2h);
                leaf_sw.routing.add(host_id, port);
                host_uplinks.push(HostUplink { host: host_id, edge_switch: leaf_id, link_to_switch: l_h2s, link_to_host: l_s2h });
            }
        }

        // Leaf ↔ Spine（全互联）
        for leaf_i in 0..n_leaf {
            for spine_j in 0..n_spine {
                let leaf_id = leaf_start + leaf_i;
                let spine_id = spine_start + spine_j;
                let l_l2s = links.add(Link { id: 0, from: leaf_id, to: spine_id, bandwidth_bps: fabric_link_bps, prop_delay_ns });
                let l_s2l = links.add(Link { id: 0, from: spine_id, to: leaf_id, bandwidth_bps: fabric_link_bps, prop_delay_ns });

                // Leaf 上加一个上行端口到这个 Spine：用于"目的不在本 Leaf 下"的所有主机
                let leaf_sw = &mut switches[leaf_i as usize];
                let leaf_up_port = leaf_sw.add_port(l_l2s);
                for other_leaf in 0..n_leaf {
                    if other_leaf == leaf_i { continue; }
                    for h in 0..hosts_per_leaf {
                        let host_id = other_leaf * hosts_per_leaf + h;
                        leaf_sw.routing.add(host_id, leaf_up_port);
                    }
                }
                // Spine 上加一个下行端口到这个 Leaf：用于该 Leaf 下所有主机
                let spine_sw = &mut switches[(n_leaf + spine_j) as usize];
                let spine_down_port = spine_sw.add_port(l_s2l);
                for h in 0..hosts_per_leaf {
                    let host_id = leaf_i * hosts_per_leaf + h;
                    spine_sw.routing.add(host_id, spine_down_port);
                }
            }
        }

        Topology { hosts, switches, links, host_uplink: host_uplinks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_spine_4x2_8hosts() {
        // 4 leaves, 2 spines, 2 hosts per leaf = 8 hosts
        let topo = LeafSpine {
            n_leaf: 4, n_spine: 2, hosts_per_leaf: 2,
            host_link_bps: 100_000_000_000,
            fabric_link_bps: 400_000_000_000,
            prop_delay_ns: 500,
            ecn_threshold_bytes: 50_000,
            buffer_bytes: 1_000_000,
        }.build();

        assert_eq!(topo.num_hosts(), 8);
        assert_eq!(topo.num_switches(), 6); // 4 leaf + 2 spine
        // 链路：8 host*2 (双向) + 4 leaf * 2 spine * 2 = 16 + 16 = 32
        assert_eq!(topo.num_links(), 32);
    }

    #[test]
    fn leaf_spine_routing_is_complete() {
        let topo = LeafSpine {
            n_leaf: 2, n_spine: 2, hosts_per_leaf: 2,
            host_link_bps: 100_000_000_000,
            fabric_link_bps: 400_000_000_000,
            prop_delay_ns: 500,
            ecn_threshold_bytes: 50_000,
            buffer_bytes: 1_000_000,
        }.build();

        // 每个 leaf 都能路由到所有 host
        for sw in &topo.switches {
            for h in 0..topo.num_hosts() as u32 {
                if sw.id == h { continue; }
                let ports = sw.routing.ports_for(h);
                assert!(ports.is_some(), "switch {} 缺少到 host {} 的路由", sw.id, h);
            }
        }
        // Leaf 到非本地主机应该有多个等价路径（多 spine）
        let leaf0 = &topo.switches[0];
        // 假设 host 0/1 在 leaf 0 下；host 2/3 在 leaf 1 下
        // leaf 0 到 host 2 的路径应该有 2 条（2 spine）
        let ports = leaf0.routing.ports_for(2).unwrap();
        assert_eq!(ports.len(), 2, "leaf 0 → host 2 应有 2 条等价路径");
    }
}
