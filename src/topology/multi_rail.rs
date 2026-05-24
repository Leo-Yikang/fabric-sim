//! 多 NIC / 多 Rail 拓扑
//!
//! 现代 AI 训练节点通常 8×GPU + 8×NIC，每个 NIC 连接不同的 Rail（spine 平面）。
//! 本模块在 LeafSpine 基础上扩展，支持每个 host 拥有多个 NIC，按 Rail 隔离。

use super::{HostUplink, Topology};
use crate::network::{Link, LinkRegistry, Switch};
use crate::EntityId;

pub struct MultiRailLeafSpine {
    pub n_leaf: u32,
    pub n_spine: u32,
    pub hosts_per_leaf: u32,
    pub nics_per_host: u32,
    pub host_link_bps: u64,
    pub fabric_link_bps: u64,
    pub prop_delay_ns: u64,
    pub ecn_threshold_bytes: u32,
    pub buffer_bytes: u32,
}

impl MultiRailLeafSpine {
    pub fn build(self) -> Topology {
        let MultiRailLeafSpine {
            n_leaf, n_spine, hosts_per_leaf, nics_per_host,
            host_link_bps, fabric_link_bps, prop_delay_ns,
            ecn_threshold_bytes, buffer_bytes,
        } = self;

        let total_hosts = (n_leaf * hosts_per_leaf * nics_per_host) as usize;
        let leaf_start = total_hosts as u32;
        let spine_start = leaf_start + n_leaf;

        let hosts: Vec<EntityId> = (0..total_hosts as u32).collect();

        let mut links = LinkRegistry::new();
        let mut switches = Vec::new();
        let mut host_uplink = Vec::with_capacity(total_hosts);

        // 创建 leaf switches
        for lid in 0..n_leaf {
            let sid = leaf_start + lid;
            switches.push(Switch::new(sid, ecn_threshold_bytes, buffer_bytes));
        }

        // 创建 spine switches
        for sid in 0..n_spine {
            switches.push(Switch::new(spine_start + sid, ecn_threshold_bytes, buffer_bytes));
        }

        // 叶子 → 主干 全互联
        for lid in 0..n_leaf {
            for sid in 0..n_spine {
                let leaf_id = leaf_start + lid;
                let spine_id = spine_start + sid;

                let leaf_port = switches[lid as usize].add_port(links.len() as u32);
                let spine_port = switches[(n_leaf + sid) as usize].add_port(links.len() as u32);

                let l1 = links.add(Link {
                    id: 0, from: leaf_id, to: spine_id,
                    bandwidth_bps: fabric_link_bps, prop_delay_ns,
                });
                let l2 = links.add(Link {
                    id: 0, from: spine_id, to: leaf_id,
                    bandwidth_bps: fabric_link_bps, prop_delay_ns,
                });

                switches[lid as usize].routing.add(spine_id, leaf_port);
                switches[(n_leaf + sid) as usize].routing.add(leaf_id, spine_port);
            }
        }

        // 主机 → 叶子 连接（每个 NIC/host 占一个 entity slot）
        for lid in 0..n_leaf {
            let leaf_sw = &mut switches[lid as usize];
            let leaf_id = leaf_start + lid;

            for hid in 0..hosts_per_leaf {
                for nic in 0..nics_per_host {
                    let host_idx = (lid * hosts_per_leaf + hid) * nics_per_host + nic;
                    let host_id = host_idx as u32;

                    let up_port = leaf_sw.add_port(links.len() as u32);
                    let down_port = leaf_sw.add_port(links.len() as u32);

                    let l_up = links.add(Link {
                        id: 0, from: host_id, to: leaf_id,
                        bandwidth_bps: host_link_bps, prop_delay_ns,
                    });
                    let l_down = links.add(Link {
                        id: 0, from: leaf_id, to: host_id,
                        bandwidth_bps: host_link_bps, prop_delay_ns,
                    });

                    leaf_sw.routing.add(host_id, down_port);
                    host_uplink.push(HostUplink {
                        host: host_id,
                        edge_switch: leaf_id,
                        link_to_switch: l_up,
                        link_to_host: l_down,
                    });
                }
            }
        }

        // 为主干交换机添加到所有叶交换机的路由
        for sid in 0..n_spine {
            let spine_sw = &mut switches[(n_leaf + sid) as usize];
            for lid in 0..n_leaf {
                let leaf_id = leaf_start + lid;
                // 叶子可能已有端口（从上一步全互联添加），这里只补充路由
                for port in 0..spine_sw.ports.len() {
                    // 每个 spine-leaf 对之间有链路，路由应已完备
                }
                // 确保所有叶子的路由在 spine 中存在
                if spine_sw.routing.ports_for(leaf_id).is_none() {
                    // 查找连接该叶子的端口
                    for p in 0..spine_sw.ports.len() as u8 {
                        let lid = spine_sw.ports[p as usize].link_id;
                        if links.get(lid).to == leaf_id {
                            spine_sw.routing.add(leaf_id, p);
                            break;
                        }
                    }
                }
            }
        }

        Topology { hosts, switches, links, host_uplink }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_rail_creates_nics_per_host() {
        let topo = MultiRailLeafSpine {
            n_leaf: 2, n_spine: 2, hosts_per_leaf: 2, nics_per_host: 2,
            host_link_bps: 100_000_000_000, fabric_link_bps: 100_000_000_000,
            prop_delay_ns: 500, ecn_threshold_bytes: 20_000, buffer_bytes: 200_000,
        }.build();
        // 2 leaf × 2 hosts × 2 NICs = 8 hosts
        assert_eq!(topo.hosts.len(), 8);
        assert_eq!(topo.host_uplink.len(), 8);
    }
}