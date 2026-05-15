//! Dumbbell 拓扑生成器
//!
//! 结构：
//! - 两台交换机，中间一条瓶颈链路
//! - 每台交换机下挂 `hosts_per_side` 个主机
//! - 总共 2 * hosts_per_side 个主机
//!
//! Entity ID 编号规则：
//! - hosts_left:  0 .. hosts_per_side
//! - hosts_right: hosts_per_side .. 2*hosts_per_side
//! - switch_left: 2*hosts_per_side
//! - switch_right: 2*hosts_per_side + 1

use super::{HostUplink, Topology};
use crate::network::{Link, LinkRegistry, Switch};
use crate::EntityId;

pub struct Dumbell {
    pub hosts_per_side: u32,
    pub host_link_bps: u64,
    pub bottleneck_link_bps: u64,
    pub prop_delay_ns: u64,
    pub ecn_threshold_bytes: u32,
    pub buffer_bytes: u32,
}

impl Dumbell {
    pub fn build(self) -> Topology {
        let n = self.hosts_per_side as usize;
        let total_hosts = 2 * n;
        let switch_left: EntityId = total_hosts as u32;
        let switch_right: EntityId = switch_left + 1;

        let hosts: Vec<EntityId> = (0..total_hosts as u32).collect();

        let mut links = LinkRegistry::new();
        let mut switches = vec![
            Switch::new(switch_left, self.ecn_threshold_bytes, self.buffer_bytes),
            Switch::new(switch_right, self.ecn_threshold_bytes, self.buffer_bytes),
        ];
        let mut host_uplinks: Vec<HostUplink> = Vec::with_capacity(total_hosts);

        // 主机 ↔ 各自交换机
        // 左侧主机连 switch_left
        for h in 0..n {
            let host_id = h as u32;
            let l_h2s = links.add(Link { id: 0, from: host_id, to: switch_left, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
            let l_s2h = links.add(Link { id: 0, from: switch_left, to: host_id, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
            let port = switches[0].add_port(l_s2h);
            switches[0].routing.add(host_id, port);
            host_uplinks.push(HostUplink { host: host_id, edge_switch: switch_left, link_to_switch: l_h2s, link_to_host: l_s2h });
        }
        // 右侧主机连 switch_right
        for h in 0..n {
            let host_id = (n + h) as u32;
            let l_h2s = links.add(Link { id: 0, from: host_id, to: switch_right, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
            let l_s2h = links.add(Link { id: 0, from: switch_right, to: host_id, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
            let port = switches[1].add_port(l_s2h);
            switches[1].routing.add(host_id, port);
            host_uplinks.push(HostUplink { host: host_id, edge_switch: switch_right, link_to_switch: l_h2s, link_to_host: l_s2h });
        }

        // 瓶颈链路：switch_left ↔ switch_right
        let l_l2r = links.add(Link { id: 0, from: switch_left, to: switch_right, bandwidth_bps: self.bottleneck_link_bps, prop_delay_ns: self.prop_delay_ns });
        let l_r2l = links.add(Link { id: 0, from: switch_right, to: switch_left, bandwidth_bps: self.bottleneck_link_bps, prop_delay_ns: self.prop_delay_ns });

        // switch_left 到右侧所有主机的路由
        let left_port = switches[0].add_port(l_l2r);
        for h in 0..n {
            switches[0].routing.add((n + h) as u32, left_port);
        }

        // switch_right 到左侧所有主机的路由
        let right_port = switches[1].add_port(l_r2l);
        for h in 0..n {
            switches[1].routing.add(h as u32, right_port);
        }

        Topology { hosts, switches, links, host_uplink: host_uplinks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumbell_2_per_side() {
        let topo = Dumbell {
            hosts_per_side: 2,
            host_link_bps: 100_000_000_000,
            bottleneck_link_bps: 40_000_000_000,
            prop_delay_ns: 500,
            ecn_threshold_bytes: 50_000,
            buffer_bytes: 1_000_000,
        }.build();

        assert_eq!(topo.num_hosts(), 4);
        assert_eq!(topo.num_switches(), 2);
        // 链路：4 host * 2 方向 + bottleneck * 2 方向 = 10
        assert_eq!(topo.num_links(), 10);
        assert_eq!(topo.host_uplink.len(), 4);
    }

    #[test]
    fn dumbell_routing_complete() {
        let topo = Dumbell {
            hosts_per_side: 3,
            host_link_bps: 100_000_000_000,
            bottleneck_link_bps: 40_000_000_000,
            prop_delay_ns: 500,
            ecn_threshold_bytes: 50_000,
            buffer_bytes: 1_000_000,
        }.build();

        for sw in &topo.switches {
            for h in 0..topo.num_hosts() as u32 {
                if sw.id == h { continue; }
                assert!(sw.routing.ports_for(h).is_some(), "switch {} 缺少到 host {} 的路由", sw.id, h);
            }
        }
    }
}
