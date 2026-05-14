//! k-ary Fat-Tree 拓扑生成器
//!
//! 三层结构：
//! - core:  (k/2)² 个核心交换机
//! - agg:   k 个 pod，每 pod k/2 个汇聚
//! - edge:  k 个 pod，每 pod k/2 个边缘，每边缘下挂 k/2 个 host
//! - 总主机数：k³/4
//!
//! 本模块提供一个简化实现：支持任意偶数 k，路由表用"每跳两段 ECMP"风格：
//! - edge → 本 pod 内其他 host：直接走对应 edge 出端口
//! - edge → 外 pod host：走上行到本 pod 的 k/2 个 agg 之一（ECMP）
//! - agg → 上行到 (k/2) 个 core 之一（ECMP）
//! - core → 下行到目的 pod 的对应 agg
//! - agg → 下行到对应 edge
//! - edge → 下行到对应 host

use super::{HostUplink, Topology};
use crate::network::{Link, LinkRegistry, Switch};
use crate::EntityId;

pub struct FatTree {
    pub k: u32, // 必须为偶数
    pub host_link_bps: u64,
    pub fabric_link_bps: u64,
    pub prop_delay_ns: u64,
    pub ecn_threshold_bytes: u32,
    pub buffer_bytes: u32,
}

impl FatTree {
    pub fn build(self) -> Topology {
        assert!(self.k % 2 == 0, "k 必须为偶数");
        let k = self.k as usize;
        let k_half = k / 2;
        let n_hosts = (k * k * k) / 4;
        let n_edge = k * k_half;     // 每 pod k/2
        let n_agg = k * k_half;
        let n_core = k_half * k_half;

        let hosts: Vec<EntityId> = (0..n_hosts as u32).collect();
        let edge_start = n_hosts as u32;
        let agg_start = edge_start + n_edge as u32;
        let core_start = agg_start + n_agg as u32;

        let mut links = LinkRegistry::new();
        let mut switches: Vec<Switch> = Vec::new();
        let mut host_uplinks: Vec<HostUplink> = Vec::new();

        let mk_sw = |idx: u32, ecn, buf| Switch::new(idx, ecn, buf);
        for i in 0..n_edge { switches.push(mk_sw(edge_start + i as u32, self.ecn_threshold_bytes, self.buffer_bytes)); }
        for i in 0..n_agg { switches.push(mk_sw(agg_start + i as u32, self.ecn_threshold_bytes, self.buffer_bytes)); }
        for i in 0..n_core { switches.push(mk_sw(core_start + i as u32, self.ecn_threshold_bytes, self.buffer_bytes)); }

        // 主机 ↔ Edge
        for pod in 0..k {
            for e in 0..k_half {
                let edge_idx_global = pod * k_half + e; // 0..n_edge
                let edge_id = edge_start + edge_idx_global as u32;
                for h in 0..k_half {
                    let host_id = (pod * k_half * k_half + e * k_half + h) as u32;
                    let l_h2s = links.add(Link { id: 0, from: host_id, to: edge_id, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
                    let l_s2h = links.add(Link { id: 0, from: edge_id, to: host_id, bandwidth_bps: self.host_link_bps, prop_delay_ns: self.prop_delay_ns });
                    let edge_sw = &mut switches[edge_idx_global];
                    let port = edge_sw.add_port(l_s2h);
                    edge_sw.routing.add(host_id, port);
                    host_uplinks.push(HostUplink { host: host_id, edge_switch: edge_id, link_to_switch: l_h2s, link_to_host: l_s2h });
                }
            }
        }

        // Edge ↔ Agg（同 pod 全互联）
        // Edge 的上行端口：连本 pod 的 k/2 个 agg
        for pod in 0..k {
            for e in 0..k_half {
                let edge_idx = pod * k_half + e;
                for a in 0..k_half {
                    let agg_idx = pod * k_half + a;
                    let edge_id = edge_start + edge_idx as u32;
                    let agg_id = agg_start + agg_idx as u32;
                    let l_e2a = links.add(Link { id: 0, from: edge_id, to: agg_id, bandwidth_bps: self.fabric_link_bps, prop_delay_ns: self.prop_delay_ns });
                    let l_a2e = links.add(Link { id: 0, from: agg_id, to: edge_id, bandwidth_bps: self.fabric_link_bps, prop_delay_ns: self.prop_delay_ns });

                    // Edge 上行端口（到 agg）：用于发往非本 edge 下挂主机的包
                    let edge_sw_idx = edge_idx;
                    let edge_sw = &mut switches[edge_sw_idx];
                    let up_port = edge_sw.add_port(l_e2a);
                    // 路由：所有非本 edge 下挂的主机
                    for tpod in 0..k {
                        for te in 0..k_half {
                            if tpod == pod && te == e { continue; }
                            for th in 0..k_half {
                                let tgt = (tpod * k_half * k_half + te * k_half + th) as u32;
                                edge_sw.routing.add(tgt, up_port);
                            }
                        }
                    }

                    // Agg 下行到本 edge：用于发往本 edge 下挂的主机
                    let agg_sw_idx = n_edge + agg_idx;
                    let agg_sw = &mut switches[agg_sw_idx];
                    let down_port = agg_sw.add_port(l_a2e);
                    for th in 0..k_half {
                        let tgt = (pod * k_half * k_half + e * k_half + th) as u32;
                        agg_sw.routing.add(tgt, down_port);
                    }
                }
            }
        }

        // Agg ↔ Core
        // 每个 agg 上行到 k/2 个 core；具体绑定规则：agg index a (0..k/2) in a pod 连 core[a*k/2 .. a*k/2 + k/2]
        for pod in 0..k {
            for a in 0..k_half {
                let agg_idx = pod * k_half + a;
                let agg_id = agg_start + agg_idx as u32;
                for c_off in 0..k_half {
                    let core_idx = a * k_half + c_off;
                    let core_id = core_start + core_idx as u32;
                    let l_a2c = links.add(Link { id: 0, from: agg_id, to: core_id, bandwidth_bps: self.fabric_link_bps, prop_delay_ns: self.prop_delay_ns });
                    let l_c2a = links.add(Link { id: 0, from: core_id, to: agg_id, bandwidth_bps: self.fabric_link_bps, prop_delay_ns: self.prop_delay_ns });

                    // Agg 上行端口（到 core）：用于发往非本 pod 主机
                    let agg_sw = &mut switches[n_edge + agg_idx];
                    let up = agg_sw.add_port(l_a2c);
                    for tpod in 0..k {
                        if tpod == pod { continue; }
                        for te in 0..k_half {
                            for th in 0..k_half {
                                let tgt = (tpod * k_half * k_half + te * k_half + th) as u32;
                                agg_sw.routing.add(tgt, up);
                            }
                        }
                    }

                    // Core 下行端口到这个 agg：用于发往该 pod 中 agg 对应的"agg 列"管辖的主机
                    // 简化处理：让 core 把"目的在该 pod"的所有包都发到该 pod 内的所有 agg（ECMP）
                    let core_sw = &mut switches[n_edge + n_agg + core_idx];
                    let down = core_sw.add_port(l_c2a);
                    for te in 0..k_half {
                        for th in 0..k_half {
                            let tgt = (pod * k_half * k_half + te * k_half + th) as u32;
                            core_sw.routing.add(tgt, down);
                        }
                    }
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
    fn fat_tree_k4_has_16_hosts() {
        let topo = FatTree {
            k: 4, host_link_bps: 100_000_000_000, fabric_link_bps: 100_000_000_000,
            prop_delay_ns: 500, ecn_threshold_bytes: 50_000, buffer_bytes: 1_000_000,
        }.build();
        assert_eq!(topo.num_hosts(), 16); // 4^3/4 = 16
        // 4 pods * 2 edge + 4 pods * 2 agg + 4 core = 8 + 8 + 4 = 20
        assert_eq!(topo.num_switches(), 20);
        // 主机层：16*2 = 32；pod内 edge-agg：4*(2*2)*2 = 32；agg-core：4*2*2*2 = 32；total = 96
        assert_eq!(topo.num_links(), 96);
    }

    #[test]
    fn fat_tree_routing_complete() {
        let topo = FatTree { k: 4, host_link_bps: 100_000_000_000, fabric_link_bps: 100_000_000_000, prop_delay_ns: 500, ecn_threshold_bytes: 50_000, buffer_bytes: 1_000_000 }.build();
        for sw in &topo.switches {
            for h in 0..topo.num_hosts() as u32 {
                if sw.routing.ports_for(h).is_none() {
                    // 允许某些 host 没路由的情况（例如 edge 不直连其他 pod 的 host）→ 实际上应通过上行端口覆盖
                    // 我们的实现里所有 switch 都给所有非本地 host 加了路由，这里严格断言
                    if sw.id != h {
                        panic!("交换机 {} 缺少到主机 {} 的路由", sw.id, h);
                    }
                }
            }
        }
    }
}
