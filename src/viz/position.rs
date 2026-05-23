//! 3D 节点坐标计算

use super::TopoKind;
use crate::topology::Topology;

use super::data::VizNode;

/// 根据拓扑类型计算所有节点的 3D 坐标
pub fn compute_positions(topo: &Topology, kind: &TopoKind) -> Vec<VizNode> {
    match kind {
        TopoKind::Dumbell { hosts_per_side } => dumbell_positions(topo, *hosts_per_side),
        TopoKind::LeafSpine { n_leaf, n_spine, hosts_per_leaf } => {
            leafspine_positions(topo, *n_leaf, *n_spine, *hosts_per_leaf)
        }
    }
}

fn dumbell_positions(topo: &Topology, hosts_per_side: u32) -> Vec<VizNode> {
    let n = hosts_per_side as usize;
    let switch_left = (2 * n) as u32;
    let switch_right = switch_left + 1;
    let mut nodes = Vec::with_capacity(topo.num_hosts() + topo.num_switches());

    // 左侧主机
    for i in 0..n {
        let y = if n > 1 {
            -2.0 + 4.0 * i as f64 / (n - 1) as f64
        } else {
            0.0
        };
        nodes.push(VizNode {
            id: i as u32,
            label: format!("H{}", i),
            kind: "host".into(),
            x: -4.0,
            y,
            z: 0.0,
        });
    }
    // 右侧主机
    for i in 0..n {
        let host_id = (n + i) as u32;
        let y = if n > 1 {
            -2.0 + 4.0 * i as f64 / (n - 1) as f64
        } else {
            0.0
        };
        nodes.push(VizNode {
            id: host_id,
            label: format!("H{}", host_id),
            kind: "host".into(),
            x: 4.0,
            y,
            z: 0.0,
        });
    }
    // 交换机
    nodes.push(VizNode { id: switch_left, label: "SW-L".into(), kind: "switch".into(), x: -2.0, y: 0.0, z: 0.0 });
    nodes.push(VizNode { id: switch_right, label: "SW-R".into(), kind: "switch".into(), x: 2.0, y: 0.0, z: 0.0 });

    nodes
}

fn leafspine_positions(topo: &Topology, n_leaf: u32, n_spine: u32, hosts_per_leaf: u32) -> Vec<VizNode> {
    let n_hosts = (n_leaf * hosts_per_leaf) as usize;
    let leaf_start = n_hosts as u32;
    let spine_start = leaf_start + n_leaf;
    let mut nodes = Vec::with_capacity(topo.num_hosts() + topo.num_switches());

    // 主机：分布在对应 leaf 下方
    let host_spacing_x = if n_leaf > 1 { 4.0 / (n_leaf - 1) as f64 } else { 1.0 };
    for leaf_i in 0..n_leaf {
        let leaf_x = -2.0 + leaf_i as f64 * host_spacing_x;
        for h in 0..hosts_per_leaf {
            let host_id = leaf_i * hosts_per_leaf + h;
            let y = if hosts_per_leaf > 1 {
                -2.0 + 4.0 * h as f64 / (hosts_per_leaf - 1) as f64
            } else {
                0.0
            };
            nodes.push(VizNode {
                id: host_id,
                label: format!("H{}", host_id),
                kind: "host".into(),
                x: leaf_x,
                y,
                z: -2.0,
            });
        }
    }

    // Leaf 交换机
    for i in 0..n_leaf {
        let x = -2.0 + i as f64 * host_spacing_x;
        nodes.push(VizNode {
            id: leaf_start + i,
            label: format!("Leaf{}", i),
            kind: "switch".into(),
            x,
            y: 0.0,
            z: 0.0,
        });
    }

    // Spine 交换机
    let spine_spacing_x = if n_spine > 1 { 4.0 / (n_spine - 1) as f64 } else { 1.0 };
    for j in 0..n_spine {
        let x = -2.0 + j as f64 * spine_spacing_x;
        nodes.push(VizNode {
            id: spine_start + j,
            label: format!("Spine{}", j),
            kind: "switch".into(),
            x,
            y: 0.0,
            z: 2.0,
        });
    }

    nodes
}