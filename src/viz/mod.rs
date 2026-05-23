//! 3D 可视化数据采集与导出
//!
//! 在仿真过程中定时采集链路利用率和队列深度，导出 JSON 后用 Python Plotly 渲染。

pub mod data;
pub mod position;
pub mod sampler;

use crate::monitor::SimSummary;
use crate::topology::Topology;

use data::{VizData, VizLink, VizTopology};

pub use data::{LinkSnapshot, VizFrame, VizNode};
pub use position::compute_positions;
pub use sampler::TimeSeriesSampler;

/// 拓扑类型（用于推断 3D 坐标）
#[derive(Debug, Clone)]
pub enum TopoKind {
    Dumbell { hosts_per_side: u32 },
    LeafSpine { n_leaf: u32, n_spine: u32, hosts_per_leaf: u32 },
}

/// 从 Topology + 采样数据构造可导出的 VizData
pub fn build_viz_data(
    topo: &Topology,
    summary: SimSummary,
    frames: Vec<VizFrame>,
    kind: &TopoKind,
) -> VizData {
    let nodes = position::compute_positions(topo, kind);
    let links: Vec<VizLink> = topo
        .links
        .iter()
        .map(|l| VizLink {
            id: l.id,
            from: l.from,
            to: l.to,
            bandwidth_bps: l.bandwidth_bps,
        })
        .collect();

    let topology = VizTopology { nodes, links };
    VizData { topology, time_series: frames, summary }
}