//! 可视化数据结构（serde 序列化）

use serde::Serialize;

use crate::monitor::SimSummary;

#[derive(Serialize)]
pub struct VizData {
    pub topology: VizTopology,
    pub time_series: Vec<VizFrame>,
    pub summary: SimSummary,
}

#[derive(Serialize)]
pub struct VizTopology {
    pub nodes: Vec<VizNode>,
    pub links: Vec<VizLink>,
}

#[derive(Serialize)]
pub struct VizNode {
    pub id: u32,
    pub label: String,
    pub kind: String, // "host" | "switch"
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Serialize)]
pub struct VizLink {
    pub id: u32,
    pub from: u32,
    pub to: u32,
    pub bandwidth_bps: u64,
}

#[derive(Serialize)]
pub struct VizFrame {
    pub time_ns: u64,
    pub links: Vec<LinkSnapshot>,
}

#[derive(Serialize)]
pub struct LinkSnapshot {
    pub utilization: f64,
    pub queue_depth_bytes: u32,
}