//! AllToAll 流量：每对节点之间各发一条流

use super::FlowDesc;
use crate::EntityId;

pub struct AllToAll {
    pub nodes: Vec<EntityId>,
    pub bytes_per_pair: u64,
    pub start_time_ns: u64,
}

impl AllToAll {
    pub fn generate(&self) -> Vec<FlowDesc> {
        let mut flows = Vec::new();
        let mut fid: u32 = 0;
        for &src in &self.nodes {
            for &dst in &self.nodes {
                if src == dst { continue; }
                flows.push(FlowDesc { flow_id: fid, src, dst, bytes: self.bytes_per_pair, start_time_ns: self.start_time_ns });
                fid += 1;
            }
        }
        flows
    }
}
