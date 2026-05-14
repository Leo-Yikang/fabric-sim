//! Ring AllReduce 流量模式
//!
//! 经典实现：N 个节点环形排列，每个节点持有一份大小为 M 的张量。
//! Ring AllReduce 共 2*(N-1) 步，每步每个节点发送 M/N 字节给下一个节点。
//!
//! 为简化我们生成"所有节点之间的链状流"：节点 i 在每一步向 (i+1) mod N 发 M/N 字节。

use super::FlowDesc;
use crate::EntityId;

pub struct RingAllReduce {
    pub nodes: Vec<EntityId>,
    pub message_bytes: u64,
    pub start_time_ns: u64,
}

impl RingAllReduce {
    pub fn generate(&self) -> Vec<FlowDesc> {
        let n = self.nodes.len();
        if n < 2 { return Vec::new(); }
        let chunk = self.message_bytes / n as u64;
        let steps = 2 * (n - 1);
        let mut flows = Vec::new();
        let mut fid: u32 = 0;
        for step in 0..steps {
            // 每步各节点同时发送给下一个；我们为了简化把它们的开始时间都设为 start_time_ns
            // （理想 ring 应该是 step k 的发送依赖 step k-1 的接收，这里简化）
            for i in 0..n {
                let src = self.nodes[i];
                let dst = self.nodes[(i + 1) % n];
                flows.push(FlowDesc {
                    flow_id: fid, src, dst,
                    bytes: chunk,
                    start_time_ns: self.start_time_ns + (step as u64) * 1000, // 错开 1us
                });
                fid += 1;
            }
        }
        flows
    }
}
