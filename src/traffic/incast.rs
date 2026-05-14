//! Incast 多对一流量模式

use super::FlowDesc;
use crate::EntityId;

pub struct Incast {
    pub senders: Vec<EntityId>,
    pub receiver: EntityId,
    pub bytes_per_sender: u64,
    pub start_time_ns: u64,
}

impl Incast {
    pub fn generate(&self) -> Vec<FlowDesc> {
        // flow_id 需要全局唯一（全部在仿真中可区别）
        // 这里用递增序号，从 1 开始避免 与缺省 0 冲突
        self.senders.iter().enumerate().map(|(i, &src)| FlowDesc {
            flow_id: (i + 1) as u32,
            src, dst: self.receiver,
            bytes: self.bytes_per_sender,
            start_time_ns: self.start_time_ns,
        }).collect()
    }
}
