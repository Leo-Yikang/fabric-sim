//! 交换机模型（P3 扩展：优先级队列 + PFC）
//!
//! 核心行为：
//! - 多个出端口（egress port），每个支持多优先级 FIFO 队列
//! - 队列字节数超过 ECN 阈值 → 给出队包打 ECN 标记
//! - 超过 buffer 上限 → 丢包
//! - 路由表：根据目的 entity 给出 (egress_port_list)
//! - PFC（Priority Flow Control）：高优先级队列满时向上游发送 pause 帧

use super::packet::Packet;
use crate::EntityId;
use std::collections::{HashMap, VecDeque};

/// 优先级数量（参考 RoCE：通常为 2~8 个 TC/优先级）
pub const NUM_PRIORITIES: usize = 2;
/// 高优先级索引（0 = 最高）
pub const HIGH_PRIORITY: usize = 0;
/// 低优先级索引
pub const LOW_PRIORITY: usize = 1;

pub type PortId = u8;

#[derive(Default, Debug, Clone)]
pub struct RoutingTable {
    table: HashMap<EntityId, Vec<PortId>>,
}

impl RoutingTable {
    pub fn new() -> Self { Self { table: HashMap::new() } }
    pub fn add(&mut self, dst: EntityId, port: PortId) {
        self.table.entry(dst).or_default().push(port);
    }
    pub fn ports_for(&self, dst: EntityId) -> Option<&[PortId]> {
        self.table.get(&dst).map(|v| v.as_slice())
    }
}

#[derive(Debug, Clone)]
pub struct PriorityQueue {
    pub queue: VecDeque<Packet>,
    pub queue_bytes: u32,
    /// PFC pause 状态：true 表示该优先级队列被上游 pause
    pub paused: bool,
    /// PFC 阈值（超过此值发送 pause）
    pub pfc_threshold_bytes: u32,
}

impl PriorityQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            queue_bytes: 0,
            paused: false,
            pfc_threshold_bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SwitchPort {
    pub id: PortId,
    pub link_id: u32,
    /// P3：多优先级队列（索引 0 = 最高优先级）
    pub priority_queues: Vec<PriorityQueue>,
    /// 所有优先级队列总字节数
    pub queue_bytes: u32,
    pub busy_until: u64,
    pub max_queue_depth_seen: u32,
    /// 是否已有 PacketDepart 事件在等待处理该端口
    pub egress_pending: bool,
}

impl SwitchPort {
    pub fn new(id: PortId, link_id: u32) -> Self {
        let mut priority_queues = Vec::with_capacity(NUM_PRIORITIES);
        for _ in 0..NUM_PRIORITIES {
            priority_queues.push(PriorityQueue::new());
        }
        Self {
            id,
            link_id,
            priority_queues,
            queue_bytes: 0,
            busy_until: 0,
            max_queue_depth_seen: 0,
            egress_pending: false,
        }
    }

    /// P3：从最高优先级非空队列出队
    pub fn dequeue_priority(&mut self) -> Option<(Packet, usize)> {
        for pri in 0..NUM_PRIORITIES {
            if let Some(pkt) = self.priority_queues[pri].queue.pop_front() {
                self.priority_queues[pri].queue_bytes = self.priority_queues[pri].queue_bytes.saturating_sub(pkt.size);
                self.queue_bytes = self.queue_bytes.saturating_sub(pkt.size);
                return Some((pkt, pri));
            }
        }
        None
    }

    /// P3：入队到指定优先级，自动更新 per-priority queue_bytes
    pub fn enqueue_priority(&mut self, pkt: Packet, priority: usize) {
        let pri = priority.min(NUM_PRIORITIES - 1);
        let sz = pkt.size;
        self.priority_queues[pri].queue_bytes += sz;
        self.priority_queues[pri].queue.push_back(pkt);
    }

    /// P3：检查某优先级队列是否触发 PFC（超过阈值且未暂停时）
    pub fn check_pfc(&mut self, priority: usize) -> bool {
        if priority >= NUM_PRIORITIES {
            return false;
        }
        let pq = &self.priority_queues[priority];
        let threshold = pq.pfc_threshold_bytes;
        if threshold > 0 && pq.queue_bytes >= threshold && !pq.paused {
            self.priority_queues[priority].paused = true;
            return true;
        }
        false
    }

    /// P3：检查是否可以发送 PFC resume（队列降至阈值以下且仍暂停）
    pub fn check_pfc_resume(&mut self, priority: usize) -> bool {
        if priority >= NUM_PRIORITIES {
            return false;
        }
        let pq = &self.priority_queues[priority];
        let resume_threshold = pq.pfc_threshold_bytes / 2;
        if pq.paused && pq.queue_bytes <= resume_threshold {
            self.priority_queues[priority].paused = false;
            return true;
        }
        false
    }

    /// P3：跳过被 pause 的高优先级队列，从下一个非空队列出队
    pub fn dequeue_priority_skip_paused(&mut self) -> Option<(Packet, usize)> {
        for pri in 0..NUM_PRIORITIES {
            if self.priority_queues[pri].paused {
                continue; // PFC paused — 跳过该优先级
            }
            if let Some(pkt) = self.priority_queues[pri].queue.pop_front() {
                self.priority_queues[pri].queue_bytes = self.priority_queues[pri].queue_bytes.saturating_sub(pkt.size);
                self.queue_bytes = self.queue_bytes.saturating_sub(pkt.size);
                return Some((pkt, pri));
            }
        }
        None
    }
}

pub struct Switch {
    pub id: EntityId,
    pub ports: Vec<SwitchPort>,
    pub ecn_threshold_bytes: u32,
    pub buffer_max_bytes: u32,
    pub routing: RoutingTable,
    pub drops: u64,
    pub ecn_marks: u64,
    /// P3：PFC pause 计数
    pub pfc_pause_sent: u64,
    /// P3：PFC resume 计数
    pub pfc_resume_sent: u64,
}

impl Switch {
    pub fn new(id: EntityId, ecn_threshold_bytes: u32, buffer_max_bytes: u32) -> Self {
        Self {
            id,
            ports: Vec::new(),
            ecn_threshold_bytes,
            buffer_max_bytes,
            routing: RoutingTable::new(),
            drops: 0,
            ecn_marks: 0,
            pfc_pause_sent: 0,
            pfc_resume_sent: 0,
        }
    }
    pub fn add_port(&mut self, link_id: u32) -> PortId {
        let pid = self.ports.len() as PortId;
        self.ports.push(SwitchPort::new(pid, link_id));
        pid
    }

    /// 入包处理：选择出端口、判 ECN、判丢包、入队
    /// 返回：(选择的出端口, 是否被丢弃)
    pub fn ingress(&mut self, mut pkt: Packet, hash_key: u32) -> (Option<PortId>, bool) {
        let chosen = {
            let ports = match self.routing.ports_for(pkt.dst) {
                Some(p) if !p.is_empty() => p,
                _ => {
                    self.drops += 1;
                    return (None, true);
                }
            };
            let idx = if pkt.routing_tag > 0 && ((pkt.routing_tag as usize) - 1) < ports.len() {
                (pkt.routing_tag as usize) - 1
            } else {
                (hash_key as usize) % ports.len()
            };
            ports[idx]
        };

        let pkt_size = pkt.size;
        let port = &mut self.ports[chosen as usize];

        if port.queue_bytes + pkt_size > self.buffer_max_bytes {
            self.drops += 1;
            return (Some(chosen), true);
        }
        if port.queue_bytes + pkt_size > self.ecn_threshold_bytes {
            pkt.ecn = true;
            self.ecn_marks += 1;
        }
        // P3：根据 routing_tag 的 high bits 选择优先级
        // routing_tag > 128 表示高优先级（临时约定，后续可改为显式 priority 字段）
        let priority = if pkt.routing_tag >= 128 { HIGH_PRIORITY } else { LOW_PRIORITY };
        port.enqueue_priority(pkt, priority);
        port.queue_bytes += pkt_size;
        if port.queue_bytes > port.max_queue_depth_seen {
            port.max_queue_depth_seen = port.queue_bytes;
        }

        // P3：检查 PFC
        if port.check_pfc(priority) {
            self.pfc_pause_sent += 1;
        }

        (Some(chosen), false)
    }

    pub fn dequeue(&mut self, port_id: PortId) -> Option<Packet> {
        let port = &mut self.ports[port_id as usize];
        let result = port.dequeue_priority_skip_paused().map(|(pkt, pri)| {
            // PFC resume 检查：出队后若队列降至阈值以下，触发 resume
            if port.check_pfc_resume(pri) {
                // resume 信号由上层处理
            }
            pkt
        });
        result
    }

    pub fn port(&self, port_id: PortId) -> &SwitchPort { &self.ports[port_id as usize] }
    pub fn port_mut(&mut self, port_id: PortId) -> &mut SwitchPort { &mut self.ports[port_id as usize] }

    /// 所有端口的总排队字节数（shared buffer 视图）
    pub fn total_queue_bytes(&self) -> u32 {
        self.ports.iter().map(|p| p.queue_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::Packet;

    #[test]
    fn no_route_drops() {
        let mut sw = Switch::new(0, 1_000_000, 2_000_000);
        sw.add_port(0);
        let pkt = Packet::data(1, 0, 0, 0, 100, 999, 0);
        let (_, dropped) = sw.ingress(pkt, 0);
        assert!(dropped);
        assert_eq!(sw.drops, 1);
    }

    #[test]
    fn ecn_marks_above_threshold() {
        let mut sw = Switch::new(0, 1024, 10_240);
        let port = sw.add_port(0);
        sw.routing.add(999, port);
        let pkt1 = Packet::data(1, 0, 0, 0, 100, 999, 0);
        let (_, d1) = sw.ingress(pkt1, 0);
        assert!(!d1);
        assert_eq!(sw.ecn_marks, 0);
        let pkt2 = Packet::data(2, 0, 0, 1, 100, 999, 0);
        let (_, d2) = sw.ingress(pkt2, 0);
        assert!(!d2);
        assert_eq!(sw.ecn_marks, 1);
        assert!(sw.ports[0].priority_queues[LOW_PRIORITY].queue.back().unwrap().ecn);
    }

    #[test]
    fn drops_when_buffer_full() {
        let mut sw = Switch::new(0, 1024, 2048);
        let p = sw.add_port(0);
        sw.routing.add(999, p);
        for i in 0..5 {
            let pkt = Packet::data(i, 0, 0, i as u32, 100, 999, 0);
            sw.ingress(pkt, 0);
        }
        assert!(sw.drops > 0);
    }

    #[test]
    fn ecmp_hash_distributes() {
        let mut sw = Switch::new(0, 1_000_000, 10_000_000);
        let p1 = sw.add_port(10);
        let p2 = sw.add_port(11);
        let p3 = sw.add_port(12);
        sw.routing.add(999, p1);
        sw.routing.add(999, p2);
        sw.routing.add(999, p3);
        let mut counts = [0u32; 3];
        for i in 0..300u32 {
            let pkt = Packet::data(i as u64, 0, 0, i, 100, 999, 0);
            let (chosen, _) = sw.ingress(pkt, i);
            counts[chosen.unwrap() as usize] += 1;
        }
        for c in counts.iter() { assert!(*c > 50); }
    }

    #[test]
    fn routing_tag_overrides_ecmp() {
        let mut sw = Switch::new(0, 1_000_000, 10_000_000);
        let p1 = sw.add_port(10);
        let p2 = sw.add_port(11);
        let p3 = sw.add_port(12);
        sw.routing.add(999, p1);
        sw.routing.add(999, p2);
        sw.routing.add(999, p3);
        // routing_tag=2 → 强制走第二个端口（p2）
        let mut pkt = Packet::data(1, 0, 0, 0, 100, 999, 0);
        pkt.routing_tag = 2;
        let (chosen, _) = sw.ingress(pkt, 9999);
        assert_eq!(chosen.unwrap(), p2);
    }
}
