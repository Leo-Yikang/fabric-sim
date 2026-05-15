//! 交换机模型
//!
//! 简化但符合数据中心交换机核心行为：
//! - 多个出端口（egress port），每个有 FIFO 队列
//! - 队列字节数超过 ECN 阈值 → 给出队包打 ECN 标记
//! - 超过 buffer 上限 → 丢包
//! - 路由表：根据目的 entity 给出 (egress_port_list)；多路径用 ECMP 或 STrack spraying

use super::packet::Packet;
use crate::EntityId;
use std::collections::{HashMap, VecDeque};

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
pub struct SwitchPort {
    pub id: PortId,
    pub link_id: u32,
    pub queue: VecDeque<Packet>,
    pub queue_bytes: u32,
    pub busy_until: u64,
    pub max_queue_depth_seen: u32,
    /// 是否已有 PacketDepart 事件在等待处理该端口（防止重复调度导致事件风暴）
    pub egress_pending: bool,
}

impl SwitchPort {
    pub fn new(id: PortId, link_id: u32) -> Self {
        Self { id, link_id, queue: VecDeque::new(), queue_bytes: 0, busy_until: 0, max_queue_depth_seen: 0, egress_pending: false }
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
}

impl Switch {
    pub fn new(id: EntityId, ecn_threshold_bytes: u32, buffer_max_bytes: u32) -> Self {
        Self { id, ports: Vec::new(), ecn_threshold_bytes, buffer_max_bytes, routing: RoutingTable::new(), drops: 0, ecn_marks: 0 }
    }
    pub fn add_port(&mut self, link_id: u32) -> PortId {
        let pid = self.ports.len() as PortId;
        self.ports.push(SwitchPort::new(pid, link_id));
        pid
    }

    /// 入包处理：选择出端口、判 ECN、判丢包、入队
    /// 返回：(选择的出端口, 是否被丢弃)
    pub fn ingress(&mut self, mut pkt: Packet, hash_key: u32) -> (Option<PortId>, bool) {
        let ports: Vec<PortId> = match self.routing.ports_for(pkt.dst) {
            Some(p) if !p.is_empty() => p.to_vec(),
            _ => {
                self.drops += 1;
                return (None, true);
            }
        };

        let chosen = if pkt.routing_tag > 0 && ((pkt.routing_tag as usize) - 1) < ports.len() {
            ports[(pkt.routing_tag as usize) - 1]
        } else {
            ports[(hash_key as usize) % ports.len()]
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
        port.queue.push_back(pkt);
        port.queue_bytes += pkt_size;
        if port.queue_bytes > port.max_queue_depth_seen {
            port.max_queue_depth_seen = port.queue_bytes;
        }
        (Some(chosen), false)
    }

    pub fn dequeue(&mut self, port_id: PortId) -> Option<Packet> {
        let port = &mut self.ports[port_id as usize];
        if let Some(pkt) = port.queue.pop_front() {
            port.queue_bytes = port.queue_bytes.saturating_sub(pkt.size);
            Some(pkt)
        } else {
            None
        }
    }

    pub fn port(&self, port_id: PortId) -> &SwitchPort { &self.ports[port_id as usize] }
    pub fn port_mut(&mut self, port_id: PortId) -> &mut SwitchPort { &mut self.ports[port_id as usize] }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::packet::Packet;

    #[test]
    fn no_route_drops() {
        let mut sw = Switch::new(0, 1_000_000, 2_000_000);
        sw.add_port(0);
        let pkt = Packet::data(1, 0, 0, 100, 999, 0);
        let (_, dropped) = sw.ingress(pkt, 0);
        assert!(dropped);
        assert_eq!(sw.drops, 1);
    }

    #[test]
    fn ecn_marks_above_threshold() {
        let mut sw = Switch::new(0, 1024, 10_240);
        let port = sw.add_port(0);
        sw.routing.add(999, port);
        let pkt1 = Packet::data(1, 0, 0, 100, 999, 0);
        let (_, d1) = sw.ingress(pkt1, 0);
        assert!(!d1);
        assert_eq!(sw.ecn_marks, 0);
        let pkt2 = Packet::data(2, 0, 1, 100, 999, 0);
        let (_, d2) = sw.ingress(pkt2, 0);
        assert!(!d2);
        assert_eq!(sw.ecn_marks, 1);
        assert!(sw.ports[0].queue.back().unwrap().ecn);
    }

    #[test]
    fn drops_when_buffer_full() {
        let mut sw = Switch::new(0, 1024, 2048);
        let p = sw.add_port(0);
        sw.routing.add(999, p);
        for i in 0..5 {
            let pkt = Packet::data(i, 0, i as u32, 100, 999, 0);
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
            let pkt = Packet::data(i as u64, 0, i, 100, 999, 0);
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
        let mut pkt = Packet::data(1, 0, 0, 100, 999, 0);
        pkt.routing_tag = 2;
        let (chosen, _) = sw.ingress(pkt, 9999);
        assert_eq!(chosen.unwrap(), p2);
    }
}
