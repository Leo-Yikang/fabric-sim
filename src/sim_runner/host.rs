//! 主机侧事件处理
//!
//! TxTick 驱动协议栈发送数据包，PacketArrive @ host 处理收到的数据/控制包。
//! 事件驱动化后，TxTick 不再固定周期轮询，而是由以下事件触发：
//!   - FlowStart / ACK / NACK 到达
//!   - RTO Timeout 到期

use crate::core::{Event, EventKind};
use crate::network::{Packet, PacketKind};
use crate::EntityId;

use super::SimRunner;

impl SimRunner {
    pub(super) fn handle_tx_tick(&mut self, host: EntityId, now: u64) {
        let pkts = if let Some(proto) = self.protocols.get_mut(host as usize) {
            proto.on_tx_tick(now)
        } else {
            return;
        };
        let (link_id, edge_switch) = match self.topo.host_uplink.iter().find(|u| u.host == host) {
            Some(u) => (u.link_to_switch, u.edge_switch),
            None => return,
        };
        for pkt in pkts {
            let size = pkt.size;
            let pid = self.packet_buf_insert(pkt);
            let start = self.link_busy_until[link_id as usize].max(now);
            let link = self.topo.links.get(link_id);
            let depart_done = start + link.serialization_ns(size);
            let arrive = depart_done + link.prop_delay_ns;
            self.link_busy_until[link_id as usize] = depart_done;
            self.link_bytes_sent[link_id as usize] += size as u64;
            self.sim.schedule(Event::new(
                arrive,
                EventKind::PacketArrive {
                    packet_id: pid,
                    src: host,
                },
                edge_switch,
            ));
        }
        // 无论是否发送了包，都根据协议栈状态决定下一步
        self.schedule_host_next_action(host, now, self.tx_tick_ns);
    }

    pub(super) fn handle_arrive_at_host(&mut self, pkt: Packet, host: EntityId, now: u64) {
        match pkt.kind {
            PacketKind::Data => {
                let returns = if let Some(proto) = self.protocols.get_mut(host as usize) {
                    proto.on_rx_data(&pkt, now)
                } else {
                    return;
                };
                let (link_id, edge_switch) = match self.topo.host_uplink.iter().find(|u| u.host == host) {
                    Some(u) => (u.link_to_switch, u.edge_switch),
                    None => return,
                };
                for ret in returns {
                    let size = ret.size;
                    let pid = self.packet_buf_insert(ret);
                    let start = self.link_busy_until[link_id as usize].max(now);
                    let link = self.topo.links.get(link_id);
                    let depart_done = start + link.serialization_ns(size);
                    let arrive = depart_done + link.prop_delay_ns;
                    self.link_busy_until[link_id as usize] = depart_done;
                    self.link_bytes_sent[link_id as usize] += size as u64;
                    self.sim.schedule(Event::new(
                        arrive,
                        EventKind::PacketArrive {
                            packet_id: pid,
                            src: host,
                        },
                        edge_switch,
                    ));
                }
                self.schedule_host_next_action(host, now, 0);
            }
            PacketKind::Control(_) => {
                if let Some(proto) = self.protocols.get_mut(host as usize) {
                    proto.on_tx_control(&pkt, now);
                    for (fid, ft) in proto.take_finished_flows() {
                        let idx = fid as usize;
                        if idx < self.fcts.len() && self.fcts[idx].finish_ns == 0 {
                            self.fcts[idx].finish_ns = ft;
                        }
                    }
                }
                self.schedule_host_next_action(host, now, 0);
            }
        }
    }

    /// 根据协议栈当前状态，决定下一步调度 TxTick（带可选延迟）还是 RTO Timeout。
    fn schedule_host_next_action(&mut self, host: EntityId, now: u64, tick_delay: u64) {
        if let Some(proto) = self.protocols.get(host as usize) {
            if proto.has_pending_work() {
                self.sim.schedule(Event::new(
                    now + tick_delay,
                    EventKind::TxTick { host },
                    host,
                ));
            } else if let Some(deadline) = proto.next_rto_deadline() {
                self.sim.schedule(Event::new(
                    deadline,
                    EventKind::Timeout { timer_id: 0 },
                    host,
                ));
            }
        }
    }
}
