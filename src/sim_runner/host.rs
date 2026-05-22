//! 主机侧事件处理
//!
//! TxTick 驱动协议栈发送数据包，PacketArrive @ host 处理收到的数据/控制包。

use crate::core::{Event, EventKind};
use crate::network::{Packet, PacketKind};
use crate::EntityId;

use super::SimRunner;

impl SimRunner {
    pub(super) fn handle_tx_tick(&mut self, host: EntityId, now: u64) {
        let mut pkts = if let Some(proto) = self.protocols.get_mut(&host) {
            proto.on_tx_tick(now)
        } else {
            return;
        };
        // 重写全局唯一的 packet id
        for p in pkts.iter_mut() {
            p.id = self.global_pid;
            self.global_pid += 1;
        }
        let has_active = self
            .protocols
            .get(&host)
            .map(|p| !p.all_flows_done())
            .unwrap_or(false);
        if pkts.is_empty() {
            if has_active {
                self.sim.schedule(Event::new(
                    now + 25_000,
                    EventKind::TxTick { host },
                    host,
                ));
            }
            return;
        }
        let Some(uplink) = self.topo.host_uplink.iter().find(|u| u.host == host) else {
            return;
        };
        let link_id = uplink.link_to_switch;
        for pkt in pkts {
            let pid = pkt.id;
            let size = pkt.size;
            self.packet_buf.insert(pid, pkt);
            let start = self.link_busy_until[link_id as usize].max(now);
            let link = self.topo.links.get(link_id);
            let depart_done = start + link.serialization_ns(size);
            let arrive = depart_done + link.prop_delay_ns;
            self.link_busy_until[link_id as usize] = depart_done;
            self.link_bytes_sent += size as u64;
            self.sim.schedule(Event::new(
                arrive,
                EventKind::PacketArrive {
                    packet_id: pid,
                    src: host,
                },
                uplink.edge_switch,
            ));
        }
        self.sim.schedule(Event::new(
            now + self.tx_tick_ns,
            EventKind::TxTick { host },
            host,
        ));
    }

    pub(super) fn handle_arrive_at_host(&mut self, pkt: Packet, host: EntityId, now: u64) {
        match pkt.kind {
            PacketKind::Data => {
                let returns = if let Some(proto) = self.protocols.get_mut(&host) {
                    proto.on_rx_data(&pkt, now)
                } else {
                    return;
                };
                let Some(uplink) = self.topo.host_uplink.iter().find(|u| u.host == host) else {
                    return;
                };
                let link_id = uplink.link_to_switch;
                for mut ret in returns {
                    ret.id = self.global_pid;
                    self.global_pid += 1;
                    let pid = ret.id;
                    let size = ret.size;
                    self.packet_buf.insert(pid, ret);
                    let start = self.link_busy_until[link_id as usize].max(now);
                    let link = self.topo.links.get(link_id);
                    let depart_done = start + link.serialization_ns(size);
                    let arrive = depart_done + link.prop_delay_ns;
                    self.link_busy_until[link_id as usize] = depart_done;
                    self.link_bytes_sent += size as u64;
                    self.sim.schedule(Event::new(
                        arrive,
                        EventKind::PacketArrive {
                            packet_id: pid,
                            src: host,
                        },
                        uplink.edge_switch,
                    ));
                }
                self.sim.schedule(Event::new(
                    now,
                    EventKind::TxTick { host },
                    host,
                ));
            }
            PacketKind::Control(_) => {
                if let Some(proto) = self.protocols.get_mut(&host) {
                    proto.on_tx_control(&pkt, now);
                    for (fid, ft) in proto.take_finished_flows() {
                        if let Some(rec) = self.fcts.get_mut(&fid) {
                            if rec.finish_ns == 0 {
                                rec.finish_ns = ft;
                            }
                        }
                    }
                }
                self.sim.schedule(Event::new(
                    now,
                    EventKind::TxTick { host },
                    host,
                ));
            }
        }
    }
}