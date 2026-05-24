//! 交换机侧事件处理
//!
//! PacketArrive @ switch → 路由 + 入队。
//! PacketDepart → 端口空闲时驱动出队发下一跳。

use crate::core::{Event, EventKind};
use crate::network::Packet;
use crate::EntityId;

use super::SimRunner;

impl SimRunner {
    pub(super) fn handle_packet_arrive(&mut self, pid: u64, target: EntityId, now: u64) {
        let pkt = match self.packet_buf_remove(pid) {
            Some(p) => p,
            None => return,
        };
        if (target as usize) < self.topo.hosts.len() {
            self.handle_arrive_at_host(pkt, target, now);
        } else {
            self.handle_arrive_at_switch(pkt, target, now);
        }
    }

    pub(super) fn handle_arrive_at_switch(
        &mut self,
        pkt: Packet,
        switch_id: EntityId,
        now: u64,
    ) {
        let sw_idx = match self.switch_index.get(switch_id as usize) {
            Some(&idx) if idx != usize::MAX => idx,
            _ => return,
        };
        let pkt_id = pkt.id;
        let hash_key = pkt.src ^ pkt.dst ^ pkt.flow_id;
        let pkt_size = pkt.size;
        let (port_opt, dropped) = self.topo.switches[sw_idx].ingress(pkt, hash_key, now);
        if dropped {
            self.packet_buf_remove(pkt_id);
            return;
        }
        let port = match port_opt {
            Some(p) => p,
            None => return,
        };
        self.try_egress(sw_idx, port, now, pkt_id, pkt_size);
    }

    pub(super) fn try_egress(
        &mut self,
        sw_idx: usize,
        port: u8,
        now: u64,
        _just_in_pid: u64,
        _pkt_size: u32,
    ) {
        let (link_id, busy_until) = {
            let sw = &self.topo.switches[sw_idx];
            let p = sw.port(port);
            (p.link_id, p.busy_until)
        };
        if busy_until <= now {
            let next_pkt = self.topo.switches[sw_idx].dequeue(port);
            if let Some(pkt) = next_pkt {
                let size = pkt.size;
                let dst = pkt.dst;
                let pid = self.packet_buf_insert(pkt);
                let link = self.topo.links.get(link_id);
                let depart_done = now + link.serialization_ns(size);
                let arrive = depart_done + link.prop_delay_ns;
                {
                    let p = self.topo.switches[sw_idx].port_mut(port);
                    p.busy_until = depart_done;
                }
                self.link_busy_until[link_id as usize] = depart_done;
                self.link_bytes_sent[link_id as usize] += size as u64;
                let next_target = link.to;
                self.sim.schedule(Event::new(
                    arrive,
                    EventKind::PacketArrive {
                        packet_id: pid,
                        src: link.from,
                    },
                    next_target,
                ));
                let still_queued = self.topo.switches[sw_idx].port(port).queue_bytes > 0;
                if still_queued {
                    let p = self.topo.switches[sw_idx].port_mut(port);
                    if !p.egress_pending {
                        p.egress_pending = true;
                        self.sim.schedule(Event::new(
                            depart_done,
                            EventKind::PacketDepart {
                                packet_id: 0,
                                dst,
                                port,
                            },
                            self.topo.switches[sw_idx].id,
                        ));
                    }
                }
            }
        } else {
            let p = self.topo.switches[sw_idx].port_mut(port);
            if !p.egress_pending {
                p.egress_pending = true;
                self.sim.schedule(Event::new(
                    busy_until,
                    EventKind::PacketDepart {
                        packet_id: 0,
                        dst: 0,
                        port,
                    },
                    self.topo.switches[sw_idx].id,
                ));
            }
        }
    }

    pub(super) fn handle_packet_depart(
        &mut self,
        _pid: u64,
        target: EntityId,
        port: u8,
        now: u64,
    ) {
        let sw_idx = match self.switch_index.get(target as usize) {
            Some(&idx) if idx != usize::MAX => idx,
            _ => return,
        };
        self.topo.switches[sw_idx].port_mut(port).egress_pending = false;
        let has_queue = self.topo.switches[sw_idx].port(port).queue_bytes > 0;
        if has_queue {
            self.try_egress(sw_idx, port, now, 0, 0);
        }
    }
}