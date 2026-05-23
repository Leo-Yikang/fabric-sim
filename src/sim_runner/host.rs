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
                let (link_id, edge_switch) =
                    match self.topo.host_uplink.iter().find(|u| u.host == host) {
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
    ///
    /// 修复冗余 Timeout：当 ACK 提前到达并触发 TxTick 时，逻辑取消之前调度的 Timeout，
    /// 避免事件队列中堆积大量过时的 Timeout 事件（P1 可观测性）。
    ///
    /// P3：支持 pacing-aware 调度。如果协议实现了 `next_tx_time()`，优先使用它
    /// 而不是固定 tick_delay，避免 rate-based 协议的大量空转 TxTick。
    fn schedule_host_next_action(&mut self, host: EntityId, now: u64, tick_delay: u64) {
        let host_idx = host as usize;
        let Some((has_pending_work, next_tx_time, next_rto_deadline)) =
            self.protocols.get(host_idx).map(|proto| {
                (
                    proto.has_pending_work(),
                    proto.next_tx_time(),
                    proto.next_rto_deadline(),
                )
            })
        else {
            return;
        };

        if has_pending_work {
            // 有工作要做：逻辑取消任何已调度的 Timeout
            self.pending_timeout_deadline[host_idx] = u64::MAX;
            self.cancel_host_timeouts(host);

            // P3：pacing-aware 调度
            let next_tick = next_tx_time.unwrap_or(now + tick_delay);
            let scheduled_time = next_tick.max(now);
            self.sim
                .schedule(Event::new(scheduled_time, EventKind::TxTick { host }, host));
        } else if let Some(deadline) = next_rto_deadline {
            // 无工作但有未确认包：需要 Timeout。
            // 如果已经有一个更早或相同的 Timeout 在队列中，不再重复调度。
            let existing = self.pending_timeout_deadline[host_idx];
            if existing != u64::MAX && existing <= deadline {
                return;
            }
            self.cancel_host_timeouts(host);
            self.pending_timeout_deadline[host_idx] = deadline;
            self.sim.schedule(Event::new(
                deadline,
                EventKind::Timeout { timer_id: 0 },
                host,
            ));
        } else {
            self.pending_timeout_deadline[host_idx] = u64::MAX;
            self.cancel_host_timeouts(host);
        }
    }

    /// 清理某 host 尚未触发的 Timeout 事件。
    ///
    /// `pending_timeout_deadline` 已经能逻辑跳过过时 Timeout；这里进一步从事件队列中
    /// 物理删除，避免大规模仿真里积累无效定时器。
    fn cancel_host_timeouts(&mut self, host: EntityId) -> usize {
        self.sim
            .cancel_where(|ev| ev.target == host && matches!(ev.kind, EventKind::Timeout { .. }))
    }
}
