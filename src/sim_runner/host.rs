//! 主机侧事件处理
//!
//! TxTick 驱动协议栈发送数据包，PacketArrive @ host 处理收到的数据/控制包。
//! 事件驱动化后，TxTick 不再固定周期轮询，而是由以下事件触发：
//!   - FlowStart / ACK / NACK 到达
//!   - RTO Timeout 到期

use crate::core::{Event, EventKind};
use crate::network::packet::{FlowId, SeqNum};
use crate::network::{Packet, PacketKind};
use crate::EntityId;

use super::{NicSelector, SimRunner};

impl SimRunner {
    /// 根据当前 NIC 选择策略，返回该 host 应使用的上行链路。
    fn pick_uplink(&mut self, host: EntityId, hint_flow_id: Option<FlowId>) -> Option<(u32, EntityId)> {
        let uplinks: Vec<&crate::topology::HostUplink> =
            self.topo.host_uplink.iter().filter(|u| u.host == host).collect();
        if uplinks.is_empty() {
            return None;
        }
        let idx = match self.nic_selector {
            NicSelector::First => 0,
            NicSelector::FlowHash => {
                let fid = hint_flow_id.unwrap_or(0) as usize;
                fid % uplinks.len()
            }
            NicSelector::RoundRobin => {
                let h = host as usize;
                let i = self.nic_rr_counters[h] % uplinks.len();
                self.nic_rr_counters[h] = i + 1;
                i
            }
        };
        let u = uplinks[idx];
        Some((u.link_to_switch, u.edge_switch))
    }

    pub(super) fn handle_tx_tick(&mut self, host: EntityId, now: u64) {
        let pkts = if let Some(proto) = self.protocols.get_mut(host as usize) {
            proto.on_tx_tick(now)
        } else {
            return;
        };
        let hint_fid = pkts.first().map(|p| p.flow_id);
        let (link_id, edge_switch) = match self.pick_uplink(host, hint_fid) {
            Some(x) => x,
            None => return,
        };
        // 记录本批包中最早和最晚的真实 NIC 出主机时间，用于后续调度
        let mut earliest_nic_time = u64::MAX;
        let mut latest_nic_time = now;
        // 记录每个 seq 对应的真实 NIC 出主机时间，用于后续更新协议层 send_times
        let mut seq_to_nic_time: Vec<(FlowId, SeqNum, u64)> = Vec::new();
        let host_idx = host as usize;
        let fixed_overhead = self.host_delays.tx_fixed_overhead(host);
        for mut pkt in pkts {
            let size = pkt.size;
            let seq = pkt.seq;
            // 每包 DMA 串行化时间（包大小决定，体现带宽成本）
            let dma_time = self.host_delays.tx_dma_time(host, size as u64);
            // DMA 开始时刻 = max(now + 每包固定开销, 主机 DMA 引擎空闲时刻)
            let dma_start = (now + fixed_overhead).max(self.host_tx_busy_until[host_idx]);
            // 真实 NIC 出主机时间 = DMA 开始 + DMA/memcpy 拷贝时间
            // DMA 是发送前置成本：数据必须先拷贝到 NIC 才能离开主机。
            let nic_depart_time = dma_start + dma_time;
            // 主机侧指标：累计固定开销、DMA时间、排队等待
            self.host_total_fixed_overhead_ns += fixed_overhead as u64;
            self.host_total_dma_time_ns += dma_time as u64;
            let queue_wait = dma_start.saturating_sub(now + fixed_overhead);
            self.host_total_queue_wait_ns += queue_wait;
            let current_depth = self.host_tx_busy_until[host_idx].saturating_sub(now);
            if current_depth > self.host_max_queue_depth {
                self.host_max_queue_depth = current_depth;
            }
            // DMA 引擎在此包送出后恢复空闲
            self.host_tx_busy_until[host_idx] = nic_depart_time;
            earliest_nic_time = earliest_nic_time.min(nic_depart_time);
            latest_nic_time = latest_nic_time.max(nic_depart_time);
            // 更新包的 depart_time 为真实 NIC 出主机时间，
            // 使协议层 send_times 和 RTO 基于一致的时间轴
            pkt.depart_time = nic_depart_time;
            seq_to_nic_time.push((pkt.flow_id, seq, nic_depart_time));
            let pid = self.packet_buf_insert(pkt);
            let start = self.link_busy_until[link_id as usize].max(nic_depart_time);
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
        // 将协议层 send_times 中本批包记录的时间从原始 now 更新为
        // 真实 NIC 出主机时间，确保 RTO 从包真正离开主机开始计时。
        if let Some(proto) = self.protocols.get_mut(host as usize) {
            for (flow_id, seq, nic_time) in seq_to_nic_time {
                proto.update_send_time(flow_id, seq, nic_time);
            }
        }
        // 调度下一次 TxTick 时，至少不早于本批包最晚的 NIC 出主机时间。
        // 这样可避免协议在主机硬件路径尚未完成时无限制地产生后续包。
        let schedule_base = if earliest_nic_time == u64::MAX {
            now
        } else {
            latest_nic_time
        };
        self.schedule_host_next_action(host, schedule_base, self.tx_tick_ns);
    }

    pub(super) fn handle_arrive_at_host(&mut self, pkt: Packet, host: EntityId, now: u64) {
        match pkt.kind {
            PacketKind::Data => {
                // 注入接收端主机内部延迟（中断 + CQ poll + DMA）
                let rx_delay = self.host_delays.rx_delay(host, pkt.size as u64);
                let rx_time = now + rx_delay;
                let returns = if let Some(proto) = self.protocols.get_mut(host as usize) {
                    proto.on_rx_data(&pkt, rx_time)
                } else {
                    return;
                };
                let (link_id, edge_switch) = match self.pick_uplink(host, Some(pkt.flow_id)) {
                        Some(x) => x,
                        None => return,
                    };
                // 记录 ACK 中最晚的 NIC 出主机时间
                let mut latest_ack_nic_time = rx_time;
                for mut ret in returns {
                    let size = ret.size;
                    // 当前简化模型：统一将控制包（ACK/NACK）生成视为主机接收处理
                    // 完成后的即时 NIC 发包，不注入发送端延迟（doorbell/PCIe 等）。
                    let ack_nic_depart = rx_time;
                    latest_ack_nic_time = latest_ack_nic_time.max(ack_nic_depart);
                    // ACK 的 depart_time 也更新为真实 NIC 出主机时间
                    ret.depart_time = ack_nic_depart;
                    let pid = self.packet_buf_insert(ret);
                    let start = self.link_busy_until[link_id as usize].max(ack_nic_depart);
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
                self.schedule_host_next_action(host, latest_ack_nic_time, 0);
            }
            PacketKind::Control(_) => {
                // 控制包（ACK）到达也注入接收端延迟
                let rx_delay = self.host_delays.rx_delay(host, pkt.size as u64);
                let rx_time = now + rx_delay;
                if let Some(proto) = self.protocols.get_mut(host as usize) {
                    proto.on_tx_control(&pkt, rx_time);
                    for (fid, ft) in proto.take_finished_flows() {
                        let idx = fid as usize;
                        if idx < self.fcts.len() && self.fcts[idx].finish_ns == 0 {
                            self.fcts[idx].finish_ns = ft;
                        }
                    }
                }
                self.schedule_host_next_action(host, rx_time, 0);
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
