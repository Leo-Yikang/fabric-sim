//! 时间序列采样器
//!
//! 在仿真运行过程中定时采集每条链路的利用率和队列深度。

use crate::topology::Topology;

use super::data::{LinkSnapshot, VizFrame};

pub struct TimeSeriesSampler {
    interval_ns: u64,
    next_sample_ns: u64,
    prev_bytes: Vec<u64>,
    pub frames: Vec<VizFrame>,
    enabled: bool,
}

impl TimeSeriesSampler {
    pub fn new(interval_ns: u64, n_links: usize) -> Self {
        Self {
            interval_ns,
            next_sample_ns: interval_ns,
            prev_bytes: vec![0; n_links],
            frames: Vec::new(),
            enabled: true,
        }
    }

    pub fn disabled() -> Self {
        Self {
            interval_ns: 0,
            next_sample_ns: u64::MAX,
            prev_bytes: Vec::new(),
            frames: Vec::new(),
            enabled: false,
        }
    }

    /// 在 run() 循环中每次事件后调用，按间隔采样
    pub fn maybe_sample(&mut self, now: u64, link_bytes_sent: &[u64], topo: &Topology) {
        if !self.enabled || now < self.next_sample_ns {
            return;
        }

        let n_links = topo.links.len();
        let mut snapshots = Vec::with_capacity(n_links);

        for link_id in 0..n_links {
            let link = topo.links.get(link_id as u32);
            let delta = link_bytes_sent[link_id].saturating_sub(self.prev_bytes[link_id]);
            let capacity = link.bandwidth_bps as f64 / 1e9 / 8.0 * self.interval_ns as f64;
            let utilization = if capacity > 0.0 {
                (delta as f64 / capacity).min(1.0)
            } else {
                0.0
            };

            // 查找该链路的发端交换机端口队列深度
            let queue_depth = queue_depth_for_link(topo, link_id as u32);

            snapshots.push(LinkSnapshot { utilization, queue_depth_bytes: queue_depth });
        }

        self.prev_bytes.copy_from_slice(link_bytes_sent);
        self.frames.push(VizFrame { time_ns: now, links: snapshots });
        self.next_sample_ns = now + self.interval_ns;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// 遍历所有交换机的所有端口，找到 link_id 对应的队列深度
fn queue_depth_for_link(topo: &Topology, link_id: u32) -> u32 {
    for sw in &topo.switches {
        for port in &sw.ports {
            if port.link_id == link_id {
                return port.queue_bytes;
            }
        }
    }
    0
}