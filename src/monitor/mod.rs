//! 指标采集与日志（阶段四）
//!
//! 监控的核心指标：
//! - FCT (Flow Completion Time)：每条流首字节到末字节的时间
//! - 链路实时利用率：周期性采样
//! - 交换机最大队列深度：每个 switch port 自带历史最大值
//! - ECN 标记数 / 丢包数
//! - 事件类型直方图、运行剖面（P1 可观测性）

use crate::core::EventKind;
use crate::network::packet::FlowId;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FlowFct {
    pub flow_id: FlowId,
    pub start_ns: u64,
    pub finish_ns: u64,
    pub bytes: u64,
}

impl Default for FlowFct {
    fn default() -> Self {
        Self { flow_id: 0, start_ns: 0, finish_ns: 0, bytes: 0 }
    }
}

impl FlowFct {
    pub fn fct_ns(&self) -> u64 { self.finish_ns.saturating_sub(self.start_ns) }
}

/// 事件类型直方图（P1 可观测性）
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct EventHistogram {
    pub packet_arrive: u64,
    pub packet_depart: u64,
    pub timeout: u64,
    pub stop: u64,
    pub flow_start: u64,
    pub tx_tick: u64,
    pub custom: u64,
}

impl EventHistogram {
    pub fn record(&mut self, kind: &EventKind) {
        match kind {
            EventKind::PacketArrive { .. } => self.packet_arrive += 1,
            EventKind::PacketDepart { .. } => self.packet_depart += 1,
            EventKind::Timeout { .. } => self.timeout += 1,
            EventKind::Stop => self.stop += 1,
            EventKind::FlowStart { .. } => self.flow_start += 1,
            EventKind::TxTick { .. } => self.tx_tick += 1,
            EventKind::Custom(_) => self.custom += 1,
        }
    }

    pub fn total(&self) -> u64 {
        self.packet_arrive
            + self.packet_depart
            + self.timeout
            + self.stop
            + self.flow_start
            + self.tx_tick
            + self.custom
    }

    pub fn pretty_print(&self) {
        let total = self.total();
        if total == 0 {
            println!("│ 事件直方图          (无事件)");
            return;
        }
        println!("│ 事件直方图          总计 {}", total);
        println!("│   PacketArrive      {:>12} ({:.2}%)", self.packet_arrive, self.packet_arrive as f64 * 100.0 / total as f64);
        println!("│   PacketDepart      {:>12} ({:.2}%)", self.packet_depart, self.packet_depart as f64 * 100.0 / total as f64);
        println!("│   Timeout           {:>12} ({:.2}%)", self.timeout, self.timeout as f64 * 100.0 / total as f64);
        println!("│   Stop              {:>12} ({:.2}%)", self.stop, self.stop as f64 * 100.0 / total as f64);
        println!("│   FlowStart         {:>12} ({:.2}%)", self.flow_start, self.flow_start as f64 * 100.0 / total as f64);
        println!("│   TxTick            {:>12} ({:.2}%)", self.tx_tick, self.tx_tick as f64 * 100.0 / total as f64);
        println!("│   Custom            {:>12} ({:.2}%)", self.custom, self.custom as f64 * 100.0 / total as f64);
    }
}

/// 运行剖面：记录仿真运行过程中的关键可观测性指标（P1 可观测性）
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct RunProfile {
    /// 每种事件类型的数量
    pub event_histogram: EventHistogram,
    /// 事件队列中曾经达到的最大 pending 长度
    pub max_pending_events: usize,
    /// 同一仿真时间戳下连续处理的最大事件数（事件风暴检测）
    pub max_same_time_burst: u64,
    /// 最后一次打印进度时的已处理事件数（用于 run_with_progress）
    pub last_progress_processed: u64,
    /// 最后一次打印进度时的仿真时间（ns）
    pub last_progress_time_ns: u64,
    /// 包追踪：trace_id -> (创建时间, 大小, 源, 目的, 事件序列)
    /// 仅在启用 packet tracing 时填充
    pub packet_traces: HashMap<u64, PacketTrace>,
}

/// 单条包的追踪记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PacketTrace {
    pub trace_id: u64,
    pub created_ns: u64,
    pub size: u32,
    pub src: u32,
    pub dst: u32,
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub time_ns: u64,
    pub node: u32,
    pub action: String,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct SimSummary {
    pub mode: String,            // "ecmp" 或 "strack"
    pub total_flows: u64,
    pub completed_flows: u64,
    pub total_time_ns: u64,
    pub total_packets_sent: u64,
    pub total_packets_retransmitted: u64,
    pub total_ecn_marks: u64,
    pub total_drops: u64,
    pub fct_p50_ns: u64,
    pub fct_p95_ns: u64,
    pub fct_p99_ns: u64,
    pub fct_max_ns: u64,
    pub avg_link_util: f64,
    pub max_queue_depth_bytes: u32,
    /// P1 可观测性：运行剖面
    pub profile: RunProfile,
}

impl SimSummary {
    pub fn from_fcts(mode: &str, fcts: &mut [FlowFct]) -> Self {
        let mut s = SimSummary { mode: mode.to_string(), ..Default::default() };
        s.total_flows = fcts.len() as u64;
        s.completed_flows = fcts.iter().filter(|f| f.finish_ns > 0).count() as u64;
        if !fcts.is_empty() {
            fcts.sort_by_key(|f| f.fct_ns());
            let n = fcts.len();
            s.fct_p50_ns = fcts[n * 50 / 100].fct_ns();
            s.fct_p95_ns = fcts[(n * 95 / 100).min(n - 1)].fct_ns();
            s.fct_p99_ns = fcts[(n * 99 / 100).min(n - 1)].fct_ns();
            s.fct_max_ns = fcts.iter().map(|f| f.fct_ns()).max().unwrap_or(0);
        }
        s
    }

    pub fn pretty_print(&self) {
        println!("┌─────────── 仿真结果 [{}] ────────────", self.mode);
        println!("│ 总流数              {}", self.total_flows);
        println!("│ 完成流数            {}", self.completed_flows);
        println!("│ 仿真总时长          {:.3} ms", self.total_time_ns as f64 / 1e6);
        println!("│ 总发送包数          {}", self.total_packets_sent);
        println!("│ 总重传包数          {}", self.total_packets_retransmitted);
        println!("│ ECN 标记总数        {}", self.total_ecn_marks);
        println!("│ 丢包总数            {}", self.total_drops);
        println!("│ FCT P50             {:.3} us", self.fct_p50_ns as f64 / 1e3);
        println!("│ FCT P95             {:.3} us", self.fct_p95_ns as f64 / 1e3);
        println!("│ FCT P99             {:.3} us", self.fct_p99_ns as f64 / 1e3);
        println!("│ FCT Max             {:.3} us", self.fct_max_ns as f64 / 1e3);
        println!("│ 平均链路利用率      {:.1}%", self.avg_link_util * 100.0);
        println!("│ 最大队列深度        {} bytes", self.max_queue_depth_bytes);
        println!("│ 最大 pending 长度   {}", self.profile.max_pending_events);
        println!("│ 最大同时间戳 burst  {}", self.profile.max_same_time_burst);
        self.profile.event_histogram.pretty_print();
        println!("└──────────────────────────────────────────");
    }

    /// CSV 表头（便于批量写入文件）
    pub fn csv_header() -> &'static str {
        "mode,total_flows,completed_flows,total_time_ms,packets_sent,packets_retransmitted,ecn_marks,drops,fct_p50_us,fct_p95_us,fct_p99_us,fct_max_us,avg_link_util_pct,max_queue_depth_bytes"
    }

    /// 转为 CSV 单行（不含换行符）
    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{:.3},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.1},{}",
            self.mode,
            self.total_flows,
            self.completed_flows,
            self.total_time_ns as f64 / 1e6,
            self.total_packets_sent,
            self.total_packets_retransmitted,
            self.total_ecn_marks,
            self.total_drops,
            self.fct_p50_ns as f64 / 1e3,
            self.fct_p95_ns as f64 / 1e3,
            self.fct_p99_ns as f64 / 1e3,
            self.fct_max_ns as f64 / 1e3,
            self.avg_link_util * 100.0,
            self.max_queue_depth_bytes,
        )
    }

    /// 导出为 JSON 字符串
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}
