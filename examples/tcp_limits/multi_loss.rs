//! 实验：多包丢失场景 — SimpleTcp vs STrack
//!
//! 本实验演示 SimpleTcp 在没有 SACK 支持时，单个窗口内多次丢包导致
//! 超时级联（timeout cascade）的问题。STrack 的 NACK + SACK bitmap 在
//! 同一场景下一次恢复所有丢失包。
//!
//! 输出：
//!   - 终端：指标对比表
//!   - examples/tcp_limits/data/per_flow.csv   — 逐流 FCT
//!   - examples/tcp_limits/data/aggregate.csv  — 聚合指标
//!
//! 运行：
//! ```bash
//! cargo run --release --example multi_loss
//! python3 scripts/.venv/bin/python3 examples/tcp_limits/plot.py
//! ```

use std::fs;
use std::io::Write;

use strack_sim::nic::{SimpleTcp, STrackMode, STrackProtocol};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::Dumbell;
use strack_sim::traffic::FlowDesc;

// ------------------------------------------------------------------
// 实验参数
// ------------------------------------------------------------------

const BOTTLENECK_BPS: u64 = 40_000_000_000;
const BUFFER_BYTES: u32 = 10_240;
const ECN_THRESHOLD: u32 = 5_120;
const SIM_TIME_NS: u64 = 200_000_000;
const FLOW_A_BYTES: u64 = 256 * 1024;
const FLOW_BC_BYTES: u64 = 64 * 1024;

/// 一次实验运行的完整输出
struct RunResult {
    label: String,
    summary: strack_sim::monitor::SimSummary,
    /// (flow_id, bytes, fct_ns)
    per_flow: Vec<(u32, u64, u64)>,
}

fn run_one(label: &str, mode: STrackMode, use_strack: bool) -> RunResult {
    let topo = Dumbell {
        hosts_per_side: 3,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: BOTTLENECK_BPS,
        prop_delay_ns: 500,
        ecn_threshold_bytes: ECN_THRESHOLD,
        buffer_bytes: BUFFER_BYTES,
    }
    .build();

    let mut runner = SimRunner::new(topo, label.to_string(), move |h, n_paths| {
        if use_strack {
            Box::new(STrackProtocol::new(h, mode, n_paths))
        } else {
            Box::new(SimpleTcp::new(h))
        }
    })
    .expect("SimRunner 初始化失败");

    let flows = make_flows();
    runner.inject_flows(flows);
    runner.run(SIM_TIME_NS);

    // 提取逐流 FCT
    let per_flow: Vec<_> = runner
        .fcts
        .iter()
        .map(|(fid, fct)| (*fid, fct.bytes, fct.fct_ns()))
        .collect();

    let summary = runner.summarize();

    RunResult {
        label: label.to_string(),
        summary,
        per_flow,
    }
}

/// 构造 4 条跨瓶颈流
fn make_flows() -> Vec<FlowDesc> {
    vec![
        FlowDesc {
            flow_id: 1,
            src: 0,
            dst: 3,
            bytes: FLOW_A_BYTES,
            start_time_ns: 1000,
        },
        FlowDesc {
            flow_id: 2,
            src: 1,
            dst: 4,
            bytes: FLOW_BC_BYTES,
            start_time_ns: 1000,
        },
        FlowDesc {
            flow_id: 3,
            src: 2,
            dst: 5,
            bytes: FLOW_BC_BYTES,
            start_time_ns: 1000,
        },
        FlowDesc {
            flow_id: 4,
            src: 1,
            dst: 5,
            bytes: FLOW_BC_BYTES,
            start_time_ns: 1000,
        },
    ]
}

// ------------------------------------------------------------------
// CSV 导出
// ------------------------------------------------------------------

fn write_per_flow_csv(results: &[RunResult], dir: &str) {
    let path = format!("{}/per_flow.csv", dir);
    let mut f = fs::File::create(&path).expect("创建 per_flow.csv 失败");
    writeln!(f, "protocol,flow_id,bytes,fct_ns,fct_us").unwrap();
    for r in results {
        for (fid, bytes, fct_ns) in &r.per_flow {
            let fct_us = *fct_ns as f64 / 1000.0;
            writeln!(f, "{},{},{},{},{:.1}", r.label, fid, bytes, fct_ns, fct_us).unwrap();
        }
    }
    println!("[CSV] {}", path);
}

fn write_aggregate_csv(results: &[RunResult], dir: &str) {
    let path = format!("{}/aggregate.csv", dir);
    let mut f = fs::File::create(&path).expect("创建 aggregate.csv 失败");
    writeln!(
        f,
        "protocol,total_flows,completed_flows,packets_sent,retransmitted,drops,ecn,p50_fct_us,p99_fct_us"
    )
    .unwrap();
    for r in results {
        let s = &r.summary;
        writeln!(
            f,
            "{},{},{},{},{},{},{},{:.1},{:.1}",
            r.label,
            s.total_flows,
            s.completed_flows,
            s.total_packets_sent,
            s.total_packets_retransmitted,
            s.total_drops,
            s.total_ecn_marks,
            s.fct_p50_ns as f64 / 1000.0,
            s.fct_p99_ns as f64 / 1000.0,
        )
        .unwrap();
    }
    println!("[CSV] {}", path);
}

// ------------------------------------------------------------------
// main
// ------------------------------------------------------------------

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  实验：多包丢失场景 — SimpleTcp vs STrack                    ║");
    println!("║  拓扑：Dumbbell 3+3 主机，40Gbps 瓶颈，10KB 缓冲            ║");
    println!("║  流量：1×256KB (elephant) + 3×64KB (competing)，同时开始    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("实验原理：");
    println!("  4 条流同时跨越瓶颈链路，10KB 缓冲瞬间溢出。");
    println!("  SimpleTcp（累计 ACK，无 SACK）→ 超时级联恢复");
    println!("  STrack（NACK + SACK bitmap）  → 一次性全部重传");
    println!();

    let tcp = run_one("SimpleTcp", STrackMode::Ecmp, false);
    let strack = run_one("STrack", STrackMode::Strack, true);

    // ---- 导出 CSV ----
    let data_dir = "examples/tcp_limits/data";
    fs::create_dir_all(data_dir).expect("创建 data 目录失败");
    let results = [tcp, strack];
    write_per_flow_csv(&results, data_dir);
    write_aggregate_csv(&results, data_dir);

    let tcp = &results[0];
    let strack = &results[1];
    let tcp_s = &tcp.summary;
    let st_s = &strack.summary;

    // ---- 终端输出对比表 ----
    println!();
    println!(
        "{:<25} | {:>14} | {:>14} | {:>10}",
        "指标", "SimpleTcp", "STrack", "差异"
    );
    println!("{}", "-".repeat(72));

    let row = |label: &str, tcp_v: u64, st_v: u64| {
        let diff = if tcp_v > st_v {
            format!("+{:.0}%", (tcp_v as f64 - st_v as f64) / st_v as f64 * 100.0)
        } else if st_v > tcp_v {
            format!("-{:.0}%", (st_v as f64 - tcp_v as f64) / tcp_v as f64 * 100.0)
        } else {
            "—".to_string()
        };
        println!("{:<25} | {:>14} | {:>14} | {:>10}", label, tcp_v, st_v, diff);
    };

    let row_f = |label: &str, tcp_v: f64, st_v: f64, unit: &str| {
        let diff = if tcp_v > st_v && st_v > 0.0 {
            format!("+{:.0}%", (tcp_v - st_v) / st_v * 100.0)
        } else if st_v > tcp_v && tcp_v > 0.0 {
            format!("-{:.0}%", (st_v - tcp_v) / tcp_v * 100.0)
        } else {
            "—".to_string()
        };
        println!(
            "{:<25} | {:>12.1} {} | {:>12.1} {} | {:>10}",
            label, tcp_v, unit, st_v, unit, diff
        );
    };

    row("总流数", tcp_s.total_flows, st_s.total_flows);
    row("完成流数", tcp_s.completed_flows, st_s.completed_flows);
    row("总发包数", tcp_s.total_packets_sent, st_s.total_packets_sent);
    row("总重传数", tcp_s.total_packets_retransmitted, st_s.total_packets_retransmitted);
    row("总丢包数", tcp_s.total_drops, st_s.total_drops);
    row("总 ECN 标记数", tcp_s.total_ecn_marks, st_s.total_ecn_marks);

    row_f("P50 FCT", tcp_s.fct_p50_ns as f64 / 1000.0, st_s.fct_p50_ns as f64 / 1000.0, "μs");
    row_f("P99 FCT", tcp_s.fct_p99_ns as f64 / 1000.0, st_s.fct_p99_ns as f64 / 1000.0, "μs");

    println!();
    println!("CSV 数据已导出到 examples/tcp_limits/data/");
    println!("运行 python3 scripts/.venv/bin/python3 examples/tcp_limits/plot.py 生成图表");
}
