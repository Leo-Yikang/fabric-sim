//! 工作负载特征扫描示例
//!
//! 固定拓扑和协议，扫描不同流大小分布 + 到达过程组合，
//! 输出对比表格，展示工作负载特征对协议表现的显著影响。
//!
//! 运行：
//! ```bash
//! cargo run --release --example workload_sweep
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};

use strack_sim::monitor::SimSummary;
use strack_sim::nic::{STrackMode, STrackProtocol};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::LeafSpine;
use strack_sim::traffic::{ArrivalProcess, FlowSizeDist, PairPattern, Synthetic};

struct WorkloadCase {
    label: &'static str,
    pair: PairPattern,
    size: FlowSizeDist,
    arrival: ArrivalProcess,
}

fn run_case(case: &WorkloadCase) -> SimSummary {
    let topo = LeafSpine {
        n_leaf: 4,
        n_spine: 4,
        hosts_per_leaf: 4,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 400_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();

    let n = topo.num_hosts() as u32;
    let nodes: Vec<u32> = (0..n).collect();
    let mut runner = SimRunner::new(topo, case.label.to_string(), |h, n_paths| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, n_paths))
    });

    let flows = Synthetic {
        nodes,
        pair_pattern: case.pair.clone(),
        flow_size: case.size,
        arrival: case.arrival,
        seed: 42,
    }
    .generate();

    runner.inject_flows(flows);
    runner.run(100_000_000); // 100 ms 上限
    runner.summarize()
}

fn avg_fct_ns(s: &SimSummary) -> f64 {
    // SimSummary 没有直接存平均 FCT，这里用 P50 近似代表中心趋势
    s.fct_p50_ns as f64
}

fn retrans_ratio(s: &SimSummary) -> f64 {
    if s.total_packets_sent == 0 {
        0.0
    } else {
        s.total_packets_retransmitted as f64 / s.total_packets_sent as f64 * 100.0
    }
}

fn main() {
    let cases = vec![
        WorkloadCase {
            label: "固定大小+同时开始",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Fixed(64 * 1024),
            arrival: ArrivalProcess::Simultaneous(1000),
        },
        WorkloadCase {
            label: "均匀分布+同时开始",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Uniform {
                min: 4 * 1024,
                max: 256 * 1024,
            },
            arrival: ArrivalProcess::Simultaneous(1000),
        },
        WorkloadCase {
            label: "重尾Pareto+同时开始",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Pareto {
                min: 4 * 1024,
                shape: 1.5,
            },
            arrival: ArrivalProcess::Simultaneous(1000),
        },
        WorkloadCase {
            label: "双模态mice/elephant+同时开始",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Bimodal {
                small: 4 * 1024,
                large: 1024 * 1024,
                large_ratio: 0.15,
            },
            arrival: ArrivalProcess::Simultaneous(1000),
        },
        WorkloadCase {
            label: "固定大小+Poisson到达",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Fixed(64 * 1024),
            arrival: ArrivalProcess::Poisson {
                start: 1000,
                mean_interval_ns: 50_000,
            },
        },
        WorkloadCase {
            label: "重尾Pareto+Poisson到达",
            pair: PairPattern::AllToAll,
            size: FlowSizeDist::Pareto {
                min: 4 * 1024,
                shape: 1.5,
            },
            arrival: ArrivalProcess::Poisson {
                start: 1000,
                mean_interval_ns: 50_000,
            },
        },
        WorkloadCase {
            label: "排列流量+固定大小",
            pair: PairPattern::Permutation,
            size: FlowSizeDist::Fixed(64 * 1024),
            arrival: ArrivalProcess::Simultaneous(1000),
        },
        WorkloadCase {
            label: "随机对+重尾分布",
            pair: PairPattern::RandomPairs(50),
            size: FlowSizeDist::Pareto {
                min: 4 * 1024,
                shape: 1.5,
            },
            arrival: ArrivalProcess::Simultaneous(1000),
        },
    ];

    let output_dir = "output";
    std::fs::create_dir_all(output_dir).expect("创建 output 目录失败");
    let csv_path = format!("{}/workload_sweep.csv", output_dir);
    let file = File::create(&csv_path).expect("创建 CSV 文件失败");
    let mut writer = BufWriter::new(file);

    // 写入 CSV 表头
    writeln!(writer, "label,{}", SimSummary::csv_header()).unwrap();

    println!("=== 工作负载特征扫描：LeafSpine × STrack ===");
    println!("拓扑：4 Leaf × 4 Spine × 4 host/leaf = 16 hosts");
    println!("CSV 输出：{}", csv_path);
    println!();

    // 表头
    println!(
        "{:22} | {:>8} | {:>8} | {:>8} | {:>8} | {:>6} | {:>6} | {:>6}",
        "工作负载", "流数", "完成", "P50(us)", "P99(us)", "重传%", "ECN", "丢包"
    );
    println!("{}", "-".repeat(90));

    for case in &cases {
        let s = run_case(case);
        // 同时写入 CSV
        writeln!(writer, "{},{}", case.label, s.to_csv_row()).unwrap();
        println!(
            "{:22} | {:>8} | {:>8} | {:>8.1} | {:>8.1} | {:>6.2} | {:>6} | {:>6}",
            case.label,
            s.total_flows,
            s.completed_flows,
            avg_fct_ns(&s) / 1000.0,
            s.fct_p99_ns as f64 / 1000.0,
            retrans_ratio(&s),
            s.total_ecn_marks,
            s.total_drops,
        );
    }
    writer.flush().unwrap();

    println!();
    println!("说明：");
    println!("- 同时开始（Simultaneous）会产生同步突发，更容易触发拥塞和 ECN/丢包");
    println!("- Poisson 到达（50us 均值）平滑了注入速率，通常降低排队和丢包");
    println!("- Pareto 重尾分布下少量 elephant flow 可能显著拉高 P99 FCT");
    println!("- 排列流量（Permutation）无热点，通常拥塞最轻");
    println!();
    println!("提示：运行 python3 scripts/plot_workloads.py 生成可视化图表");
}
