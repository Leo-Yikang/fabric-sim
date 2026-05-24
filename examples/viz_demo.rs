//! 3D 可视化数据采集示例
//!
//! 运行 LeafSpine + AllToAll 流量，采样链路利用率时间序列，导出 JSON。
//!
//! 用法：
//!   cargo run --release --example viz_demo
//!   python3 scripts/visualize_3d.py output/viz_data.json

use std::fs;

use fabric_sim::nic::{STrackMode, STrackProtocol};
use fabric_sim::sim_runner::SimRunner;
use fabric_sim::topology::LeafSpine;
use fabric_sim::traffic::{ArrivalProcess, FlowSizeDist, PairPattern, Synthetic};
use fabric_sim::viz::{self, TopoKind};

fn main() {
    // 大规模拓扑：64 主机，用 RandomPairs 控制流数避免 O(n²) 爆炸
    let topo = LeafSpine {
        n_leaf: 8,
        n_spine: 4,
        hosts_per_leaf: 8,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 400_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();

    let n = topo.num_hosts() as u32;
    let nodes: Vec<u32> = (0..n).collect();

    let mut runner = SimRunner::new(topo, "strack".to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
    })
    .expect("SimRunner init failed")
    .with_sampling(100_000); // 每 100us 采样一次

    // 64 主机，4000 条随机配对流量（而非 AllToAll 的 4032 条）
    // 大流制造持续拥塞，200us 间隔让流量分布更均匀
    let flows = Synthetic {
        nodes,
        pair_pattern: PairPattern::RandomPairs(4000),
        flow_size: FlowSizeDist::Bimodal {
            small: 256 * 1024,
            large: 2 * 1024 * 1024,
            large_ratio: 0.3,
        },
        arrival: ArrivalProcess::Poisson {
            start: 1000,
            mean_interval_ns: 2_000,
        },
        seed: 42,
    }
    .generate();

    runner.inject_flows(flows);
    runner.run(20_000_000); // 运行 20ms

    let summary = runner.summarize();
    summary.pretty_print();

    let kind = TopoKind::LeafSpine {
        n_leaf: 8,
        n_spine: 4,
        hosts_per_leaf: 8,
    };
    let viz_data = viz::build_viz_data(
        &runner.topo,
        summary,
        runner.sampler.frames,
        &kind,
    );

    fs::create_dir_all("output").expect("create output dir");
    let json = serde_json::to_string_pretty(&viz_data).expect("serialize viz data");
    fs::write("output/viz_data.json", &json).expect("write viz_data.json");
    println!("JSON written to output/viz_data.json ({} frames)", viz_data.time_series.len());
}