//! 端到端示例：Incast 拥塞场景下 ECMP vs STrack 对比
//!
//! 拓扑：4 Leaf × 2 Spine × 4 host/leaf = 16 hosts
//! 流量：15 个发送端同时向 host 0 发 64KB（典型 Incast）

use strack_sim::nic::{STrackProtocol, STrackMode};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::LeafSpine;
use strack_sim::traffic::Incast;

fn run_one(mode: STrackMode, label: &str) {
    println!("\n========== 模式：{} ==========", label);

    let topo = LeafSpine {
        n_leaf: 4,
        n_spine: 8,                            // 8 个 spine，为 STrack 多路径提供空间
        hosts_per_leaf: 4,
        host_link_bps: 100_000_000_000,       // 100 Gbps
        fabric_link_bps: 400_000_000_000,     // 400 Gbps
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,           // 20KB ECN 阈值
        buffer_bytes: 200_000,                  // 200KB 缓冲上限
    }.build();

    let n_hosts = topo.num_hosts();
    let mut runner = SimRunner::new(topo, label.to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, mode, topo))
    })
    .expect("SimRunner 初始化失败");

    // 1 receiver = host 0；其他 15 个都是发送端
    let senders: Vec<u32> = (1..n_hosts as u32).collect();
    let incast = Incast {
        senders,
        receiver: 0,
        bytes_per_sender: 512 * 1024,  // 512 KB per sender （总量 7.5 MB 拥入 1 个接收者）
        start_time_ns: 1000,
    };
    let flows = incast.generate();
    println!("注入流数：{}", flows.len());
    runner.inject_flows(flows);

    let t0 = std::time::Instant::now();
    runner.run(50_000_000); // 50 ms 仿真上限
    let dt = t0.elapsed();

    let summary = runner.summarize();
    println!("墙钟仿真耗时：{:?}", dt);
    summary.pretty_print();
}

fn main() {
    println!("=== STrack-Sim 端到端示例：Incast 场景 ===");
    println!("拓扑：4 Leaf × 2 Spine × 4 host/leaf = 16 hosts");
    println!("流量：15 个发送端同时向 host 0 发 64 KB（Incast）");

    run_one(STrackMode::Ecmp, "ECMP baseline");
    run_one(STrackMode::Strack, "STrack");
}
