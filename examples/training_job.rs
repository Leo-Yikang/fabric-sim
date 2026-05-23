//! P2 端到端示例：TrainingJob + CollectiveOp 对比
//!
//! 拓扑：4 Leaf × 2 Spine × 4 host/leaf = 16 hosts
//! 训练：4 个 rank，2 个 iteration，每个 iteration 包含 AllReduce + AllToAll

use strack_sim::nic::{STrackProtocol, STrackMode};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::LeafSpine;
use strack_sim::training::{ChunkConfig, CollectiveAlgorithm, CollectiveKind, CollectiveOp, Iteration, TrainingJob};

fn run_training(label: &str, algorithm: CollectiveAlgorithm) {
    println!("\n========== 模式：{} / {} ==========", label, format!("{:?}", algorithm).to_lowercase());

    let topo = LeafSpine {
        n_leaf: 4,
        n_spine: 8,
        hosts_per_leaf: 4,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 400_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();

    let n_hosts = topo.num_hosts();
    let nodes: Vec<u32> = (0..n_hosts as u32).collect();

    let job = TrainingJob {
        name: format!("{}_{:?}", label, algorithm),
        nodes: nodes.clone(),
        iterations: vec![
            Iteration {
                iter_id: 0,
                compute_delay_ns: 0,
                collectives: vec![
                    CollectiveOp {
                        kind: CollectiveKind::AllReduce,
                        algorithm,
                        nodes: nodes.clone(),
                        message_bytes: 4 * 1024 * 1024, // 4MB tensor
                        chunk_config: ChunkConfig {
                            chunk_size_bytes: 64 * 1024, // 64KB chunk
                            num_channels: 1,
                            pipeline_depth: 1,
                        },
                    },
                ],
            },
            Iteration {
                iter_id: 1,
                compute_delay_ns: 0,
                collectives: vec![
                    CollectiveOp {
                        kind: CollectiveKind::AllReduce,
                        algorithm,
                        nodes: nodes.clone(),
                        message_bytes: 4 * 1024 * 1024,
                        chunk_config: ChunkConfig {
                            chunk_size_bytes: 64 * 1024,
                            num_channels: 1,
                            pipeline_depth: 1,
                        },
                    },
                ],
            },
        ],
    };

    let mut runner = SimRunner::new(topo, label.to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
    })
    .expect("SimRunner 初始化失败");

    runner.inject_training_job(&job);

    let t0 = std::time::Instant::now();
    runner.run(50_000_000);
    let dt = t0.elapsed();

    let processed = runner.sim.processed();
    let summary = runner.summarize();
    println!("墙钟仿真耗时：{:?}", dt);
    println!(
        "处理事件总数：{}，有效吞吐：{:.2} M events/sec",
        processed,
        processed as f64 / dt.as_secs_f64() / 1_000_000.0
    );
    summary.pretty_print();
    runner.training_metrics.pretty_print();
}

fn main() {
    println!("=== STrack-Sim P2 示例：TrainingJob + CollectiveOp ===");
    println!("拓扑：4 Leaf × 8 Spine × 4 host/leaf = 16 hosts");
    println!("训练：2 iterations × AllReduce(4MB, 64KB chunk)");

    run_training("STrack", CollectiveAlgorithm::Ring);
    run_training("STrack", CollectiveAlgorithm::ReduceScatterThenAllGather);
    run_training("STrack", CollectiveAlgorithm::Tree);
}
