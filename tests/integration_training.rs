//! P2 集成测试：TrainingJob + CollectiveOp 端到端验证

use strack_sim::nic::{STrackProtocol, STrackMode};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::Dumbell;
use strack_sim::training::{
    ChunkConfig, CollectiveAlgorithm, CollectiveKind, CollectiveOp, Iteration, TrainingJob,
};

fn make_runner() -> SimRunner {
    let topo = Dumbell {
        hosts_per_side: 4,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 100_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();
    SimRunner::new(topo, "strack".to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
    })
    .expect("SimRunner 初始化失败")
}

#[test]
fn training_job_ring_allreduce_completes() {
    let mut runner = make_runner();
    let nodes: Vec<u32> = (0..8).collect();
    let job = TrainingJob {
        name: "test_ring".to_string(),
        nodes: nodes.clone(),
        iterations: vec![Iteration {
            iter_id: 0,
            compute_delay_ns: 0,
            collectives: vec![CollectiveOp {
                kind: CollectiveKind::AllReduce,
                algorithm: CollectiveAlgorithm::Ring,
                nodes,
                message_bytes: 1024 * 1024,
                chunk_config: ChunkConfig {
                    chunk_size_bytes: 64 * 1024,
                    num_channels: 1,
                    pipeline_depth: 1,
                },
            }],
        }],
    };
    runner.inject_training_job(&job);
    runner.run(10_000_000);
    let summary = runner.summarize();
    assert_eq!(summary.total_flows, 8 * 7 * 2, "Ring AllReduce 应产生 2*(N-1)*N 条流");
    assert_eq!(summary.completed_flows, summary.total_flows, "所有流应完成");
    assert_eq!(runner.training_metrics.completed_iterations, 1);
    assert!(!runner.training_metrics.iteration_times_ns.is_empty());
    assert!(
        runner.training_metrics.iteration_times_ns[0] > 0,
        "iteration time 应大于 0"
    );
}

#[test]
fn training_job_reduce_scatter_all_gather_completes() {
    let mut runner = make_runner();
    let nodes: Vec<u32> = (0..8).collect();
    let job = TrainingJob {
        name: "test_rs_ag".to_string(),
        nodes: nodes.clone(),
        iterations: vec![Iteration {
            iter_id: 0,
            compute_delay_ns: 0,
            collectives: vec![CollectiveOp {
                kind: CollectiveKind::AllReduce,
                algorithm: CollectiveAlgorithm::ReduceScatterThenAllGather,
                nodes,
                message_bytes: 1024 * 1024,
                chunk_config: ChunkConfig {
                    chunk_size_bytes: 64 * 1024,
                    num_channels: 1,
                    pipeline_depth: 1,
                },
            }],
        }],
    };
    runner.inject_training_job(&job);
    runner.run(10_000_000);
    let summary = runner.summarize();
    assert_eq!(summary.total_flows, 8 * 7 * 2);
    assert_eq!(summary.completed_flows, summary.total_flows);
    assert_eq!(runner.training_metrics.completed_iterations, 1);
}

#[test]
fn training_job_tree_completes() {
    let mut runner = make_runner();
    let nodes: Vec<u32> = (0..8).collect();
    let job = TrainingJob {
        name: "test_tree".to_string(),
        nodes: nodes.clone(),
        iterations: vec![Iteration {
            iter_id: 0,
            compute_delay_ns: 0,
            collectives: vec![CollectiveOp {
                kind: CollectiveKind::AllReduce,
                algorithm: CollectiveAlgorithm::Tree,
                nodes,
                message_bytes: 1024 * 1024,
                chunk_config: ChunkConfig {
                    chunk_size_bytes: 64 * 1024,
                    num_channels: 1,
                    pipeline_depth: 1,
                },
            }],
        }],
    };
    runner.inject_training_job(&job);
    runner.run(10_000_000);
    let summary = runner.summarize();
    // Tree AllReduce: 2*(N-1) flows for power-of-two
    assert_eq!(summary.total_flows, 14);
    assert_eq!(summary.completed_flows, 14);
    assert_eq!(runner.training_metrics.completed_iterations, 1);
}

#[test]
fn training_job_two_iterations() {
    let mut runner = make_runner();
    let nodes: Vec<u32> = (0..4).collect();
    let job = TrainingJob {
        name: "test_two_iters".to_string(),
        nodes: nodes.clone(),
        iterations: vec![
            Iteration {
                iter_id: 0,
                compute_delay_ns: 0,
                collectives: vec![CollectiveOp {
                    kind: CollectiveKind::AllReduce,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes: nodes.clone(),
                    message_bytes: 256 * 1024,
                    chunk_config: ChunkConfig::default(),
                }],
            },
            Iteration {
                iter_id: 1,
                compute_delay_ns: 0,
                collectives: vec![CollectiveOp {
                    kind: CollectiveKind::AllReduce,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes,
                    message_bytes: 256 * 1024,
                    chunk_config: ChunkConfig::default(),
                }],
            },
        ],
    };
    runner.inject_training_job(&job);
    runner.run(10_000_000);
    let summary = runner.summarize();
    assert_eq!(summary.completed_flows, summary.total_flows);
    assert_eq!(runner.training_metrics.completed_iterations, 2);
    assert_eq!(runner.training_metrics.iteration_times_ns.len(), 2);
}

#[test]
fn training_job_multiple_collectives_per_iteration() {
    let mut runner = make_runner();
    let nodes: Vec<u32> = (0..4).collect();
    let job = TrainingJob {
        name: "test_multi_collective".to_string(),
        nodes: nodes.clone(),
        iterations: vec![Iteration {
            iter_id: 0,
            compute_delay_ns: 0,
            collectives: vec![
                CollectiveOp {
                    kind: CollectiveKind::AllReduce,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes: nodes.clone(),
                    message_bytes: 256 * 1024,
                    chunk_config: ChunkConfig::default(),
                },
                CollectiveOp {
                    kind: CollectiveKind::AllToAll,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes,
                    message_bytes: 256 * 1024,
                    chunk_config: ChunkConfig::default(),
                },
            ],
        }],
    };
    runner.inject_training_job(&job);
    runner.run(10_000_000);
    let summary = runner.summarize();
    assert_eq!(summary.completed_flows, summary.total_flows);
    assert_eq!(runner.training_metrics.completed_iterations, 1);
    assert_eq!(runner.training_metrics.collective_completion_ns.len(), 2);
}
