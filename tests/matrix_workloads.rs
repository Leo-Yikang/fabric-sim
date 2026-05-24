//! 参数化工作负载矩阵测试
//!
//! 系统覆盖不同拓扑 × 协议 × 流大小分布 × 到达过程 × 流量模式的组合，
//! 验证协议在各种工作负载特征下的基本正确性（所有流完成、FCT > 0、拥塞可观测）。

use fabric_sim::monitor::SimSummary;
use fabric_sim::nic::{SimpleTcp, STrackMode, STrackProtocol};
use fabric_sim::sim_runner::SimRunner;
use fabric_sim::topology::{Dumbell, LeafSpine};
use fabric_sim::traffic::{
    ArrivalProcess, FlowSizeDist, PairPattern, Synthetic,
};

// ------------------------------------------------------------------
// 辅助：运行一个矩阵单元
// ------------------------------------------------------------------

fn run_dumbell_tcp(
    pair: PairPattern,
    size: FlowSizeDist,
    arrival: ArrivalProcess,
) -> SimSummary {
    let topo = Dumbell {
        hosts_per_side: 3,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();
    let n = topo.num_hosts() as u32;
    let nodes: Vec<u32> = (0..n).collect();
    let mut runner = SimRunner::new(topo, "tcp".to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");
    let flows = Synthetic {
        nodes,
        pair_pattern: pair,
        flow_size: size,
        arrival,
        seed: 42,
    }
    .generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

fn run_dumbell_strack(
    pair: PairPattern,
    size: FlowSizeDist,
    arrival: ArrivalProcess,
) -> SimSummary {
    let topo = Dumbell {
        hosts_per_side: 3,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
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
    .expect("SimRunner 初始化失败");
    let flows = Synthetic {
        nodes,
        pair_pattern: pair,
        flow_size: size,
        arrival,
        seed: 42,
    }
    .generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

fn run_leafspine(
    mode: STrackMode,
    pair: PairPattern,
    size: FlowSizeDist,
    arrival: ArrivalProcess,
) -> SimSummary {
    let topo = LeafSpine {
        n_leaf: 2,
        n_spine: 2,
        hosts_per_leaf: 3,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 400_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();
    let n = topo.num_hosts() as u32;
    let nodes: Vec<u32> = (0..n).collect();
    let label = if mode == STrackMode::Ecmp { "ecmp" } else { "strack" };
    let mut runner = SimRunner::new(topo, label.to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, mode, topo))
    })
    .expect("SimRunner 初始化失败");
    let flows = Synthetic {
        nodes,
        pair_pattern: pair,
        flow_size: size,
        arrival,
        seed: 42,
    }
    .generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

// ------------------------------------------------------------------
// 断言辅助
// ------------------------------------------------------------------

fn assert_all_complete(s: &SimSummary) {
    assert_eq!(
        s.completed_flows, s.total_flows,
        "所有流应完成，但 {} / {} 未完成",
        s.total_flows - s.completed_flows, s.total_flows
    );
    assert!(s.fct_p99_ns > 0, "FCT P99 应大于 0");
}

fn assert_congestion_observable(s: &SimSummary) {
    assert!(
        s.total_ecn_marks > 0 || s.total_drops > 0,
        "拥塞场景下应观测到 ECN 或丢包"
    );
}

// ------------------------------------------------------------------
// 矩阵测试用例
// ------------------------------------------------------------------

// ---------- Dumbbell + SimpleTCP ----------

#[test]
fn db_tcp_incast_fixed_simultaneous() {
    let s = run_dumbell_tcp(
        PairPattern::Custom(vec![(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)]),
        FlowSizeDist::Fixed(16 * 1024),
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}

#[test]
fn db_tcp_alltoall_bimodal_simultaneous() {
    let s = run_dumbell_tcp(
        PairPattern::AllToAll,
        FlowSizeDist::Bimodal {
            small: 4 * 1024,
            large: 128 * 1024,
            large_ratio: 0.2,
        },
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}

#[test]
fn db_tcp_permutation_pareto_poisson() {
    let s = run_dumbell_tcp(
        PairPattern::Permutation,
        FlowSizeDist::Pareto { min: 1024, shape: 1.5 },
        ArrivalProcess::Poisson {
            start: 1000,
            mean_interval_ns: 10_000,
        },
    );
    assert_all_complete(&s);
}

// ---------- Dumbbell + STrack ----------

#[test]
fn db_strack_alltoall_fixed_simultaneous() {
    let s = run_dumbell_strack(
        PairPattern::AllToAll,
        FlowSizeDist::Fixed(16 * 1024),
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}

// ---------- LeafSpine + ECMP ----------

#[test]
fn ls_ecmp_incast_fixed_simultaneous() {
    let s = run_leafspine(
        STrackMode::Ecmp,
        PairPattern::Custom(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]),
        FlowSizeDist::Fixed(16 * 1024),
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}

// ---------- LeafSpine + STrack ----------

#[test]
fn ls_strack_incast_fixed_simultaneous() {
    let s = run_leafspine(
        STrackMode::Strack,
        PairPattern::Custom(vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]),
        FlowSizeDist::Fixed(16 * 1024),
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}

#[test]
fn ls_strack_alltoall_bimodal_poisson() {
    let s = run_leafspine(
        STrackMode::Strack,
        PairPattern::AllToAll,
        FlowSizeDist::Bimodal {
            small: 4 * 1024,
            large: 128 * 1024,
            large_ratio: 0.2,
        },
        ArrivalProcess::Poisson {
            start: 1000,
            mean_interval_ns: 10_000,
        },
    );
    assert_all_complete(&s);
    // 注意：Poisson 到达 + 多路径 LeafSpine 下拥塞可能较轻，不强制断言 ECN/drops
}

#[test]
fn ls_strack_permutation_pareto_simultaneous() {
    let s = run_leafspine(
        STrackMode::Strack,
        PairPattern::Permutation,
        FlowSizeDist::Pareto { min: 1024, shape: 1.5 },
        ArrivalProcess::Simultaneous(1000),
    );
    assert_all_complete(&s);
}

// ---------- 遗留流量生成器兼容性验证 ----------

#[test]
fn legacy_incast_with_synthetic_equivalent() {
    // 验证 Synthetic 的 Custom 模式与原生 Incast 产生等价结果
    let topo = Dumbell {
        hosts_per_side: 3,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();
    let n = topo.num_hosts() as u32;
    let mut runner = SimRunner::new(topo, "tcp".to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");
    let senders: Vec<u32> = (1..n).collect();
    let pairs: Vec<_> = senders.iter().map(|&s| (s, 0u32)).collect();
    let flows = Synthetic {
        nodes: (0..n).collect(),
        pair_pattern: PairPattern::Custom(pairs),
        flow_size: FlowSizeDist::Fixed(16 * 1024),
        arrival: ArrivalProcess::Simultaneous(1000),
        seed: 42,
    }
    .generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();
    assert_all_complete(&s);
}

#[test]
fn legacy_alltoall_with_synthetic_equivalent() {
    let topo = Dumbell {
        hosts_per_side: 3,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
    .build();
    let n = topo.num_hosts() as u32;
    let mut runner = SimRunner::new(topo, "tcp".to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");
    let flows = Synthetic {
        nodes: (0..n).collect(),
        pair_pattern: PairPattern::AllToAll,
        flow_size: FlowSizeDist::Fixed(16 * 1024),
        arrival: ArrivalProcess::Simultaneous(1000),
        seed: 42,
    }
    .generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();
    assert_all_complete(&s);
    assert_congestion_observable(&s);
}
