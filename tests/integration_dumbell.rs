//! Dumbbell + All-to-All 集成测试

use strack_sim::nic::{STrackProtocol, STrackMode, SimpleTcp};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::Dumbell;
use strack_sim::traffic::AllToAll;

fn make_dumbell() -> Dumbell {
    Dumbell {
        hosts_per_side: 3,                     // 3+3=6 主机，流数 30，测试规模适中
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }
}

fn run_strack() -> strack_sim::monitor::SimSummary {
    let topo = make_dumbell().build();
    let n = topo.num_hosts();
    let mut runner = SimRunner::new(topo, "strack".to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
    })
    .expect("SimRunner 初始化失败");
    let nodes: Vec<u32> = (0..n as u32).collect();
    let flows = AllToAll { nodes, bytes_per_pair: 16 * 1024, start_time_ns: 1000 }.generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

fn run_tcp() -> strack_sim::monitor::SimSummary {
    let topo = make_dumbell().build();
    let n = topo.num_hosts();
    let mut runner = SimRunner::new(topo, "tcp".to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");
    let nodes: Vec<u32> = (0..n as u32).collect();
    let flows = AllToAll { nodes, bytes_per_pair: 16 * 1024, start_time_ns: 1000 }.generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

#[test]
fn strack_dumbell_alltoall_completes() {
    let s = run_strack();
    // 6 主机 All-to-All = 30 条流
    assert_eq!(s.total_flows, 30);
    assert_eq!(s.completed_flows, 30, "STrack 应完成所有流");
    assert!(s.fct_p99_ns > 0);
}

#[test]
fn tcp_dumbell_alltoall_completes() {
    let s = run_tcp();
    assert_eq!(s.total_flows, 30);
    assert_eq!(s.completed_flows, 30, "SimpleTcp 应完成所有流");
    assert!(s.fct_p99_ns > 0);
}

#[test]
fn dumbell_bottleneck_creates_congestion() {
    // 验证瓶颈链路确实产生了拥塞（ECN 或丢包）
    let s = run_strack();
    assert!(
        s.total_ecn_marks > 0 || s.total_drops > 0,
        "40Gbps 瓶颈 + All-to-All 应产生 ECN 或丢包"
    );
}
