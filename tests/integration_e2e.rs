//! 端到端集成测试：完整跑 Incast 场景，验证两种模式都能完成

use fabric_sim::nic::{STrackProtocol, STrackMode};
use fabric_sim::sim_runner::SimRunner;
use fabric_sim::topology::LeafSpine;
use fabric_sim::traffic::Incast;

fn run(mode: STrackMode) -> fabric_sim::monitor::SimSummary {
    let topo = LeafSpine {
        n_leaf: 2, n_spine: 2, hosts_per_leaf: 3,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 400_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }.build();
    let n = topo.num_hosts();
    let proto_name = if mode == STrackMode::Ecmp { "ecmp" } else { "strack" };
    let mut runner = SimRunner::new(topo, proto_name.to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, mode, topo))
    })
    .expect("SimRunner 初始化失败");
    let senders: Vec<u32> = (1..n as u32).collect();
    let flows = Incast { senders, receiver: 0, bytes_per_sender: 16 * 1024, start_time_ns: 1000 }.generate();
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

#[test]
fn ecmp_incast_completes() {
    let s = run(STrackMode::Ecmp);
    assert_eq!(s.total_flows, 5);
    assert_eq!(s.completed_flows, 5, "ECMP 5 个 incast 流应当全部完成");
    assert!(s.fct_p99_ns > 0);
}

#[test]
fn strack_incast_completes() {
    let s = run(STrackMode::Strack);
    assert_eq!(s.total_flows, 5);
    assert_eq!(s.completed_flows, 5, "STrack 5 个 incast 流应当全部完成");
    assert!(s.fct_p99_ns > 0);
}

#[test]
fn ecn_and_drops_observable_under_incast() {
    // 验证拥塞场景下 ECN 标记 / 丢包确实被触发
    let s = run(STrackMode::Ecmp);
    assert!(s.total_ecn_marks > 0 || s.total_drops > 0, "拥塞场景下应该看到 ECN 或丢包");
}
