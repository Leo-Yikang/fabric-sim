//! RDMA 端到端集成测试
//!
//! 验证 RdmaProtocol 能在一个小拓扑中完成多包 RDMA Write 消息，
//! 且 SimRunner::summarize() 能正确统计完成 flow。

use fabric_sim::monitor::SimSummary;
use fabric_sim::nic::RdmaProtocol;
use fabric_sim::sim_runner::SimRunner;
use fabric_sim::topology::Dumbell;
use fabric_sim::traffic::FlowDesc;

/// 构建最小的 2-host Dumbbell 拓扑：左侧 1 host，右侧 1 host
fn tiny_dumbell() -> Dumbell {
    Dumbell {
        hosts_per_side: 1,
        host_link_bps: 100_000_000_000, // 100Gbps
        bottleneck_link_bps: 100_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 200_000,
        buffer_bytes: 2_000_000,
    }
}

/// 运行一次仿真并返回摘要
fn run_rdma(flows: Vec<FlowDesc>) -> SimSummary {
    let topo = tiny_dumbell().build();
    let mut runner = SimRunner::new(topo, "rdma".to_string(), |h, _topo| {
        Box::new(RdmaProtocol::new(h))
    })
    .expect("SimRunner 初始化失败");
    runner.inject_flows(flows);
    runner.run(50_000_000);
    runner.summarize()
}

#[test]
fn rdma_single_packet_write_completes() {
    // 单包 Write（1 × MTU），验证最小端到端路径
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 1024, // 1 × MTU
        start_time_ns: 1000,
    }];
    let s = run_rdma(flows);
    assert_eq!(s.total_flows, 1);
    assert_eq!(s.completed_flows, 1, "单包 Write 应完成");
    assert!(s.fct_p50_ns > 0);
}

#[test]
fn rdma_multi_packet_write_completes() {
    // 多包 Write（4 × MTU），核心端到端验证
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024, // 4 × MTU
        start_time_ns: 1000,
    }];
    let s = run_rdma(flows);
    assert_eq!(s.total_flows, 1);
    assert_eq!(s.completed_flows, 1, "4 包 Write 应全部完成");
    assert!(s.fct_p99_ns > 0, "FCT 应大于 0");
    assert!(s.total_packets_sent > 0, "应有包发送");
}

#[test]
fn rdma_two_flows_complete() {
    // 两条并发流，验证多条流也能全部完成
    let flows = vec![
        FlowDesc {
            flow_id: 0,
            src: 0,
            dst: 1,
            bytes: 3 * 1024,
            start_time_ns: 1000,
        },
        FlowDesc {
            flow_id: 1,
            src: 0,
            dst: 1,
            bytes: 5 * 1024,
            start_time_ns: 2000,
        },
    ];
    let s = run_rdma(flows);
    assert_eq!(s.total_flows, 2);
    assert_eq!(s.completed_flows, 2, "两条并发 Write 流应全部完成");
    assert!(s.fct_p50_ns > 0);
    assert!(s.fct_p99_ns > 0);
}

#[test]
fn rdma_bidirectional_flows_complete() {
    // 双向流：host0→host1 和 host1→host0 同时发
    let flows = vec![
        FlowDesc {
            flow_id: 0,
            src: 0,
            dst: 1,
            bytes: 4 * 1024,
            start_time_ns: 1000,
        },
        FlowDesc {
            flow_id: 1,
            src: 1,
            dst: 0,
            bytes: 4 * 1024,
            start_time_ns: 1000,
        },
    ];
    let s = run_rdma(flows);
    assert_eq!(s.total_flows, 2);
    assert_eq!(s.completed_flows, 2, "双向流应全部完成");
}

#[test]
fn rdma_protocol_name_in_summary() {
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 1024,
        start_time_ns: 1000,
    }];
    let s = run_rdma(flows);
    assert_eq!(s.mode, "rdma", "协议名称应正确");
}

#[test]
fn rdma_post_recv_allows_send_to_complete() {
    // 使用 post_write 路径（start_flow 默认）验证多包 flow 完成
    // Send + recv WQE 的完整语义由单元测试覆盖（rnr_nak_on_send_without_recv_wqe）
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 2 * 1024,
        start_time_ns: 1000,
    }];
    let s = run_rdma(flows);
    assert_eq!(s.completed_flows, 1);
    assert!(
        s.total_packets_sent >= 2,
        "2 包 flow 至少发送 2 个数据包 + ACK"
    );
}
