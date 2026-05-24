//! RDMA 端到端集成测试
//!
//! 验证 RdmaProtocol 能在一个小拓扑中完成多包 RDMA Write 消息，
//! 且 SimRunner::summarize() 能正确统计完成 flow。

use fabric_sim::monitor::SimSummary;
use fabric_sim::network::HostDelayConfig;
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

/// 运行一次仿真并返回摘要（无硬件延迟）
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

/// 运行一次仿真并返回摘要（带 RDMA 硬件延迟）
fn run_rdma_with_delay(flows: Vec<FlowDesc>) -> SimSummary {
    let topo = tiny_dumbell().build();
    let n_hosts = topo.hosts.len();
    let mut runner = SimRunner::new(topo, "rdma-delay".to_string(), |h, _topo| {
        Box::new(RdmaProtocol::new(h))
    })
    .expect("SimRunner 初始化失败");
    // 启用 RDMA 零拷贝延迟模型：doorbell + PCIe + CQ poll
    let delays = HostDelayConfig::uniform(
        fabric_sim::network::HostDelayModel::rdma_zerocopy(),
        n_hosts,
    );
    runner.host_delays = delays;
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

#[test]
fn rdma_with_host_delay_completes_and_slower() {
    // 验证启用硬件延迟后：
    // 1. flow 仍能完成
    // 2. FCT 比零延迟场景更大（体现延迟注入有效）
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024, // 4 × MTU
        start_time_ns: 1000,
    }];

    let s_no_delay = run_rdma(flows.clone());
    let s_with_delay = run_rdma_with_delay(flows);

    assert_eq!(s_no_delay.completed_flows, 1, "零延迟场景应完成");
    assert_eq!(s_with_delay.completed_flows, 1, "带延迟场景应完成");
    assert!(
        s_with_delay.fct_p50_ns > s_no_delay.fct_p50_ns,
        "带硬件延迟的 FCT 应大于零延迟场景: delay={}ns vs no_delay={}ns",
        s_with_delay.fct_p50_ns,
        s_no_delay.fct_p50_ns
    );
}

#[test]
fn rdma_large_host_delay_no_false_retransmit() {
    // 配置一个明显大于 RTO 的 host tx delay（200μs > RTO 100μs），
    // 验证不会因为"包还没离开主机"而提前触发错误重传。
    // 如果 send_times 记录的是原始 now 而非真实 NIC 出主机时间，
    // RTO 会在包尚未注入网络时就触发，导致大量错误重传。
    let topo = tiny_dumbell().build();
    let n_hosts = topo.hosts.len();
    let mut runner = SimRunner::new(topo, "rdma-big-delay".to_string(), |h, _topo| {
        Box::new(RdmaProtocol::new(h))
    })
    .expect("SimRunner 初始化失败");

    // 构造一个 tx delay 200μs（大于 RTO 100μs）的模型
    let mut big_delay = fabric_sim::network::HostDelayModel::rdma_zerocopy();
    big_delay.doorbell_ns = 200_000; // 200μs
    runner.host_delays = HostDelayConfig::uniform(big_delay, n_hosts);

    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024,
        start_time_ns: 1000,
    }];
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();

    assert_eq!(s.completed_flows, 1, "大延迟场景仍应完成");
    assert_eq!(
        s.total_packets_retransmitted, 0,
        "tx delay > RTO 时不应触发错误重传；若重传>0 说明 send_times 用了错误的原始时间"
    );
}

#[test]
fn host_delay_model_integer_math() {
    // 验证 HostDelayModel 的整数延迟计算：
    // 1. 小包（64B）仍有固定开销（doorbell + PCIe 不会被截断为 0）
    // 2. 大包延迟大于小包
    use fabric_sim::network::HostDelayModel;

    let model = HostDelayModel::rdma_zerocopy();
    let small = model.tx_delay_ns(64);
    let large = model.tx_delay_ns(64 * 1024);

    // 零拷贝模型：固定开销 = doorbell(200) + PCIe(500) = 700ns
    assert!(
        small >= 700,
        "64B 小包至少应有固定开销 700ns，实际={}ns",
        small
    );
    assert!(
        large >= small,
        "大包延迟应不小于小包: large={}ns >= small={}ns",
        large,
        small
    );

    // TCP 模型：验证 memcpy 带宽计算
    let tcp = HostDelayModel::tcp_kernel();
    let tcp_small = tcp.tx_delay_ns(64);
    let tcp_large = tcp.tx_delay_ns(1024);
    assert!(
        tcp_small >= tcp.kernel_stack_ns,
        "TCP 小包至少应有内核栈延迟 {}ns",
        tcp.kernel_stack_ns
    );
    assert!(
        tcp_large > tcp_small,
        "TCP 大包应包含 memcpy 延迟: large={}ns > small={}ns",
        tcp_large,
        tcp_small
    );
}

/// 同一 host 上多条 RDMA flow 同时发送，验证 update_send_time
/// 不会误写其他 flow 的 send_times。
#[test]
fn rdma_multi_flow_no_cross_contamination() {
    let topo = tiny_dumbell().build();
    let n_hosts = topo.hosts.len();
    let mut runner = SimRunner::new(topo, "rdma-multi".to_string(), |h, _topo| {
        Box::new(RdmaProtocol::new(h))
    })
    .expect("SimRunner 初始化失败");

    let mut model = fabric_sim::network::HostDelayModel::rdma_zerocopy();
    model.doorbell_ns = 100_000; // 100μs ≈ RTO，确保延迟足够大
    runner.host_delays = HostDelayConfig::uniform(model, n_hosts);

    // 同一 host 上两条 flow，都从 seq=0 开始
    let flows = vec![
        FlowDesc { flow_id: 0, src: 0, dst: 1, bytes: 3 * 1024, start_time_ns: 1000 },
        FlowDesc { flow_id: 1, src: 0, dst: 1, bytes: 3 * 1024, start_time_ns: 1000 },
    ];
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();

    assert_eq!(s.completed_flows, 2, "两条 flow 应全部完成");
}

/// 非 RDMA 协议（SimpleTcp）在 tx_delay > RTO 时不应错误重传。
#[test]
fn tcp_large_host_delay_no_false_retransmit() {
    use fabric_sim::nic::SimpleTcp;

    let topo = tiny_dumbell().build();
    let n_hosts = topo.hosts.len();
    let mut runner = SimRunner::new(topo, "tcp-delay".to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");

    let mut big_delay = fabric_sim::network::HostDelayModel::tcp_kernel();
    // 令门铃延迟 300μs > TCP RTO 100μs
    big_delay.doorbell_ns = 300_000;
    big_delay.pcie_roundtrip_ns = 0;
    runner.host_delays = HostDelayConfig::uniform(big_delay, n_hosts);

    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024,
        start_time_ns: 1000,
    }];
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();

    assert_eq!(s.completed_flows, 1, "TCP flow 应完成");
    assert_eq!(
        s.total_packets_retransmitted, 0,
        "tx delay > RTO 时 TCP 不应触发错误重传；实际 retransmitted={}",
        s.total_packets_retransmitted
    );
}

/// 验证 DMA 带宽>0 时，多包 batch 被主机 DMA 引擎串行化：
/// 同批次的第 N 个包不能和第 1 个包同时离开 NIC，需等待前 N-1 包的 DMA 完成。
#[test]
fn host_dma_serializes_burst() {
    use fabric_sim::network::HostDelayModel;

    let topo = tiny_dumbell().build();
    let n_hosts = topo.hosts.len();
    let mut runner = SimRunner::new(topo, "rdma-dma".to_string(), |h, _topo| {
        Box::new(RdmaProtocol::new(h))
    })
    .expect("SimRunner 初始化失败");

    // rdma_with_staging: DMA 100 Gbps → 1024B 约 82ns DMA 时间
    let model = HostDelayModel::rdma_with_staging();
    runner.host_delays = HostDelayConfig::uniform(model, n_hosts);

    // 4 包 flow，cwnd=16，全部在同一 batch 发出
    let flows = vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024,
        start_time_ns: 1000,
    }];
    runner.inject_flows(flows);
    runner.run(50_000_000);
    let s = runner.summarize();

    assert_eq!(s.completed_flows, 1, "flow 应完成");
    // 直接断言 host_metrics：DMA 带宽>0 时应产生 DMA 时间、排队等待和队列深度
    assert!(
        s.host_metrics.total_dma_time_ns > 0,
        "DMA 带宽 100 Gbps 时累计 DMA 时间应 >0"
    );
    assert!(
        s.host_metrics.max_queue_depth_ns > 0,
        "多包 burst 应有 DMA 排队深度 >0"
    );
    assert!(
        s.host_metrics.total_queue_wait_ns > 0,
        "多包 burst 应有排队等待 >0"
    );
    // DMA 串行化会拉长 burst，因此 FCT 应大于无延迟场景
    let s_no_delay = run_rdma(vec![FlowDesc {
        flow_id: 0,
        src: 0,
        dst: 1,
        bytes: 4 * 1024,
        start_time_ns: 1000,
    }]);
    assert!(
        s.fct_p50_ns > s_no_delay.fct_p50_ns + 200,
        "DMA 串行化应使 FCT 明显大于零延迟: dma={}ns vs no_dma={}ns",
        s.fct_p50_ns,
        s_no_delay.fct_p50_ns
    );
}

/// P2：验证 NicSelector 不会破坏只有一条上行链路的拓扑。
#[test]
fn nic_selector_with_single_uplink_still_works() {
    use fabric_sim::sim_runner::NicSelector;

    let run_with = |sel: NicSelector| -> fabric_sim::monitor::SimSummary {
        let topo = tiny_dumbell().build();
        let mut runner = SimRunner::new(topo, "test".to_string(), |h, _| {
            Box::new(RdmaProtocol::new(h))
        })
        .expect("init")
        .with_nic_selector(sel);
        let flows = vec![FlowDesc {
            flow_id: 0, src: 0, dst: 1, bytes: 2 * 1024, start_time_ns: 1000,
        }];
        runner.inject_flows(flows);
        runner.run(50_000_000);
        runner.summarize()
    };

    for sel in [NicSelector::First, NicSelector::FlowHash, NicSelector::RoundRobin] {
        let s = run_with(sel);
        assert_eq!(s.completed_flows, 1, "NicSelector::{:?} 应正常完成 flow", sel);
    }
}
