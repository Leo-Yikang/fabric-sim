//! Dumbbell + All-to-All 协议对比实验
//!
//! 拓扑：Dumbbell（2 交换机 + 1 条瓶颈链路）
//!        每侧 4 主机，共 8 主机
//! 流量：All-to-All（每对节点之间各发一条流，8×7=56 条流）
//! 对比：STrack vs SimpleTcp
//!
//! # 实验设计指南：如何通过控制变量"描述"协议
//!
//! Dumbbell 是拥塞控制的经典实验床，因为只有一条瓶颈链路，所有跨侧流量
//! 都必须经过它。All-to-All 会在瓶颈处产生激烈竞争，适合观察：
//!
//! | 控制变量      | 修改位置                          | 观察指标              | 预期差异 |
//! |--------------|-----------------------------------|----------------------|---------|
//! | 瓶颈带宽      | `bottleneck_link_bps`             | FCT、avg_link_util   | 带宽越低，队列堆积越严重 |
//! | Buffer 大小   | `buffer_bytes`                    | drops、retransmitted | Buffer 小则丢包多，快速重传触发频繁 |
//! | 每流数据量    | `bytes_per_pair`                  | FCT P99              | 大流更能体现拥塞避免阶段差异 |
//! | 主机数量      | `hosts_per_side`                  | total_flows、公平性   | 流数越多，竞争越激烈 |
//! | ECN 阈值      | `ecn_threshold_bytes`             | ecn_marks、cwnd 变化 | 阈值低则 ECN 激进，降窗频繁 |
//!
//! **注意**：Dumbbell 只有单条跨侧链路，STrack 的多路径喷洒在此退化为
//! 单路径（与 ECMP 等价），因此本场景更适合观察"STrack 的 SACK + 切路
//! 逻辑 vs TCP 的 AIMD + 快速重传"在单瓶颈下的差异，而非多路径优势。
//! 若要测试 STrack 的多路径增益，请换用 LeafSpine / FatTree 拓扑。

use fabric_sim::nic::{STrackProtocol, STrackMode, SimpleTcp};
use fabric_sim::sim_runner::SimRunner;
use fabric_sim::topology::Dumbell;
use fabric_sim::traffic::AllToAll;

fn run_strack(label: &str) {
    println!("\n========== 模式：{} ==========", label);

    let topo = Dumbell {
        hosts_per_side: 4,                     // 每侧 4 主机，共 8 主机
        host_link_bps: 100_000_000_000,        // 100 Gbps 主机链路
        bottleneck_link_bps: 40_000_000_000,   // 40 Gbps 瓶颈（明显降级）
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,           // 20KB ECN 阈值
        buffer_bytes: 200_000,                 // 200KB 缓冲
    }.build();

    let n_hosts = topo.num_hosts();
    let mut runner = SimRunner::new(topo, label.to_string(), |h, topo| {
        Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
    })
    .expect("SimRunner 初始化失败");

    let nodes: Vec<u32> = (0..n_hosts as u32).collect();
    let alltoall = AllToAll {
        nodes,
        bytes_per_pair: 64 * 1024,  // 64 KB per pair
        start_time_ns: 1000,
    };
    let flows = alltoall.generate();
    println!("注入流数：{}（8 主机 All-to-All）", flows.len());
    runner.inject_flows(flows);

    let t0 = std::time::Instant::now();
    runner.run(50_000_000); // 50 ms 上限
    let dt = t0.elapsed();

    let summary = runner.summarize();
    println!("墙钟仿真耗时：{:?}", dt);
    summary.pretty_print();
}

fn run_tcp(label: &str) {
    println!("\n========== 模式：{} ==========", label);

    let topo = Dumbell {
        hosts_per_side: 4,
        host_link_bps: 100_000_000_000,
        bottleneck_link_bps: 40_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 200_000,
    }.build();

    let n_hosts = topo.num_hosts();
    let mut runner = SimRunner::new(topo, label.to_string(), |h, _topo| {
        Box::new(SimpleTcp::new(h))
    })
    .expect("SimRunner 初始化失败");

    let nodes: Vec<u32> = (0..n_hosts as u32).collect();
    let alltoall = AllToAll {
        nodes,
        bytes_per_pair: 64 * 1024,
        start_time_ns: 1000,
    };
    let flows = alltoall.generate();
    println!("注入流数：{}（8 主机 All-to-All）", flows.len());
    runner.inject_flows(flows);

    let t0 = std::time::Instant::now();
    runner.run(50_000_000);
    let dt = t0.elapsed();

    let summary = runner.summarize();
    println!("墙钟仿真耗时：{:?}", dt);
    summary.pretty_print();
}

fn main() {
    println!("=== Dumbbell + All-to-All 协议对比实验 ===");
    println!("拓扑：Dumbbell（4+4 主机，40Gbps 瓶颈）");
    println!("流量：All-to-All，每对 64KB，共 56 条流");
    println!("对比：STrack vs SimpleTcp");

    run_strack("STrack");
    run_tcp("SimpleTcp");
}
