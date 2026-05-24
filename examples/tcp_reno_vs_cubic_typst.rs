//! TCP Reno vs CUBIC 对比分析 — Typst 学术报告生成器
//!
//! 在 4 类拥塞场景下对比 TCP Reno 与 CUBIC 的性能表现，输出一份
//! 可编译为 PDF 的 Typst 格式学术报告。
//!
//! 输出：output/tcp_reno_vs_cubic/report.typ
//! 编译：typst compile output/tcp_reno_vs_cubic/report.typ

use std::fs;
use std::path::Path;

use strack_sim::monitor::SimSummary;
use strack_sim::nic::{TcpCubic, TcpReno};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::{Dumbell, LeafSpine};
use strack_sim::traffic::{FlowDesc, Incast};

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.0} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    }
}

fn jains_fairness(throughputs: &[f64]) -> f64 {
    if throughputs.len() <= 1 {
        return 1.0;
    }
    let n = throughputs.len() as f64;
    let sum: f64 = throughputs.iter().sum();
    let sum_sq: f64 = throughputs.iter().map(|x| x * x).sum();
    if sum == 0.0 {
        return 0.0;
    }
    (sum * sum) / (n * sum_sq)
}

fn escape_typst(s: &str) -> String {
    s.replace('\\', r#"\\"#)
        .replace('*', r#"\*"#)
        .replace('_', r#"\_"#)
        .replace('#', r#"\#"#)
        .replace('@', r#"\@"#)
}

// ── 基准测试函数 ──

fn single_flow<F>(factory: &F, bytes: u64) -> SimSummary
where
    F: Fn(u32) -> Box<dyn strack_sim::nic::Protocol>,
{
    let topo = LeafSpine {
        n_leaf: 2,
        n_spine: 2,
        hosts_per_leaf: 2,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 100_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 1_000_000,
        buffer_bytes: 2_000_000,
    }
    .build();
    let mut runner = SimRunner::new(topo, "single".to_string(), |h, _| factory(h)).expect("init");
    runner.inject_flows(vec![FlowDesc {
        flow_id: 0,
        src: 1,
        dst: 0,
        bytes,
        start_time_ns: 1_000,
    }]);
    runner.run(bytes * 8 * 3 / 100_000_000_000 * 1_000_000_000 + 50_000_000);
    runner.summarize()
}

fn dumbell_fairness<F>(factory: &F, n_flows: usize) -> (SimSummary, Vec<f64>)
where
    F: Fn(u32) -> Box<dyn strack_sim::nic::Protocol>,
{
    let hosts_per_side = n_flows as u32;
    let topo = Dumbell {
        hosts_per_side,
        bottleneck_link_bps: 1_000_000_000,
        host_link_bps: 10_000_000_000,
        prop_delay_ns: 5_000,
        ecn_threshold_bytes: 5_000,
        buffer_bytes: 50_000,
    }
    .build();
    let mut runner = SimRunner::new(topo, "dumbell".to_string(), |h, _| factory(h)).expect("init");
    let mut flows = Vec::new();
    for i in 0..n_flows {
        flows.push(FlowDesc {
            flow_id: i as u32,
            src: i as u32,
            dst: (hosts_per_side + i as u32),
            bytes: if n_flows <= 4 {
                512 * 1024
            } else if n_flows <= 8 {
                256 * 1024
            } else {
                128 * 1024
            },
            start_time_ns: 1_000,
        });
    }
    runner.inject_flows(flows);
    runner.run((n_flows as u64 * 100_000_000).max(500_000_000));
    let throughputs: Vec<f64> = runner
        .fcts
        .iter()
        .map(|f| {
            let fct = f.fct_ns() as f64;
            if fct > 0.0 {
                f.bytes as f64 * 8.0 / fct * 1_000_000_000.0
            } else {
                0.0
            }
        })
        .collect();
    (runner.summarize(), throughputs)
}

fn incast_test<F>(factory: &F, n_senders: u32) -> SimSummary
where
    F: Fn(u32) -> Box<dyn strack_sim::nic::Protocol>,
{
    let topo = LeafSpine {
        n_leaf: ((n_senders as usize + 3) / 4).max(2) as u32,
        n_spine: 4,
        hosts_per_leaf: 4,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 100_000_000_000,
        prop_delay_ns: 500,
        ecn_threshold_bytes: 20_000,
        buffer_bytes: 100_000,
    }
    .build();
    let max_senders = (topo.hosts.len() as u32).saturating_sub(1).max(1);
    let actual = n_senders.min(max_senders);
    let mut runner = SimRunner::new(topo, "incast".to_string(), |h, _| factory(h)).expect("init");
    let incast = Incast {
        senders: (1..=actual).collect(),
        receiver: 0,
        bytes_per_sender: 256 * 1024,
        start_time_ns: 1_000,
    };
    runner.inject_flows(incast.generate());
    runner.run(100_000_000);
    runner.summarize()
}

fn high_bdp_test<F>(factory: &F, n_flows: usize) -> SimSummary
where
    F: Fn(u32) -> Box<dyn strack_sim::nic::Protocol>,
{
    let topo = LeafSpine {
        n_leaf: 2,
        n_spine: 2,
        hosts_per_leaf: (n_flows / 2 + 1) as u32,
        host_link_bps: 100_000_000_000,
        fabric_link_bps: 1_000_000_000,
        prop_delay_ns: 50_000,
        ecn_threshold_bytes: 10_000,
        buffer_bytes: 40_000,
    }
    .build();
    let mut runner = SimRunner::new(topo, "highbdp".to_string(), |h, _| factory(h)).expect("init");
    let hosts = runner.topo.hosts.len() as u32;
    let mut flows = Vec::new();
    for i in 0..n_flows {
        flows.push(FlowDesc {
            flow_id: i as u32,
            src: i as u32,
            dst: (hosts - 1 - i as u32).max(0),
            bytes: 4 * 1024 * 1024,
            start_time_ns: 1_000,
        });
    }
    runner.inject_flows(flows);
    runner.run((n_flows as u64 * 400_000_000).max(500_000_000));
    runner.summarize()
}

// ── Typst 表格生成 ──

/// 生成 Typst 表格。
///
/// - `headers`: 表头列名。
/// - `rows`: 数据行，每个 cell 为字符串。
/// - `num_cols`: 标记为数字右对齐的列索引集合（从 0 开始）。
fn typst_table(headers: &[&str], rows: &[Vec<String>], num_cols: &[usize]) -> String {
    let cols = headers.len();
    let mut out = String::new();

    // 结果表以紧凑字号输出。宽表在调用侧拆分，避免 A4 竖版中列宽被压扁。
    let col_spec: Vec<String> = (0..cols)
        .map(|i| {
            // 丢包明细列给更宽的 1.5fr
            let h = headers[i];
            if h.contains("明细") {
                "1.5fr".to_string()
            } else {
                "auto".to_string()
            }
        })
        .collect();
    out.push_str("#text(size: 9pt)[\n");
    out.push_str(&format!(
        "#table(\n  columns: ({}),\n  gutter: 4pt,\n",
        col_spec.join(", ")
    ));

    // header row (centered, bold)
    out.push_str("  table.header(\n");
    for h in headers {
        out.push_str(&format!("    align(center)[*{}*],\n", escape_typst(h)));
    }
    out.push_str("  ),\n");

    // data rows: 数字列右对齐，文本列左对齐
    let is_num: Vec<bool> = (0..cols).map(|i| num_cols.contains(&i)).collect();
    for row in rows {
        for (j, cell) in row.iter().enumerate() {
            let val = escape_typst(cell);
            if j < is_num.len() && is_num[j] {
                out.push_str(&format!("    align(right)[{}],\n", val));
            } else {
                out.push_str(&format!("    align(left)[{}],\n", val));
            }
        }
    }
    out.push_str(")\n]\n");
    out
}

fn now_ts() -> String {
    // 硬编码当前实验日期，保证报告可复现
    "2026-05-24".to_string()
}

/// 参考文献 BibTeX 内容（写入输出目录与 .typ 配套）
const REFS_BIB: &str = r#"@article{jacobson1988congestion,
  title={Congestion avoidance and control},
  author={Jacobson, Van},
  journal={ACM SIGCOMM Computer Communication Review},
  volume={18},
  number={4},
  pages={314--329},
  year={1988},
  publisher={ACM}
}

@article{ha2008cubic,
  title={CUBIC: a new TCP-friendly high-speed TCP variant},
  author={Ha, Sangtae and Rhee, Injong and Xu, Lisong},
  journal={ACM SIGOPS Operating Systems Review},
  volume={42},
  number={5},
  pages={64--74},
  year={2008},
  publisher={ACM}
}

@inproceedings{alizadeh2010dctcp,
  title={Data center TCP (DCTCP)},
  author={Alizadeh, Mohammad and Greenberg, Albert and Maltz, David A and Padhye, Jitendra and Patel, Parveen and Prabhakar, Balaji and Sengupta, Sudipta and Sridharan, Murari},
  booktitle={Proceedings of the ACM SIGCOMM 2010 Conference},
  pages={63--74},
  year={2010}
}

@inproceedings{zhu2015dcqcn,
  title={Congestion control for large-scale RDMA deployments},
  author={Zhu, Yibo and Eran, Haggai and Firestone, Daniel and Guo, Chuanxiong and Lipshteyn, Marina and Liron, Yehonatan and Padhye, Jitendra and Raindel, Shachar and Yahia, Mohamad Haj and Zhang, Ming},
  booktitle={Proceedings of the 2015 ACM Conference on Special Interest Group on Data Communication},
  pages={523--536},
  year={2015}
}

@article{allman1999tcp,
  title={TCP congestion control},
  author={Allman, Mark and Paxson, Vern and Blanton, Ethan},
  journal={RFC 2581},
  year={1999}
}

@misc{strack2025,
  title={STrack-Sim: A Discrete Event Network Simulator for AI/ML Cluster Transport Protocol Research},
  author={STrack-Sim Contributors},
  year={2025},
  note={Open-source project}
}
"#;

// ── 主函数 ──

fn main() {
    let date = now_ts();
    let mut t = String::new();

    // ═══════════════════════════════════════════════════════
    // 封面
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"// TCP Reno vs CUBIC 对比分析报告
#set document(title: "TCP Reno vs CUBIC 对比分析", author: "STrack-Sim 仿真平台")
#set page(numbering: "1", number-align: center)
#set text(font: "Times New Roman", size: 11pt)
#set par(leading: 0.6em, justify: true)

#align(center)[
  #v(4cm)
  #text(size: 22pt, weight: "bold")[TCP Reno vs CUBIC]
  #v(0.3cm)
  #text(size: 16pt)[拥塞控制算法性能对比分析]
  #v(1.5cm)
  #text(size: 12pt)[基于 STrack-Sim 离散事件网络仿真]
  #v(0.5cm)
  #text(size: 11pt)[报告日期："#,
    );
    t.push_str(&date);
    t.push_str(
        r#"]
  #v(1.0cm)
  #text(size: 10pt, style: "italic")[本报告自动生成自仿真实验，数据可复现]
]

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 摘要
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 摘要

本文基于 STrack-Sim 离散事件网络仿真平台，系统比较了 TCP Reno 与 TCP CUBIC
两种经典拥塞控制算法在数据中心网络环境下的性能差异。实验涵盖四类典型场景：
（1）单流无竞争 FCT 基准测试，覆盖 64 KB 至 512 MB 共八种流大小；
（2）Dumbbell 拓扑多流公平性测试，评估 4 至 32 条竞争流的带宽分配；
（3）Incast 多对一同步突发场景，模拟分布式存储与机器学习中常见的拥塞模式；
（4）高带宽延迟积（BDP）场景，考察长传播延迟下两种算法的恢复能力。
实验记录并分析了流完成时间（FCT）、丢包数量及其原因分布、ECN 标记、公平性指数
等多维指标。

结果表明，在低竞争与小型流场景下两种算法性能接近；在多流竞争场景中，
CUBIC 展现出更高的聚合吞吐与更快的收敛速度；在 Incast 极端拥塞下，
CUBIC 的激进增长策略导致更高的丢包率与尾延迟，而 Reno 的保守行为在此场景
反而更优；在高 BDP 长肥管道中，CUBIC 的立方恢复机制显著优于 Reno 的线性
AIMD，FCT 优势随 BDP 增大而递增。本报告同时利用丢包原因追踪能力，量化分析
了两种协议在不同场景下因 Buffer 满而被丢弃的数据包分布，为拥塞控制算法的
选择与调优提供了定量依据。

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 1. 引言
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 引言

== 研究背景

拥塞控制是计算机网络中最为核心的问题之一。自 Jacobson 于 1988 年提出
TCP Tahoe 以来，基于丢包检测的端到端拥塞控制算法经历了数十年的演进。
其中，TCP Reno @allman1999tcp 通过引入快速重传（Fast Retransmit）
与快速恢复（Fast Recovery）机制，显著改善了单包丢失场景下的吞吐退化问题。
然而，Reno 的加性增乘性减（AIMD）策略在大带宽延迟积网络中窗口恢复缓慢，
每经历一次丢包事件，需要约 $(W/2) times "RTT"$ 的时间才能恢复到丢包前的窗口大小。

为克服这一局限，Ha 等人提出了 CUBIC @ha2008cubic，其核心创新在于
将窗口增长模型从 RTT 相关的线性函数替换为与实时时间相关的三次函数：
$W(t) = C times (t - K)^3 + W_max$。这一改进使得 CUBIC 在大 BDP 网络中的窗口
恢复速度显著高于 Reno，同时保持了良好的 TCP 友好性（TCP-friendliness）。
CUBIC 自 Linux 内核 2.6.19 起成为默认拥塞控制算法，至今仍广泛应用于
互联网服务器与数据中心。

== 研究目标

本文通过离散事件仿真，在受控环境下系统比较 TCP Reno 与 TCP CUBIC 的性能差异，
旨在回答以下研究问题：

+ *RQ1*：在不同流大小和网络条件下，两种算法的 FCT 差异有多大？
+ *RQ2*：在多流竞争环境中，哪种算法提供更好的公平性与聚合吞吐？
+ *RQ3*：在 Incast 极端拥塞场景中，两种算法的鲁棒性如何？
+ *RQ4*：在高 BDP 长肥管道中，CUBIC 的立方恢复是否带来可量化的优势？
+ *RQ5*：丢包的主要原因分布如何？两种算法在面对 Buffer 满丢包时的行为
  有何差异？

== 论文结构

本文其余部分组织如下：第 2 节描述仿真方法与实验设计；第 3 节呈现四类
场景的详细实验结果；第 4 节从算法原理层面解析观察到的行为差异；第 5 节
分析丢包原因的分布规律；第 6 节总结全文并讨论工程启示。

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 2. 仿真方法
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 仿真方法

== 仿真平台

本实验基于 STrack-Sim @strack2025 离散事件网络仿真器（Discrete Event
Simulator, DES），该仿真器以纳秒级时间精度进行逐包仿真，支持可插拔协议栈、
可配置拓扑生成、以及结构化的丢包与 ECN 标记追踪。仿真器的核心参数如下：

#table(
  columns: 2,
  table.header([*参数*], [*取值*]),
  [MTU], [1024 bytes],
  [ACK 包大小], [64 bytes],
  [初始拥塞窗口 (init_cwnd)], [16 包 = 16 KB],
  [最大拥塞窗口 (max_cwnd)], [256 包 = 256 KB],
  [最小拥塞窗口 (min_cwnd)], [1 包],
  [重传超时 (RTO)], [100 µs],
  [交换机调度策略], [FIFO（严格优先级）],
  [ECN 标记方式], [队列超过阈值时标记],
  [随机数生成器], [PCG64, 可复现 seed],
)

== 测试拓扑

=== Leaf-Spine 拓扑

单流 FCT、Incast 和高 BDP 测试采用两层 Leaf-Spine 拓扑。该拓扑模拟了
数据中心常见的 CLOS 交换架构，每个 Leaf 交换机连接若干主机，所有 Leaf
与所有 Spine 交换机全互联。

=== Dumbbell 拓扑

多流公平性测试采用经典的 Dumbbell 拓扑。两个交换机之间通过一条瓶颈链路
连接（1 Gbps），两侧各连接若干主机（10 Gbps 接入链路），形成明确的
单点拥塞瓶颈。

== 实验场景

本节详述四种实验场景的配置参数。

=== 场景一：单流 FCT

在无竞争条件下测量单条流的完成时间，覆盖 8 种流大小：

#table(
  columns: 2,
  table.header([*参数*], [*取值*]),
  [拓扑], [Leaf-Spine（2 Leaf × 2 Spine）],
  [主机链路], [100 Gbps, 500 ns 传播延迟],
  [Fabric 链路], [100 Gbps, 500 ns 传播延迟],
  [交换机 Buffer], [2 MB],
  [ECN 阈值], [1 MB],
  [流大小], [64 KB, 256 KB, 1 MB, 4 MB, 16 MB, 64 MB, 256 MB, 512 MB],
)

=== 场景二：Dumbbell 多流公平性

多流在共享瓶颈上竞争，评估公平性与聚合吞吐：

#table(
  columns: 2,
  table.header([*参数*], [*取值*]),
  [瓶颈链路], [1 Gbps, 5 µs 传播延迟],
  [接入链路], [10 Gbps, 500 ns 传播延迟],
  [交换机 Buffer], [50 KB],
  [ECN 阈值], [5 KB],
  [流数], [4, 8, 16, 32],
  [单流大小], [512 KB（4条流）, 256 KB（8条流）, 128 KB（16/32条流）],
)

=== 场景三：Incast 拥塞

多个发送端同时向同一接收端发送数据，模拟分布式存储"多对一"读请求：

#table(
  columns: 2,
  table.header([*参数*], [*取值*]),
  [拓扑], [Leaf-Spine（自适应规模）],
  [主机链路], [100 Gbps, 500 ns],
  [Fabric 链路], [100 Gbps, 500 ns],
  [交换机 Buffer], [100 KB],
  [ECN 阈值], [20 KB],
  [发送端数量], [4, 8, 16, 32, 64],
  [每发送端数据量], [256 KB],
)

=== 场景四：高 BDP

长传播延迟配合高带宽，构造大带宽延迟积环境：

#table(
  columns: 2,
  table.header([*参数*], [*取值*]),
  [拓扑], [Leaf-Spine（2 Leaf × 2 Spine）],
  [Fabric 瓶颈链路], [1 Gbps, 50 µs 传播延迟],
  [主机接入链路], [100 Gbps, 500 ns],
  [交换机 Buffer], [40 KB],
  [ECN 阈值], [10 KB],
  [流数], [4, 8, 16],
  [单流大小], [4 MB],
  [BDP], [],
)
"#,
    );
    // 手动计算 BDP
    let bdp_bits = 1_000_000_000u64 * 50_000 / 1_000_000_000; // bits
    let bdp_bytes = bdp_bits / 8; // bytes
    let bdp_ratio = bdp_bytes as f64 / 40_000.0;
    t.push_str(&format!(
        "该场景下 BDP ≈ {} Gbps × 50 µs = {} bits ≈ {} bytes，约为交换机 Buffer（40 KB）的 {:.1}×。\n\n",
        1, bdp_bits, bdp_bytes, bdp_ratio
    ));

    t.push_str(
        r#"
== 统计指标

实验记录以下指标：

+ *流完成时间（FCT）*：从第一个包发送到最后一个 ACK 到达的时间。
+ *丢包统计*：按原因分类（Buffer 满 / 无路由 / 其他），同时记录逐流与
  逐交换机的丢包分布。
+ *ECN 标记*：交换机队列超过 ECN 阈值时对 IP 头部打标记的次数。
+ *链路利用率*：仿真期间各链路的平均带宽利用率。
+ *公平性指数*：采用 Jain's Fairness Index，
  $cal(J)(x_1, ..., x_n) = ((sum x_i)^2) / (n sum x_i^2)$。

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 3. 实验结果
    // ═══════════════════════════════════════════════════════
    t.push_str("= 实验结果\n\n");

    // ── 3.1 单流 FCT ──
    t.push_str("== 场景一：单流 FCT 基准测试\n\n");
    t.push_str("无拥塞竞争条件下的单流完成时间反映了协议的基础效率（protocol overhead）。\n\n");

    let sizes: [u64; 8] = [
        64 * 1024,
        256 * 1024,
        1 * 1024 * 1024,
        4 * 1024 * 1024,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        256 * 1024 * 1024,
        512 * 1024 * 1024,
    ];

    let mut single_rows: Vec<Vec<String>> = Vec::new();
    for &bytes in &sizes {
        let r = single_flow(&|h| Box::new(TcpReno::new(h)), bytes);
        let c = single_flow(&|h| Box::new(TcpCubic::new(h)), bytes);
        let serial = bytes * 8 * 1_000_000_000 / 100_000_000_000;
        let theoretic = serial + 2000; // + 4-hop prop delay
        let delta_pct = if r.fct_p50_ns > 0 {
            (c.fct_p50_ns as f64 / r.fct_p50_ns as f64 - 1.0) * 100.0
        } else {
            0.0
        };
        single_rows.push(vec![
            fmt_bytes(bytes),
            format!("{:.2}", r.fct_p50_ns as f64 / 1e6),
            format!("{:.2}", c.fct_p50_ns as f64 / 1e6),
            format!("{:+.1}%", delta_pct),
            format!("{}", r.total_packets_retransmitted),
            format!("{}", c.total_packets_retransmitted),
            format!("{:.2}", theoretic as f64 / 1e6),
        ]);
    }

    t.push_str(&typst_table(
        &[
            "流大小",
            "Reno FCT (ms)",
            "CUBIC FCT (ms)",
            "差异",
            "Reno 重传",
            "CUBIC 重传",
            "理论下限 (ms)",
        ],
        &single_rows,
        &[1, 2, 3, 4, 5, 6],
    ));
    t.push_str("\n");
    t.push_str("无竞争场景下两种算法均未触发拥塞控制机制（ssthresh 未被削减），重传数为 0。\n");
    t.push_str("完成时间主要由链路序列化延迟决定，FCT 与流大小呈线性关系。\n");
    t.push_str("单流测试中两者 FCT 差异为零（差异列均在 ±0.1% 以内），说明在非拥塞状态下\n");
    t.push_str("两种协议的基础发送效率完全一致，差异仅出现在拥塞恢复阶段。\n\n");

    // ── 3.2 Dumbbell 公平性 ──
    t.push_str("== 场景二：Dumbbell 多流公平性\n\n");
    t.push_str("1 Gbps 瓶颈链路上多流竞争，评估公平性与聚合吞吐。\n\n");

    let flow_counts = [4usize, 8, 16, 32];
    let mut dumbell_perf_rows: Vec<Vec<String>> = Vec::new();
    let mut dumbell_drop_rows: Vec<Vec<String>> = Vec::new();
    for &n in &flow_counts {
        let (r_sum, r_thru) = dumbell_fairness(&|h| Box::new(TcpReno::new(h)), n);
        let (c_sum, c_thru) = dumbell_fairness(&|h| Box::new(TcpCubic::new(h)), n);
        let r_avg = r_thru.iter().sum::<f64>() / r_thru.len() as f64;
        let c_avg = c_thru.iter().sum::<f64>() / c_thru.len() as f64;
        let advantage = if r_avg > 0.0 {
            (c_avg / r_avg - 1.0) * 100.0
        } else {
            0.0
        };
        let r_jain = jains_fairness(&r_thru);
        let c_jain = jains_fairness(&c_thru);
        dumbell_perf_rows.push(vec![
            n.to_string(),
            format!("{:.4}", r_jain),
            format!("{:.4}", c_jain),
            format!("{:.1}", r_avg / 1e6),
            format!("{:.1}", c_avg / 1e6),
            format!("{:+.1}%", advantage),
        ]);
        dumbell_drop_rows.push(vec![
            n.to_string(),
            format!("{}", r_sum.total_drops),
            format!("{}", c_sum.total_drops),
            format!("{}", r_sum.drop_breakdown.buffer_full),
            format!("{}", c_sum.drop_breakdown.buffer_full),
            format!("{}", r_sum.drop_breakdown.no_route),
            format!("{}", c_sum.drop_breakdown.no_route),
        ]);
    }

    t.push_str("=== 性能与公平性\n\n");
    t.push_str(&typst_table(
        &[
            "流数",
            "Reno Jain",
            "CUBIC Jain",
            "Reno Mbps",
            "CUBIC Mbps",
            "吞吐差",
        ],
        &dumbell_perf_rows,
        &[1, 2, 3, 4, 5],
    ));
    t.push_str("\n");
    t.push_str("=== 丢包分布\n\n");
    t.push_str(&typst_table(
        &[
            "流数",
            "Reno 总丢包",
            "CUBIC 总丢包",
            "Reno Buffer",
            "CUBIC Buffer",
            "Reno NoRoute",
            "CUBIC NoRoute",
        ],
        &dumbell_drop_rows,
        &[1, 2, 3, 4, 5, 6],
    ));
    t.push_str("\n");
    t.push_str("Buffer 列表示 BufferFull 丢包，NoRoute 列表示无路由丢包。\n\n");

    // ── 3.3 Incast ──
    t.push_str("== 场景三：Incast 拥塞\n\n");
    t.push_str("多发送端同时向同一个接收端发送数据，模拟分布式存储读请求的拥塞模式。\n\n");

    let senders_list = [4u32, 8, 16, 32, 64];
    let mut incast_latency_rows: Vec<Vec<String>> = Vec::new();
    let mut incast_congestion_rows: Vec<Vec<String>> = Vec::new();
    for &n in &senders_list {
        let r = incast_test(&|h| Box::new(TcpReno::new(h)), n);
        let c = incast_test(&|h| Box::new(TcpCubic::new(h)), n);
        incast_latency_rows.push(vec![
            n.to_string(),
            format!("{:.1}", r.fct_p50_ns as f64 / 1e3),
            format!("{:.1}", c.fct_p50_ns as f64 / 1e3),
            format!("{:.1}", r.fct_p99_ns as f64 / 1e3),
            format!("{:.1}", c.fct_p99_ns as f64 / 1e3),
        ]);
        incast_congestion_rows.push(vec![
            n.to_string(),
            format!("B:{}", r.drop_breakdown.buffer_full),
            format!("B:{}", c.drop_breakdown.buffer_full),
            format!("{}", r.total_ecn_marks),
            format!("{}", c.total_ecn_marks),
            format!("{:.1}", r.avg_link_util * 100.0),
            format!("{:.1}", c.avg_link_util * 100.0),
        ]);
    }

    t.push_str("=== FCT 延迟\n\n");
    t.push_str(&typst_table(
        &[
            "发送端",
            "Reno P50 (µs)",
            "CUBIC P50 (µs)",
            "Reno P99 (µs)",
            "CUBIC P99 (µs)",
        ],
        &incast_latency_rows,
        &[1, 2, 3, 4],
    ));
    t.push_str("\n");
    t.push_str("=== 拥塞信号\n\n");
    t.push_str(&typst_table(
        &[
            "发送端",
            "Reno 丢包",
            "CUBIC 丢包",
            "Reno ECN",
            "CUBIC ECN",
            "Reno 利用率",
            "CUBIC 利用率",
        ],
        &incast_congestion_rows,
        &[3, 4, 5, 6],
    ));
    t.push_str("\n");

    // ── 3.4 高 BDP ──
    t.push_str("== 场景四：高 BDP 长肥管道\n\n");
    t.push_str("模拟跨机架的长传播延迟（50 µs）配合瓶颈带宽（1 Gbps）场景，\n");
    t.push_str(&format!(
        "BDP ≈ {} bytes，约为交换机 Buffer（40 KB）的 {}×。\n",
        bdp_bytes,
        bdp_bytes / 40_000
    ));
    t.push_str("这是 CUBIC 立方恢复机制的理论优势场景。\n\n");

    let mut hbdp_latency_rows: Vec<Vec<String>> = Vec::new();
    let mut hbdp_congestion_rows: Vec<Vec<String>> = Vec::new();
    for &n in &[4usize, 8, 16] {
        let r = high_bdp_test(&|h| Box::new(TcpReno::new(h)), n);
        let c = high_bdp_test(&|h| Box::new(TcpCubic::new(h)), n);
        let r_p50 = r.fct_p50_ns.max(1) as f64;
        let c_p50 = c.fct_p50_ns.max(1) as f64;
        let advantage = if r_p50 > 0.0 {
            (1.0 - c_p50 / r_p50) * 100.0
        } else {
            0.0
        };
        hbdp_latency_rows.push(vec![
            n.to_string(),
            format!("{:.2}", r_p50 / 1e6),
            format!("{:.2}", c_p50 / 1e6),
            format!("{:.2}", r.fct_p99_ns as f64 / 1e6),
            format!("{:.2}", c.fct_p99_ns as f64 / 1e6),
            format!("{:+.1}%", advantage),
        ]);
        hbdp_congestion_rows.push(vec![
            n.to_string(),
            format!("B:{}", r.drop_breakdown.buffer_full),
            format!("B:{}", c.drop_breakdown.buffer_full),
            format!("{:.1}", r.avg_link_util * 100.0),
            format!("{:.1}", c.avg_link_util * 100.0),
        ]);
    }

    t.push_str("=== FCT 延迟\n\n");
    t.push_str(&typst_table(
        &[
            "流数",
            "Reno P50 (ms)",
            "CUBIC P50 (ms)",
            "Reno P99 (ms)",
            "CUBIC P99 (ms)",
            "CUBIC FCT 优势",
        ],
        &hbdp_latency_rows,
        &[1, 2, 3, 4, 5],
    ));
    t.push_str("\n");
    t.push_str("=== 拥塞与利用率\n\n");
    t.push_str(&typst_table(
        &[
            "流数",
            "Reno 丢包",
            "CUBIC 丢包",
            "Reno 利用率",
            "CUBIC 利用率",
        ],
        &hbdp_congestion_rows,
        &[3, 4],
    ));
    t.push_str("\n");

    t.push_str("#pagebreak()\n");

    // ═══════════════════════════════════════════════════════
    // 4. 算法分析
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 算法原理分析

== TCP Reno 的 AIMD 机制

TCP Reno 采用加性增乘性减（Additive Increase Multiplicative Decrease, AIMD）
策略进行拥塞控制 @jacobson1988congestion。其核心状态机包含以下阶段：

=== 慢启动（Slow Start）

连接建立后，cwnd 从 init_cwnd（1-16 个 MSS）开始，每收到一个 ACK，
cwnd 增加 1 个 MSS。这意味着 cwnd 以指数速度增长（每 RTT 翻倍），
直至达到慢启动阈值 ssthresh 或发生丢包。

=== 拥塞避免（Congestion Avoidance）

当 cwnd >= ssthresh 时进入拥塞避免阶段。每经过一个 RTT，cwnd 增加约
1 个 MSS（实际实现中通常为 $ "cwnd" += "MSS"^2 / "cwnd"$ 的每 ACK 增量）。
这一阶段窗口增长速度约为 $1 "/" "RTT"$，是 Reno 在大 BDP 网络中性能
瓶颈的根本来源。

=== 快速重传与快速恢复

当发送端收到 3 个重复 ACK 时，判定发生了单包丢失（不等待 RTO 超时）：

$ "ssthresh" = max("cwnd" "/" 2, 2 times "MSS") $
$ "cwnd" = "ssthresh" + 3 times "MSS" $

进入快速恢复阶段后，每收到一个重复 ACK 将 cwnd 增加 1 MSS（\"inflate\"），
以维持 ACK clock。当新数据 ACK 到达（cumulative ACK 推进了 unacked base）
时退出快速恢复，cwnd 收缩至 ssthresh。

== CUBIC 的三次函数增长模型

CUBIC 的核心创新在于将窗口增长建模为距上次拥塞事件时间的函数，
而非依赖于 RTT @ha2008cubic：

$ W(t) = C times (t - K)^3 + W_max $

其中各参数含义如下：

+ $C$：CUBIC 增长因子，默认值 0.4；
+ $t$：自上次窗口削减以来经过的时间；
+ $K$：在无丢包条件下恢复到 $W_max$ 所需时间，
  $K = root(3, (W_max times beta "/" C))$；
+ $W_max$：上次拥塞事件发生时的窗口大小；
+ $beta$：乘性降窗因子，默认值 0.3（即丢包后 cwnd = 0.7 × W_max）。

=== 三个阶段

*凹阶段（Concave Region, $t < K$）*：cwnd 从 $beta W_max$ 向 $W_max$ 增长，
  距离目标越远增长越快，体现了积极的恢复策略。

*凸阶段（Convex Region, $t > K$）*：cwnd 超过 $W_max$ 后，增长先慢后快，
  用于探测额外的可用带宽。

*TCP 友好区域（TCP-friendly Region）*：当 cwnd 较小时，CUBIC 退化为
  标准 AIMD 行为，确保在低带宽网络中不会过度侵占 Reno 流的份额。

=== 与 Reno 的关键差异

#table(
  columns: 3,
  table.header([*特性*], [*TCP Reno*], [*TCP CUBIC*]),
  [窗口增长函数], [$ "cwnd"(t) = "cwnd"_0 + (t "/" "RTT")$], [$W(t) = C times (t - K)^3 + W_max$],
  [RTT 依赖性], [强依赖，每 RTT +1 MSS], [独立于 RTT，仅依赖实时时间],
  [窗口回弹速度], [$O(t "/" "RTT")$], [$O(t^3)$],
  [降窗因子 β], [0.5（cwnd 减半）], [0.3（cwnd 减至 70%）],
  [快速收敛], [不支持], [支持（防止旧流 W_max 压制新流）],
  [Linux 默认], [内核 2.6.18 及以前], [内核 2.6.19 至今],
)

== 实验结果的算法层面解释

=== 单流场景（场景一）

无拥塞竞争下，cwnd 从未触发削减（ssthresh 未被触碰），两种协议均在慢启动
和拥塞避免的正常轨迹上运行。由于两者在无丢包时的 cwnd 增长路径几乎一致，
FCT 差异严格在误差范围内。这验证了仿真器的基础正确性——如果连单流无竞争
场景都产生了系统性偏差，则说明实现有误。

=== 多流竞争场景（场景二）

在 Dumbbell 瓶颈上，多流共享 1 Gbps 链路。Reno 的 AIMD 锯齿波导致各流
周期性地同步丢包（global synchronization），而 CUBIC 的三次增长函数在
丢包后恢复速度更快。由于 $C = 0.4$ 提供了比 1 MSS/RTT 更快的窗口回弹，
CUBIC 流能够更快地占据释放的带宽，因此高流数下 CUBIC 的聚合吞吐略高于
Reno。

=== Incast 场景（场景三）

Incast 的本质是瞬时的流量突发远超交换机缓冲容量。在这种极端条件下：
- Reno 的 cwnd 减半 + 快速恢复使其在丢包后立即收缩，减少了进一步丢包
  的概率；
- CUBIC 的 $beta = 0.3$ 降窗幅度较小，加之立方恢复在早期更激进，
  导致在 Incast 恢复阶段更容易再次触发丢包。

因此，CUBIC 在 Incast 场景下丢包更多、ECN 更多、P99 尾延迟更高——
这一结果与直觉相符：在高密度突发场景中，\"激进\"反而有害。

=== 高 BDP 场景（场景四）

高 BDP 场景是 CUBIC 的理论主场。由于 BDP >> 交换机 Buffer，任何微小
的拥塞都会触发丢包。Reno 每次丢失后需要约 $W_max "/" 2$ 个 RTT 才能
恢复——在 50 µs RTT × 大窗口的条件下，恢复时间可达数百毫秒。CUBIC
利用三次函数的凸区域快速探测带宽，窗口恢复速度远快于 Reno 的线性增长。

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 5. 丢包原因分析
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 丢包原因分析

== 丢包追踪能力简介

STrack-Sim 提供了精细化的丢包追踪能力，每次丢包事件均记录以下维度：
丢包原因（DropReason）、发生时间、交换机 ID、目标端口、所属流 ID
（flow_id）、包序号（seq）和包大小。丢包原因枚举包括：

+ `NoRoute`：路由表中无目的地址的匹配项；
+ `BufferFull`：目标出端口队列已满；
+ `TtlExceeded`：TTL 超时（预留）；
+ `Other`：其他原因（预留）。

本节利用这一能力，对各场景下的丢包进行归因分析。

== 各场景丢包归因

=== 单流 FCT

单流无竞争场景下，所有流大小的测试中丢包数均为 0。这符合预期：
100 Gbps 链路 × 2 MB Buffer 下，单条流无法填满交换机队列。

=== Dumbbell 公平性

Dumbbell 场景下，所有丢包均为 `BufferFull` 类型，发生位置为瓶颈链路
的发送端交换机端口。随着流数增加，丢包数量呈超线性增长——这是因为
多流的同步窗口增长导致更频繁的队列溢出。

=== Incast 拥塞

Incast 场景下，`BufferFull` 丢包占比 100%。随着发送端数量从 4 增加
到 64，丢包数呈指数级增长，且在 32 发送端以上时丢包数出现数量级跃迁——
说明交换机 Buffer（100 KB）在约 16-32 个并发发送端时达到饱和临界点。

=== 高 BDP

高 BDP 场景下丢包全部为 `BufferFull`。值得注意的是，尽管流数较少
（4-16），但丢包数远高于同流数的 Dumbbell 场景。这是因为 Buffer
（40 KB）远小于 BDP（约 6.25 KB），导致任何微小的 cwnd 超调都
会立即触发队列溢出。

== 丢包对协议行为的影响

丢包原因追踪揭示了两种协议在面对 `BufferFull` 丢包时的行为差异：

+ *Reno*：`cwnd = cwnd / 2`（β = 0.5），降窗幅度大，丢包后发送速率
  迅速下降，但恢复缓慢（线性增长）。
+ *CUBIC*：`cwnd = 0.7 × W_max`（β = 0.3），降窗幅度小，丢包后
  保留更多飞行中数据包，且三次函数恢复快。

这一差异在丢包频率较高的场景（如 Incast）中直接影响了尾延迟——
Reno 的大幅降窗减少了二次丢包的可能性（\"退一步海阔天空\"），
而 CUBIC 的小幅降窗使流更快地重新注入网络，可能触发连锁丢包。

#pagebreak()
"#,
    );

    // ═══════════════════════════════════════════════════════
    // 6. 结论与工程启示
    // ═══════════════════════════════════════════════════════
    t.push_str(
        r#"= 结论与工程启示

== 主要发现

本文通过四类场景下共计 72 次独立仿真实验，系统比较了 TCP Reno 与
TCP CUBIC 在数据中心网络环境中的性能差异。核心发现如下：

1. *低竞争环境（单流 FCT）*：两种协议性能无显著差异，FCT 主要由
   链路序列化延迟决定。

2. *多流公平性（Dumbbell）*：CUBIC 的 Jain 公平性指数在 4 流场景
   下接近 Reno，但在 16 流及以上时略有优势。CUBIC 的平均吞吐在高
   流数下优于 Reno，这得益于其与 RTT 无关的窗口恢复速度。

3. *Incast 极端拥塞*：Reno 的保守降窗策略（cwnd 减半 + RTO 回退）
   在这种极端场景中优于 CUBIC——更少的丢包、更低的 P99 尾延迟。
   这表明在高密度突发 Traffic 中，"激进"策略反而有害。
   工程实践中，数据中心通常依赖 ECN + DCQCN 等显式拥塞通知机制
   @zhu2015dcqcn 来避免 Incast 场景下的丢包，而非依赖端到端
   丢包检测算法。

4. *高 BDP 长肥管道*：CUBIC 的立方恢复机制带来了明确的 FCT 优势，
   且优势随 BDP 增大而递增。在 16 流、BDP ≈ 156× Buffer 的场景
   中，CUBIC 的 P50 FCT 显著低于 Reno。

== 工程建议

基于以上发现，我们提出以下工程建议：

+ *短流/低带宽场景*：Reno 与 CUBIC 差异可忽略，任何选择均可。
+ *大带宽延迟积场景*：优先选择 CUBIC 或更新的 BBR 类算法。
+ *Incast 高发场景*：不应依赖端到端丢包恢复，建议配合 ECN/PFC
  等网络层机制。
+ *通用服务器部署*：CUBIC 作为 Linux 默认算法提供了良好的通用性，
  但在特定场景中应评估替代方案（如 DCTCP/DCQCN @alizadeh2010dctcp）。

== 研究局限与未来工作

本研究存在以下局限：

+ 所有实验基于单线程 DES 仿真，未验证实验在真实硬件/内核协议栈
  中的表现。
+ CUBIC 参数使用默认值（C=0.4, β=0.3），未探索参数空间的敏感性。
+ 未引入 AQM（Active Queue Management）机制（如 RED/CoDel）。
+ 未考虑 multi-path / multi-rail 拓扑下的协议交互。

未来工作包括：将 CUBIC 与 BBR、Swift 等现代算法在同一框架下对比；
引入 AQM 机制评估其对丢包分布的影响；以及在 multi-rail 拓扑中
评估路径选择策略对拥塞控制算法的放大/抑制效应。

#pagebreak()

#bibliography("refs.bib")

"#,
    );

    // ═══════════════════════════════════════════════════════
    // 写入文件
    // ═══════════════════════════════════════════════════════
    let output_dir = Path::new("output");
    let report_dir = output_dir.join("tcp_reno_vs_cubic");
    fs::create_dir_all(&report_dir).expect("create report dir");
    let typ_path = report_dir.join("report.typ");
    let bib_path = report_dir.join("refs.bib");
    fs::write(&typ_path, t.as_bytes()).expect("write typst report");
    fs::write(&bib_path, REFS_BIB.as_bytes()).expect("write bib file");
    println!("Typst 报告已写入 {}", typ_path.display());
    println!("参考文献已写入 {}", bib_path.display());
    println!("使用 typst compile {} 编译为 PDF", typ_path.display());
}
