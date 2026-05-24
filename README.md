# Fabric-Sim · AI 集群网络传输协议离散事件仿真器

> 基于 Rust 从零搭建的**可插拔协议离散事件网络模拟器**，面向 AI/ML 集群的多协议传输行为研究。
> 已内置 **STrack、TCP Reno/CUBIC、DCQCN、HPCC、Swift、RDMA** 等协议，提供丢包归因、ECN、FCT、公平性等多维指标。

---

## 🎯 核心能力

### 多协议对比

| 协议 | 路径策略 | 拥塞控制 | 恢复机制 |
|------|---------|---------|---------|
| ECMP baseline | 哈希分流（单路径/流） | DCQCN 风格降窗 | 超时重传 |
| **STrack** | Packet Spraying | 先切路再降窗 | SACK Bitmap 选择性重传 |
| TCP Reno | 单路径 | AIMD + Fast Recovery | 3 dup ACK + RTO |
| TCP CUBIC | 单路径 | 三次函数窗口增长 | Fast Retransmit + RTO |
| DCQCN | 单路径 | ECN-based rate control | CNP 触发降速 |
| HPCC | 单路径 | INT-based 精确速率 | 链路利用率反馈 |
| Swift | 单路径 | 延迟目标驱动 | ACK 时钟恢复 |
| RDMA | 多 QP | Go-Back-N / 选择性重传 | RNR NAK 流控 |

### 精细化丢包追踪

支持分原因、逐流、逐端口的丢包归因，见[丢包追踪详情](docs/design.md#丢包追踪)。

---

## 📊 实测示例

### Incast：ECMP vs STrack

**设置**：4 Leaf × 8 Spine × 4 host/leaf = 16 hosts，15 个 sender 同时向 host 0 发送 512 KB

```
┌─────────── ECMP baseline ────────────         ┌─────────── STrack ────────────────────
│ 完成流数            15                         │ 完成流数            15
│ 仿真总时长          874 us                     │ 仿真总时长          762 us   ✅ -12.8%
│ FCT P50             816.6 us                   │ FCT P50             704.7 us ✅ -13.7%
│ FCT P99             848.3 us                   │ FCT P99             735.9 us ✅ -13.3%
│ 总发送包数          7752                       │ 总发送包数          8652
│ 总重传包数          72                         │ 总重传包数          972
│ ECN 标记总数        4182                       │ ECN 标记总数        5134
│ 丢包总数 (BufferFull) 43                       │ 丢包总数 (BufferFull) 399
└──────────────────────────────────────         └──────────────────────────────────────
```

STrack 在 FCT 上有 **~13% 改善**，代价是更多重传与丢包——符合 Packet Spraying 的预期。

### TCP Reno vs CUBIC 学术报告

生成 Typst 格式的完整实验分析报告（含丢包归因）：

```bash
cargo run --release --example tcp_reno_vs_cubic_typst
typst compile output/tcp_reno_vs_cubic/report.typ
```

---

## 📦 项目结构

```
fabric-sim/
├── Cargo.toml
├── README.md
├── docs/
│   ├── design.md          ← 完整设计文档
│   ├── limit.md           ← 性能瓶颈与限制分析
│   └── learning.md        ← 网络协议学习笔记
├── src/
│   ├── lib.rs             ← crate 入口
│   ├── error.rs           ← SimError + SimResult
│   ├── core/              ✅ 阶段一：DES 引擎
│   │   ├── event.rs         · Event + EventKind
│   │   ├── queue.rs         · 4-ary EventQueue
│   │   └── simulator.rs     · Simulator
│   ├── network/           ✅ 阶段二：物理层
│   │   ├── packet.rs        · Packet（含 ECN、RDMA 字段）
│   │   ├── link.rs          · Link + LinkRegistry
│   │   ├── switch.rs        · Switch + 优先级队列 + PFC
│   │   ├── drop.rs          · DropReason + DropCounters + 丢包追踪
│   │   └── host_delay.rs    · 主机内部延迟模型
│   ├── topology/          ✅ 阶段二：拓扑生成
│   │   ├── leaf_spine.rs    · 2 层 Leaf-Spine
│   │   ├── fat_tree.rs      · k-ary Fat-Tree
│   │   ├── dumbell.rs       · Dumbbell
│   │   └── multi_rail.rs    · 多轨拓扑
│   ├── nic/               ✅ 阶段三：可插拔协议栈
│   │   ├── protocol.rs      · Protocol trait
│   │   ├── strack.rs        · STrack（Ecmp / Strack 双模式）
│   │   ├── tcp.rs           · SimpleTcp 基线
│   │   ├── reno.rs          · TCP Reno（完整 Fast Recovery）
│   │   ├── cubic.rs         · TCP CUBIC
│   │   ├── dcqcn.rs         · DCQCN（ECN-based rate control）
│   │   ├── hpcc.rs          · HPCC（INT-based）
│   │   ├── swift.rs         · Swift（延迟目标驱动）
│   │   ├── rdma.rs          · RDMA QP / WQE / CQE 状态机
│   │   └── rdma_protocol.rs · RdmaProtocol 实现
│   ├── traffic/           ✅ 阶段四：流量生成
│   │   ├── incast.rs        · N-to-1
│   │   ├── all_reduce.rs    · Ring AllReduce
│   │   ├── all_to_all.rs    · 全员两两交换
│   │   ├── synthetic.rs     · 通用合成流量
│   │   ├── permute.rs       · 排列流量
│   │   └── mix.rs           · 混合流量
│   ├── training/          ✅ 训练作业抽象
│   │   ├── mod.rs           · TrainingJob + Iteration + CollectiveOp
│   │   └── dag.rs           · 训练 DAG
│   ├── monitor/           ✅ 指标采集
│   │   └── mod.rs           · FlowFct + SimSummary + DropBreakdown + RunProfile
│   ├── sim_runner/        ✅ 端到端仿真主循环
│   │   ├── mod.rs           · SimRunner + PacketSlab + 集中分发
│   │   ├── host.rs          · 主机侧事件（TxTick / RTO / ACK）
│   │   └── switch.rs        · 交换机侧事件（ingress / egress）
│   └── viz/               ✅ 3D 可视化
│       ├── data.rs           · VizData / VizNode / VizLink
│       ├── position.rs       · 3D 坐标
│       └── sampler.rs        · 时间序列采样器
├── examples/
│   ├── des_demo.rs              · DES 引擎演示
│   ├── incast_compare.rs        · ECMP vs STrack 对比
│   ├── protocol_compare.rs      · 多协议对比
│   ├── tcp_reno_vs_cubic.rs     · Reno vs CUBIC（Markdown）
│   ├── tcp_reno_vs_cubic_typst.rs · Reno vs CUBIC（Typst 学术报告）
│   ├── dumbell_alltoall.rs      · Dumbbell AllToAll
│   ├── training_job.rs          · 训练作业模拟
│   ├── workload_sweep.rs        · 多维度参数扫描
│   └── viz_demo.rs              · 3D 可视化导出
├── scripts/
│   ├── visualize_3d.py          · Plotly 3D 渲染
│   └── plot_workloads.py        · 结果图表生成
├── benches/
│   ├── des_bench.rs             · DES 吞吐基准
│   └── optimization_compare.rs  · 优化方案对比
└── tests/
    ├── integration_des.rs       · 百万级 DES 事件
    ├── integration_e2e.rs       · 端到端完整性
    ├── integration_dumbell.rs   · Dumbbell 拓扑
    ├── integration_protocols.rs · 多协议集成
    ├── integration_training.rs  · 训练作业集成
    ├── integration_rdma.rs      · RDMA 集成
    └── matrix_workloads.rs      · 参数化矩阵
```

---

## 🚀 快速开始

### 环境要求
- Rust 1.74+
- macOS / Linux

### 编译

```bash
git clone https://github.com/Leo-Yikang/fabric-sim.git
cd fabric-sim
cargo build --release
```

### 运行测试（127 个，全部通过）

```bash
cargo test --release
```

### 端到端演示

```bash
# ECMP vs STrack
cargo run --release --example incast_compare

# 多协议对比（Reno / CUBIC / DCQCN / HPCC / Swift）
cargo run --release --example protocol_compare

# 生成 Reno vs CUBIC 学术报告（Typst → PDF）
cargo run --release --example tcp_reno_vs_cubic_typst
typst compile output/tcp_reno_vs_cubic/report.typ
```

### 3D 可视化

```bash
cargo run --release --example viz_demo
pip install plotly
python3 scripts/visualize_3d.py output/viz_data.json
```

### 性能基准

```bash
cargo bench
```

---

## 📝 关键设计

### 可插拔协议架构

所有协议实现 `Protocol` trait，`SimRunner` 通过 `Box<dyn Protocol>` 动态分发事件。新增协议只需实现 6 个核心方法。

### 集中式事件分发

`SimRunner` 根据 `EventKind` 查实体表直接修改状态，可在一个事件中自由读写协议、拓扑、链路、监控等多组件。

### 事件驱动的 TxTick / RTO

TxTick 不再固定轮询——协议栈通过 `has_pending_work()` / `next_rto_deadline()` 告知何时需要下一次 tick，消除了大规模仿真中的空转开销。

### PacketSlab 分配器

包暂存从 `HashMap` 替换为自实现的 Slab allocator（`Vec<Option<Packet>>` + 空闲列表），insert/remove 均为 O(1) 数组索引，实测吞吐提升 4-7 倍。

### 丢包归因追踪

每次丢包记录原因（NoRoute / BufferFull / TtlExceeded）、端口、流、序号、时间，支持分维度聚合分析。

### 可复现

所有随机数使用 `rand_pcg` + 显式 seed，仿真结果完全可复现。

---

## 📚 参考资料

- **STrack 论文**：Le et al., *"STrack: A Reliable Multipath Transport for AI/ML Clusters"* (arXiv:2407.15266, 2024)
- **CUBIC**：Ha et al., *"CUBIC: a new TCP-friendly high-speed TCP variant"* (SIGOPS 2008)
- **DCQCN**：Zhu et al., *"Congestion control for large-scale RDMA deployments"* (SIGCOMM 2015)
- **HPCC**：Li et al., *"HPCC: High Precision Congestion Control"* (SIGCOMM 2019)
- **Swift**：Kumar et al., *"Swift: Delay is Simple and Effective for Congestion Control in the Datacenter"* (SIGCOMM 2020)

---

## 📄 许可证

MIT License · 2026 · kiwios-cn