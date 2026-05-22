# STrack-Sim 项目指南

> 本文件面向 AI 编程助手（agent）。如果你从未接触过本项目，请先阅读此文件。

---

## 1. 项目概述

**STrack-Sim** 是一个用 Rust 从零搭建的离散事件网络模拟器（Discrete Event Simulator, DES），专门用于研究 **STrack** 多路径 RDMA 传输协议在 AI/ML 集群环境下的性能表现。

核心对比目标：
- **ECMP baseline**：传统单路径流哈希 + DCQCN 风格降窗
- **STrack**：Packet Spraying 多路径 + 先切路再降窗 + SACK Bitmap 选择性重传

项目已完成四阶段全部实现（DES 引擎 → 拓扑/物理层 → NIC 协议栈 → 流量/Monitor），并通过端到端集成测试验证。主语言为**中文**（注释、文档、提交信息均使用中文）。

---

## 2. 技术栈与环境

| 项目 | 版本/说明 |
|------|----------|
| 语言 | Rust 2021 Edition |
| 最低 Rust 版本 | 1.74+ |
| 构建工具 | Cargo |
| 平台 | macOS / Linux |
| 随机数 | `rand` + `rand_pcg`（可复现，显式 seed） |
| 日志 | `log` / `env_logger` / `tracing` / `tracing-subscriber` |
| 序列化 | `serde` + `serde_json`（指标导出） |
| CLI | `clap` 已加入依赖但**尚未使用**（当前所有参数硬编码在源码中） |
| 错误处理 | `thiserror` + `anyhow`（已加入依赖，但代码中仍有大量裸 `unwrap()`） |
| 基准测试 | `criterion` |

**重要**：所有时间计算使用**整数运算**（`u64` 纳秒），避免浮点误差。链路序列化延迟公式：
```rust
(size_bytes as u64 * 8 * 1_000_000_000) / bandwidth_bps
```

---

## 3. 构建与运行命令

```bash
# 编译（release 模式有显著性能提升，DES 引擎吞吐从 ~2M 提升到 ~20M events/sec）
cargo build --release

# 运行全部测试（共 26 个：21 单元 + 4 集成 + 1 文档）
cargo test --release

# 运行端到端演示（核心成果：ECMP vs STrack 对比）
cargo run --release --example incast_compare

# 运行 DES 引擎演示
cargo run --release --example des_demo

# 性能基准测试
cargo bench

# 日志级别控制（通过 RUST_LOG）
RUST_LOG=info  cargo run --release --example des_demo
RUST_LOG=debug cargo run --release --example des_demo
```

日志文件输出到 `logs/*.log`（目录已加入 `.gitignore`）。

---

## 4. 代码组织与模块划分

```
src/
├── lib.rs              # crate 入口，导出全部模块，定义 SimTime / EntityId 类型别名
├── error.rs            # SimError + SimResult（轻量判错系统，见第 11 节错误处理约定）
├── sim_runner/          # 端到端仿真主循环（核心 orchestrator）
│   ├── mod.rs           # SimRunner 结构体、初始化、事件分发、统计汇总
│   ├── host.rs          # 主机侧：TxTick 驱动发送、PacketArrive 处理收包
│   └── switch.rs        # 交换机侧：ingress 路由入队、egress 出队转发
├── core/               # 阶段一：DES 引擎（与网络概念完全解耦）
│   ├── event.rs        # Event + EventKind（含全局原子 seq 保证 FIFO 稳定性）
│   ├── queue.rs        # EventQueue（BinaryHeap 封装，反向 Ord 实现最小堆）
│   └── simulator.rs    # Simulator（时钟 + handler 注册 + step/run/run_until）
├── network/            # 阶段二：物理层
│   ├── packet.rs       # Packet 数据结构（Data/Ack/Nack，含 ECN + SACK bitmap）
│   ├── link.rs         # Link + LinkRegistry（单向链路，整数运算序列化延迟）
│   └── switch.rs       # Switch + Port + RoutingTable（FIFO + ECN 标记 + 丢包）
├── topology/           # 阶段二：拓扑生成器
│   ├── leaf_spine.rs   # 两层 Leaf-Spine
│   ├── fat_tree.rs     # k-ary 三层 Fat-Tree
│   └── dumbell.rs      # Dumbbell 拓扑
├── nic/                # 阶段三：STrack 协议栈
│   ├── protocol.rs     # Protocol trait（可插拔接口）
│   ├── strack.rs       # STrack 协议实现（Spraying + SACK + 多路径 CC）
│   └── tcp.rs          # SimpleTcp 基线实现（单路径 + 累计 ACK + 快速重传）
├── traffic/            # 阶段四：流量生成器
│   ├── incast.rs       # N-to-1 多对一拥塞
│   ├── all_reduce.rs   # Ring AllReduce
│   ├── all_to_all.rs   # 全员两两交换
│   ├── synthetic.rs    # 通用合成流量（流大小分布 × 到达过程 × 通信对模式）
│   ├── permute.rs      # 排列流量（Random / Shift / BitReversal）
│   └── mix.rs          # 混合流量（多组件按比例组合）
└── monitor/            # 阶段四：指标采集
    └── mod.rs          # FlowFct + SimSummary（serde Serialize，支持 pretty_print）
```

---

## 5. 代码风格与约定

### 5.1 注释与文档
- 所有模块顶部使用 `//!` 写模块级 doc 注释。
- 所有 pub struct / pub fn 使用 `///` 写文档注释。
- 注释语言为**中文**。

### 5.2 命名
- 时间戳/时长字段后缀统一为 `_ns`（纳秒）或 `_us`（微秒）。
- 类型别名：`SimTime = u64`, `EntityId = u32`, `PacketId = u64`, `FlowId = u32`, `SeqNum = u32`, `PortId = u8`, `LinkId = u32`。
- 布尔标志：如 `ecn`, `done`, `dropped`。

### 5.3 错误处理（当前现状）
- **大量裸 `unwrap()` 存在**，尤其在 `sim_runner.rs`（约 10 处）和 `nic/tx.rs`（约 6 处）。
- 修改这些文件时，应优先使用 `anyhow::Result` 或 `Option` 做 graceful degradation，而不是新增 `unwrap()`。
- 单元测试和集成测试中允许 `unwrap()` / `expect()`。

### 5.4 事件类型约定
`EventKind` 中 `FlowStart` 和 `TxTick` 已升级为结构化枚举变体（不再使用 `Custom(String)` 编码）：
```rust
FlowStart { flow_id: u32, src: EntityId, dst: EntityId, bytes: u64 }
TxTick { host: EntityId }
```
`Custom(String)` 变体保留，仅用于单元测试、示例和基准测试。新增事件类型应优先定义为结构化变体，避免字符串解析。

### 5.5 可复现性
- 所有随机数使用 `rand_pcg` + 显式 seed。
- 拓扑生成是确定性的，不涉及随机。
- `EVENT_SEQ` 原子计数器保证同时间戳事件的 FIFO 稳定性。

---

## 6. 测试策略

| 类型 | 数量 | 位置 | 说明 |
|------|------|------|------|
| 单元测试 | 45 | 各 `src/**/*.rs` 的 `#[cfg(test)]` 模块 | 覆盖 core / network / nic / topology / traffic |
| 集成测试 | 13 | `tests/` | `integration_des.rs` + `integration_e2e.rs` + `integration_dumbell.rs` + `matrix_workloads.rs` |
| 文档测试 | 1 | `lib.rs` | crate-level 用法示例 |
| **合计** | **59** | — | **全部应通过** |

### 测试运行要求
- 使用 `--release` 模式运行测试，因为 DES 引擎在 debug 模式下性能不足，集成测试中有吞吐阈值断言。
- `integration_des.rs` 中百万级事件测试要求吞吐 > 0.5 M ev/s（debug 模式余量）；release 模式下实际可达 10–25 M ev/s。

### 添加新测试的规范
- 模块级单元测试：写在对应源文件的 `#[cfg(test)] mod tests` 中。
- 端到端场景测试：写在 `tests/integration_e2e.rs` 或 `tests/matrix_workloads.rs` 中，使用 `SimRunner` + `LeafSpine`/`FatTree`/`Dumbell` 构建完整拓扑。
- 参数化矩阵测试：写在 `tests/matrix_workloads.rs` 中，系统覆盖拓扑 × 协议 × 流大小分布 × 到达过程 × 流量模式的组合。

---

## 7. 运行时架构关键设计

### 7.1 集中式事件分发
没有使用 `Simulator::register_handler` 的回调机制（受 borrow checker 限制），而是由 `SimRunner` 集中式分发：
1. `Simulator` 只负责事件排序（最小堆）。
2. `SimRunner::dispatch()` 根据 `EventKind` 查实体表，直接修改状态。

这允许在单个事件处理中自由读写 `tx_nics` / `rx_nics` / `topo.switches` 等多个集合。

### 7.2 全局 Packet ID
每个 `TxNic` 各自从 1 开始分配 `packet_id`，会在全局 `packet_buf` 中冲突。`SimRunner` 维护 `global_pid: u64` 字段，在 `handle_tx_tick` 中**重写所有出包的 id**。

**注意**：修改包生成逻辑时，务必确保每个注入 `packet_buf` 的包都有全局唯一 id。

### 7.3 持续 TxTick
当 `cwnd` 满时，仅靠 ACK 触发 `TxTick` 不够（丢包后永远无 ACK）。因此未完成流会定期 tick（默认 25us，由 `tx_tick_ns` 控制，RTO/4）检查超时重传。

### 7.4 链路 Serialization 模型
链路用 `link_busy_until: Vec<u64>` 数组建模：每个包的发送开始时刻 = `max(now, link_busy_until[link_id])`，发送完成后再更新数组。这实现了物理链路的“一次只能发一个包”行为。

### 7.5 交换机 Egress
`Switch::ingress()` 只负责选端口 + 判丢包 + 入队。出包由 `SimRunner::try_egress()` 在端口空闲时驱动。若端口忙，则调度一个 `PacketDepart` 事件在未来 `busy_until` 时刻重试。

---

## 8. 关键常量与默认值

| 常量 | 值 | 位置 |
|------|-----|------|
| MTU | 1024 bytes | `network/packet.rs` |
| 初始 CWND | 16 包 | `nic/tx.rs` (`init_cwnd`) |
| 最大 CWND | 256 包 | `nic/tx.rs` (`max_cwnd`) |
| 最小 CWND | 1 包 | `nic/tx.rs` (`min_cwnd`) |
| RTO | 100,000 ns (100us) | `nic/tx.rs` (`rto_ns`) |
| 路径黑名单时长 | 50,000 ns (50us) | `nic/tx.rs` (`blacklist_duration_ns`) |
| TxTick 周期 | 200 ns | `sim_runner.rs` (`tx_tick_ns`) |
| ACK 包大小 | 64 bytes | `network/packet.rs` |
| NACK 包大小 | 64 bytes | `network/packet.rs` |
| SACK Bitmap 宽度 | 64 位 | `network/packet.rs` (`sack_bits: u64`) |

---

## 9. 安全与风险考量

- **本项目是研究级学术模拟器，非生产代码**。不直接处理用户网络流量，也不暴露网络接口。
- **无输入验证**：当前所有参数硬编码在示例/测试代码中，`clap` 尚未接入。若后续实现 CLI 或场景配置文件，需对流描述、拓扑参数做边界检查。
- ** panic 风险**：`sim_runner.rs` 和 `nic/tx.rs` 中的 `unwrap()` 在 malformed event 或数据不一致时会 panic 整个仿真进程。修改这些文件时应逐步替换为 `Result`。
- **日志文件体积**：启用 `tracing::info` 时，百万级事件可产生约 100MB 日志。`logs/` 目录已加入 `.gitignore`，但长时间运行高密度仿真时仍需注意磁盘空间。

---

## 10. 已知简化与学术假设

与真实硬件存在以下 gap（与 htsim、Astra-Sim 等主流学术模拟器一致）：
1. 不模拟 PCIe / DMA 开销（NIC 操作零延迟）。
2. 不模拟交换机查表延迟（路由瞬时完成）。
3. 链路误码率默认 0（bit error 通过显式注入测试，尚未实现）。
4. 不模拟 PFC 暂停帧（focus 在 STrack 自身的拥塞响应）。
5. MTU 固定 1KB。
6. TxTick 粒度 200ns，影响小流精度。
7. CC 简化：未实现 EWMA、HPCC、Swift 等高级算法。

---

## 11. 常见问题与修改建议

### Q: 我想新增一个流量模式（如 BurstyFlow）
A: 
1. 在 `src/traffic/` 下新建文件（如 `bursty.rs`），实现 `generate(&self) -> Vec<FlowDesc>`。
2. 在 `src/traffic/mod.rs` 中 `pub mod bursty;` 并导出。
3. 在 `tests/integration_e2e.rs` 或 `tests/matrix_workloads.rs` 或新建 `examples/` 中写端到端调用。
4. 运行 `cargo test --release` 确保全部通过。

**推荐优先使用 `Synthetic`**：对于参数化流量（不同流大小分布、到达过程、通信对组合），不需要新建文件，直接用 `Synthetic` 的组合能力即可。例如：
```rust
let flows = Synthetic {
    nodes: (0..16).collect(),
    pair_pattern: PairPattern::AllToAll,
    flow_size: FlowSizeDist::Pareto { min: 4096, shape: 1.5 },
    arrival: ArrivalProcess::Poisson { start: 1000, mean_interval_ns: 50_000 },
    seed: 42,
}.generate();
```

### Q: 我想修改拥塞控制算法
A: 
- 核心逻辑在 `nic/tx.rs` 的 `on_ack()` 和 `try_send()` 中。
- `nic/cc.rs` 的 `CongestionMode` 枚举控制模式切换。
- 新增模式时：在 `CongestionMode` 加变体 → 在 `on_ack()` 加 match arm → 在 `sim_runner.rs` 的 `summarize()` 中加模式字符串映射 → 写测试。

### Q: 我想新增拓扑
A: 
1. 在 `src/topology/` 下新建文件，实现 `build(self) -> Topology`。
2. `Topology` 结构需要填充：`hosts`, `switches`, `links`, `host_uplink`。
3. 注意 `EntityId` 编号规则（hosts 从 0 开始连续，switches 接在后面）。
4. 务必给每个 `Switch` 配置完整路由表（`routing.add(dst, port)`）。

### Q: 项目使用什么错误处理约定？
A: 项目使用**分层判错**策略（详见 `src/error.rs` 模块文档）：

**第一层：对外 API 返回 `SimResult<T>`**
- `SimRunner::new()` 返回 `SimResult<Self>` — 拓扑不一致（缺少 uplink、交换机缺失）时返回 `Err(SimError::Topology(...))`，调用方可以 graceful 处理。
- `SimError` 定义在 `src/error.rs`，使用 `thiserror::Error` derive，当前有两个变体：`Topology` 和 `Init`。
- 类型别名：`pub type SimResult<T> = Result<T, SimError>;`

**第二层：内部不变量使用 `expect()`**
- Protocol 实现（`strack.rs`、`tcp.rs`）中的 flow 查找使用 `.expect("invariant: 刚迭代的活跃流必存在于 tx_flows")`。
- 这类失败意味着代码 bug 而非运行时异常 — 即时 panic 是合理行为（研究模拟器，非生产服务）。
- **不修改 `Protocol` trait 签名**：trait 方法不返回 `Result`，避免全项目级联修改。

**第三层：已守卫的 `unwrap()` 改写为模式匹配**
- 典型模式：`if path.is_none() { break; } ... path.unwrap()` → `let Some(path) = self.pick_path(now) else { break; }; ... path`
- 意图显式化，消除裸 unwrap。

**第四层：测试代码**
- 测试中的 `unwrap()` / `expect()` **完全允许**，不做修改。

**新增代码时应遵循以上四层约定，禁止新增裸 `unwrap()`。**

---

## 12. 参考资料

- `README.md`：面向人类的快速开始与实测结果。
- `docs/design.md`：四阶段完整设计文档、数据结构定义、实测数据、待办事项清单。
- `src/error.rs`：判错系统设计文档（模块级注释说明分层策略与使用约定）。
- `logs/README.md`：日志目录说明与日志级别控制指南。
- STrack 论文：*"STrack: A Reliable Multipath Transport for AI/ML Clusters"* (Meta, NSDI'24)
