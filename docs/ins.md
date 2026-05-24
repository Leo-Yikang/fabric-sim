# 给 Crush 的任务提示：完善 RDMA enhancement 端到端可用性

你是当前 Rust 项目 `fabric-sim` 的 coding agent。请在本仓库内工作，目标是让 RDMA enhancement 从“结构/原型级”推进到“最小端到端可跑且有测试覆盖”的状态。

请先只聚焦 RDMA，不要做大范围重构，不要修改无关模块，不要全仓格式化。

## 背景

项目是 Rust 2021 离散事件网络模拟器。当前主路径由 `SimRunner` 统一调度事件，协议通过 `Protocol` trait 插入。已有 Fabric、SimpleTcp、DCQCN、HPCC、Swift，以及新加入但成熟度不足的 RDMA 相关模块。

请重点阅读：

- `docs/limit.md` 中 “二、RDMA 语义与数据中心网络特性差距”
- `src/nic/rdma.rs`
- `src/nic/rdma_protocol.rs`
- `src/nic/protocol.rs`
- `src/network/packet.rs`
- `src/sim_runner/mod.rs`
- `src/sim_runner/host.rs`
- `tests/` 下现有集成测试风格

当前 RDMA 文档有“已实现”和“旧缺口表”并存的问题。代码里确实有 RDMA 骨架，但还需要确认是否真正端到端可跑。

## 目标

完成最小必要修复，使 `RdmaProtocol` 能在一个小拓扑中通过 `SimRunner` 完成一条 RDMA Send 多包消息，并能被 `summarize()` 统计为完成 flow。

同时补充测试，证明这个行为可持续回归。

## 重要问题检查

请重点检查并修复以下问题。

### 1. QPN 从 0 开始的问题

`Packet` 注释中写着 `qpn = 0` 表示非 RDMA 包，但 `RdmaProtocol::next_qpn` 当前可能从 0 开始。

接收端代码里有类似：

```rust
if pkt.qpn > 0 {
    // RDMA 包处理
}
```

如果第一条 QP 是 0，那么第一条 RDMA 流会被当成非 RDMA 包，导致 RDMA 重组、RNR 等逻辑不执行。

要求：

- 明确 `qpn = 0` 是否保留为非 RDMA sentinel。
- 如果保留，请让 `RdmaProtocol` 从 `qpn = 1` 开始分配。
- 补测试覆盖第一条 QP 不是 0，且第一条 RDMA flow 能走 RDMA 分支。

### 2. PendingMessage 完成条件可能不推进

`PendingMessage::done()` 依赖 `next_packet_idx >= total_packets`。请检查发送路径是否真的推进了 `next_packet_idx`。

如果发送路径只推进 `FlowTxState.next_seq`，但不推进 `PendingMessage.next_packet_idx`，那么 message 可能永远不被认为完成。

要求：

- 修复 `next_packet_idx` 推进逻辑。
- 确认多包消息发完后 `PendingMessage::done()` 为 true。
- 确认 ACK 后 flow 能标记完成，`finished_flows` 能被 `SimRunner` 消费。

### 3. Flow 完成统计

`SimRunner` 依赖协议的 `take_finished_flows()` 更新 `FlowFct.finish_ns`。请确认 `RdmaProtocol` 在消息/flow 完成时会：

- 标记对应 `FlowTxState.done = true`
- 设置合理的 `finish_time`
- push 到 `finished_flows`
- 不重复 push 同一 flow

要求：

- 小拓扑端到端测试中 `summary.completed_flows == summary.total_flows`。
- `summary.fct_p50_ns` 或 `summary.fct_p99_ns` 大于 0。

### 4. ACK 语义和消息完成

当前 RDMA ACK 使用 `PacketKind::Control(0)`，ACK seq 为 `pkt.seq + 1`。请检查：

- 多包消息是否每个包都 ACK。
- `on_ack()` 是否能推进 `un_acked_base`。
- 所有 seq 被 ACK 后是否会触发 flow/message 完成。

不要实现完整 IB ACK 语义，先做最小一致模型即可，但要能端到端跑通。

### 5. RNR 语义最小测试

如果时间允许，补一个最小 RNR 测试。可以是单元测试，不一定端到端。

要求至少验证：

- 没有 recv WQE 时，RDMA Send 可触发 RNR NAK。
- 收到 RNR NAK 后，发送侧设置 `rnr_retry_at_ns`，并增加 retry 计数。

如果 RNR 当前设计无法可靠端到端恢复，请不要大重构；写清楚限制，只补可验证的最小单元测试。

## 约束

- 不要修改 `.claude/`。
- 不要修改 `scripts/__pycache__/`。
- 不要全仓 `cargo fmt`。只对你修改的 Rust 文件运行 `rustfmt <file...>`。
- 不要做无关协议重构。
- 不要为了消除 warning 大范围格式化或重写文件。
- 保持项目中文注释风格。
- 测试代码允许 `unwrap()` / `expect()`。

## 建议实现范围

优先修改：

- `src/nic/rdma_protocol.rs`
- 必要时修改 `src/nic/rdma.rs`
- 必要时修改 `src/network/packet.rs` 注释或构造函数
- 新增或修改 `tests/integration_protocols.rs` / 新建 `tests/integration_rdma.rs`

尽量不要修改：

- 非 RDMA 协议实现
- 拓扑生成器
- SimRunner 主逻辑，除非发现 RDMA 完成统计必须接入

## 必须新增测试

请新增一个端到端集成测试，建议文件：

```text
tests/integration_rdma.rs
```

测试建议：

```rust
#[test]
fn rdma_send_multi_packet_completes() {
    // 构建小 Dumbbell 或 LeafSpine 拓扑
    // 使用 RdmaProtocol
    // 注入 1 条或少量 flow，大小 > MTU，例如 4 * MTU
    // runner.run(...)
    // summary = runner.summarize()
    // assert completed_flows == total_flows
    // assert fct > 0
}
```

如果发现当前 `RdmaProtocol` 的默认 `start_flow()` 是 RDMA Send 且接收端没有预置 recv WQE 会触发 RNR，导致端到端无法完成，那么有两个可接受方向：

1. 在测试或协议初始化里为对端预置 recv WQE；
2. 或者让默认 `start_flow()` 使用 `post_write()`，Send/RNR 单独用单元测试覆盖。

请选择最小、语义最清楚的方案，并在注释里说明原因。

## 推荐测试命令

先跑 RDMA 相关：

```bash
cargo test --release rdma -- --nocapture
```

再跑新增集成测试：

```bash
cargo test --release --test integration_rdma -- --nocapture
```

最后如果时间允许，跑全量：

```bash
cargo test --release
```

## 输出报告格式

完成后请输出以下结构化报告：

```text
## 检查过的文件
- ...

## 发现的问题
1. ...
2. ...

## 修改的文件
- path: 修改内容摘要

## 测试结果
- 命令：...
  结果：通过/失败

## 剩余风险
- ...
```

如果你无法完成代码修改，也请至少输出诊断报告，明确阻塞点。
