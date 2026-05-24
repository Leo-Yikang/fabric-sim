# 给 Coding Agent 的任务提示：收敛硬件层次模拟范围并修正 Host Delay 语义

你是当前 Rust 项目 `fabric-sim` 的 coding agent。请在本仓库内工作，遵守 `AGENTS.md` 项目规范：Rust 2021、中文注释/文档、不做无关重构、不全仓格式化、不删除用户已有改动。测试优先使用 release 模式。

本任务不是要求实现完整硬件仿真，而是把当前“主机侧硬件延迟注入”收敛为**简化但时间语义一致**的模型，并明确哪些硬件细节不需要模拟。

## 背景

项目目标是研究 STrack/Fabric 多路径 RDMA 传输在 AI/ML 集群网络中的表现。对这个目标来说，必要的是会影响拥塞、RTT、RTO、FCT、重传和多路径选择的模型；不必要的是 cycle-accurate 的 PCIe/NVLink/NIC 固件细节。

当前工作区已有第一版 Host Delay 接入：

- `src/network/host_delay.rs`：`HostDelayModel` / `HostDelayConfig`
- `src/sim_runner/host.rs`：发送/接收路径注入主机延迟
- `src/nic/protocol.rs`：新增 `update_send_time(...)`
- `src/nic/rdma_protocol.rs`：RDMA 实现了 `update_send_time(...)`
- `tests/integration_rdma.rs`：新增 host delay 相关测试
- `docs/design.md` / `docs/limit.md`：部分同步了文档状态

但当前实现仍有几个关键语义风险，需要修正。

## 总体原则

优先实现以下必要模型：

- 链路 serialization delay、传播延迟、switch buffer、ECN/drop。
- 协议层 ACK/NACK/SACK/RTO/重传/cwnd 或 pacing。
- RDMA message 分段、完成边界、RNR NAK 最小语义。
- 主机侧粗粒度开销：doorbell、PCIe 固定延迟、DMA/memcpy 带宽、CQ poll/interrupt。
- 简化 NIC/host 队列语义：同一 host 的发送时间、RTO 和 FCT 必须在同一条仿真时间轴上。

不要实现以下内容：

- cycle-accurate PCIe / NVLink / NVSwitch。
- PCIe TLP、lane、credit、BAR、BlueFlame、doorbell record 等真实硬件细节。
- NIC firmware 内部状态机、MR key cache、真实 CQE/WQE 二进制格式。
- GPU SM/warp/cache/HBM bank 等计算微架构。
- 完整 NUMA/cache coherence/page migration。

一句话：**会改变拥塞和 FCT 的抽象要做准；硬件内部实现细节不要做。**

## 必须修正的问题

### 1. `update_send_time` 必须能唯一定位 flow

当前 `Protocol::update_send_time(seq, nic_depart_time)` 只传 `seq`。这在多 flow 场景下不唯一，因为同一个 host 上多条 flow 都会从 `seq = 0` 开始。

请改成至少包含 `flow_id` 的接口，例如：

```rust
fn update_send_time(&mut self, flow_id: FlowId, seq: SeqNum, nic_depart_time: u64) {}
```

要求：

- `SimRunner::handle_tx_tick()` 记录 `(flow_id, seq, nic_depart_time)`。
- `RdmaProtocol::update_send_time(...)` 只更新对应 flow 的 `send_times`。
- 不要依赖 `HashMap` 遍历顺序。
- 补测试覆盖同一 host 多条 RDMA flow 同时发送时不会误写 `send_times`，且不会触发错误重传。

### 2. 明确 Host Delay 对非 RDMA 协议的支持范围

当前只有 RDMA 实现了 `update_send_time`，其他协议默认 no-op。但 `HostDelayConfig` 是通用 API，且 `HostDelayModel::tcp_kernel()` 暗示 TCP 也可使用。

请二选一，选择更小且更清晰的方案：

方案 A：支持所有现有协议。

- 为 `SimpleTcp`、`STrackProtocol`、`TcpReno`、`TcpCubic`、`DcqcnProtocol`、`HpccProtocol`、`SwiftProtocol` 等使用 `send_times` 的协议实现 `update_send_time(flow_id, seq, time)`。
- 补至少一个非 RDMA 回归测试：配置 `tx_delay > RTO` 时不应因为包尚未离开主机而错误重传。

方案 B：当前只保证 RDMA。

- 将 API/文档/注释明确写成：Host Delay 的 RTO 时间轴一致性当前只对实现 `update_send_time` 的协议生效，现阶段只验证 RDMA。
- `HostDelayModel::tcp_kernel()` 可保留为参数预设，但文档必须说明非 RDMA 协议尚未完成 RTO 一致性验证。

优先推荐方案 A。如果改动过大，采用方案 B，但必须写清楚限制。

### 3. 控制包发送延迟不能用 RDMA 注释覆盖所有协议

当前 `handle_arrive_at_host()` 对所有控制包回包都不注入发送端延迟，注释理由是“RDMA ACK 由 NIC 固件生成”。这个理由不适用于所有协议。

请修正为语义清楚的方案：

- 如果保持所有控制包零发送延迟，请把注释改成“当前简化模型统一把控制包生成视为主机接收处理完成后的即时 NIC 发包”，并在文档说明这是简化。
- 如果要区分协议，请增加明确机制，不要用字符串模式随意判断。
- 不要让 TCP kernel 模型的控制包路径和注释互相矛盾。

### 4. 修正 `HostDelayModel` 的带宽单位注释

`HostDelayModel` 字段名是 `*_bw_bps`，公式也是 bps：

```rust
bytes * 8 * 1_000_000_000 / bandwidth_bps
```

请检查并修正注释中的单位。例如当前类似“100 GB/s”但数值是 `100_000_000_000` bps，实际是 100 Gbps，不是 100 GB/s。

要求：

- 注释、数值、测试期望三者一致。
- 继续使用整数时间计算，不要恢复 `f64 ns_per_byte`。

### 5. 保持模型边界清楚

不要把 `NodeTopology` 静态 GPU/NIC 延迟矩阵强行接入主路径。当前阶段只需要：

- host 仍是最小通信实体。
- 可选 Host Delay 注入保持默认关闭。
- 文档写清楚：仍缺少 GPU/rank/NIC 映射、NUMA、多 NIC 选路、真实硬件队列/带宽争用。

如果要新增队列模型，只做最小必要的 host/NIC 发送时间一致性，不要扩展到完整 GPU/NVSwitch。

## 建议阅读文件

请先阅读：

- `src/sim_runner/host.rs`
- `src/sim_runner/mod.rs`
- `src/nic/protocol.rs`
- `src/nic/rdma_protocol.rs`
- `src/nic/tcp.rs`
- `src/nic/strack.rs`
- `src/network/host_delay.rs`
- `tests/integration_rdma.rs`
- `docs/design.md`
- `docs/limit.md`

如果选择支持所有协议，再阅读对应协议文件：

- `src/nic/reno.rs`
- `src/nic/cubic.rs`
- `src/nic/dcqcn.rs`
- `src/nic/hpcc.rs`
- `src/nic/swift.rs`

## 测试要求

必须保留并通过现有 RDMA host delay 测试，同时新增或加强以下覆盖：

1. RDMA 单 flow：启用 host delay 后 flow 完成，FCT 大于无延迟场景。
2. RDMA 大延迟：`tx_delay > RTO` 时不应错误重传。
3. RDMA 多 flow：同一 host 上多条 flow 都从 `seq = 0` 开始时，`update_send_time` 不应误写其他 flow。
4. `HostDelayModel` 整数计算：小包有固定开销，大包延迟不小于小包，注释单位与测试一致。
5. 如果选择方案 A，再补至少一个非 RDMA 协议的大延迟无误重传测试。

推荐运行：

```bash
cargo test --release --test integration_rdma -- --nocapture
cargo test --release
```

如果只改了少量文件，可以先跑更窄的测试，但最终请跑全量 release 测试。

## 文档要求

同步更新：

- `docs/design.md`
- `docs/limit.md`

文档应表达：

- Host Delay 已有可选简化注入，默认关闭。
- 当前模型不是完整硬件层次模拟。
- 必要模拟是粗粒度主机/NIC 延迟、队列和时间一致性。
- 不必要模拟是 cycle-accurate PCIe/NVLink/NIC/GPU 内部细节。
- 如果非 RDMA 协议未实现 `update_send_time`，必须明确写出限制。

## 约束

- 不要修改无关模块。
- 不要全仓 `cargo fmt`。只对修改过的 Rust 文件运行 `rustfmt <file...>`。
- 不要新增裸 `unwrap()`；测试中允许 `expect()`。
- 不要删除或回退用户已有改动。
- 不要为了消 warning 做大范围重构。
- 保持中文注释和中文文档风格。

## 输出报告格式

完成后请输出：

```text
## 修改摘要
- ...

## 语义说明
- send_times / RTO / Packet.depart_time 如何保持一致
- Host Delay 当前支持哪些协议
- 哪些硬件细节仍未模拟

## 测试结果
- 命令：...
  结果：...

## 剩余风险
- ...
```
