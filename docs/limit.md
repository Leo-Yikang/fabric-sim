# STrack-Sim 限制与差距分析

> **文档版本**：v0.5.0-20260524
> **对应代码版本**：strack-sim 0.1.0 + RDMA enhancement
> **分析范围**：性能瓶颈 + RDMA 语义差距 + 模型简化点 + **实施进度**

---

## 一、性能瓶颈（单线程 DES 架构约束）

> 本节从 **引擎层、内存层、事件密度、模型层** 四个维度，分析当前单线程 DES 架构在面向大规模通信集群时的结构性性能瓶颈。这些瓶颈不属于代码缺陷，而是架构设计在规模扩展时的天然约束。
>
> **单线程优化实施状态**：P1/P2/P3/P5 已完成并验证通过，P4 待评估。

### 1.1 事件队列：O(log N) 的堆操作是头号瓶颈

**现状**：`EventQueue` 基于 `std::collections::BinaryHeap`，`push/pop` 均为 **O(log N)**，且内存布局呈树状，缓存局部性差。

**大规模下的问题**：

- 一个 1024 节点的 Leaf-Spine 拓扑，AllToAll 流量下，每秒仿真时间可能产生 **数千万到上亿事件**（TxTick + PacketArrive + PacketDepart + 定时器重传）。
- BinaryHeap 的 sift-up/sift-down 涉及随机内存访问，CPU cache miss 比例随队列长度增加而恶化。
- 全局 `EVENT_SEQ: AtomicU64` 在超高频事件创建时，虽然用了 `Relaxed`，但在多核缓存一致性协议下仍有一定开销。

#### 已实施方案：4-ary heap（四叉堆）

将 `BinaryHeap` 替换为**自实现 4-ary heap**（`src/core/queue.rs`）。

- **原理**：4-ary heap 的树高约为同规模二叉堆的一半（log₄ N vs log₂ N），sift-up/sift-down 的内存跳转次数更少。
- **关键细节**：`Event` 的 `Ord` 实现是为 `BinaryHeap`（最大堆）**反向**设计的，因此 4-ary heap 内部直接比较 `time` 和 `seq`，不依赖 `Ord` trait。
- **复杂度**：push/pop 仍为 O(log N)，但常数因子降低约 30~50%（更少的 swap 次数 + 更浅的树）。
- **风险与回退**：实现简单，仅 60 行代码；如果后续发现正确性问题，可直接替换回 `BinaryHeap`（接口完全兼容）。

### 1.2 包生命周期：HashMap 是第二个热点

**现状**：`packet_buf: HashMap<u64, Packet>` 作为包的"全局暂存区"，每个包从生成到销毁经历两次 HashMap 操作（`insert` + `remove`）。

**大规模下的问题**：

- `HashMap` 的 insert/remove 涉及哈希计算、桶定位、可能的重新哈希/内存分配、节点插入/移除。
- 在 100Gbps 链路、MTU=1KB 的设定下，一条链路每 80ns 就能发一个包。一个 48 端口交换机满负载时，事件处理频率极高，`packet_buf` 成为热点。

#### 已实施方案：自实现 PacketSlab allocator

引入 `PacketSlab` 结构（`src/sim_runner/mod.rs`），用 `Vec<Option<Packet>>` + 空闲列表替代 `HashMap`。

- **原理**：Slab allocator 通过数组索引直接定位，insert/remove 均为 O(1)，且无哈希开销。`remove` 后索引放入 `free` 列表供后续复用。
- **关键改动**：
  - `packet_buf_insert(pkt: Packet) -> u64`：插入包并返回 slab 索引，**同时覆盖 `pkt.id`** 为 slab 索引，确保 Event 携带的 `packet_id` 与 slab 位置一一对应。
  - `packet_buf_remove(pid: u64) -> Option<Packet>`：按索引直接取出。
  - 交换机出队转发时，包重新插入 Slab 获得**新索引**（原位置已被 `remove` 释放），Event 使用新索引。
- **收益**：消除了每次包传输的两次哈希操作，内存布局更连续。
- **副作用**：去掉了 `global_pid` 全局递增计数器，包的 `id` 不再是全局单调递增（而是 slab 索引，可复用）。不影响仿真逻辑，只影响调试追踪。

### 1.3 事件密度爆炸：TxTick 的"轮询"代价

**现状**：每个主机每 **200ns** 固定触发一个 `TxTick`。当 `cwnd` 满或没有活跃流时，会退化为 **25us** 的轮询。

**大规模下的问题**：

- 假设 10,000 节点集群，全部活跃 → 每 200ns 产生 **10,000 个 TxTick 事件**，即 **50M events/ms**。
- 即使 release 模式达到 25M events/sec，光 TxTick 就占用了 2000 倍的处理能力——这根本跑不动。
- 实际上大部分 TxTick 是"空转"（`pkts.is_empty()`），但模拟器仍需调度 → 弹出 → 分发 → 判断。

**根本矛盾**：精细的时间粒度（200ns tick）与大规模节点数之间存在 **N × granularity** 的乘积效应。

#### 已实施方案：TxTick 完全事件驱动 + RTO Timeout 独立定时

**核心思路**：让协议栈决定"我什么时候需要下一次 tick"，模拟器不再主动空转轮询。

##### Protocol trait 扩展

新增两个方法（`src/nic/protocol.rs`）：

```rust
/// 当前是否有待发送的工作（cwnd 有空间可发新数据，或重传队列非空）
fn has_pending_work(&self) -> bool;

/// 返回最早的未确认包的 RTO 截止时间（send_time + rto_ns）
fn next_rto_deadline(&self) -> Option<u64>;
```

- `has_pending_work`：STrack 实现中遍历活跃流，检查 `(in_flight < cwnd && next_seq < total_packets)` 或 `retransmit_queue` 非空。
- `next_rto_deadline`：遍历所有 `send_times`，找最小 `send_t + rto_ns`。

##### 调度逻辑重构（`src/sim_runner/host.rs`）

- **`FlowStart`**：调度 TxTick @ now（初始触发）。
- **`handle_tx_tick` 发送完包后**：
  - 若 `has_pending_work()` → 调度 TxTick @ now + `tx_tick_ns`（保留 pacing 间隔）。
  - 否则若 `next_rto_deadline()` 返回 Some(t) → 调度 `Timeout { timer_id: 0 }` @ t（独立 RTO 定时器）。
  - 否则完全静默，等外部事件驱动。
- **`handle_arrive_at_host`（ACK/NACK 到达后）**：
  - 若 `has_pending_work()` → 调度 TxTick @ now（立即响应，无延迟）。
  - 否则按 RTO 定时或静默。
- **`dispatch` 新增 `Timeout` 处理**：RTO Timeout 到期 → 触发 TxTick @ now，让协议栈检查超时重传。

##### 收益与验证

- **彻底消除空转**：cwnd 满时不再有 25us 间隔的轮询事件。
- **RTO 检查不受频率限制**：由独立 Timeout 事件精确在 `send_time + rto_ns` 触发，不再依赖 TxTick 的采样精度。
- **59 个测试全部通过**，包括 incast、alltoall、permutation、poisson 到达等多种流量模式。

### 1.4 交换机模型：内存分散与重复分配

**现状**：`SwitchPort` 每个端口有一个 `VecDeque<Packet>`，`Switch::ingress` 中路由查表返回 `Vec<PortId>`。

**大规模下的问题**：

- `VecDeque` 的 ring buffer 在频繁入队/出队时表现不错，但每个端口独立分配内存，缓存局部性差。
- `ingress()` 中 `ports.to_vec()` 每次都会**分配一个临时 Vec**（即使只有 2~4 个端口），这是完全不必要的堆分配。

#### 已实施方案：去掉 `to_vec()`，用内部 block 释放借用

（`src/network/switch.rs`）

- **问题根源**：`ports.to_vec()` 是为了避免 `self.routing` 的不可变借用和 `self.ports` 的可变借用冲突。
- **解法**：用内部 block 先计算 `chosen`（`PortId` 是 `Copy`），block 结束后释放对 `self.routing` 的借用，再访问 `self.ports`。

```rust
let chosen = {
    let ports = match self.routing.ports_for(pkt.dst) {
        Some(p) if !p.is_empty() => p,
        _ => { ... }
    };
    let idx = ...;
    ports[idx]   // 返回 PortId（Copy），借用随 block 结束释放
};
let port = &mut self.ports[chosen as usize];  // 可变借用，无冲突
```

- **收益**：每次入包节省一次小 Vec 堆分配（路由端口通常只有 2~4 个）。
- **风险**：零风险，一行语义等价替换。

### 1.5 协议栈：动态分发的间接开销

**现状**：`protocols: Vec<Box<dyn Protocol>>`，每个 host 一个协议实例。

**问题**：

- 每次访问协议栈需要 **两次间接跳转**：Vec 索引 → Box 解引用 → vtable 查找。
- `Protocol` trait 的 `on_tx_tick()` 返回 `Vec<Packet>`，即使只发一个 ACK 也要做一次 Vec 分配。

#### 未实施方案：泛型化 `SimRunner<P: Protocol>`

- **方案**：将 `Box<dyn Protocol>` 替换为泛型参数 `P: Protocol`，编译器可在热路径上内联 `on_tx_tick`/`on_ack`。
- **阻碍**：`SimRunner` 在 **15+ 处** examples/tests 中使用（`incast_compare`、`workload_sweep`、`matrix_workloads` 等），全部需要显式标注类型（如 `SimRunner<STrackProtocol>`）。改动面广，但逻辑简单。
- **评估**：vtable 开销在已完成的 P1/P2/P3 优化后占比降低，当前投入产出比偏低。建议作为**后续可选项**，在需要榨干最后 5~10% 性能时实施。

### 1.6 单线程天花板：无法横向扩展

**这是最根本的瓶颈。**

DES 的因果一致性要求事件按时间顺序处理，天然串行。当前架构完全没有并行化：

- 没有 **LP（Logical Process）** 分区（如 ROSS、OMNeT++ 的并行 DES）。
- 没有 **乐观时间推进**（Time Warp）。
- 没有 **保守同步**（Chandy-Misra-Bryant，空消息算法）。

对于 10K+ 节点的 AI 集群 AllReduce 模拟，单线程 DES 的吞吐量上限决定了：

- 要么缩小拓扑规模
- 要么降低时间精度
- 要么忍受数小时的仿真时间

### 1.7 瓶颈优先级与实施状态

| 优先级 | 瓶颈 | 实施状态 | 改造难度 | 关键文件 |
|--------|------|----------|----------|----------|
| P0 | **单线程 DES 架构** | 未开始 | 高 | — |
| P1 | **TxTick 事件密度** | ✅ 已完成 | 中 | `protocol.rs`, `host.rs`, `mod.rs` |
| P2 | **事件队列堆操作** | ✅ 已完成（4-ary heap）| 中 | `core/queue.rs` |
| P3 | **packet_buf HashMap** | ✅ 已完成（Slab）| 中 | `mod.rs`, `host.rs`, `switch.rs` |
| P4 | `Box<dyn Protocol>` vtable | ⏳ 待评估 | 低 | `mod.rs`, `lib.rs`, `examples/*`, `tests/*` |
| P5 | `ingress()` 中的 `to_vec()` | ✅ 已完成 | 低 | `network/switch.rs` |

### 1.8 优化建议

- **已完成的 Phase 1**（P1+P2+P3+P5）：
  - 消除了 TxTick 空转轮询
  - 事件队列从二叉堆升级为四叉堆
  - 包暂存从 HashMap 换为 Slab allocator
  - 交换机入包去掉临时 Vec 分配
  - **下一步**：运行 `cargo bench` 和 `incast_compare` example，量化实际吞吐提升。

- **Phase 2（可选）**：P4 泛型化。如果 benchmark 显示 vtable 仍是热点，再实施。

- **Phase 3（远期）**：保守并行 DES（LP 分区）。当单线程优化触及天花板、且必须模拟 10K+ 节点时启动。

---

## 二、RDMA 语义与数据中心网络特性差距

> **实施状态**：P1-P4 已全部实现（2026-05-24），详见各子节 ✅ 标记。

### 2.1 协议层：RDMA 核心语义缺失 ✅ P1A/P1B/P1C 已实现

| 缺失项 | 当前状态 | 实现文件 |
|--------|----------|----------|
| **QP/Queue Pair 状态机** | ✅ 已实现 | `src/nic/rdma.rs` — QpState(RESET→INIT→RTR→RTS) |
| **WQE/CQE 队列模型** | ✅ 已实现 | `src/nic/rdma.rs` — Wqe/Cqe + QP.send_queue/recv_queue |
| **Message 边界** | ✅ 已实现 | `src/nic/rdma_protocol.rs` — segment_message(First/Middle/Last/Solo) |
| **RDMA Write/Send** | ✅ 已实现 | `Protocol::post_send` / `post_write` + RdmaOpcode |
| **RNR (Receiver Not Ready)** | ✅ 已实现 | 指数退避重试(100μs→100ms)，RNR NAK控制包(Control(1)) |
| **Selective Repeat** | ✅ SACK bitmap | STrack 已有 SACK bitmap（64位） |
|--------|----------|----------------|------|
| **QP/Queue Pair 状态机** | 无 | 每连接独立 PSN 空间、WQE/CQE 队列 | 无法模拟连接生命周期、PSN 回绕、错误恢复 |
| **WQE/CQE 队列模型** | 无 | post/send/recv → doorbell → completion event | 无法模拟软件提交延迟、completion batching |
| **Message 边界** | packet 级 | RDMA Write/Send 由多包组成一个 message | 需要 message segmentation/reassembly |
| **RDMA Read/Write/Atomic** | 仅类似 Send/Recv | one-sided 操作是 RDMA 核心优势 | 无法模拟 bypass CPU 的零拷贝路径 |
| **RNR (Receiver Not Ready)** | 无 | 接收端 QP 无可用 receive WQE 时触发 | 核心流控机制缺失 |
| **Selective Repeat** | SACK bitmap | 真实 RDMA 是 go-back-N 或 selective repeat | 重传语义可能不对齐 |

**结论**：当前模拟器更接近「多路径 TCP」而非「RDMA 模拟器」。

### 2.2 拥塞控制：缺少数据中心关键机制

| 缺失项 | 当前状态 | 需要补充 |
|--------|----------|----------|
| **PFC (Priority Flow Control)** | 完全未实现 | 无损以太网基础，head-of-line blocking 根源 |
| **CNP (Congestion Notification Packet)** | 未实现 | DCQCN 核心反馈机制，替代 ECN 或直接协同 |
| **Rate-based CC** | 仅 cwnd-based | DCQCN/HPCC/Swift 均为 rate-based，需 rate limiter |
| **ECN + PFC 协同** | 单一 ECN threshold | 动态 threshold、ECN marking profile (K_min, K_max, P_max) |
| **Priority/Traffic Class** | 无 | lossy/lossless 多优先级共存 |

文档中 DCQCN 为「基于速率的量化拥塞控制」，但实现细节（alpha 更新、速率恢复曲线、CNP 生成）需与真实 RoCEv2 对齐。

### 2.3 硬件层次：单 NIC 零延迟过于理想

```
当前模型:  Host ──→ NIC ──→ Leaf Switch
真实模型:  GPU → NVLink → NVSwitch → PCIe → NIC → Leaf
                    ↓
              多 GPU / 多 NIC / 多 Rail
```

- **无 NVLink/NVSwitch**：无法区分 intra-node 和 inter-node 通信
- **无 PCIe/DMA 延迟**：NIC 操作零延迟，无法评估 GPUDirect RDMA 优势
- **无多 NIC/Rail**：现代训练节点通常 8×GPU + 8×NIC (rail-optimized)
- **无 NUMA 效应**：CPU-GPU-NIC 亲和性影响未建模

### 2.4 训练语义：Flow 级 vs Job 级

P2 已完成 `TrainingJob` / `CollectiveOp`，但关键缺口仍在：

1. **无 Compute-Communication Overlap**：真实训练 forward/backward 计算与 all-reduce 通信重叠
2. **无 Collective DAG**：AllReduce → AllGather → Barrier 的依赖关系未建模
3. **无 Pipeline Bubble**：PP (Pipeline Parallelism) 的空泡效应
4. **无 Tensor/Model/Expert Parallel 组合**：无法模拟真实大模型训练拓扑

### 2.5 交换机模型：与真实硬件差距

| 当前模型 | 真实交换机 |
|----------|------------|
| 每端口独立 FIFO | Shared Buffer + Dynamic Threshold |
| 单一 ECN threshold | ECN marking profile |
| 无优先级队列 | 多优先级 + WRR/SP 调度 |
| 无 PFC | PFC pause/resume per priority |
| 路由零延迟 | Switch pipeline + lookup delay |
| 无 Adaptive Routing | Flowlet、Congestion-Aware Routing |

### 2.6 建议改进优先级

```
P1 (核心 RDMA 语义):
  ├─ 增加 QP 状态机 + PSN + WQE/CQE 抽象
  ├─ 实现 Message 级边界 (RDMA Write/Send/Read)
  └─ 增加 RNR 和基本流控

P2 (数据中心网络特性):
  ├─ 实现 PFC pause/resume
  ├─ 实现 CNP + Rate-based DCQCN
  └─ 增加 Priority Queue + Shared Buffer

P3 (硬件层次):
  ├─ 多 NIC / 多 Rail 拓扑
  ├─ NVLink/NVSwitch 简化模型
  └─ PCIe/DMA 延迟注入

P4 (训练语义完善):
  ├─ Compute-Communication Overlap
  ├─ Collective DAG + Barrier
  └─ Pipeline Parallelism 空泡

P5 (规模化):
  ├─ Flow-level / Hybrid 仿真模式
  └─ Parallel DES (LP 分区)
```

> 若目标是复现 NSDI'24 STrack 论文实验并与真实硬件对标，**P1 和 P2 为最关键差距**。

---

## 相关文件

- `src/core/queue.rs` — 4-ary heap 事件队列实现
- `src/core/event.rs` — 事件类型与全局 seq
- `src/nic/rdma.rs` — QP状态机、PSN、WQE/CQE、MsgBoundary
- `src/nic/rdma_protocol.rs` — RdmaProtocol(消息分段重组+RNR流控)
- `src/nic/protocol.rs` — Protocol trait(RDMA扩展方法)
- `src/network/switch.rs` — PFC修复+Shared Buffer
- `src/network/host_delay.rs` — NVLink/NVSwitch/PCIe延迟模型
- `src/topology/multi_rail.rs` — 多NIC/Rail拓扑
- `src/training/dag.rs` — 训练DAG+Compute-Comm Overlap
- `src/sim_runner/mod.rs` — SimRunner 主循环 + PacketSlab
- `src/sim_runner/host.rs` — 事件驱动 TxTick + RTO Timeout 调度
- `src/sim_runner/switch.rs` — 交换机事件处理

## 三、实施进度总览

| 优先级 | 模块 | 状态 | 关键文件 | 提交版本 |
|--------|------|------|----------|----------|
| P1A | QP状态机+PSN | ✅ | `nic/rdma.rs` | `8bf1a2a` |
| P1B | 消息分段重组+Protocol扩展 | ✅ | `nic/rdma_protocol.rs` | `5f573da` |
| P1C | RNR流控 | ✅ | `nic/rdma_protocol.rs` | `c9cdbe1` |
| P2A | PFC修复 | ✅ | `network/switch.rs` | `8cd840f` |
| P2B | Shared Buffer | ✅ | `network/switch.rs` | `ec9e785` |
| P3A | 多NIC/Rail | ✅ | `topology/multi_rail.rs` | `ec9e785` |
| P3B | NVLink/PCIe | ✅ | `network/host_delay.rs` | `ec9e785` |
| P4 | Training DAG+Overlap | ✅ | `training/dag.rs` | `ec9e785` |
| P5 | 并行DES | ⏳ 远期 | — | — |
