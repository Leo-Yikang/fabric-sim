# Fabric-Sim 限制与差距分析

> **文档版本**：v0.5.0-20260524
> **对应代码版本**：fabric-sim 0.1.0 + RDMA enhancement
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

> **实施状态说明（2026-05-24）**：RDMA enhancement 已有第一版结构和最小端到端路径，但不能按“P1-P4 全部成熟完成”理解。当前更准确的定位是：**RDMA Write 最小端到端原型可跑；RDMA Send/RNR、PFC、Multi-Rail、Host Delay、Training DAG 仍处于结构级或单元测试级原型**。

成熟度分级：

- **结构完成**：类型、字段、模块和基础 API 已存在。
- **原型可跑**：局部逻辑有单元测试或可被调用。
- **端到端验证**：通过 `SimRunner` 和真实拓扑/事件流完成。
- **可校准可信**：与真实硬件/论文模型的语义和参数对齐。

### 2.1 协议层：RDMA 核心语义成熟度

| 能力 | 当前成熟度 | 实现文件 | 说明 |
|------|------------|----------|------|
| **QP/Queue Pair 状态机** | 结构完成 | `src/nic/rdma.rs` | 有 `QpState`、`QueuePair`、PSN、WQE/CQE 结构；连接生命周期、错误恢复、PSN 回绕未校准 |
| **WQE/CQE 队列模型** | 结构完成 | `src/nic/rdma.rs` | 有 WQE/CQE 类型和队列字段；doorbell、completion batching、CQ polling 仍未进入主事件流 |
| **Message 边界** | 原型可跑 | `src/nic/rdma_protocol.rs` | 支持 Solo/First/Middle/Last 分段和基础重组 |
| **RDMA Write** | 最小端到端验证 | `src/nic/rdma_protocol.rs`, `tests/integration_rdma.rs` | 默认 `start_flow()` 使用 Write，单包/多包/双向流可通过 `SimRunner` 完成并统计 FCT |
| **RDMA Send** | 原型可跑 | `src/nic/rdma_protocol.rs` | `post_send()` 存在；Send + posted recv 尚未通过端到端测试闭环 |
| **RNR (Receiver Not Ready)** | 单元测试级原型 | `src/nic/rdma_protocol.rs` | 可产生 RNR NAK 并设置退避；RNR 后恢复发送仍未端到端验证 |
| **RDMA Read / Atomic** | 未实现 | — | enum 有 opcode，但没有真正 read request/response 或 atomic 语义 |
| **重传语义** | 简化 | `src/nic/rdma_protocol.rs` | 当前复用 cwnd/RTO 和 packet ACK；不是完整 IB/RoCE ACK/NAK/PSN 语义 |

当前结论：RDMA 模块已经从“纯结构”推进到 **Write 路径最小可跑**，但仍不是成熟 RoCE/RDMA 模拟器。它适合继续做协议原型，不适合直接用于真实 RDMA 对标。

### 2.2 拥塞控制与无损网络机制成熟度

| 能力 | 当前成熟度 | 实现文件 | 说明 |
|------|------------|----------|------|
| **CNP / DCQCN** | 简化原型 | `src/nic/dcqcn.rs` | 有 CNP 控制包、alpha、rate-based pacing；参数、恢复曲线、硬件语义未校准 |
| **HPCC / Swift** | 占位原型 | `src/nic/hpcc.rs`, `src/nic/swift.rs` | 文件内明确是简化/占位实现，不能作为可信 baseline |
| **Priority Queue** | 原型可跑 | `src/network/switch.rs` | 有 2 个优先级队列，按 `routing_tag` 临时映射优先级 |
| **PFC** | 结构/计数器级原型 | `src/network/switch.rs` | 有 `paused` 标志和 pause/resume 计数；未建模 pause frame 上游反压和 HOL blocking |
| **Shared Buffer** | 结构级视图 | `src/network/switch.rs` | 有 `total_queue_bytes()` 视图；丢包/阈值仍主要按端口队列，不是真 shared buffer allocator |
| **ECN + PFC 协同** | 未成熟 | `src/network/switch.rs` | 仍是单一 ECN threshold，没有 Kmin/Kmax/Pmax 或动态阈值 |

当前结论：数据中心 RDMA 网络机制已有可扩展接口和部分简化行为，但 PFC/Shared Buffer 还不能用于分析真实无损以太网的 head-of-line blocking 或 pause storm。

### 2.3 硬件层次：可选简化延迟注入，仍缺少完整 GPU/NIC 映射

```
当前模型:  Host ──→ NIC ──→ Leaf Switch
真实模型:  GPU → NVLink → NVSwitch → PCIe → NIC → Leaf
                    ↓
              多 GPU / 多 NIC / 多 Rail
```

- **已有可选简化主机延迟注入**：`src/network/host_delay.rs` 提供 `HostDelayModel`，支持通过
  `SimRunner::with_host_delays()` 在发送/接收路径注入 DMA、doorbell、PCIe 往返、CQ poll
  等简化延迟。默认关闭，向后兼容。全部 8 个协议均已实现 `Protocol::update_send_time()`，
  `SimRunner::handle_tx_tick()` 通过 `flow_id` 唯一定位流，避免多流场景下的误写。
- **控制包简化**：ACK/NACK 在当前模型中视为接收处理完成后的即时 NIC 发包，不注入发送端延迟。
- **仍缺少 GPU/rank/NIC 映射**：当前 host 仍是最小通信实体，不区分 GPU 内存、CPU 内存、DMA 引擎。
- **多 NIC 选路已有原型**：`NicSelector` 支持 First / FlowHash / RoundRobin 三种策略，通过 `SimRunner::pick_uplink()` 在多个上行链路中选取。当前 `MultirailLeafSpine` 将每个 (host,nic) 建模为独立实体而非同一 host 的多 uplink，真正同 host 多 uplink 场景尚未测试（P2 原型级）。
- **已有主机侧 DMA 串行化原型**：通过 `host_tx_busy_until[host]` 建模，同一 host 上多个包的 DMA/memcpy 会串行化，产生排队等待和队列深度。仍缺少：PCIe/NIC 多队列、多 DMA engine、GPU/rank/NIC 亲和映射。

### 2.4 训练语义：Flow 级 vs Job 级

P2 已完成 `TrainingJob` / `CollectiveOp` 的静态展开，P4 也有 `training/dag.rs` 辅助结构，但关键缺口仍在：

1. **Compute-Communication Overlap 未接入主事件流**：`training/dag.rs` 可构造 DAG/offset，但不是运行时调度器。
2. **无运行时 Collective DAG**：AllReduce → AllGather → Barrier 的依赖关系仍静态化。
3. **Pipeline Bubble 仅有抽象字段/辅助函数**：PP 空泡没有资源占用模型。
4. **无 Tensor/Model/Expert Parallel 组合**：无法模拟真实大模型训练拓扑

### 2.5 交换机模型：与真实硬件差距

| 当前模型 | 成熟度 | 真实交换机仍缺 |
|----------|--------|----------------|
| 每端口多优先级 FIFO | 原型 | WRR/SP/WFQ、VOQ、cell switching |
| 单一 ECN threshold | 简化 | ECN marking profile、dynamic threshold |
| PFC paused 标志 | 结构级 | pause frame、上游反压、HOL blocking |
| `total_queue_bytes()` shared buffer 视图 | 结构级 | shared buffer 分配/抢占策略 |
| 路由零延迟 | 简化 | Switch pipeline + lookup delay |
| 静态 ECMP/routing_tag | 简化 | Flowlet、Congestion-Aware Routing |

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

> 若目标是复现 STrack 论文实验并与真实硬件对标，**P1 和 P2 为最关键差距**。

---

## 相关文件

- `src/core/queue.rs` — 4-ary heap 事件队列实现
- `src/core/event.rs` — 事件类型与全局 seq
- `src/nic/rdma.rs` — QP状态机、PSN、WQE/CQE、MsgBoundary
- `src/nic/rdma_protocol.rs` — RdmaProtocol（Write 最小端到端可跑；Send/RNR 仍为原型）
- `src/nic/protocol.rs` — Protocol trait(RDMA扩展方法)
- `src/network/switch.rs` — 多优先级队列、PFC 标志、shared-buffer 视图（未完整硬件语义）
- `src/network/host_delay.rs` — NVLink/NVSwitch/PCIe 延迟矩阵（未接入主路径）
- `src/topology/multi_rail.rs` — 多 NIC/Rail 拓扑原型
- `src/training/dag.rs` — 训练 DAG/Overlap 辅助结构（非运行时调度器）
- `src/sim_runner/mod.rs` — SimRunner 主循环 + PacketSlab
- `src/sim_runner/host.rs` — 事件驱动 TxTick + RTO Timeout 调度
- `src/sim_runner/switch.rs` — 交换机事件处理

## 三、实施进度总览

| 优先级 | 模块 | 状态 | 关键文件 | 提交版本 |
|--------|------|------|----------|----------|
| P1A | QP状态机+PSN | 结构完成 | `nic/rdma.rs` | `8bf1a2a` |
| P1B | 消息分段重组+Protocol扩展 | 原型可跑 | `nic/rdma_protocol.rs` | `5f573da` |
| P1C | RDMA Write 端到端 | 最小端到端验证 | `nic/rdma_protocol.rs`, `tests/integration_rdma.rs` | 当前工作区 |
| P1D | RNR流控 | 单元测试级原型 | `nic/rdma_protocol.rs` | `c9cdbe1` |
| P2A | PFC | 结构/计数器级原型 | `network/switch.rs` | `8cd840f` |
| P2B | Shared Buffer | 结构级视图 | `network/switch.rs` | `ec9e785` |
| P3A | 多NIC/Rail | 拓扑原型 | `topology/multi_rail.rs` | `ec9e785` |
| P3B | NVLink/PCIe | 独立延迟模型 | `network/host_delay.rs` | `ec9e785` |
| P4 | Training DAG+Overlap | 辅助结构 | `training/dag.rs` | `ec9e785` |
| P5 | 并行DES | ⏳ 远期 | — | — |
