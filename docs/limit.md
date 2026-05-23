# STrack-Sim 大规模集群性能瓶颈分析

> **文档版本**：v0.1.0-20260524  
> **对应代码版本**：strack-sim 0.1.0（当前分支）  
> **分析范围**：面向 1K~10K 节点规模 AI/ML 集群的离散事件仿真性能瓶颈

---

## 概述

本文档从 **引擎层、内存层、事件密度、模型层** 四个维度，分析当前单线程 DES 架构在面向大规模通信集群时的结构性性能瓶颈。这些瓶颈不属于代码缺陷，而是架构设计在规模扩展时的天然约束。

**单线程优化实施状态**：P1/P2/P3/P5 已完成并验证通过，P4 待评估。

---

## 1. 事件队列：O(log N) 的堆操作是头号瓶颈

**现状**：`EventQueue` 基于 `std::collections::BinaryHeap`，`push/pop` 均为 **O(log N)**，且内存布局呈树状，缓存局部性差。

**大规模下的问题**：

- 一个 1024 节点的 Leaf-Spine 拓扑，AllToAll 流量下，每秒仿真时间可能产生 **数千万到上亿事件**（TxTick + PacketArrive + PacketDepart + 定时器重传）。
- BinaryHeap 的 sift-up/sift-down 涉及随机内存访问，CPU cache miss 比例随队列长度增加而恶化。
- 全局 `EVENT_SEQ: AtomicU64` 在超高频事件创建时，虽然用了 `Relaxed`，但在多核缓存一致性协议下仍有一定开销。

### 已实施方案：4-ary heap（四叉堆）

将 `BinaryHeap` 替换为**自实现 4-ary heap**（`src/core/queue.rs`）。

- **原理**：4-ary heap 的树高约为同规模二叉堆的一半（log₄ N vs log₂ N），sift-up/sift-down 的内存跳转次数更少。
- **关键细节**：`Event` 的 `Ord` 实现是为 `BinaryHeap`（最大堆）**反向**设计的，因此 4-ary heap 内部直接比较 `time` 和 `seq`，不依赖 `Ord` trait。
- **复杂度**：push/pop 仍为 O(log N)，但常数因子降低约 30~50%（更少的 swap 次数 + 更浅的树）。
- **风险与回退**：实现简单，仅 60 行代码；如果后续发现正确性问题，可直接替换回 `BinaryHeap`（接口完全兼容）。

---

## 2. 包生命周期：HashMap 是第二个热点

**现状**：`packet_buf: HashMap<u64, Packet>` 作为包的"全局暂存区"，每个包从生成到销毁经历两次 HashMap 操作（`insert` + `remove`）。

**大规模下的问题**：

- `HashMap` 的 insert/remove 涉及哈希计算、桶定位、可能的重新哈希/内存分配、节点插入/移除。
- 在 100Gbps 链路、MTU=1KB 的设定下，一条链路每 80ns 就能发一个包。一个 48 端口交换机满负载时，事件处理频率极高，`packet_buf` 成为热点。

### 已实施方案：自实现 PacketSlab allocator

引入 `PacketSlab` 结构（`src/sim_runner/mod.rs`），用 `Vec<Option<Packet>>` + 空闲列表替代 `HashMap`。

- **原理**：Slab allocator 通过数组索引直接定位，insert/remove 均为 O(1)，且无哈希开销。`remove` 后索引放入 `free` 列表供后续复用。
- **关键改动**：
  - `packet_buf_insert(pkt: Packet) -> u64`：插入包并返回 slab 索引，**同时覆盖 `pkt.id`** 为 slab 索引，确保 Event 携带的 `packet_id` 与 slab 位置一一对应。
  - `packet_buf_remove(pid: u64) -> Option<Packet>`：按索引直接取出。
  - 交换机出队转发时，包重新插入 Slab 获得**新索引**（原位置已被 `remove` 释放），Event 使用新索引。
- **收益**：消除了每次包传输的两次哈希操作，内存布局更连续。
- **副作用**：去掉了 `global_pid` 全局递增计数器，包的 `id` 不再是全局单调递增（而是 slab 索引，可复用）。不影响仿真逻辑，只影响调试追踪。

---

## 3. 事件密度爆炸：TxTick 的"轮询"代价

**现状**：每个主机每 **200ns** 固定触发一个 `TxTick`。当 `cwnd` 满或没有活跃流时，会退化为 **25us** 的轮询。

**大规模下的问题**：

- 假设 10,000 节点集群，全部活跃 → 每 200ns 产生 **10,000 个 TxTick 事件**，即 **50M events/ms**。
- 即使 release 模式达到 25M events/sec，光 TxTick 就占用了 2000 倍的处理能力——这根本跑不动。
- 实际上大部分 TxTick 是"空转"（`pkts.is_empty()`），但模拟器仍需调度 → 弹出 → 分发 → 判断。

**根本矛盾**：精细的时间粒度（200ns tick）与大规模节点数之间存在 **N × granularity** 的乘积效应。

### 已实施方案：TxTick 完全事件驱动 + RTO Timeout 独立定时

**核心思路**：让协议栈决定"我什么时候需要下一次 tick"，模拟器不再主动空转轮询。

#### 3.1 Protocol trait 扩展

新增两个方法（`src/nic/protocol.rs`）：

```rust
/// 当前是否有待发送的工作（cwnd 有空间可发新数据，或重传队列非空）
fn has_pending_work(&self) -> bool;

/// 返回最早的未确认包的 RTO 截止时间（send_time + rto_ns）
fn next_rto_deadline(&self) -> Option<u64>;
```

- `has_pending_work`：STrack 实现中遍历活跃流，检查 `(in_flight < cwnd && next_seq < total_packets)` 或 `retransmit_queue` 非空。
- `next_rto_deadline`：遍历所有 `send_times`，找最小 `send_t + rto_ns`。

#### 3.2 调度逻辑重构（`src/sim_runner/host.rs`）

- **`FlowStart`**：调度 TxTick @ now（初始触发）。
- **`handle_tx_tick` 发送完包后**：
  - 若 `has_pending_work()` → 调度 TxTick @ now + `tx_tick_ns`（保留 pacing 间隔）。
  - 否则若 `next_rto_deadline()` 返回 Some(t) → 调度 `Timeout { timer_id: 0 }` @ t（独立 RTO 定时器）。
  - 否则完全静默，等外部事件驱动。
- **`handle_arrive_at_host`（ACK/NACK 到达后）**：
  - 若 `has_pending_work()` → 调度 TxTick @ now（立即响应，无延迟）。
  - 否则按 RTO 定时或静默。
- **`dispatch` 新增 `Timeout` 处理**：RTO Timeout 到期 → 触发 TxTick @ now，让协议栈检查超时重传。

#### 3.3 收益与验证

- **彻底消除空转**：cwnd 满时不再有 25us 间隔的轮询事件。
- **RTO 检查不受频率限制**：由独立 Timeout 事件精确在 `send_time + rto_ns` 触发，不再依赖 TxTick 的采样精度。
- **59 个测试全部通过**，包括 incast、alltoall、permutation、poisson 到达等多种流量模式。

---

## 4. 交换机模型：内存分散与重复分配

**现状**：`SwitchPort` 每个端口有一个 `VecDeque<Packet>`，`Switch::ingress` 中路由查表返回 `Vec<PortId>`。

**大规模下的问题**：

- `VecDeque` 的 ring buffer 在频繁入队/出队时表现不错，但每个端口独立分配内存，缓存局部性差。
- `ingress()` 中 `ports.to_vec()` 每次都会**分配一个临时 Vec**（即使只有 2~4 个端口），这是完全不必要的堆分配。

### 已实施方案：去掉 `to_vec()`，用内部 block 释放借用

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

---

## 5. 协议栈：动态分发的间接开销

**现状**：`protocols: Vec<Box<dyn Protocol>>`，每个 host 一个协议实例。

**问题**：

- 每次访问协议栈需要 **两次间接跳转**：Vec 索引 → Box 解引用 → vtable 查找。
- `Protocol` trait 的 `on_tx_tick()` 返回 `Vec<Packet>`，即使只发一个 ACK 也要做一次 Vec 分配。

### 未实施方案：泛型化 `SimRunner<P: Protocol>`

- **方案**：将 `Box<dyn Protocol>` 替换为泛型参数 `P: Protocol`，编译器可在热路径上内联 `on_tx_tick`/`on_ack`。
- **阻碍**：`SimRunner` 在 **15+ 处** examples/tests 中使用（`incast_compare`、`workload_sweep`、`matrix_workloads` 等），全部需要显式标注类型（如 `SimRunner<STrackProtocol>`）。改动面广，但逻辑简单。
- **评估**：vtable 开销在已完成的 P1/P2/P3 优化后占比降低，当前投入产出比偏低。建议作为**后续可选项**，在需要榨干最后 5~10% 性能时实施。

---

## 6. 单线程天花板：无法横向扩展

**这是最根本的瓶颈。**

DES 的因果一致性要求事件按时间顺序处理，天然串行。当前架构完全没有并行化：

- 没有 **LP（Logical Process）** 分区（如 ROSS、OMNeT++ 的并行 DES）。
- 没有 **乐观时间推进**（Time Warp）。
- 没有 **保守同步**（Chandy-Misra-Bryant，空消息算法）。

对于 10K+ 节点的 AI 集群 AllReduce 模拟，单线程 DES 的吞吐量上限决定了：

- 要么缩小拓扑规模
- 要么降低时间精度
- 要么忍受数小时的仿真时间

---

## 瓶颈优先级与实施状态

| 优先级 | 瓶颈 | 实施状态 | 改造难度 | 关键文件 |
|--------|------|----------|----------|----------|
| P0 | **单线程 DES 架构** | 未开始 | 高 | — |
| P1 | **TxTick 事件密度** | ✅ 已完成 | 中 | `protocol.rs`, `host.rs`, `mod.rs` |
| P2 | **事件队列堆操作** | ✅ 已完成（4-ary heap）| 中 | `core/queue.rs` |
| P3 | **packet_buf HashMap** | ✅ 已完成（Slab）| 中 | `mod.rs`, `host.rs`, `switch.rs` |
| P4 | `Box<dyn Protocol>` vtable | ⏳ 待评估 | 低 | `mod.rs`, `lib.rs`, `examples/*`, `tests/*` |
| P5 | `ingress()` 中的 `to_vec()` | ✅ 已完成 | 低 | `network/switch.rs` |

---

## 优化建议（更新）

- **已完成的 Phase 1**（P1+P2+P3+P5）：
  - 消除了 TxTick 空转轮询
  - 事件队列从二叉堆升级为四叉堆
  - 包暂存从 HashMap 换为 Slab allocator
  - 交换机入包去掉临时 Vec 分配
  - **下一步**：运行 `cargo bench` 和 `incast_compare` example，量化实际吞吐提升。

- **Phase 2（可选）**：P4 泛型化。如果 benchmark 显示 vtable 仍是热点，再实施。

- **Phase 3（远期）**：保守并行 DES（LP 分区）。当单线程优化触及天花板、且必须模拟 10K+ 节点时启动。

---

## 相关文件

- `src/core/queue.rs` — 4-ary heap 事件队列实现
- `src/core/event.rs` — 事件类型与全局 seq
- `src/sim_runner/mod.rs` — SimRunner 主循环 + PacketSlab
- `src/sim_runner/host.rs` — 事件驱动 TxTick + RTO Timeout 调度
- `src/sim_runner/switch.rs` — 交换机事件处理（Slab 索引转发）
- `src/network/switch.rs` — Switch::ingress 无分配优化
- `src/nic/protocol.rs` — Protocol trait（新增 `has_pending_work` / `next_rto_deadline`）
- `src/nic/strack.rs` / `src/nic/tcp.rs` — 两个方法的具体实现
