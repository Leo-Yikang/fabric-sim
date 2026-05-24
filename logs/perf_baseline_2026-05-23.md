# 性能 Baseline 记录：优化前 vs 当前实现

记录时间：2026-05-23 19:25:20 CST

本文件记录后续继续优化时使用的性能 baseline。当前工作区包含尚未提交的优化改动；“优化前”指 `HEAD` 提交 `b8588ed` 中对应实现，“当前实现”指本记录生成时的工作区实现。

## 优化前实现细节

来源：`git show HEAD:<path>` 只读查看。

### 事件队列

- 文件：`src/core/queue.rs`
- 实现：`std::collections::BinaryHeap<Event>`
- 语义：依赖 `Event::Ord` 里的反向比较实现最小堆。
- 复杂度：`push/pop` 均为 `O(log N)`。

### 包暂存区

- 文件：`src/sim_runner/mod.rs`
- 字段：`pub packet_buf: HashMap<u64, Packet>`
- 生命周期：包生成后插入 `HashMap`，事件只携带 `packet_id`，到达下一跳后按 id `remove`。
- packet id：`global_pid: u64` 全局单调递增，host 侧发包/回控制包时重写 packet id。

### 协议与流记录索引

- `protocols: HashMap<EntityId, Box<dyn Protocol>>`
- `fcts: HashMap<u32, FlowFct>`
- `switch_index: HashMap<EntityId, usize>`

### TxTick 调度

- 文件：`src/sim_runner/host.rs`
- `FlowStart` 后触发 `TxTick @ now`。
- `handle_tx_tick` 每次发包后固定调度 `TxTick @ now + tx_tick_ns`。
- 若 `pkts.is_empty()` 且仍有活跃流，则调度 `TxTick @ now + 25_000`。
- ACK/NACK/Data 到 host 后无条件调度 `TxTick @ now`。
- 没有独立 `Timeout` 事件处理；RTO 依赖后续 TxTick 采样。

### 交换机 ingress

- 文件：`src/network/switch.rs`
- `Switch::ingress()` 中通过 `ports_for(...).to_vec()` 复制端口列表，以释放借用后再访问 `self.ports`。
- 每个入包都会产生一次小 `Vec` 分配/复制。

## 当前实现细节

### 事件队列

- 文件：`src/core/queue.rs`
- 实现：自实现 4-ary min-heap，内部直接比较 `time + seq`，不依赖 `Event::Ord`。
- 目标：降低大队列下 heap 高度和 sift 跳转次数。

### 包暂存区

- 文件：`src/sim_runner/mod.rs`
- 实现：`PacketSlab { slots: Vec<Option<Packet>>, free: Vec<u64> }`
- `insert/remove` 通过数组索引定位，避免 HashMap 哈希和桶访问。
- `insert` 会覆盖 `pkt.id = slab_index`；id 可复用，不再全局单调。

### 协议与流记录索引

- `protocols: Vec<Box<dyn Protocol>>`，索引等于 host id。
- `fcts: Vec<FlowFct>`，索引等于 flow id。
- `switch_index: Vec<usize>`，索引等于 switch EntityId。

### TxTick 与 RTO 调度

- `Protocol` 增加：
  - `has_pending_work()`
  - `next_rto_deadline()`
- `handle_tx_tick` 发送后仅在协议仍有待发送工作时调度下一次 TxTick。
- 否则若存在未确认包，则调度独立 `Timeout @ next_rto_deadline`。
- `Timeout` 到期后触发一次 `TxTick` 检查重传。
- RTO 判断已修复为 `now - send_time >= rto_ns`，避免 `Timeout @ deadline` 到期但协议侧不认为超时，导致同一时间戳反复重排。

### 交换机 ingress

- `Switch::ingress()` 用内部 block 计算 `chosen: PortId`，释放 routing 借用后再可变访问端口。
- 去掉每包 `to_vec()`。

## Benchmark

新增基准：

```bash
cargo bench --bench optimization_compare
```

benchmark 文件：

- `benches/optimization_compare.rs`

它在同一个二进制中同时放入旧实现和当前实现：

- old event queue：`BinaryHeap<Event>`
- new event queue：当前 crate 的 `EventQueue`（4-ary heap）
- old packet buffer：`HashMap<u64, Packet>`
- new packet buffer：bench 内复刻的 slab allocator

## Benchmark 结果

运行环境：当前开发机，release/bench profile，Criterion 默认配置。Gnuplot 不存在，使用 plotters backend。

| 热点 | 规模 | 优化前 | 当前实现 | 结论 |
|---|---:|---:|---:|---|
| 事件队列 push+pop | 100k events | BinaryHeap `13.31M elem/s` | 4-ary heap `11.04M elem/s` | 当前实现慢约 17% |
| 事件队列 push+pop | 1M events | BinaryHeap `5.14M elem/s` | 4-ary heap `6.05M elem/s` | 当前实现快约 18% |
| Packet buffer insert+remove | 100k packets | HashMap `63.15M elem/s` | Slab `264.49M elem/s` | 当前实现快约 4.2x |
| Packet buffer insert+remove | 1M packets | HashMap `24.19M elem/s` | Slab `178.55M elem/s` | 当前实现快约 7.4x |

## 端到端 sanity check

命令：

```bash
cargo run --release --example incast_compare
```

结果：

- ECMP baseline：15/15 流完成，`120706` events，墙钟约 `27.95ms`，有效吞吐约 `4.32M events/s`。
- STrack：15/15 流完成，`127959` events，墙钟约 `21.36ms`，有效吞吐约 `5.99M events/s`。

注意：端到端墙钟受系统负载影响明显，后续比较应优先使用 Criterion benchmark；端到端 example 只作为“不会卡住/不会事件活锁”的 sanity check。

## 解读

- `PacketSlab` 是明确有效优化，收益随规模增加更明显。
- 4-ary heap 不是无条件优化：100k 事件规模下慢于标准库 BinaryHeap，1M 事件规模下开始快于 BinaryHeap。后续如果优化事件队列，应保留小队列和大队列两档对比。
- 事件驱动 TxTick/RTO 的收益没有在本 benchmark 中直接量化，因为它需要同时编译旧/新 SimRunner 做端到端对比。若后续需要，可增加一个 `legacy_runner` bench 或用 Git worktree 分别跑同一 workload。

## 验证

```bash
cargo test --release
```

结果：全部通过。

