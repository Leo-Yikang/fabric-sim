# Fabric-Sim 设计文档

> 版本：v1.1  
> 维护：kiwios-cn · 2026-05-23  
> 定位：面向 AI/ML 集群传输协议研究的通用 packet-level 离散事件网络模拟器

---

## 1. 项目定位

项目名称仍为 **Fabric-Sim**，但当前代码已经不再是“只模拟 Fabric”的专用原型，而是一个可插拔协议栈的离散事件网络模拟器。

当前已支持：

- DES 事件引擎：稳定 FIFO 事件排序、纳秒级整数时间。
- 网络物理层：单向链路、serialization delay、传播延迟、交换机 FIFO 队列、ECN、丢包。
- 拓扑生成：Leaf-Spine、Fat-Tree、Dumbbell。
- 可插拔协议：`Protocol` trait，内置 `STrackProtocol` 和 `SimpleTcp`。
- 流量生成：Incast、AllToAll、Ring AllReduce、Permutation、Synthetic、Mix。
- 指标采集：FCT、ECN、drop、重传、最大队列、平均链路利用率。
- 可视化：链路利用率时间序列采样和 3D 拓扑数据导出。

因此更准确的工程目标是：

> 提供一个足够透明、可复现、易扩展的 packet-level DES 网络模拟器，用于比较多种传输协议在 AI/ML 集群通信模式下的行为。

它目前仍是研究型模拟器，不是生产级网络仿真平台。和真实大规模模型训练网络相比，最主要的差距在于：训练语义、硬件细节、网络模型精度、规模化性能和校准方法仍然简化。

---

## 2. 当前总体架构

```text
┌─────────────────────────────────────────────────────────────┐
│                    Traffic Generator                         │
│  Incast / AllToAll / RingAllReduce / Synthetic / Mix         │
└──────────────────────────┬──────────────────────────────────┘
                           │ Vec<FlowDesc>
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                         SimRunner                            │
│  FlowStart / TxTick / Timeout / PacketArrive / PacketDepart  │
│  PacketSlab / link_busy_until / sampler / monitor state      │
└──────────────┬──────────────────────┬───────────────────────┘
               │                      │
               ▼                      ▼
┌─────────────────────────┐   ┌───────────────────────────────┐
│     Protocol trait       │   │      Topology + Network        │
│  STrackProtocol/TCP/...  │   │  Link / Switch / RoutingTable  │
└─────────────────────────┘   └───────────────────────────────┘
               │                      │
               └──────────┬───────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                         DES Core                             │
│           Event / EventKind / EventQueue / Simulator         │
└─────────────────────────────────────────────────────────────┘
```

设计原则：

- `core/` 不依赖网络概念，保持可复用。
- `network/` 只关心包、链路、交换机和路由，不关心协议语义。
- `nic/` 通过 `Protocol` trait 插入具体传输协议。
- `sim_runner/` 是集中式事件分发器，负责跨模块状态协调。
- `traffic/` 只生成 `FlowDesc`，不直接操纵模拟器内部状态。
- `monitor/` 和 `viz/` 从仿真状态读取指标，避免影响协议逻辑。

---

## 3. DES 引擎

### 3.1 Event

```rust
pub struct Event {
    pub time: SimTime,
    pub kind: EventKind,
    pub target: EntityId,
    pub seq: u64,
}
```

`seq` 由全局 `AtomicU64` 分配，用于同一时间戳事件的 FIFO 稳定性。时间单位统一为纳秒 `u64`。

### 3.2 EventKind

```rust
pub enum EventKind {
    PacketArrive { packet_id: u64, src: EntityId },
    PacketDepart { packet_id: u64, dst: EntityId, port: u8 },
    Timeout { timer_id: u64 },
    Stop,
    FlowStart { flow_id: u32, src: EntityId, dst: EntityId, bytes: u64 },
    TxTick { host: EntityId },
    Custom(String),
}
```

`FlowStart` 和 `TxTick` 已经是结构化事件，不再依赖 `Custom(String)` 编码。`Custom` 仅用于 DES 单元测试、示例和 benchmark。

### 3.3 EventQueue

当前实现是自定义 **4-ary min-heap**：

- 文件：`src/core/queue.rs`
- 内部存储：`Vec<Event>`
- 比较逻辑：直接比较 `(time, seq)`，不依赖 `Event::Ord`
- 目标：降低大队列下 heap 高度和 sift 跳转次数

注意：性能不是单调优于标准库 `BinaryHeap`。当前 `optimization_compare` benchmark 显示：

| 规模 | BinaryHeap | 4-ary heap | 结论 |
|---:|---:|---:|---|
| 100k events | 13.31M elem/s | 11.04M elem/s | 4-ary 较慢 |
| 1M events | 5.14M elem/s | 6.05M elem/s | 4-ary 较快 |

后续如果继续优化事件队列，必须同时保留小队列和大队列两档 benchmark。

---

## 4. 网络模型

### 4.1 Packet

当前 `Packet` 是协议无关的最小公共结构：

```rust
pub enum PacketKind {
    Data,
    Control(u8),
}

pub struct Packet {
    pub id: PacketId,
    pub kind: PacketKind,
    pub flow_id: FlowId,
    pub seq: SeqNum,
    pub size: u32,
    pub src: EntityId,
    pub dst: EntityId,
    pub ecn: bool,
    pub routing_tag: u8,
    pub payload: Vec<u8>,
    pub depart_time: u64,
}
```

协议特定信息不再直接放在 `Packet` 字段中，而是通过：

- `kind: Control(u8)` 区分 ACK/NACK/自定义控制包。
- `payload: Vec<u8>` 承载 SACK bitmap、base seq 等协议自定义内容。
- `routing_tag` 表示协议建议的路径/端口，`0` 表示交给交换机 ECMP 哈希。

这比早期 `path_hint/sack_base/sack_bits` 直接挂在 Packet 上更适合多协议扩展。

### 4.2 Link

链路是单向的。双向物理链路通过两条反向 `Link` 表示。

序列化延迟使用整数运算：

```rust
(size_bytes as u64 * 8 * 1_000_000_000) / bandwidth_bps
```

这避免浮点误差，但会向下取整。对于极高速、小包场景，后续如果要提高精度，可以考虑 fixed-point remainder 累积。

### 4.3 Switch

交换机模型包含：

- 多出端口。
- 每端口 FIFO `VecDeque<Packet>`。
- 每端口 `queue_bytes` 和 `busy_until`。
- ECN threshold。
- buffer 上限丢包。
- 目的主机到端口列表的路由表。

当前 `Switch::ingress()` 已去掉早期每包 `to_vec()` 的临时分配，通过内部 block 计算 `chosen: PortId` 后再可变访问端口。

### 4.4 链路 serialization

`SimRunner` 用 `link_busy_until: Vec<u64>` 建模链路串行化：

```text
send_start = max(now, link_busy_until[link_id])
send_done  = send_start + serialization_ns(size)
arrive     = send_done + propagation_delay
```

这保证同一条单向链路同一时间只能发送一个包。

---

## 5. 拓扑模型

### 5.1 Leaf-Spine

两层 Leaf-Spine：

- hosts 从 `0` 开始连续编号。
- leaf switch 接在 host id 后。
- spine switch 接在 leaf id 后。
- 每个 leaf 和每个 spine 全互联。
- leaf 到远端 leaf 下 host 有多条等价路径。

适合模拟 AI 集群中常见的二层 CLOS/fabric 简化形态。

### 5.2 Fat-Tree

`k`-ary 三层 Fat-Tree：

- hosts 数量为 `k^3 / 4`。
- 包含 edge、aggregation、core 三层交换机。
- 路由表在生成时完整填充。

当前只支持确定性拓扑生成，不包含 oversubscription profile、故障域、机架/Pod 物理布局等真实部署属性。

### 5.3 Dumbbell

Dumbbell 拓扑用于构造明确瓶颈链路，适合验证：

- 队列堆积。
- ECN 标记。
- 丢包。
- 重传。
- TCP/Fabric 在单瓶颈下的差异。

---

## 6. 协议抽象

### 6.1 Protocol trait

```rust
pub trait Protocol {
    fn start_flow(&mut self, flow_id: FlowId, dst: EntityId, total_bytes: u64, now: u64);
    fn on_tx_tick(&mut self, now: u64) -> Vec<Packet>;
    fn on_rx_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet>;
    fn on_tx_control(&mut self, pkt: &Packet, now: u64);
    fn all_flows_done(&self) -> bool;
    fn take_finished_flows(&mut self) -> Vec<(FlowId, u64)>;
    fn stats(&self) -> ProtocolStats;
    fn has_pending_work(&self) -> bool;
    fn next_rto_deadline(&self) -> Option<u64>;
}
```

`has_pending_work()` 和 `next_rto_deadline()` 是当前性能优化的关键：SimRunner 不再固定轮询所有活跃 host，而是由协议栈告知是否需要继续 TxTick 或等待 RTO。

### 6.2 STrackProtocol

当前 Fabric 实现支持两种模式：

- `Ecmp`：单路径哈希基线，遇 ECN 直接降窗。
- `Strack`：多路径 spraying，遇 ECN 优先黑名单路径，全部路径不可用时再降窗。

主要状态：

- `tx_flows: HashMap<FlowId, FlowTxState>`
- `rx_flows: HashMap<FlowId, FlowRxState>`
- `paths: Vec<PathState>`
- `retransmit_queue`
- `send_times`
- `finished`

简化点：

- cwnd 是包数，不是 byte/window rate。
- ECN 响应是简化 AIMD。
- SACK bitmap 通过控制包 payload 编码。
- 路径质量评估仍然很粗糙。

重要风险：

- 当前 ECN 路径归因仍需继续加强。理想实现应记录 `seq -> path/routing_tag`，ACK 携带或能反查原 data 包路径，否则黑名单路径可能不是实际拥塞路径。

### 6.3 SimpleTcp

`SimpleTcp` 用于验证 `Protocol` trait 的通用性，并提供非 Fabric baseline：

- 单路径。
- 累计 ACK。
- 简化乱序缓存。
- 慢启动。
- 拥塞避免。
- 3 duplicate ACK 快速重传。
- RTO 超时重传。

它不是完整 Linux TCP，也不是 RoCE/DCQCN 的真实替代，只适合作为行为基线。

---

## 7. SimRunner

### 7.1 集中式事件分发

当前仍采用集中式分发，而不是 `Simulator::register_handler`：

```text
FlowStart      -> Protocol::start_flow + TxTick
TxTick         -> Protocol::on_tx_tick + host uplink injection
PacketArrive   -> host receive 或 switch ingress
PacketDepart   -> switch egress retry/dequeue
Timeout        -> TxTick，触发协议检查 RTO
Stop           -> stop
```

这样可以在单个事件处理中同时读写协议、拓扑、链路 busy 状态、packet buffer 和监控状态。

### 7.2 PacketSlab

早期版本使用 `HashMap<u64, Packet>`；当前使用轻量 slab：

```text
PacketSlab {
    slots: Vec<Option<Packet>>,
    free: Vec<u64>,
}
```

优点：

- `insert/remove` 都是数组索引访问。
- 避免 HashMap 哈希和桶访问。
- 内存布局更连续。

代价：

- `Packet.id` 不再全局单调，而是可复用 slab index。
- 调试逐包路径时需要额外 trace id，不能再依赖 packet id 表示生命周期唯一性。

当前 benchmark 显示：

| 规模 | HashMap | Slab | 结论 |
|---:|---:|---:|---|
| 100k packets | 63.15M elem/s | 264.49M elem/s | Slab 快约 4.2x |
| 1M packets | 24.19M elem/s | 178.55M elem/s | Slab 快约 7.4x |

### 7.3 事件驱动 TxTick/RTO

早期版本：

- 发包后固定调度 `TxTick @ now + tx_tick_ns`。
- 空转但活跃时调度 `TxTick @ now + 25us`。
- ACK/NACK 到达后调度 `TxTick @ now`。
- RTO 依赖 TxTick 采样。

当前版本：

- 有待发送工作时才调度下一次 TxTick。
- 无待发送工作但存在未确认包时，调度最早 RTO `Timeout`。
- `Timeout` 到期后触发一次 TxTick，由协议检查重传。
- RTO 边界条件使用 `>=`，避免 `Timeout @ deadline` 到期但协议不认为超时，造成零时间事件循环。

---

## 8. 流量与监控

### 8.1 流量生成

当前 `traffic/` 提供：

| 模式 | 用途 |
|---|---|
| `Incast` | N-to-1 同步突发 |
| `AllToAll` | 全员两两交换 |
| `RingAllReduce` | 简化集合通信 |
| `Permutation` | 无热点排列流量 |
| `Synthetic` | 分布 × 到达过程 × 通信对组合 |
| `Mix` | 多组件混合 workload |

`Synthetic` 是后续推荐扩展入口：

- `FlowSizeDist`: Fixed / Uniform / Pareto / Bimodal
- `ArrivalProcess`: Simultaneous / FixedInterval / Poisson
- `PairPattern`: AllToAll / Permutation / RandomPairs / Custom

### 8.2 指标

`SimSummary` 当前包含：

- 总流数、完成流数。
- 总仿真时间。
- 总发送包数、总重传包数。
- ECN 标记数、丢包数。
- FCT P50/P95/P99/Max。
- 平均链路利用率。
- 最大队列深度。

### 8.3 可视化

`viz/` 支持：

- `TimeSeriesSampler` 周期采样链路利用率和队列深度。
- `VizData/VizNode/VizLink/VizFrame` JSON 导出。
- `scripts/visualize_3d.py` 用 Plotly 渲染 3D 拓扑动画。

---

## 9. 当前测试与性能记录

### 9.1 测试覆盖

当前 `cargo test --release` 覆盖：

| 类型 | 数量 | 说明 |
|---|---:|---|
| 单元测试 | 48 | core/network/nic/topology/traffic |
| 集成/矩阵测试 | 17 | DES、Dumbbell、E2E、workload matrix |
| 文档测试 | 1 | crate-level 示例 |
| 合计 | 66 | 当前全部通过 |

测试必须优先使用 `--release`，因为 DES 事件吞吐在 debug 模式下不足以代表真实运行。

### 9.2 性能 baseline

性能 baseline 已记录在：

- `logs/perf_baseline_2026-05-23.md`
- `benches/optimization_compare.rs`

端到端 sanity check：

```bash
cargo run --release --example incast_compare
```

当前结果示例：

- ECMP baseline：15/15 流完成，约 `120706` events，墙钟约 `27.95ms`。
- Fabric：15/15 流完成，约 `127959` events，墙钟约 `21.36ms`。

端到端墙钟受系统负载影响明显；后续优化应优先使用 Criterion benchmark 比较热路径。

---

## 10. 与大规模模型训练网络相比的主要差距

这一节是后续改进的核心。当前项目适合研究传输协议局部行为，但距离“可信模拟大规模模型训练网络”还有明显差距。

### 10.1 训练作业语义不足

真实训练网络不是独立 flow 集合，而是由训练迭代驱动：

- forward。
- backward。
- gradient all-reduce / reduce-scatter / all-gather。
- optimizer step。
- pipeline bubble。
- tensor/model/data/expert parallel 的组合。

当前模型已经有 `TrainingJob` / `Iteration` / `CollectiveOp`，并能静态展开为
`FlowDesc { src, dst, bytes, start_time }`。P2 后的能力包括：

- iteration 结构。
- collective 顺序计划。
- `compute_delay_ns` 对 iteration 起点的推进。
- Ring / ReduceScatter+AllGather / Tree 的计划展开。
- collective completion time 与 iteration time 指标。

仍然缺少：

- compute 与 communication overlap。
- 运行时 collective operation DAG。
- barrier 和依赖关系。
- 多 job 共存和调度。
- rank placement 对通信矩阵的影响。

改进建议：

1. 增加 `workload/training.rs` 或 `job/` 模块。
2. 定义 `TrainingJob`、`Iteration`、`CollectiveOp`、`TensorShard`。
3. 支持 `AllReduce`、`ReduceScatter`、`AllGather`、`AllToAll`、MoE dispatch/combine。
4. 指标从单流 FCT 扩展到 iteration time、collective completion time、step time、job throughput。

### 10.2 Collective 算法过于简化

当前 `RingAllReduce` 是简化环形模式。真实训练中常见：

- Ring AllReduce。
- Tree / Double Binary Tree。
- Hierarchical AllReduce。
- ReduceScatter + AllGather。
- 多 rail / 多 NIC 分片。
- NCCL channel 并行。
- topology-aware collective。

当前已有：

- `ChunkConfig { chunk_size_bytes, num_channels, pipeline_depth }`。
- 显式 chunking：一个逻辑 edge 可拆成多条 flow。
- channel 并行：同一 wave 中可同时启动多个 chunk。
- pipeline overlap：step 间隔按 pipeline depth 压缩，用于近似流水重叠。

当前缺口：

- 没有 intra-node NVLink/NVSwitch 与 inter-node fabric 的两级通信。
- 没有 rank 到 host/GPU/NIC 的映射。

改进建议：

1. 把 `RingAllReduce` 从“流量模式”提升为 `CollectiveAlgorithm`。
2. 增加 `chunk_size`、`num_channels`、`rail_count`。
3. 明确 `rank -> gpu -> host -> nic -> leaf` 映射。
4. 输出 collective-level 指标，而不仅是 flow FCT。

### 10.3 GPU/主机/NIC 层次缺失

真实节点通常不是“一个 host 一个 NIC 一个协议栈”这么简单。一个训练节点可能包含：

- 多 GPU。
- NVLink/NVSwitch。
- 多 NIC。
- PCIe switch。
- NUMA。
- GPUDirect RDMA。
- 多 QP / 多 traffic class。

当前模型：

- host 是最小通信实体。
- host 只有一个 uplink。
- NIC 操作零延迟。
- 不区分 GPU 内存、CPU 内存、DMA、PCIe。

改进建议：

1. 增加 node 内部拓扑：`GpuId`、`NicId`、`HostId`。
2. 支持 host 多 uplink / 多 rail。
3. 增加 NIC serialization queue、DMA delay、PCIe/NVLink 带宽限制。
4. 支持 intra-node collective 和 inter-node collective 的组合。

### 10.4 RDMA/RoCE 细节不足

当前协议模型已经从“packet + cwnd + ACK/NACK”的简化传输层扩展出 RDMA 原型：
`src/nic/rdma.rs` 提供 QP/PSN/WQE/CQE 等结构，`src/nic/rdma_protocol.rs`
提供 `RdmaProtocol`，并已有 RDMA Write 的最小端到端测试。但它仍然不是成熟
RoCE/RDMA 模拟器。真实 RoCE/RDMA 还涉及：

- QP。
- PSN。
- WQE/CQE。
- message segmentation。
- selective repeat。
- RNR。
- CNP。
- DCQCN rate control。
- PFC/ECN 协同。
- priority / traffic class。
- lossless fabric 的 head-of-line blocking。

当前缺口：

- QP 状态机已有结构，但连接生命周期、错误恢复、PSN 回绕未校准。
- RDMA Write 已能端到端完成；RDMA Send + posted recv 尚未端到端验证。
- RNR NAK/退避有单元测试，但 RNR 后恢复发送未形成端到端闭环。
- DCQCN/CNP/rate-based pacing 有简化实现，但参数和恢复曲线未与真实 RoCEv2 校准。
- PFC/priority/shared-buffer 已有结构或计数器，但不是完整 pause frame 与上游反压模型。

改进建议：

1. 补 RDMA Send + posted recv 端到端测试。
2. 补 RNR NAK → backoff → 恢复发送的端到端或半端到端测试。
3. 将 PFC 从本地 paused 标志推进到 pause/resume frame 与上游反压。
4. 清理 `RdmaProtocol` 的 public/private API 边界，并校准 DCQCN 参数。

### 10.5 交换机与队列模型过于理想化

当前 switch 模型：

- 每端口一个 FIFO。
- 单一 ECN threshold。
- 单一 buffer max。
- 路由查表零延迟。
- 无共享 buffer。
- 无 VOQ。
- 无 priority queue。
- 无 packet scheduling policy。

真实训练网络可能需要模拟：

- shared buffer。
- dynamic threshold。
- ECN marking profile。
- PFC。
- 多优先级。
- WRR/SP/WFQ。
- cut-through vs store-and-forward。
- packet/cell switching。
- ECMP group、flowlet、adaptive routing。
- 链路故障和收敛。

改进建议：

1. 把 `SwitchPort.queue` 抽象为 trait 或 enum：FIFO / Priority / SharedBuffer。
2. 增加 `QueueDiscipline` 和 `BufferModel`。
3. 增加 switch pipeline delay。
4. 增加 adaptive routing policy。

### 10.6 路由与拓扑部署细节不足

当前拓扑是干净的理论拓扑。真实集群还包含：

- oversubscription。
- rail-optimized design。
- multi-plane fabric。
- rack/pod/failure domain。
- host placement。
- asymmetric link speed。
- failed/degraded links。
- ECMP seed 和 hash field。

当前缺口：

- 没有多 rail 拓扑。
- 没有 placement 策略。
- 没有 link failure。
- ECMP hash 简化为 `src ^ dst ^ flow_id`。

改进建议：

1. 增加 topology 配置文件，支持不同 link speed 和 oversubscription。
2. 增加 rank placement 策略：compact/spread/random/topology-aware。
3. 增加 failure injection。
4. 支持 flowlet/adaptive routing。

### 10.7 时间精度与事件规模冲突

packet-level DES 的最大问题是事件数量。

在 100Gbps、1KB MTU 下，一条链路每约 82ns 可发送一个包。若模拟 1K-10K 节点、AllToAll 或 MoE all-to-all，事件数会快速爆炸。

当前已做优化：

- 事件驱动 TxTick/RTO。
- PacketSlab。
- 4-ary heap。
- 去掉 switch ingress 临时分配。

但结构性限制仍在：

- 单线程全局事件队列。
- 每包至少多个事件。
- 每包进入 packet buffer。
- 每 ACK/NACK 也作为包模拟。

改进方向：

1. 增加 flow-level 或 hybrid 模式：大 elephant flow 用 fluid/analytic model，小 flow 用 packet-level。
2. 增加 packet coalescing：多个同质 packet 合并成 batch event。
3. 增加 per-link calendar queue / timing wheel，减少全局 heap 压力。
4. 增加 parallel DES：按 topology partition 为 logical process。
5. 增加 deterministic fast path：对无拥塞链路直接计算 arrival，不逐包入队。

### 10.8 校准与验证不足

当前验证主要是内部一致性测试：

- 是否完成。
- 是否触发 ECN/drop。
- FCT 是否非零。
- DES 排序是否正确。

但要可信模拟真实训练网络，需要外部校准：

- 与真实集群 telemetry 对比。
- 与 ns-3 / htsim / OMNeT++ / Astra-sim 类工具对比。
- 与已知论文场景复现。
- 对不同 MTU、RTT、带宽、buffer、ECN threshold 做 sensitivity analysis。

改进建议：

1. 增加 `experiments/` 目录保存固定场景配置和结果。
2. 增加 CSV/JSON trace 导出。
3. 增加 notebook 或 Python 脚本做图。
4. 增加基准场景：single bottleneck、incast、all-to-all、ring allreduce、reduce-scatter/allgather。

### 10.9 可配置性不足

当前很多参数仍硬编码在 examples/tests 中：

- 拓扑规模。
- link bandwidth。
- ECN threshold。
- buffer。
- flow size。
- protocol mode。
- simulation end time。

改进建议：

1. 接入 `clap`。
2. 增加 TOML/JSON scenario 配置。
3. 支持命令行选择 topology/protocol/workload/output。
4. 把结果统一输出到 `output/`，日志输出到 `logs/`。

### 10.10 可观测性不足

当前 summary 适合快速比较，但不足以定位大规模性能/协议问题。

需要增加：

- per-flow timeline。
- per-link utilization timeseries。
- per-queue occupancy timeseries。
- packet drop reason。
- retransmission cause。
- ECN mark location。
- control packet statistics。
- event type histogram。
- simulator wall-clock profiling。

尤其是事件驱动优化后，必须能快速发现：

- 同一时间戳事件风暴。
- RTO 反复调度。
- 某 host/pair 产生异常多事件。
- 某端口队列长期不出队。

---

## 11. 改进路线图

### P0：保持当前 correctness baseline

- [x] `cargo test --release` 全部通过。
- [x] RTO exact-deadline 回归测试。
- [x] `incast_compare` 不再卡住。
- [x] `logs/perf_baseline_2026-05-23.md` 记录性能 baseline。

### P1：协议与模拟器可观测性

- [x] 增加 event type histogram。
- [x] 增加 `run_with_progress()` 或 profiling mode。
- [x] 输出每类事件数量、最大 pending queue 长度、同时间戳连续事件数量。
- [x] packet trace 使用独立 `trace_id`，不要依赖 slab `packet.id`。

### P2：训练 workload 抽象

- [x] 增加 `TrainingJob` / `CollectiveOp`。
- [x] 支持 reduce-scatter + all-gather。
- [x] 支持 chunk/channel/pipeline。
- [x] 输出 iteration time / collective completion time。

### P3：更真实的协议 baseline

- [x] 完整 DCQCN baseline。
- [x] HPCC/Swift 类协议占位或简化实现。
- [~] PFC/CNP/priority queue：已有简化原型，硬件级语义未成熟。
- [~] 多 QP / 多 NIC：QP 结构和 multi-rail 拓扑原型已存在，尚未完整接入主仿真语义。

### P4：规模化性能

- [ ] 继续评估 4-ary heap；如小队列退化明显，考虑混合策略或回退。
- [ ] 减少 `Vec<Packet>` 返回分配：smallvec、callback sink 或 packet builder。
- [ ] `next_rto_deadline()` 维护 per-protocol min-heap，避免每次遍历所有 send_times。
- [ ] 批量 packet/event。
- [ ] flow-level/hybrid 模式。

### P5：配置、实验和校准

- [ ] CLI + scenario 配置文件。
- [ ] 固定 experiments 目录。
- [ ] 输出 CSV/JSON trace。
- [ ] 与外部工具或真实 telemetry 做校准。

### P6：并行 DES

- [ ] 拓扑 partition。
- [ ] Logical Process 抽象。
- [ ] 保守同步或 lookahead。
- [ ] 明确跨 partition link event 语义。

并行 DES 是大工程，不建议在训练 workload、可观测性和校准之前启动。

---

## 12. 当前已知技术债

1. `Packet.id` 现在是 slab index，可复用；如果要做逐包 trace，需要新增稳定 `trace_id`。
2. `summarize()` 仍用 `start_ns > 0` 过滤 flow，0ns 起始流会被忽略；应改成 `Option<FlowFct>` 或显式有效标志。
3. `STrackProtocol` 的 ECN 路径归因需要用 `seq -> path` 或 ACK payload 明确绑定。
4. `Protocol::on_tx_tick()` 和 `on_rx_data()` 返回 `Vec<Packet>`，高频小包/控制包场景会产生分配压力。
5. `next_rto_deadline()` 每次遍历所有 `send_times`，大规模流数下会成为热点。
6. `Protocol` 仍是 `Box<dyn Protocol>`，保留了 vtable 开销；后续可评估 `SimRunner<P>` 泛型化。
7. `FlowFct` 当前只适合独立 flow，不适合 coflow/collective/job-level 指标。
8. 当前拓扑缺少多 rail、多 NIC、oversubscription、failure domain。

---

## 13. 参考文件

- `src/core/queue.rs`：4-ary event queue。
- `src/core/event.rs`：Event / EventKind。
- `src/sim_runner/mod.rs`：SimRunner、PacketSlab、主分发。
- `src/sim_runner/host.rs`：TxTick/RTO、host 收发。
- `src/sim_runner/switch.rs`：switch ingress/egress。
- `src/network/switch.rs`：FIFO queue、ECN/drop、routing。
- `src/nic/protocol.rs`：可插拔协议接口。
- `src/nic/strack.rs`：Fabric/Ecmp 实现。
- `src/nic/tcp.rs`：SimpleTcp 实现。
- `src/traffic/synthetic.rs`：推荐的通用 workload 入口。
- `src/viz/`：可视化数据采样。
- `docs/limit.md`：大规模性能瓶颈分析。
- `logs/perf_baseline_2026-05-23.md`：当前性能 baseline。
