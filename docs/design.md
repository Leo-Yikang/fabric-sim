# STrack-Sim 设计文档

> 版本：v1.0（**四阶段全部实现 ✅**）
> 维护：kiwios-cn · 2026-05-14

---

## 1. 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                  Traffic Generator (阶段四 ✅)              │
│        AllReduce │ AllToAll │ Incast                        │
└──────────────────────────┬──────────────────────────────────┘
                           │ FlowDesc
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              SimRunner (端到端仿真主循环)                    │
│   FlowStart → TxTick → Packet 流转 → ACK/NACK → CC → ...    │
└──────────────────────────┬──────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │ NIC + STrack │ │  Topology    │ │  Monitor     │
   │ (阶段三 ✅)  │ │  (阶段二 ✅) │ │ (阶段四 ✅)  │
   └──────────────┘ └──────────────┘ └──────────────┘
            │              │              │
            └──────────────┼──────────────┘
                           ▼
                ┌────────────────────────┐
                │ DES Engine (阶段一 ✅) │
                │ Simulator + EventQueue │
                └────────────────────────┘
```

---

## 2. 阶段一：离散事件引擎

### 2.1 数据结构

```rust
pub struct Event {
    pub time: SimTime,        // 触发时刻（ns）
    pub kind: EventKind,      // 事件类型
    pub target: EntityId,     // 目标实体
    pub seq: u64,             // 全局序号（FIFO 稳定性）
}

pub enum EventKind {
    PacketArrive { packet_id: u64, src: EntityId },
    PacketDepart { packet_id: u64, dst: EntityId, port: u8 },
    Timeout { timer_id: u64 },
    Stop,
    FlowStart { flow_id: u32, src: EntityId, dst: EntityId, bytes: u64 },
    TxTick { host: EntityId },
    Custom(String),  // 仅用于测试/示例
}
```

### 2.2 关键技巧

**BinaryHeap 反向 Ord** 实现最小堆：

```rust
impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time).then_with(|| other.seq.cmp(&self.seq))
    }
}
```

### 2.3 性能基线（Apple Silicon）

| 场景 | 吞吐 |
|------|------|
| 链式 10 万事件 | ~23 M ev/s |
| 100 万乱序事件 | ~13 M ev/s |

---

## 3. 阶段二：网络拓扑与物理层

### 3.1 Packet

```rust
pub struct Packet {
    pub id: PacketId,       // 全局唯一（SimRunner 分配）
    pub kind: PacketKind,    // Data | Ack | Nack
    pub flow_id: FlowId,
    pub seq: SeqNum,
    pub size: u32,
    pub src: EntityId,
    pub dst: EntityId,
    pub ecn: bool,
    pub path_hint: u8,       // STrack: 期望走的端口（1-indexed；0 表示不指定）
    pub sack_base: SeqNum,
    pub sack_bits: u64,
    pub depart_time: u64,
}
```

### 3.2 Link

```rust
pub fn serialization_ns(&self, size_bytes: u32) -> u64 {
    (size_bytes as u64 * 8 * 1_000_000_000) / self.bandwidth_bps
}
pub fn arrive_time(&self, now_ns: u64, size_bytes: u32) -> u64 {
    now_ns + self.serialization_ns(size_bytes) + self.prop_delay_ns
}
```

### 3.3 Switch

```rust
pub fn ingress(&mut self, pkt: Packet, hash_key: u32) -> (Option<PortId>, bool) {
    // 1. 查路由表
    let ports = self.routing.ports_for(pkt.dst)?;
    // 2. STrack: path_hint 优先；否则 ECMP 哈希
    let chosen = if pkt.path_hint > 0 { ports[pkt.path_hint - 1] }
                 else { ports[hash_key % ports.len()] };
    // 3. 检查 buffer 是否满 → 丢包
    if port.queue_bytes + pkt.size > self.buffer_max_bytes { drop; }
    // 4. ECN 标记
    if port.queue_bytes + pkt.size > self.ecn_threshold_bytes { pkt.ecn = true; }
    // 5. 入队
    port.queue.push_back(pkt);
}
```

### 3.4 拓扑

**LeafSpine**：n_leaf × n_spine 全互联，每 leaf 下挂 hosts_per_leaf 个主机。

**FatTree**：k-ary 三层，n_hosts = k³/4。详见 `src/topology/fat_tree.rs`。

---

## 4. 阶段三：NIC + STrack 协议栈

### 4.1 TxNic 状态机

```
[Idle] --start_flow--> [Sending] --try_send--> 包发出
   ▲                     │ ▲          │
   │                     │ │ ACK      │ ECN/RTO
   │                     │ └──cwnd++  │
   │                     ▼            ▼
   └──[all_acked]──── [WaitAck]   [Retx]
```

### 4.2 拥塞控制（核心创新）

```rust
fn on_ack(&mut self, ack: &Packet, now: u64) {
    // 累计 ACK，更新 in_flight，移除 send_times
    flow.un_acked_base = ack.seq;
    flow.in_flight -= delta;
    if ack.ecn {
        match self.mode {
            Strack => {
                // 先切路：把当前路径黑名单 50us
                self.paths[path].blacklisted_until = now + 50_000;
                let avail = self.paths.iter().filter(|p| p.is_available(now)).count();
                if avail == 0 { flow.cwnd /= 2; }  // 全线拥塞才降窗
            }
            Ecmp => {
                flow.cwnd /= 2;  // 单路径直接降窗
            }
        }
    } else {
        flow.cwnd += 1;  // AIMD 加性增加
    }
}
```

### 4.3 RTO 超时重传

每次 `try_send` 时检查：

```rust
for (seq, send_t) in &flow.send_times {
    if now - send_t > self.rto_ns {  // RTO = 100us
        retransmit_queue.push(*seq);
    }
}
```

### 4.4 RxNic 与 SACK

```rust
// 64-bit bitmap 表示 [next_expected, next_expected+64) 的接收状态
if seq == next_expected {
    next_expected += 1;
    // 向右移 bitmap，吃掉连续已收
    while bits & 1 == 1 { next_expected++; bits >>= 1; }
} else if seq > next_expected {
    let offset = seq - next_expected;
    if offset < 64 { bits |= 1 << offset; }
    // 检测到 gap → 发 NACK
}
```

---

## 5. 阶段四：流量生成与指标

### 5.1 基础流量模式

| 模式 | 特点 | 适用场景 |
|------|------|---------|
| `Incast` | N→1 同步突发 | 测试拥塞控制、Buffer 压力 |
| `AllToAll` | 全员两两交换 | 测试全网负载均衡 |
| `RingAllReduce` | 环形传递 | 测试 AI 集合通信 |
| `Permutation` | 无热点排列 | 测试无偏路由 |
| `Synthetic` | 三维可组合（分布×到达×通信对） | 系统化参数扫描 |
| `Mix` | 多组件按比例混合 | 模拟真实混合工作负载 |

### 5.2 Synthetic 通用合成流量

支持以下维度自由组合：

**流大小分布 `FlowSizeDist`**：
- `Fixed(u64)` — 固定大小
- `Uniform { min, max }` — 均匀分布
- `Pareto { min, shape }` — 重尾分布（数据中心典型）
- `Bimodal { small, large, large_ratio }` — 双模态 mice/elephant

**到达过程 `ArrivalProcess`**：
- `Simultaneous(t)` — 同时开始
- `FixedInterval { start, interval_ns }` — 固定间隔
- `Poisson { start, mean_interval_ns }` — 泊松到达

**通信对 `PairPattern`**：
- `AllToAll` / `Permutation` / `RandomPairs(n)` / `Custom`

### 5.3 Incast（遗留，仍可用）

```rust
pub struct Incast {
    pub senders: Vec<EntityId>,
    pub receiver: EntityId,
    pub bytes_per_sender: u64,
    pub start_time_ns: u64,
}
```

### 5.4 Ring AllReduce

N 节点环形，2(N-1) 步，每步每节点发送 M/N 字节给下一节点。

### 5.5 SimSummary

```rust
pub struct SimSummary {
    pub mode: String,
    pub total_flows: u64,
    pub completed_flows: u64,
    pub total_time_ns: u64,
    pub total_packets_sent: u64,
    pub total_packets_retransmitted: u64,
    pub total_ecn_marks: u64,
    pub total_drops: u64,
    pub fct_p50_ns: u64,
    pub fct_p95_ns: u64,
    pub fct_p99_ns: u64,
    pub fct_max_ns: u64,
    pub avg_link_util: f64,
    pub max_queue_depth_bytes: u32,
}
```

---

## 6. 端到端：SimRunner 主循环

### 6.1 事件分发器

```rust
match ev.kind {
    FlowStart { flow_id, src, dst, bytes } => proto.start_flow + schedule TxTick,
    TxTick { host }   => handle_tx_tick(host),
    PacketArrive @ switch => switch.ingress + try_egress,
    PacketArrive @ host   => rx_nic.on_data / tx_nic.on_ack / on_nack,
    PacketDepart      => try_egress 下一个包,
}
```

### 6.2 全局 PID 生成器（重要 fix）

不同 TxNic 各自从 1 开始的 packet_id 在全局 `packet_buf` 中会冲突。`SimRunner` 维护 `global_pid` 字段，在 `handle_tx_tick` 中重写所有出包的 id。

### 6.3 持续 TxTick

当 cwnd 满时 ACK 才触发下一次 TxTick，但丢包后永远无 ACK；因此未完成流定期 tick 25us 检查 RTO。

---

## 7. 实测结果

**实验**：4 Leaf × 8 Spine × 4 host/leaf，15 sender × 512 KB → host 0

| 指标 | ECMP | STrack | Δ |
|------|------|--------|---|
| 完成流数 | 15/15 | 15/15 | — |
| 仿真总时长 | 874 us | **762 us** | **-12.8%** |
| FCT P50 | 816.6 us | **704.7 us** | **-13.7%** |
| FCT P99 | 848.3 us | **735.9 us** | **-13.3%** |
| 重传包数 | 72 | 972 | +1250% |
| ECN 标记 | 4182 | 5134 | +23% |
| 丢包数 | 43 | 574 | +1234% |

**结论**：
- ✅ STrack FCT 改善 **~13%**，验证多路径价值
- ⚠️ STrack 重传/丢包显著增加，因为 Packet Spraying 把负载分散到多个 spine 后单个 spine 的瞬时拥塞反而更激烈
- 这是已知 trade-off：要进一步优化 CC（如 EWMA-based path quality scoring）才能压低重传率

---

## 8. 测试覆盖

| 测试类型 | 数量 | 覆盖 |
|---------|------|------|
| 单元测试 | 45 | core/network/nic/topology/traffic 全部模块 |
| 集成测试 | 13 | DES 百万事件 + 端到端 Incast/Dumbell + 矩阵工作负载 |
| 文档测试 | 1 | crate-level 用法示例 |
| **合计** | **59** | **全部通过** ✅ |

---

## 9. 已知简化（与真实硬件的 gap）

1. **不模拟 PCIe / DMA 开销**：所有 NIC 操作零延迟
2. **不模拟交换机查表延迟**：路由瞬时完成
3. **链路误码率默认为 0**：bit error 通过显式注入测试
4. **不模拟 PFC 暂停帧**：focus 在 STrack 自身的拥塞响应
5. **MTU 固定**：默认 1 KB
6. **TxTick 粒度**：200 ns，影响小流的精度
7. **CC 简化**：未实现 EWMA、HPCC 等高级算法

这些假设与 htsim、Astra-Sim 等主流学术模拟器一致。

---

## 10. 待办事项

### 🔴 高优先级

- [ ] **CLI + 场景配置文件**：`clap` 依赖已加入但未使用；所有参数硬编码在源码中，需要 JSON/TOML 场景文件 + 命令行入口，让模拟器作为独立工具运行
- [ ] **Dumbell / AllToAll / RingAllReduce 无端到端示例**：三个模块已实现但无 example 或集成测试调用，需要各写一个端到端 example（`examples/dumbell_demo.rs`、`allreduce_demo.rs`、`alltoall_demo.rs`）
- [x] **错误处理：替换裸 `unwrap()`** ✅：已实现轻量判错系统（`src/error.rs`）。`SimRunner::new()` 返回 `SimResult`；Protocol 内部不变量使用 `expect()`；已守卫的 unwrap 改为模式匹配。生产代码中 12 处裸 unwrap 已全部消除。

### 🟡 中优先级

- [ ] **更多 CC baseline**：实现完整 DCQCN（RTT-based rate control）、HPCC（INT-based）、Swift。当前 `Ecmp` 模式只是简化版 DCQCN（直接降窗，无 rate-based）
- [ ] **故障注入**：`LinkFault` / `PacketCorruption` 事件类型，支持链路故障、瞬时拥塞、bit error 注入
- [ ] **逐包 Trace 导出**：逐包事件时间线（send/arrive/ECN/drop/retx）导出为 JSON/CSV，配合 Python/Matplotlib 绘图脚本做可视化分析
- [ ] **更细粒度监控**：逐流 FCT 打印、逐链路利用率时间序列、buffer 队列深度时间序列
- [ ] **Dumbell 拓扑的端到端 example**：演示瓶颈链路拥塞场景下 ECMP vs STrack 对比

### 🟢 低优先级

- [ ] **网络层 Benchmark**：拓扑构建、CC 决策、包转发路径的 criterion bench（当前只有一个 DES 引擎 bench）
- [ ] **并行仿真**：拆 actor 模型处理大规模拓扑（k=16+ FatTree，2048+ 主机）
- [ ] **真实流量重放**：CAIDA trace 或其他真实数据中心 trace 重放
