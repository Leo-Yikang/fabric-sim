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
    PacketDepart { packet_id: u64, dst: EntityId },
    Timeout { timer_id: u64 },
    Stop,
    Custom(String),  // SimRunner 用它编码 FlowStart / TxTick
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

### 5.1 Incast

```rust
pub struct Incast {
    pub senders: Vec<EntityId>,
    pub receiver: EntityId,
    pub bytes_per_sender: u64,
    pub start_time_ns: u64,
}
```

每个 sender 同时向 receiver 发 `bytes_per_sender` 字节，触发拥塞风暴。

### 5.2 Ring AllReduce

N 节点环形，2(N-1) 步，每步每节点发送 M/N 字节给下一节点。

### 5.3 SimSummary

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
    Custom("FlowStart:..") => tx_nic.start_flow + schedule TxTick,
    Custom("TxTick:..")    => handle_tx_tick(host),
    PacketArrive @ switch  => switch.ingress + try_egress,
    PacketArrive @ host    => rx_nic.on_data / tx_nic.on_ack / on_nack,
    PacketDepart           => try_egress 下一个包,
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
| 单元测试 | 21 | core/network/nic/topology 全部模块 |
| 集成测试 | 4 | DES 百万事件 + 3 个端到端 Incast 场景 |
| 文档测试 | 1 | crate-level 用法示例 |
| **合计** | **26** | **全部通过** ✅ |

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

## 10. 未来扩展方向

- [ ] **可视化**：导出事件 trace → web 时序图（D3.js / matplotlib）
- [ ] **更多 baseline**：DCQCN、HPCC、Swift
- [ ] **并行仿真**：拆 actor 模型处理大规模拓扑
- [ ] **真实流量**：CAIDA trace 重放
- [ ] **CSV 导出 + Python 绘图脚本**
- [ ] **失败注入**：链路故障、瞬时拥塞、bit error 模型
