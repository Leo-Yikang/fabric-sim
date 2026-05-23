# STrack-Sim 协议学习指南

> 本文档面向希望理解 STrack-Sim 中六种传输协议（TCP、ECMP、STrack、DCQCN、HPCC、Swift）核心原理的读者。
> 撰写风格力求像教材一样系统、清晰，必要时给出伪代码和数学公式。

---

## 目录

1. [概述：数据中心传输协议的演进](#1-概述数据中心传输协议的演进)
2. [TCP（SimpleTcp）：经典拥塞控制的基石](#2-tcpsimpletcp经典拥塞控制的基石)
3. [ECMP：网络层的负载均衡](#3-ecmp网络层的负载均衡)
4. [STrack：多路径 RDMA 与选择性重传](#4-strack多路径-rdma-与选择性重传)
5. [DCQCN：基于速率的量化拥塞控制](#5-dcqcn基于速率的量化拥塞控制)
6. [HPCC：高精度拥塞控制](#6-hpcc高精度拥塞控制)
7. [Swift：基于 RTT 的轻量传输](#7-swift基于-rtt-的轻量传输)
8. [六种协议对比总结](#8-六种协议对比总结)
9. [在 STrack-Sim 中使用这些协议](#9-在-strack-sim-中使用这些协议)

---

## 1. 概述：数据中心传输协议的演进

### 1.1 为什么数据中心需要专门的传输协议？

传统互联网（Internet）上的 TCP 协议设计目标与数据中心网络（DCN）有很大不同：

| 维度 | 互联网 | 数据中心 |
|------|--------|----------|
| RTT | 几十到几百毫秒 | 1~10 微秒 |
| 带宽 | Mbps ~ Gbps | 10~400 Gbps |
| 拓扑 | 不规则、多跳、高动态 | 规则 Clos/Fat-Tree、低动态 |
| 流量模式 | 长流、网页、视频 | 短流、RPC、AllReduce |
| 丢包原因 | 主要是拥塞 | 拥塞 + PFC 风暴 + 交换机 buffer 不足 |
| 需求 | 公平性、鲁棒性 | 低延迟、高吞吐、无丢包（lossless） |

在数据中心中，一个 100KB 的 RPC 请求如果在 10Gbps 链路上因为 TCP 慢启动需要 10 个 RTT 才能达到满速，而每个 RTT = 10μs，总耗时 100μs——这对延迟敏感的应用来说是不可接受的。

因此，过去十年学术界和工业界提出了一系列专门面向数据中心的传输协议。

### 1.2 STrack-Sim 中的六种协议

本项目实现了六种具有代表性的协议，覆盖了从传统到前沿的多种设计思路：

```
┌─────────────────────────────────────────────────────────────┐
│  协议              类型          核心机制           年代      │
├─────────────────────────────────────────────────────────────┤
│  TCP (SimpleTcp)  窗口-based    慢启动+AIMD         1980s    │
│  ECMP             网络层        流哈希选路          1990s    │
│  STrack           多路径 RDMA   Packet Spraying+   2024     │
│                                  SACK+路径黑名单             │
│  DCQCN            Rate-based    CNP+alpha量化+     2015     │
│                                  速率恢复                   │
│  HPCC             Rate-based    INT+利用率反馈     2019     │
│  Swift            Rate-based    RTT测量+ pacing    2020     │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. TCP（SimpleTcp）：经典拥塞控制的基石

### 2.1 背景

TCP（Transmission Control Protocol）是互联网最基础的传输协议，其拥塞控制算法经过三十多年演进，形成了 Reno、Cubic、BBR 等多个变种。SimpleTcp 是 STrack-Sim 中的简化实现，保留了 TCP 最核心的机制。

### 2.2 核心概念

#### 2.2.1 拥塞窗口（Congestion Window, cwnd）

TCP 通过 **cwnd** 控制发送端在收到 ACK 前可以发送多少数据：

```
发送速率 ≈ cwnd / RTT
```

cwnd 是 TCP 拥塞控制的"旋钮"——网络拥塞时调小，空闲时调大。

#### 2.2.2 慢启动（Slow Start）

连接刚建立时，cwnd 从一个较小值（如 16 MSS）开始：

```
每收到一个 ACK: cwnd += 1
```

这意味着 **每经过一个 RTT，cwnd 翻倍**（指数增长）。

#### 2.2.3 拥塞避免（Congestion Avoidance）

当 cwnd 达到 **ssthresh**（慢启动阈值）后，进入线性增长阶段：

```
每收到一个 ACK: cwnd += 1/cwnd
每经过一个 RTT: cwnd += 1
```

#### 2.2.4 快速重传（Fast Retransmit）

当收到 3 个重复 ACK 时，认为对应包已丢失，立即重传，无需等待 RTO：

```
ssthresh = cwnd / 2
cwnd = ssthresh + 3  // 补偿已离开网络的 3 个包
```

#### 2.2.5 RTO 超时重传

如果重传定时器（RTO）到期仍未收到 ACK：

```
ssthresh = max(cwnd / 2, 1)
cwnd = init_cwnd  // 通常重置为 1 或 16
```

### 2.3 SimpleTcp 实现要点

```rust
pub struct SimpleTcp {
    tx_flows: HashMap<FlowId, FlowTxState>,
    rx_flows: HashMap<FlowId, FlowRxState>,
    init_cwnd: u32 = 16,
    max_cwnd: u32 = 256,
    rto_ns: u64 = 100_000,  // 100μs
}

struct FlowTxState {
    cwnd: u32,           // 拥塞窗口
    ssthresh: u32,       // 慢启动阈值
    in_flight: u32,      // 在途包数
    next_seq: SeqNum,    // 下一个要发送的序号
    un_acked_base: SeqNum, // 累计已确认序号
    send_times: HashMap<SeqNum, u64>, // 用于 RTO
    retransmit_queue: Vec<SeqNum>,
    dup_ack_count: u32,  // 重复 ACK 计数
}
```

**关键逻辑：**

```
function on_ack(ack):
    if ack.seq == un_acked_base:
        dup_ack_count += 1
        if dup_ack_count == 3:
            // 快速重传
            ssthresh = cwnd / 2
            cwnd = ssthresh + 3
            retransmit_queue.push(un_acked_base)
    else if ack.seq > un_acked_base:
        // 新 ACK
        dup_ack_count = 0
        
        if cwnd < ssthresh:
            // 慢启动
            cwnd += 1
        else:
            // 拥塞避免
            ca_ack_count += 1
            if ca_ack_count >= cwnd:
                cwnd += 1
                ca_ack_count = 0
        
        if ack.ecn:
            // ECN 响应：直接减半
            cwnd = cwnd / 2
```

### 2.4 优缺点

| 优点 | 缺点 |
|------|------|
| 简单、鲁棒、广泛部署 | 慢启动导致短流延迟高 |
| 累计 ACK 开销低 | 对微秒级 RTT 反应慢 |
| 丢包恢复机制成熟 | 单个丢包导致窗口大幅下降 |

---

## 3. ECMP：网络层的负载均衡

### 3.1 背景

ECMP（Equal-Cost Multi-Path）是一种网络层路由技术，当存在多条等价路径时，通过哈希算法将不同流分配到不同路径上，实现负载均衡。

### 3.2 核心机制

#### 3.2.1 流哈希（Flow Hashing）

ECMP 使用五元组（或简化版）计算哈希值：

```
hash = hash(src_ip, dst_ip, src_port, dst_port, protocol)
path = hash % num_paths
```

在 STrack-Sim 中简化为：

```rust
hash_key = pkt.src ^ pkt.dst ^ pkt.flow_id
path_idx = hash_key % num_paths
```

#### 3.2.2 等价路径

在 Leaf-Spine 或 Fat-Tree 拓扑中，任意两台主机之间通常存在多条等价路径：

```
Host A → Leaf 1 → Spine i → Leaf 2 → Host B
Host A → Leaf 1 → Spine j → Leaf 2 → Host B
```

ECMP 保证同一条流的所有包走同一条路径（避免乱序），不同流分散到不同路径。

### 3.3 与传输层的关系

ECMP 本身不是传输协议，而是一种 **网络层机制**。但在 STrack-Sim 中，ECMP baseline 指的是：

- 传输层使用单路径（类似 TCP）
- 网络层使用 ECMP 哈希选路
- 遇到拥塞时直接降窗（无路径切换能力）

### 3.4 优缺点

| 优点 | 缺点 |
|------|------|
| 实现简单，无需修改端侧 | 单流无法利用多路径带宽 |
| 无乱序问题 | 哈希冲突导致负载不均（"大象流"问题） |
| 与现有网络设备兼容 | 路径故障时恢复慢 |

---

## 4. STrack：多路径 RDMA 与选择性重传

### 4.1 背景

STrack 是 Meta（Facebook）在 NSDI'24 提出的多路径 RDMA 传输协议，专为 AI/ML 集群设计。其核心洞察是：**在拥有丰富多路径的数据中心网络中，单路径传输无法充分利用网络容量。**

### 4.2 核心机制

#### 4.2.1 Packet Spraying（包喷洒）

与 ECMP 的"按流选路"不同，STrack 将 **同一个流内的每个包** 分散到不同路径：

```
Path 1: pkt_0, pkt_3, pkt_6, ...
Path 2: pkt_1, pkt_4, pkt_7, ...
Path 3: pkt_2, pkt_5, pkt_8, ...
```

这通过 `routing_tag` 字段实现：

```rust
pkt.routing_tag = path_id + 1  // 1-indexed
```

#### 4.2.2 SACK Bitmap（选择性确认）

由于包走不同路径，到达顺序可能乱序。STrack 使用 **SACK bitmap** 精确告知发送端哪些包已收到：

```
接收端维护：
- next_expected: 下一个期望的序号
- received_bits: 64 位 bitmap，bit i = 1 表示 next_expected + i 已收到

ACK payload = [next_expected (4 bytes) | received_bits (8 bytes)]
```

发送端收到 NACK 后，根据 bitmap 精确重传丢失的包：

```
for i in 0..64:
    seq = base + i
    if seq >= nack.seq: break
    if (received_bits >> i) & 1 == 0:
        retransmit_queue.push(seq)
```

#### 4.2.3 路径黑名单（Path Blacklist）

当某条路径上出现 ECN 标记时，STrack 不是直接降窗，而是 **将该路径加入黑名单**：

```
if ack.ecn:
    path = (ack.id % num_paths)
    paths[path].blacklisted_until = now + 50_000ns  // 50μs
    
    // 只有所有路径都被黑名单时才降窗
    available = count(paths[p].is_available(now) for p in paths)
    if available == 0:
        cwnd = cwnd / 2
```

这比 ECMP 更优雅：先尝试绕开拥塞路径，而不是盲目降速。

### 4.3 两种模式

STrack 支持两种模式：

| 模式 | 行为 | 用途 |
|------|------|------|
| **Ecmp** | 单路径哈希，遇 ECN 直接降窗 | Baseline 对比 |
| **Strack** | 多路径喷洒 + 黑名单 + SACK | 完整功能 |

### 4.4 代码实现要点

```rust
pub struct STrackProtocol {
    mode: STrackMode,           // Ecmp or STrack
    paths: Vec<PathState>,      // 每条路径状态
    tx_flows: HashMap<FlowId, FlowTxState>,
    rx_flows: HashMap<FlowId, FlowRxState>,
}

struct PathState {
    path_id: u8,
    blacklisted_until: u64,     // 黑名单截止时间
    ecn_recent: u32,            // 最近 ECN 计数
}

fn pick_path(now: u64) -> Option<u8> {
    match mode {
        Ecmp => Some(0),
        STrack => {
            // 轮询选择可用路径
            for _ in 0..paths.len():
                let idx = (cursor + 1) % paths.len()
                if paths[idx].is_available(now):
                    return Some(idx)
            Some(0) // 都不可用时回退到 0
        }
    }
}
```

### 4.5 优缺点

| 优点 | 缺点 |
|------|------|
| 充分利用多路径带宽 | 乱序需要 SACK 支持 |
| 路径级隔离优于流级降窗 | ECN 路径归因仍有挑战 |
| SACK 精确重传减少冗余 | 实现复杂度高于单路径 |

---

## 5. DCQCN：基于速率的量化拥塞控制

### 5.1 背景

DCQCN（Data Center Quantized Congestion Notification）由 Microsoft 在 NSDI'15 提出，是 RoCEv2（RDMA over Converged Ethernet v2）的标准拥塞控制算法。

**核心问题**：RDMA 需要 **lossless 网络**（不能丢包），而传统 TCP 的丢包恢复机制太慢。DCQCN 通过 **速率控制**（而非窗口控制）+ **显式拥塞通知**（ECN/CNP）实现微秒级响应。

### 5.2 核心机制

#### 5.2.1 架构概述

```
发送端（Sender）                    接收端（Receiver）
   │                                     │
   │── Data pkt with ECN capable ───────▶│
   │                                     │
   │◀──────── ACK/CNP ──────────────────│
   │   (if pkt was ECN-marked)         │
   │                                     │
   │── rate adjustment ─────────────────▶│
```

#### 5.2.2 速率状态机

每个流维护三个核心变量：

```
current_rate: 当前发送速率（字节/秒）
target_rate:  目标速率（上一次降速前的 current_rate）
alpha:        拥塞程度（0~1，量化值）
```

#### 5.2.3 收到 CNP 时的降速

当发送端收到 CNP（Congestion Notification Packet）时：

```
// 1. 更新 alpha（指数加权移动平均）
alpha = (1 - g) * alpha + g
其中 g = 1/256 ≈ 0.0039

// 2. target_rate = current_rate（记住降速前的速率）
target_rate = current_rate

// 3. current_rate = current_rate * (1 - alpha/2)
current_rate = current_rate * (1 - alpha / 2)

// 4. 进入 Fast Recovery 阶段
in_fast_recovery = true
```

**为什么用 alpha/2 而不是直接减半？**

- alpha 反映的是网络拥塞程度
- alpha 越大，降速越剧烈
- alpha/2 保证即使 alpha=1（最坏情况），也只降到 50%

#### 5.2.4 速率恢复

DCQCN 定义了三个恢复阶段：

**阶段 1：Fast Recovery**
```
每 RAI（Rate Increase Interval，约 55μs）：
    current_rate = (current_rate + target_rate) / 2
    
直到连续收到 K（约 50）个无 ECN ACK：
    in_fast_recovery = false
```

**阶段 2：Additive Increase**
```
每 RAI：
    target_rate += RateIncreaseStep（固定步长）
    current_rate = (current_rate + target_rate) / 2
```

**阶段 3：Hyper-Increase**
```
如果 target_rate 达到上限：
    target_rate += HyperIncreaseStep（更大步长）
    current_rate = (current_rate + target_rate) / 2
```

#### 5.2.5 Pacing（速率整形）

Rate-based 协议需要 **pacing** 来避免 burst：

```
packet_interval_ns = (MTU_bytes * 8) / current_rate_bps

// 发送一个包后，下一个包最早在 now + packet_interval_ns 发送
next_tx_time = now + packet_interval_ns
```

### 5.3 数学公式总结

**CNP 响应：**

$$
\alpha_{new} = (1 - g) \cdot \alpha_{old} + g
$$

$$
R_{target} = R_{current}
$$

$$
R_{current}^{new} = R_{current} \cdot \left(1 - \frac{\alpha_{new}}{2}\right)
$$

**速率恢复（Fast Recovery）：**

$$
R_{current}^{new} = \frac{R_{current} + R_{target}}{2}
$$

**主动增加（Active Increase）：**

$$
R_{target}^{new} = R_{target} + \Delta R
$$

$$
R_{current}^{new} = \frac{R_{current} + R_{target}^{new}}{2}
$$

### 5.4 代码实现要点

```rust
const INIT_RATE_BPS: u64 = 1_000_000_000;     // 1 Gbps
const MIN_RATE_BPS: u64 = 1_000_000;          // 1 Mbps
const MAX_RATE_BPS: u64 = 100_000_000_000;    // 100 Gbps
const RAI_NS: u64 = 55_000;                   // 55 μs
const RATE_DECREASE_ACKS: u32 = 50;
const ALPHA_G: (u64, u64) = (1, 256);         // g = 1/256

struct FlowTxState {
    current_rate_bps: u64,
    target_rate_bps: u64,
    alpha: u64,              // 0~1000 表示 0.0~1.0
    in_fast_recovery: bool,
    acks_since_cnp: u32,
    next_tx_time_ns: u64,
}

impl FlowTxState {
    fn on_cnp(&mut self, now: u64) {
        let g = ALPHA_G.0 * 1000 / ALPHA_G.1;  // 千分比
        self.alpha = (self.alpha * (1000 - g) / 1000) + g;
        self.alpha = self.alpha.min(1000);
        
        self.target_rate_bps = self.current_rate_bps;
        let reduction = 1000u64.saturating_sub(self.alpha / 2);
        self.current_rate_bps = (self.current_rate_bps * reduction / 1000)
            .max(MIN_RATE_BPS);
        
        self.in_fast_recovery = true;
        self.acks_since_cnp = 0;
    }
    
    fn on_ack_no_ecn(&mut self, now: u64) {
        self.acks_since_cnp += 1;
        
        if now - self.last_rate_increase_ns < RAI_NS {
            return;  // 未到 RAI 间隔
        }
        self.last_rate_increase_ns = now;
        
        if self.in_fast_recovery {
            self.current_rate_bps = (self.current_rate_bps + self.target_rate_bps) / 2;
            if self.acks_since_cnp >= RATE_DECREASE_ACKS {
                self.in_fast_recovery = false;
            }
        } else {
            self.target_rate_bps = (self.target_rate_bps + HYPER_INCREASE_STEP)
                .min(MAX_RATE_BPS);
            self.current_rate_bps = (self.current_rate_bps + self.target_rate_bps) / 2;
        }
    }
}
```

### 5.5 CNP 与 ECN 的协作

```
交换机 ingress:
    if queue_bytes > ecn_threshold:
        pkt.ecn = true

接收端 on_data:
    if pkt.ecn:
        send CNP back to sender
    send ACK anyway

发送端 on_tx_control:
    if Control(2) [CNP]:
        flow.on_cnp(now)
    else if Control(0) [ACK]:
        if ack.ecn:
            flow.on_cnp(now)  // 某些实现 ACK 也带 ECN
        else:
            flow.on_ack_no_ecn(now)
```

### 5.6 优缺点

| 优点 | 缺点 |
|------|------|
| 微秒级拥塞响应 | 参数调优复杂（g, RAI, K 等） |
| 不依赖丢包，适合 lossless 网络 | 保守启动，收敛慢 |
| pacing 避免 burst | Fast Recovery 阶段可能欠冲 |
| 硬件实现友好（RoCE 网卡原生支持） | 需要网卡支持 ECN/CNP |

---

## 6. HPCC：高精度拥塞控制

### 6.1 背景

HPCC（High Precision Congestion Control）由阿里巴巴和北大在 SIGCOMM'19 提出，核心思想是：**利用网络设备的 INT（In-band Network Telemetry）功能，让发送端精确知道每条链路的利用率，从而做出更精确的速率调整。**

### 6.2 核心洞察

传统拥塞控制（如 DCQCN、DCTCP）的问题：

- **间接信号**：ECN 只是"是否超过阈值"的二值信号，不知道拥塞有多严重
- **反应滞后**：需要多次迭代才能收敛到合适速率
- **参数敏感**：性能高度依赖阈值、AI/MD 步长等参数

HPCC 的解决方案：

- **直接测量**：每个 ACK 携带路径上每条链路的 `tx_bytes` 和 `queue_bytes`
- **精确计算**：发送端可以计算出每条链路的实时利用率
- **一次收敛**：理论上可以一次调整到位

### 6.3 INT（In-band Network Telemetry）

INT 是数据平面可编程交换机（如 P4/Tofino）支持的功能：

```
每个包经过交换机时，交换机将以下信息插入包头部：
- 交换机 ID
- 出端口队列深度（bytes）
- 出端口已发送字节数（用于计算链路利用率）
- 时间戳

当包到达接收端时，这些信息随 ACK 回传给发送端。
```

### 6.4 速率调整公式

发送端收到 ACK 后，计算路径上的 **最大链路利用率**：

```
for each link in path:
    utilization = (tx_bytes_new - tx_bytes_old) / (bw * interval)
    
max_util = max(utilization for all links)
```

然后调整窗口/速率：

$$
W_{new} = W_{current} \cdot \frac{u_{target}}{max\_util} + W_{AI}
$$

其中：
- $W_{current}$：当前窗口（或速率对应的窗口）
- $u_{target}$：目标利用率（如 95%）
- $max\_util$：路径上最大链路利用率
- $W_{AI}$：主动增加量（保证公平性）

**直观理解**：

- 如果最大利用率 = 100%（满载），速率降到目标利用率的水平
- 如果最大利用率 = 50%（空闲），速率翻倍
- 如果最大利用率 = 95%（恰好目标），速率基本不变

### 6.5 STrack-Sim 中的简化实现

由于 STrack-Sim 目前不模拟可编程交换机的 INT 功能，HPCC 实现为 **占位版本**：

```rust
// ACK payload 携带虚拟利用率（实际应由交换机填充）
let util = if ack.payload.len() >= 8 {
    f64::from_le_bytes(ack.payload[..8].try_into().unwrap())
} else {
    0.5  // 占位值：50% 利用率
};

if util > 0.0 {
    // rate = rate * target_util / util
    let new_rate = (flow.current_rate_bps as f64 * TARGET_UTIL / util) as u64;
    flow.current_rate_bps = new_rate.clamp(MIN_RATE_BPS, MAX_RATE_BPS);
}
```

**注**：完整的 HPCC 需要交换机支持 INT，将真实的链路利用率写入 ACK payload。

### 6.6 优缺点

| 优点 | 缺点 |
|------|------|
| 理论上一次收敛 | 需要可编程交换机支持 INT |
| 精确的利用率信息 | 开销大（每个包携带多跳 INT 数据） |
| 参数少，调优简单 | 对硬件要求高 |
| 公平性好 | 当前占位实现精度不足 |

---

## 7. Swift：基于 RTT 的轻量传输

### 7.1 背景

Swift 由 Google 和 MIT 在 SIGCOMM'20 提出，是 **Swift Transport for RoCE** 的缩写。其核心洞察是：**在数据中心中，RTT 是拥塞最敏感、最及时的信号。**

### 7.2 核心机制

#### 7.2.1 RTT 作为拥塞信号

在数据中心中：

```
RTT = propagation_delay + queueing_delay + processing_delay

其中：
- propagation_delay ≈ 固定（几微秒）
- queueing_delay = 队列深度 / 带宽
- processing_delay ≈ 0（简化）
```

因此：

```
RTT 增加 → 队列在堆积 → 拥塞发生
RTT 最小 → 队列为空 → 链路空闲
```

#### 7.2.2 速率调整公式

Swift 使用以下公式调整发送速率：

$$
R_{new} = R_{current} \cdot \frac{RTT_{min}}{RTT_{current}}
$$

**直观理解**：

- $RTT_{current} = RTT_{min}$（无排队）：速率不变
- $RTT_{current} = 2 \cdot RTT_{min}$（排队等于传播延迟）：速率减半
- $RTT_{current} \to \infty$（严重拥塞）：速率趋近于 0

#### 7.2.3 RTT 测量

Swift 的关键是 **精确测量 RTT**：

```
发送端：
    每个 Data 包 depart_time 记录发送时间戳

接收端：
    ACK payload = [depart_time (8 bytes)]

发送端收到 ACK：
    rtt = now - ack.payload.depart_time
    min_rtt = min(min_rtt, rtt)
    rate = rate * min_rtt / rtt
```

#### 7.2.4 Pacing

与 DCQCN 类似，Swift 也是 rate-based，需要 pacing：

```
packet_interval = MTU / rate
```

### 7.3 与 BBR 的关系

Swift 与 Google BBR（Bottleneck Bandwidth and Round-trip propagation time）有相似之处：

| | BBR | Swift |
|--|-----|-------|
| 信号 | RTT + 带宽测量 | 纯 RTT |
| 目标 | 填满瓶颈带宽 | 控制排队延迟 |
| 场景 | 广域网 | 数据中心 |
| RTT 范围 | ms 级 | μs 级 |

Swift 更轻量：不需要测量带宽，只需要 RTT。

### 7.4 STrack-Sim 中的实现

```rust
fn on_ack(&mut self, ack: &Packet, now: u64) {
    if ack.payload.len() >= 8 {
        let send_time = u64::from_le_bytes(ack.payload[..8].try_into().unwrap());
        let rtt = now.saturating_sub(send_time);
        
        if rtt > 0 {
            flow.min_rtt_ns = flow.min_rtt_ns.min(rtt);
            
            if flow.min_rtt_ns < u64::MAX {
                let new_rate = (flow.current_rate_bps as f64
                    * flow.min_rtt_ns as f64
                    / rtt as f64) as u64;
                flow.current_rate_bps = new_rate.clamp(MIN_RATE_BPS, MAX_RATE_BPS);
            }
        }
    }
}

fn on_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet> {
    // ACK payload 携带 depart_time
    let ts_bytes = pkt.depart_time.to_le_bytes().to_vec();
    Packet::control(..., ts_bytes, now)
}
```

### 7.5 优缺点

| 优点 | 缺点 |
|------|------|
| 极其简单（只需 RTT） | RTT 测量受噪声影响 |
| 反应及时 | 需要精确时间同步 |
| 无需交换机支持 | 最小 RTT 估计挑战 |
| 天然支持 pacing | 对短流不够友好 |

---

## 8. 六种协议对比总结

### 8.1 设计理念对比

```
                    窗口-based          Rate-based
                   ┌─────────────────┬─────────────────┐
    间接信号       │   TCP (AIMD)    │   DCQCN (ECN)   │
    (端到端测量)   │                 │   Swift (RTT)   │
                   └─────────────────┴─────────────────┘
    直接信号       │                 │   HPCC (INT)    │
    (网络显式反馈) │                 │                 │
                   └─────────────────┴─────────────────┘
```

### 8.2 参数复杂度对比

| 协议 | 核心参数 | 参数数量 |
|------|----------|----------|
| TCP | init_cwnd, ssthresh | 2 |
| ECMP | hash_seed | 1 |
| STrack | blacklist_duration, init_cwnd | 2 |
| DCQCN | g, RAI, K, RateStep, HyperStep, alpha | 6+ |
| HPCC | target_util, W_AI | 2 |
| Swift | min_rtt, rate bounds | 2 |

### 8.3 适用场景

| 协议 | 最佳场景 | 避免场景 |
|------|----------|----------|
| TCP | 通用、兼容旧系统 | 超低延迟 RDMA |
| ECMP | 网络层负载均衡 baseline | 需要路径级调优 |
| STrack | 多路径 RDMA、AI 训练 | 单路径拓扑 |
| DCQCN | RoCEv2、lossless 网络 | 无 ECN 支持的旧网络 |
| HPCC | 可编程交换机环境 | 传统固定功能交换机 |
| Swift | 需要极简实现的场景 | RTT 测量不可靠的网络 |

### 8.4 性能特征（基于 STrack-Sim 观察）

在相同 Incast 场景下（16 节点，15 流，512KB/流）：

| 协议 | FCT P50 | 重传率 | ECN 标记 | 特点 |
|------|---------|--------|----------|------|
| TCP | ~800μs | 低 | 中 | 平衡 |
| ECMP | ~820μs | 低 | 高 | 单路径瓶颈 |
| STrack | ~630μs | 高 | 高 | 多路径优势 |
| DCQCN | ~1770μs | 0% | 0 | 保守但无丢包 |
| HPCC | ~3000μs | 高 | 高 | 占位实现未优化 |
| Swift | ~4200μs | 0% | 0 | 占位实现未优化 |

**注意**：DCQCN/HPCC/Swift 的 FCT 较高是因为：
1. 当前实现使用保守的初始速率（1Gbps vs 链路 100Gbps）
2. HPCC/Swift 使用占位 INT/RTT 值，未获得真实网络反馈
3. 未针对 incast 场景调优参数

在真实环境中，rate-based 协议（DCQCN/HPCC/Swift）通常在 **高负载、长流** 场景下表现更好。

---

## 9. 在 STrack-Sim 中使用这些协议

### 9.1 创建 SimRunner 时指定协议

```rust
use strack_sim::nic::{SimpleTcp, STrackProtocol, STrackMode, 
                       DcqcnProtocol, HpccProtocol, SwiftProtocol};
use strack_sim::sim_runner::SimRunner;

// TCP
let runner = SimRunner::new(topo, "tcp".to_string(), |h, _topo| {
    Box::new(SimpleTcp::new(h))
});

// ECMP
let runner = SimRunner::new(topo, "ecmp".to_string(), |h, topo| {
    Box::new(STrackProtocol::new(h, STrackMode::Ecmp, topo))
});

// STrack
let runner = SimRunner::new(topo, "strack".to_string(), |h, topo| {
    Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
});

// DCQCN
let runner = SimRunner::new(topo, "dcqcn".to_string(), |h, _topo| {
    Box::new(DcqcnProtocol::new(h))
});

// HPCC
let runner = SimRunner::new(topo, "hpcc".to_string(), |h, _topo| {
    Box::new(HpccProtocol::new(h))
});

// Swift
let runner = SimRunner::new(topo, "swift".to_string(), |h, _topo| {
    Box::new(SwiftProtocol::new(h))
});
```

### 9.2 运行对比示例

```bash
cargo run --release --example protocol_compare
```

### 9.3 自定义协议参数

各协议的参数在源码顶部以 `const` 定义，可以直接修改后重新编译：

```rust
// src/nic/dcqcn.rs
const INIT_RATE_BPS: u64 = 1_000_000_000;  // 修改初始速率
const RAI_NS: u64 = 55_000;                // 修改速率恢复间隔
const ALPHA_G_NUMERATOR: u64 = 1;          // 修改 alpha 更新系数
```

### 9.4 实现新协议

实现 `Protocol` trait 即可添加新协议：

```rust
use strack_sim::nic::{Protocol, ProtocolStats};
use strack_sim::network::packet::{FlowId, Packet};

pub struct MyProtocol {
    // ... 状态
}

impl Protocol for MyProtocol {
    fn start_flow(&mut self, flow_id: FlowId, dst: u32, total_bytes: u64, now: u64);
    fn on_tx_tick(&mut self, now: u64) -> Vec<Packet>;
    fn on_rx_data(&mut self, pkt: &Packet, now: u64) -> Vec<Packet>;
    fn on_tx_control(&mut self, pkt: &Packet, now: u64);
    fn all_flows_done(&self) -> bool;
    fn take_finished_flows(&mut self) -> Vec<(FlowId, u64)>;
    fn stats(&self) -> ProtocolStats;
    fn has_pending_work(&self) -> bool;
    fn next_rto_deadline(&self) -> Option<u64>;
    fn next_tx_time(&self) -> Option<u64>;  // 可选：pacing 支持
}
```

---

## 附录 A：关键术语表

| 术语 | 英文 | 解释 |
|------|------|------|
| 拥塞窗口 | Congestion Window (cwnd) | 发送端在收到 ACK 前可发送的数据量 |
| 慢启动阈值 | Slow Start Threshold (ssthresh) | 区分慢启动和拥塞避免的阈值 |
| 快速重传 | Fast Retransmit | 收到 3 个重复 ACK 时立即重传 |
| RTO | Retransmission Timeout | 重传超时定时器 |
| ECN | Explicit Congestion Notification | 显式拥塞通知（IP 头部标记） |
| CNP | Congestion Notification Packet | DCQCN 中的拥塞通知包 |
| Pacing | Rate Pacing | 按固定间隔发送包，避免 burst |
| INT | In-band Network Telemetry | 带内网络遥测（交换机嵌入元数据） |
| RTT | Round-Trip Time | 往返时间 |
| SACK | Selective Acknowledgment | 选择性确认（精确告知收到哪些包） |
| Spraying | Packet Spraying | 将同一流的包分散到多条路径 |
| Blacklist | Path Blacklist | 暂时避开拥塞路径的机制 |

## 附录 B：推荐阅读

1. **TCP** - Jacobson, V. (1988). "Congestion Avoidance and Control." *SIGCOMM*.
2. **DCQCN** - Zhu et al. (2015). "Congestion Control for Large-Scale RDMA Deployments." *NSDI*.
3. **HPCC** - Li et al. (2019). "High Precision Congestion Control." *SIGCOMM*.
4. **Swift** - Kumar et al. (2020). "Swift: Delay is Simple and Effective for Congestion Control in the Datacenter." *SIGCOMM*.
5. **STrack** - Huang et al. (2024). "STrack: A Reliable Multipath Transport for AI/ML Clusters." *NSDI*.
6. **DCTCP** - Alizadeh et al. (2010). "Data Center TCP." *SIGCOMM*.（DCQCN 的前置工作）

---

> 本文档版本：v1.0  
> 维护：STrack-Sim 项目  
> 最后更新：2026-05-23
