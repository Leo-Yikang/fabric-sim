# Fabric-Sim · 多路径 RDMA 网络模拟器

> 一个用 Rust 从零搭建的 **Fabric 协议专属离散事件模拟器**，用于研究 AI/ML 集群环境下多路径 RDMA 的性能表现。
> **四个阶段全部完成 ✅**：DES 引擎 + 拓扑层 + Fabric 协议栈 + 流量/Monitor + 端到端对比示例。

---

## 🎯 项目目标

复现并对比下列方案在 Fat-Tree / Leaf-Spine 拓扑下的表现：

| 方案 | 负载均衡 | 拥塞响应 | 恢复机制 |
|------|---------|---------|---------|
| RoCEv2 + ECMP | 哈希分流（单路径/流） | DCQCN 风格降窗 | 超时重传 |
| **Fabric** | Packet Spraying | 先切路再降窗 | SACK Bitmap |

针对的核心痛点：
1. **ECMP 哈希冲突**导致链路利用率只能跑到 30–50%；
2. RoCEv2 依赖无损以太网，bit error 回退开销不可接受；
3. 硬件卸载场景下多路径状态难以维护。

---

## 📊 实测结果（Incast 场景）

**实验设置**：4 Leaf × 8 Spine × 4 host/leaf = 16 hosts，15 个 sender 同时向 host 0 发送 512 KB

```
┌─────────── ECMP baseline ────────────         ┌─────────── Fabric ────────────────────
│ 完成流数            15                         │ 完成流数            15
│ 仿真总时长          874 us                     │ 仿真总时长          762 us   ✅ -12.8%
│ FCT P50             816.6 us                   │ FCT P50             704.7 us ✅ -13.7%
│ FCT P99             848.3 us                   │ FCT P99             735.9 us ✅ -13.3%
│ 总发送包数          7752                       │ 总发送包数          8652
│ 总重传包数          72                         │ 总重传包数          972
│ ECN 标记总数        4182                       │ ECN 标记总数        5134
└──────────────────────────────────────         └──────────────────────────────────────
```

可以看到 Fabric 在 FCT 上有 **~13% 的明显改善**，代价是更多重传——符合 Packet Spraying 的预期行为。

---

## 📦 项目结构

```
fabric-sim/
├── Cargo.toml
├── README.md
├── docs/
│   └── design.md          ← 四阶段完整设计文档
├── src/
│   ├── lib.rs             ← crate 入口
│   ├── core/              ✅ 阶段一：DES 引擎
│   │   ├── event.rs         · Event + EventKind
│   │   ├── queue.rs         · EventQueue (BinaryHeap min-heap)
│   │   └── simulator.rs     · Simulator + Handler
│   ├── network/           ✅ 阶段二：物理层
│   │   ├── packet.rs        · Packet（含 ECN + SACK bitmap）
│   │   ├── link.rs          · Link + LinkRegistry
│   │   └── switch.rs        · Switch + Port + RoutingTable + ECN
│   ├── topology/          ✅ 阶段二：拓扑生成
│   │   ├── leaf_spine.rs    · 2 层 Leaf-Spine
│   │   ├── fat_tree.rs      · k-ary Fat-Tree
│   │   └── dumbell.rs       · Dumbbell 拓扑
│   ├── nic/               ✅ 阶段三：Fabric 协议栈
│   │   ├── protocol.rs      · Protocol trait（可插拔接口）
│   │   ├── strack.rs        · Fabric 协议实现
│   │   └── tcp.rs           · SimpleTcp 基线实现
│   ├── traffic/           ✅ 阶段四：流量生成
│   │   ├── incast.rs        · N-to-1 多对一拥塞
│   │   ├── all_reduce.rs    · Ring AllReduce
│   │   ├── all_to_all.rs    · 全员两两交换
│   │   ├── synthetic.rs     · 通用合成流量
│   │   ├── permute.rs       · 排列流量
│   │   └── mix.rs           · 混合流量
│   ├── monitor/           ✅ 阶段四：指标采集
│   │   └── mod.rs           · FlowFct + SimSummary
│   ├── sim_runner/        ✅ 端到端仿真主循环
│   │   ├── mod.rs           · SimRunner 集中分发
│   │   ├── host.rs          · 主机侧事件处理
│   │   └── switch.rs        · 交换机侧事件处理
│   └── viz/               ✅ 3D 可视化数据采集
│       ├── data.rs           · VizData / VizNode / VizLink（serde）
│       ├── position.rs       · 3D 坐标计算（Dumbell / LeafSpine）
│       └── sampler.rs        · TimeSeriesSampler 链路利用率采样
├── examples/
│   ├── des_demo.rs        · 阶段一：纯 DES 引擎演示
│   ├── incast_compare.rs  · 端到端：ECMP vs Fabric 对比
│   ├── workload_sweep.rs  · 多维度参数扫描
│   └── viz_demo.rs        · 3D 可视化数据导出
├── scripts/
│   └── visualize_3d.py    · Python Plotly 3D 交互式渲染
├── benches/
│   └── des_bench.rs       · DES 引擎吞吐基准
├── tests/
│   ├── integration_des.rs · 百万级 DES 事件
│   ├── integration_e2e.rs · 端到端 Incast 完整性
│   ├── integration_dumbell.rs · Dumbbell 拓扑集成
│   └── matrix_workloads.rs   · 参数化矩阵测试
└── logs/                  · 运行时日志输出目录
```

---

## 🚀 快速开始

### 环境要求
- Rust 1.74+
- macOS / Linux

### 编译
```bash
cd ~/Desktop/fabric-sim
cargo build --release
```

### 运行测试（59 个测试，全部通过）
```bash
cargo test --release
```

预期：
- 45 个单元测试（core / network / nic / topology / traffic 模块）
- 13 个集成测试（DES 引擎 / 端到端 / Dumbbell / 参数化矩阵）
- 1 个文档测试

### 运行端到端演示（核心成果）
```bash
cargo run --release --example incast_compare
```

输出 ECMP vs Fabric 在同一 Incast 场景下的对比指标。

### 运行 DES 引擎演示
```bash
cargo run --release --example des_demo
```

性能：~12–25 M events/sec（取决于场景）

### 跑性能基准
```bash
cargo bench
```

### 3D 交互式拓扑可视化

仿真过程中采集链路利用率时间序列，导出 JSON 后用 Python Plotly 渲染 3D 动画拓扑图。

```bash
# 1. 运行仿真并导出数据
cargo run --release --example viz_demo

# 2. 安装 Python 依赖（首次）
pip install plotly

# 3. 在浏览器中打开 3D 可视化
python3 scripts/visualize_3d.py output/viz_data.json
```

可视化特性：
- 节点：蓝色圆点（主机）+ 橙色菱形（交换机），带标签
- 链路：颜色从绿→黄→红随利用率渐变，线宽随利用率增大
- 底部时间滑块可拖拽或自动播放
- 支持 3D 旋转、缩放、平移
- 暗色主题，标题栏显示 FCT P99 / 平均利用率等摘要指标

---

## 🧭 四阶段开发蓝图（实现状态）

### ✅ 阶段一：离散事件引擎

- [x] `Event` + `EventKind`（含 PacketArrive/Depart/Timeout/Stop/Custom）
- [x] `EventQueue`（`BinaryHeap` + 反向 Ord 实现最小堆）
- [x] `Simulator`（时钟 + Handler 注册 + step/run/run_until）
- [x] FIFO 稳定性（同时间戳事件保插入顺序）
- [x] 单元测试 + 集成测试 + 基准

### ✅ 阶段二：网络拓扑与物理层

- [x] `Packet`：含 ECN、SACK base/bits、path_hint、depart_time
- [x] `Link`：带宽 + 传播延迟，整数运算无浮点误差
- [x] `Switch`：FIFO 队列 + ECN 标记 + buffer 上限丢包 + 路由表（ECMP 多路径）
- [x] `LeafSpine`：参数化生成 + 完整路由
- [x] `FatTree`：k-ary 三层完整实现

### ✅ 阶段三：NIC + Fabric 协议栈

- [x] **TxNic**：
  - 流量分段（应用层字节 → MTU 包）
  - Packet Spraying（轮询挑选可用路径）
  - CWND 维护 + AIMD 加性增加
  - ECN 响应：先切路（路径黑名单 50us）再降窗
  - RTO 超时重传（100us）
- [x] **RxNic**：
  - Reorder Buffer（基于 next_expected + 64-bit bitmap）
  - 累计 ACK + SACK Bitmap
  - 乱序时发送 NACK 触发选择性重传
- [x] **CongestionMode**：`Ecmp` baseline 与 `Strack` 双模式

### ✅ 阶段四：流量生成与指标

- [x] `Incast`：N-to-1 拥塞场景
- [x] `RingAllReduce`：经典 ring 模式
- [x] `AllToAll`：全员两两交换
- [x] `Monitor`：FCT、ECN 标记、丢包、最大队列深度、平均链路利用率
- [x] `SimSummary`：CSV/JSON 可序列化（`serde::Serialize`）

### ✅ 端到端集成

- [x] `SimRunner`：集中式仿真主循环，处理所有事件类型
- [x] `incast_compare` 示例：ECMP vs Fabric 一键对比
- [x] 实测 FCT 改善 ~13%（512 KB Incast）

---

## 📝 关键设计要点

### 1. 引擎与网络解耦
`core/` 不依赖任何网络概念，可独立测试与替换为其他 DES 实现。

### 2. 集中式仿真主循环
没有使用 `Simulator` 的 handler 注册（borrow checker 太严），改为 `SimRunner` 集中分发事件。这允许在事件处理中自由读写所有实体状态。

### 3. 全局唯一 packet id
多个 TxNic 的本地 packet_id 在全局 `packet_buf` 中会冲突，因此 `SimRunner` 维护一个全局 PID 生成器，重写所有出包的 id。

### 4. RTO 与持续 TxTick
当 cwnd 满时仅靠 ACK 触发的 TxTick 不够（丢包后永远无 ACK），所以未完成流会定期 tick（25us）检查 RTO 超时并重传。

### 5. 可复现
所有随机数走 `rand_pcg` + 显式 seed（拓扑里仍然是确定性，不需要随机）。

---

## 📚 参考资料

- **Fabric 论文**：*"Fabric: A Reliable Multipath Transport for AI/ML Clusters"* (Meta NSDI'24)
- **RoCEv2 / DCQCN**：[RFC 8888](https://datatracker.ietf.org/doc/html/rfc8888)
- **htsim**：UCL 的 C++ DES 网络模拟器
- **ns-3**：研究级网络模拟器

---

## 📄 许可证

MIT License · 2026 · kiwios-cn
