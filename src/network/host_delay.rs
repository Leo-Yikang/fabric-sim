//! 主机硬件延迟模型
//!
//! 模拟 GPU → NVLink/NVSwitch → PCIe → NIC 路径上的延迟。
//! 真实 AI 训练节点中，跨 GPU 通信可能走 NVLink（~100ns），
//! 跨节点走 NIC（~1-5μs PCIe + 网络）。
//!
//! 本模块提供两种使用方式：
//! 1. 静态延迟矩阵（NodeTopology）：用于查询 GPU↔NIC↔GPU 的延迟
//! 2. 动态延迟注入（HostDelayModel）：接入 SimRunner 事件流，在包发送/接收时
//!    注入 DMA 延迟、doorbell 延迟、PCIe 往返延迟，体现 RDMA "零拷贝" vs
//!    传统 memcpy 的差异。
//!
//! **所有时间计算使用整数纳秒（u64），与项目全局约定一致。**

use crate::EntityId;

/// GPU 间连接类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuLink {
    /// NVLink 直连（GPU-GPU 在同一节点，~100ns）
    NvLink,
    /// NVSwitch 全互联（多 GPU 通过 NVSwitch，~200ns）
    NvSwitch,
    /// PCIe + NIC（跨节点，~1-5μs）
    PcieNic,
}

impl GpuLink {
    /// 每种链路的单向延迟（ns）
    pub fn delay_ns(&self) -> u64 {
        match self {
            GpuLink::NvLink => 100,
            GpuLink::NvSwitch => 200,
            GpuLink::PcieNic => 2000, // 2μs 典型值
        }
    }

    /// 带宽（bps）
    pub fn bandwidth_bps(&self) -> u64 {
        match self {
            GpuLink::NvLink => 600_000_000_000,  // 600 Gbps
            GpuLink::NvSwitch => 600_000_000_000,
            GpuLink::PcieNic => 400_000_000_000,  // 400 Gbps (PCIe 5.0 ×16)
        }
    }
}

/// 节点拓扑：GPU 编号 → NIC 编号映射
#[derive(Debug, Clone)]
pub struct NodeTopology {
    /// 节点内 GPU 数量
    pub num_gpus: u32,
    /// 节点内 NIC 数量
    pub num_nics: u32,
    /// GPU → 最亲和的 NIC（同 PCIe switch）
    pub gpu_to_nic: Vec<u32>,
    /// GPU-GPU 连接类型矩阵 [src_gpu][dst_gpu]
    pub gpu_links: Vec<Vec<GpuLink>>,
    /// NIC-GPU 延迟矩阵 [nic][gpu]（ns，包含 PCIe 往返）
    pub nic_to_gpu_delay: Vec<Vec<u64>>,
}

impl NodeTopology {
    /// 典型 8-GPU 节点（NVSwitch 全互联 + 8 NIC rail-optimized）
    pub fn typical_8gpu_8nic() -> Self {
        let gpus = 8;
        let nics = 8;
        let mut gpu_links = Vec::with_capacity(gpus as usize);
        for i in 0..gpus {
            let mut row = Vec::with_capacity(gpus as usize);
            for j in 0..gpus {
                row.push(if i == j {
                    GpuLink::NvLink // self: treat as NVLink
                } else {
                    GpuLink::NvSwitch // NVSwitch full mesh
                });
            }
            gpu_links.push(row);
        }
        // Rail-optimized: GPU i ↔ NIC i 延迟最小（同 PCIe switch）
        let gpu_to_nic: Vec<u32> = (0..gpus).collect();
        let mut nic_to_gpu_delay = vec![vec![0u64; gpus as usize]; nics as usize];
        for nic in 0..nics {
            for gpu in 0..gpus {
                nic_to_gpu_delay[nic as usize][gpu as usize] = if nic == gpu {
                    500 // 同 PCIe switch，~500ns
                } else {
                    3000 // 跨 PCIe switch，~3μs
                };
            }
        }
        Self { num_gpus: gpus, num_nics: nics, gpu_to_nic, gpu_links, nic_to_gpu_delay }
    }

    /// GPU 间通信延迟（ns）
    pub fn gpu_to_gpu_delay(&self, src_gpu: u32, dst_gpu: u32) -> u64 {
        if src_gpu < self.num_gpus && dst_gpu < self.num_gpus {
            self.gpu_links[src_gpu as usize][dst_gpu as usize].delay_ns()
        } else {
            GpuLink::PcieNic.delay_ns()
        }
    }

    /// GPU → NIC 发送路径延迟（ns）
    pub fn gpu_to_nic_delay(&self, gpu: u32, nic: u32) -> u64 {
        if nic < self.num_nics && gpu < self.num_gpus {
            self.nic_to_gpu_delay[nic as usize][gpu as usize]
        } else {
            3000
        }
    }
}

// ------------------------------------------------------------------
// 动态延迟注入模型（接入 SimRunner 事件流）
// ------------------------------------------------------------------

/// 主机内部硬件延迟参数。
///
/// 用于在 SimRunner 中注入发送/接收路径上的延迟，体现 RDMA 与传统 TCP 在
/// 主机侧开销上的差异：
/// - RDMA Write：GPU → NIC 直接 DMA，无需 CPU 参与
/// - RDMA Send：需要 post recv WQE，有 doorbell 开销
/// - TCP：需要 CPU 拷贝到内核 socket buffer，延迟更高
///
/// **所有延迟参数均为整数（u64），带宽参数用于整数除法计算拷贝时间。**
#[derive(Debug, Clone, Copy)]
pub struct HostDelayModel {
    /// DMA 拷贝带宽（bps）。0 表示零拷贝（RDMA 理想场景）。
    /// 延迟 = (bytes * 8 * 1_000_000_000) / bandwidth_bps
    pub dma_bw_bps: u64,
    /// Doorbell 延迟：通知 NIC 有新 WQE（ns）
    pub doorbell_ns: u64,
    /// PCIe 往返延迟：CPU ↔ NIC 配置寄存器（ns）
    pub pcie_roundtrip_ns: u64,
    /// 内核协议栈处理延迟（TCP 场景，ns）
    pub kernel_stack_ns: u64,
    /// 用户态→内核态拷贝带宽（bps）。0 表示无拷贝。
    pub memcpy_bw_bps: u64,
    /// 接收端 DMA 完成中断延迟（ns）
    pub rx_interrupt_ns: u64,
    /// CQE 轮询延迟（ns）
    pub cq_poll_ns: u64,
}

impl HostDelayModel {
    /// RDMA 零拷贝理想模型（仅 doorbell + PCIe + CQ poll）
    pub fn rdma_zerocopy() -> Self {
        Self {
            dma_bw_bps: 0,          // 零拷贝
            doorbell_ns: 200,       // ~200ns
            pcie_roundtrip_ns: 500, // ~500ns
            kernel_stack_ns: 0,
            memcpy_bw_bps: 0,       // 无 memcpy
            rx_interrupt_ns: 500,   // ~500ns
            cq_poll_ns: 100,        // ~100ns
        }
    }

    /// RDMA 带 staging buffer（小消息需要 CPU bounce buffer）
    /// DMA 带宽约 100 Gbps（~12.5 GB/s），1 KB 约 80ns
    pub fn rdma_with_staging() -> Self {
        Self {
            dma_bw_bps: 100_000_000_000, // 100 Gbps (~12.5 GB/s)
            doorbell_ns: 200,
            pcie_roundtrip_ns: 500,
            kernel_stack_ns: 0,
            memcpy_bw_bps: 0,
            rx_interrupt_ns: 500,
            cq_poll_ns: 100,
        }
    }

    /// 传统 TCP（内核 socket，需要拷贝）
    /// memcpy 带宽约 20 Gbps（~2.5 GB/s），1 KB 约 400ns
    pub fn tcp_kernel() -> Self {
        Self {
            dma_bw_bps: 0,
            doorbell_ns: 0,
            pcie_roundtrip_ns: 500,
            kernel_stack_ns: 2000,       // ~2μs
            memcpy_bw_bps: 20_000_000_000, // 20 Gbps (~2.5 GB/s)
            rx_interrupt_ns: 2000,       // ~2μs
            cq_poll_ns: 0,
        }
    }

    /// 计算发送端总延迟（ns）。
    ///
    /// 公式：固定开销(doorbell + PCIe + kernel) + 拷贝延迟(bytes*8*1e9/bw)
    /// 带宽为 0 时拷贝延迟为 0（零拷贝语义）。
    /// 注意：此为简化包级延迟，不建模多流争用 DMA 引擎的串行化行为。
    /// 如需主机侧队列语义，请使用 `tx_fixed_overhead_ns()` + `tx_dma_time_ns()`。
    pub fn tx_delay_ns(&self, bytes: u64) -> u64 {
        self.tx_fixed_overhead_ns()
            .saturating_add(self.tx_dma_time_ns(bytes))
    }

    /// 发送端每包固定开销（ns）：doorbell + PCIe 往返 + 内核栈。
    /// 这些开销不依赖包大小，是每包必须支付的固定成本。
    pub fn tx_fixed_overhead_ns(&self) -> u64 {
        self.doorbell_ns
            .saturating_add(self.pcie_roundtrip_ns)
            .saturating_add(self.kernel_stack_ns)
    }

    /// 发送端 DMA/memcpy 占用时间（ns）：包大小决定，体现主存/NIC 之间
    /// 拷贝的带宽成本。此时间占用主机 DMA 引擎，后续包的 NIC 出包时间
    /// 应加上此串行化延迟。
    pub fn tx_dma_time_ns(&self, bytes: u64) -> u64 {
        let dma = if self.dma_bw_bps > 0 {
            (bytes.saturating_mul(8).saturating_mul(1_000_000_000)) / self.dma_bw_bps
        } else {
            0
        };
        let memcpy = if self.memcpy_bw_bps > 0 {
            (bytes.saturating_mul(8).saturating_mul(1_000_000_000)) / self.memcpy_bw_bps
        } else {
            0
        };
        dma.saturating_add(memcpy)
    }

    /// 计算接收端总延迟（ns）
    pub fn rx_delay_ns(&self, bytes: u64) -> u64 {
        let dma = if self.dma_bw_bps > 0 {
            (bytes.saturating_mul(8).saturating_mul(1_000_000_000)) / self.dma_bw_bps
        } else {
            0
        };
        let memcpy = if self.memcpy_bw_bps > 0 {
            (bytes.saturating_mul(8).saturating_mul(1_000_000_000)) / self.memcpy_bw_bps
        } else {
            0
        };
        self.rx_interrupt_ns
            .saturating_add(self.cq_poll_ns)
            .saturating_add(dma)
            .saturating_add(memcpy)
    }
}

impl Default for HostDelayModel {
    fn default() -> Self {
        Self::rdma_zerocopy()
    }
}

// ------------------------------------------------------------------
// 每个 host 的延迟配置（接入 SimRunner）
// ------------------------------------------------------------------

/// SimRunner 中每个 host 的硬件延迟配置。
///
/// 索引 = host_id。SimRunner::new() 时初始化为全零延迟（向后兼容），
/// 用户可通过 `with_host_delays()` 启用真实延迟。
#[derive(Debug, Clone)]
pub struct HostDelayConfig {
    /// 每个 host 的延迟模型
    pub models: Vec<HostDelayModel>,
    /// 是否启用延迟注入
    pub enabled: bool,
}

impl HostDelayConfig {
    pub fn new(n_hosts: usize) -> Self {
        Self {
            models: vec![HostDelayModel::default(); n_hosts],
            enabled: false,
        }
    }

    /// 为所有 host 设置相同模型
    pub fn uniform(model: HostDelayModel, n_hosts: usize) -> Self {
        Self {
            models: vec![model; n_hosts],
            enabled: true,
        }
    }

    /// 为单个 host 设置模型
    pub fn set(&mut self, host: EntityId, model: HostDelayModel) {
        let idx = host as usize;
        if idx < self.models.len() {
            self.models[idx] = model;
        }
    }

    /// 获取 host 的发送延迟
    pub fn tx_delay(&self, host: EntityId, bytes: u64) -> u64 {
        if !self.enabled {
            return 0;
        }
        let idx = host as usize;
        if idx < self.models.len() {
            self.models[idx].tx_delay_ns(bytes)
        } else {
            0
        }
    }

    /// 获取 host 的每包固定开销（ns）
    pub fn tx_fixed_overhead(&self, host: EntityId) -> u64 {
        if !self.enabled {
            return 0;
        }
        let idx = host as usize;
        if idx < self.models.len() {
            self.models[idx].tx_fixed_overhead_ns()
        } else {
            0
        }
    }

    /// 获取 host 的 DMA/memcpy 占用时间（ns）
    pub fn tx_dma_time(&self, host: EntityId, bytes: u64) -> u64 {
        if !self.enabled {
            return 0;
        }
        let idx = host as usize;
        if idx < self.models.len() {
            self.models[idx].tx_dma_time_ns(bytes)
        } else {
            0
        }
    }

    /// 获取 host 的接收延迟
    pub fn rx_delay(&self, host: EntityId, bytes: u64) -> u64 {
        if !self.enabled {
            return 0;
        }
        let idx = host as usize;
        if idx < self.models.len() {
            self.models[idx].rx_delay_ns(bytes)
        } else {
            0
        }
    }
}
