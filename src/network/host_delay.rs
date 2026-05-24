//! 主机硬件延迟模型
//!
//! 模拟 GPU → NVLink/NVSwitch → PCIe → NIC 路径上的延迟。
//! 真实 AI 训练节点中，跨 GPU 通信可能走 NVLink（~100ns），
//! 跨节点走 NIC（~1-5μs PCIe + 网络）。

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
            GpuLink::NvLink => 600_000_000_000,  // 600 GB/s
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