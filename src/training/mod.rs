//! 训练 Workload 抽象（P2）
//!
//! 提供 `TrainingJob`、`Iteration`、`CollectiveOp` 等高层语义，
//! 用于模拟分布式训练中的 collective communication 模式。
//!
//! 核心概念：
//! - `TrainingJob`：一次完整训练，包含多个 `Iteration`
//! - `Iteration`：一个训练迭代，包含 forward + backward + collective 序列
//! - `CollectiveOp`：集合通信操作（AllReduce / ReduceScatter / AllGather / AllToAll）
//! - `CollectiveAlgorithm`：实现算法（Ring / ReduceScatter+AllGather / Tree）
//! - `ChunkConfig`：chunk 大小、channel 数、pipeline 深度

use crate::EntityId;
use crate::traffic::FlowDesc;
use serde::{Serialize, Deserialize};

/// 集合通信操作种类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectiveKind {
    AllReduce,
    ReduceScatter,
    AllGather,
    AllToAll,
}

/// 集合通信算法
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectiveAlgorithm {
    /// 经典 Ring AllReduce（2*(N-1) 步）
    Ring,
    /// Reduce-Scatter + All-Gather 组合
    ReduceScatterThenAllGather,
    /// 简化 Tree AllReduce（binary tree，两阶段 up+down）
    Tree,
}

/// Chunk / Channel / Pipeline 配置
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ChunkConfig {
    /// 每个 chunk 的字节数。0 表示不切分（整个 tensor 作为一个 flow）
    pub chunk_size_bytes: u64,
    /// 并行 channel 数（类似 NCCL channel）
    pub num_channels: u32,
    /// Pipeline 阶段数。>1 时允许 send 和 receive 重叠
    pub pipeline_depth: u32,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 0,
            num_channels: 1,
            pipeline_depth: 1,
        }
    }
}

/// 单个集合通信操作描述
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectiveOp {
    /// 操作类型
    pub kind: CollectiveKind,
    /// 使用的算法
    pub algorithm: CollectiveAlgorithm,
    /// 参与节点（通常对应 GPU rank）
    pub nodes: Vec<EntityId>,
    /// 总消息大小（字节）。对于 AllReduce/AllGather 是每个 rank 的 tensor 大小
    pub message_bytes: u64,
    /// chunk / channel / pipeline 配置
    pub chunk_config: ChunkConfig,
}

/// 一个训练迭代
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Iteration {
    /// 迭代序号
    pub iter_id: u32,
    /// 该迭代包含的 collective 序列（按顺序执行）
    pub collectives: Vec<CollectiveOp>,
    /// 可选：forward+backward 产生的计算延迟（ns），在每个 collective 前插入
    pub compute_delay_ns: u64,
}

/// 训练作业
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingJob {
    /// 作业名称
    pub name: String,
    /// 迭代列表
    pub iterations: Vec<Iteration>,
    /// 参与节点（rank -> host 映射）
    pub nodes: Vec<EntityId>,
}

impl TrainingJob {
    /// 将 TrainingJob 展开为扁平 FlowDesc 列表。
    ///
    /// 当前简化假设：
    /// - 同一个 iteration 内的 collective **顺序串行**（前一个完成后才启动下一个）
    /// - 不同 iteration 之间也串行
    /// - 忽略 compute delay（后续可扩展为在 FlowStart 中插入 Compute 事件）
    ///
    /// 返回 `(flows, collective_boundaries, collectives_per_iteration)`：
    /// - `collective_boundaries[i]` = 第 i 个 collective 对应 flow_id 范围 `[start, end)`
    /// - `collectives_per_iteration[j]` = 第 j 个 iteration 包含多少个 collective
    pub fn generate(&self) -> (Vec<FlowDesc>, Vec<(u32, u32)>, Vec<usize>) {
        let mut flows = Vec::new();
        let mut boundaries = Vec::new();
        let mut collectives_per_iteration = Vec::with_capacity(self.iterations.len());
        let mut fid: u32 = 0;
        let mut base_time_ns: u64 = 0;

        for iter in &self.iterations {
            let mut count = 0usize;
            for collective in &iter.collectives {
                let start_fid = fid;
                let cflows = collective.generate(base_time_ns, fid);
                fid += cflows.len() as u32;
                flows.extend(cflows);
                boundaries.push((start_fid, fid));
                count += 1;

                // 简化：该 collective 的完成时间 = 最后一条流的 start_time_ns（后续由 SimRunner 修正为实际完成）
                // 这里用最后一个 flow 的 start_time 作为下一个 collective 的起始时间基准
                base_time_ns = flows.last().map(|f| f.start_time_ns).unwrap_or(base_time_ns);
            }
            collectives_per_iteration.push(count);
        }

        (flows, boundaries, collectives_per_iteration)
    }
}

impl CollectiveOp {
    /// 生成该 collective 对应的 FlowDesc 列表
    ///
    /// `base_time_ns`：该 collective 最早可启动时间
    /// `base_fid`：起始 flow_id
    pub fn generate(&self, base_time_ns: u64, base_fid: u32) -> Vec<FlowDesc> {
        match self.algorithm {
            CollectiveAlgorithm::Ring => self.generate_ring(base_time_ns, base_fid),
            CollectiveAlgorithm::ReduceScatterThenAllGather => {
                self.generate_reduce_scatter_all_gather(base_time_ns, base_fid)
            }
            CollectiveAlgorithm::Tree => self.generate_tree(base_time_ns, base_fid),
        }
    }

    fn generate_ring(&self, base_time_ns: u64, base_fid: u32) -> Vec<FlowDesc> {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return Vec::new();
        }
        let chunk_size = if self.chunk_config.chunk_size_bytes > 0 {
            self.chunk_config.chunk_size_bytes
        } else {
            self.message_bytes / n
        };
        let steps = 2 * (n - 1);
        let mut flows = Vec::new();
        let mut fid = base_fid;

        for step in 0..steps {
            for i in 0..self.nodes.len() {
                let src = self.nodes[i];
                let dst = self.nodes[(i + 1) % self.nodes.len()];
                flows.push(FlowDesc {
                    flow_id: fid,
                    src,
                    dst,
                    bytes: chunk_size,
                    start_time_ns: base_time_ns + (step as u64) * 1000,
                });
                fid += 1;
            }
        }

        flows
    }

    fn generate_reduce_scatter_all_gather(&self, base_time_ns: u64, base_fid: u32) -> Vec<FlowDesc> {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return Vec::new();
        }
        let chunk_size = if self.chunk_config.chunk_size_bytes > 0 {
            self.chunk_config.chunk_size_bytes
        } else {
            self.message_bytes / n
        };
        let mut flows = Vec::new();
        let mut fid = base_fid;

        // Phase 1: Reduce-Scatter（N-1 步 ring）
        let rs_steps = n - 1;
        for step in 0..rs_steps {
            for i in 0..self.nodes.len() {
                let src = self.nodes[i];
                let dst = self.nodes[(i + 1) % self.nodes.len()];
                flows.push(FlowDesc {
                    flow_id: fid,
                    src,
                    dst,
                    bytes: chunk_size,
                    start_time_ns: base_time_ns + (step as u64) * 1000,
                });
                fid += 1;
            }
        }

        // Phase 2: All-Gather（N-1 步 ring）
        let ag_base = base_time_ns + rs_steps * 1000;
        for step in 0..rs_steps {
            for i in 0..self.nodes.len() {
                let src = self.nodes[i];
                let dst = self.nodes[(i + 1) % self.nodes.len()];
                flows.push(FlowDesc {
                    flow_id: fid,
                    src,
                    dst,
                    bytes: chunk_size,
                    start_time_ns: ag_base + (step as u64) * 1000,
                });
                fid += 1;
            }
        }

        flows
    }

    fn generate_tree(&self, base_time_ns: u64, base_fid: u32) -> Vec<FlowDesc> {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return Vec::new();
        }
        let chunk_size = if self.chunk_config.chunk_size_bytes > 0 {
            self.chunk_config.chunk_size_bytes
        } else {
            self.message_bytes / n
        };
        let mut flows = Vec::new();
        let mut fid = base_fid;

        // 简化 binary tree：假设节点数 = 2^k
        // Reduce 阶段：子节点 -> 父节点
        let mut stride = 1u64;
        let mut step = 0u64;
        while stride < n {
            for i in (0..n).step_by((stride * 2) as usize) {
                let child = self.nodes[(i + stride) as usize % self.nodes.len()];
                let parent = self.nodes[i as usize];
                flows.push(FlowDesc {
                    flow_id: fid,
                    src: child,
                    dst: parent,
                    bytes: chunk_size,
                    start_time_ns: base_time_ns + step * 1000,
                });
                fid += 1;
            }
            stride *= 2;
            step += 1;
        }

        // Broadcast 阶段：父节点 -> 子节点
        stride = n / 2;
        while stride >= 1 {
            for i in (0..n).step_by((stride * 2) as usize) {
                let child = self.nodes[(i + stride) as usize % self.nodes.len()];
                let parent = self.nodes[i as usize];
                flows.push(FlowDesc {
                    flow_id: fid,
                    src: parent,
                    dst: child,
                    bytes: chunk_size,
                    start_time_ns: base_time_ns + step * 1000,
                });
                fid += 1;
            }
            stride /= 2;
            step += 1;
        }

        flows
    }
}

/// Training 级别的指标
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct TrainingMetrics {
    pub job_name: String,
    pub total_iterations: u32,
    pub completed_iterations: u32,
    /// 每个 iteration 的完成时间（ns）
    pub iteration_times_ns: Vec<u64>,
    /// 每个 collective 的完成时间（ns），索引对应 boundaries
    pub collective_completion_ns: Vec<u64>,
    /// 每个 collective 的算法和类型（用于诊断）
    pub collective_labels: Vec<String>,
}

impl TrainingMetrics {
    pub fn pretty_print(&self) {
        println!("┌─────────── Training 指标 [{}] ────────────", self.job_name);
        println!("│ 总迭代数            {}", self.total_iterations);
        println!("│ 完成迭代数          {}", self.completed_iterations);
        if !self.iteration_times_ns.is_empty() {
            let total: u64 = self.iteration_times_ns.iter().sum();
            let avg = total as f64 / self.iteration_times_ns.len() as f64;
            let min = self.iteration_times_ns.iter().min().copied().unwrap_or(0);
            let max = self.iteration_times_ns.iter().max().copied().unwrap_or(0);
            println!("│ Iteration Time 平均 {:.3} ms", avg / 1e6);
            println!("│ Iteration Time 最小 {:.3} ms", min as f64 / 1e6);
            println!("│ Iteration Time 最大 {:.3} ms", max as f64 / 1e6);
        }
        for (i, (t, label)) in self.collective_completion_ns.iter().zip(&self.collective_labels).enumerate() {
            println!("│ Collective {:>2}      {:>10.3} ms  {}", i, *t as f64 / 1e6, label);
        }
        println!("└──────────────────────────────────────────");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_allreduce_flow_count() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Ring,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 1024 * 1024,
            chunk_config: ChunkConfig::default(),
        };
        let flows = op.generate(0, 0);
        let n = 4;
        let expected = 2 * (n - 1) * n;
        assert_eq!(flows.len(), expected);
    }

    #[test]
    fn reduce_scatter_all_gather_flow_count() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::ReduceScatterThenAllGather,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 1024 * 1024,
            chunk_config: ChunkConfig::default(),
        };
        let flows = op.generate(0, 0);
        let n = 4;
        let expected = 2 * (n - 1) * n;
        assert_eq!(flows.len(), expected);
    }

    #[test]
    fn tree_flow_count_for_power_of_two() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Tree,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 1024 * 1024,
            chunk_config: ChunkConfig::default(),
        };
        let flows = op.generate(0, 0);
        // binary tree: reduce + broadcast = 2*(n-1) flows
        let n = 4;
        assert_eq!(flows.len(), 2 * (n - 1));
    }

    #[test]
    fn training_job_generates_flows() {
        let job = TrainingJob {
            name: "test_job".to_string(),
            nodes: vec![0, 1, 2, 3],
            iterations: vec![
                Iteration {
                    iter_id: 0,
                    compute_delay_ns: 0,
                    collectives: vec![
                        CollectiveOp {
                            kind: CollectiveKind::AllReduce,
                            algorithm: CollectiveAlgorithm::Ring,
                            nodes: vec![0, 1, 2, 3],
                            message_bytes: 1024 * 1024,
                            chunk_config: ChunkConfig::default(),
                        },
                    ],
                },
            ],
        };
        let (flows, boundaries, collectives_per_iteration) = job.generate();
        assert!(!flows.is_empty());
        assert_eq!(boundaries.len(), 1);
        assert_eq!(boundaries[0].0, 0);
        assert_eq!(boundaries[0].1, flows.len() as u32);
        assert_eq!(collectives_per_iteration, vec![1]);
    }

    #[test]
    fn chunk_size_respected() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Ring,
            nodes: vec![0, 1, 2],
            message_bytes: 1024 * 1024,
            chunk_config: ChunkConfig {
                chunk_size_bytes: 4096,
                num_channels: 1,
                pipeline_depth: 1,
            },
        };
        let flows = op.generate(0, 0);
        assert!(flows.iter().all(|f| f.bytes == 4096));
    }
}
