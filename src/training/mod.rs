//! 训练 Workload 抽象（P2）

pub mod dag;

pub use dag::{TrainingDag, DagNode, TrainingPhase, build_dag, apply_overlap};
///
/// 提供 `TrainingJob`、`Iteration`、`CollectiveOp` 等高层语义，
/// 用于模拟分布式训练中的 collective communication 模式。
///
/// 核心概念：
/// - `TrainingJob`：一次完整训练，包含多个 `Iteration`
/// - `Iteration`：一个训练迭代，包含 forward + backward + collective 序列
/// - `CollectiveOp`：集合通信操作（AllReduce / ReduceScatter / AllGather / AllToAll）
/// - `CollectiveAlgorithm`：实现算法（Ring / ReduceScatter+AllGather / Tree）
/// - `ChunkConfig`：chunk 大小、channel 数、pipeline 深度

use crate::traffic::FlowDesc;
use crate::EntityId;
use serde::{Deserialize, Serialize};

const STEP_SPACING_NS: u64 = 1_000;

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

/// 单个 collective 展开后的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectivePlan {
    /// 所属 iteration 序号
    pub iter_id: u32,
    /// 操作类型
    pub kind: CollectiveKind,
    /// 使用的算法
    pub algorithm: CollectiveAlgorithm,
    /// 该 collective 对应的 flow_id 范围 `[start, end)`
    pub flow_range: (u32, u32),
    /// 逻辑计划开始时间（ns）
    pub planned_start_ns: u64,
    /// 逻辑计划结束时间（ns）
    pub planned_end_ns: u64,
}

/// TrainingJob 展开后的完整计划
#[derive(Debug, Clone, Default)]
pub struct TrainingPlan {
    /// 展开后的底层 flow 列表
    pub flows: Vec<FlowDesc>,
    /// 每个 collective 的边界和计划时间
    pub collectives: Vec<CollectivePlan>,
    /// 每个 iteration 包含多少个 collective
    pub collectives_per_iteration: Vec<usize>,
}

impl TrainingJob {
    /// 将 TrainingJob 展开为扁平 FlowDesc 列表。
    ///
    /// 当前模型是静态展开，不在运行时根据前一个 collective 的真实完成时间动态调度。
    /// 但展开时会保留顺序语义：
    /// - 每个 iteration 开始前加入 `compute_delay_ns`
    /// - 同一个 iteration 内的 collective 按生成出的逻辑结束时间串行
    /// - 不同 iteration 之间也按逻辑结束时间串行
    ///
    /// 返回 `(flows, collective_boundaries, collectives_per_iteration)`：
    /// - `collective_boundaries[i]` = 第 i 个 collective 对应 flow_id 范围 `[start, end)`
    /// - `collectives_per_iteration[j]` = 第 j 个 iteration 包含多少个 collective
    pub fn generate(&self) -> (Vec<FlowDesc>, Vec<(u32, u32)>, Vec<usize>) {
        let plan = self.plan();
        let boundaries = plan.collectives.iter().map(|c| c.flow_range).collect();
        (plan.flows, boundaries, plan.collectives_per_iteration)
    }

    /// 将 TrainingJob 展开为带元数据的训练计划。
    pub fn plan(&self) -> TrainingPlan {
        let mut flows = Vec::new();
        let mut collectives = Vec::new();
        let mut collectives_per_iteration = Vec::with_capacity(self.iterations.len());
        let mut fid: u32 = 0;
        let mut base_time_ns: u64 = 0;

        for iter in &self.iterations {
            base_time_ns = base_time_ns.saturating_add(iter.compute_delay_ns);
            let mut count = 0usize;
            for collective in &iter.collectives {
                let start_fid = fid;
                let (cflows, planned_end_ns) = collective.generate_with_end(base_time_ns, fid);
                fid += cflows.len() as u32;
                flows.extend(cflows);
                collectives.push(CollectivePlan {
                    iter_id: iter.iter_id,
                    kind: collective.kind,
                    algorithm: collective.algorithm,
                    flow_range: (start_fid, fid),
                    planned_start_ns: base_time_ns,
                    planned_end_ns,
                });
                count += 1;
                base_time_ns = planned_end_ns;
            }
            collectives_per_iteration.push(count);
        }

        TrainingPlan {
            flows,
            collectives,
            collectives_per_iteration,
        }
    }
}

impl CollectiveOp {
    /// 生成该 collective 对应的 FlowDesc 列表
    ///
    /// `base_time_ns`：该 collective 最早可启动时间
    /// `base_fid`：起始 flow_id
    pub fn generate(&self, base_time_ns: u64, base_fid: u32) -> Vec<FlowDesc> {
        self.generate_with_end(base_time_ns, base_fid).0
    }

    /// 生成该 collective 对应的 flow，并返回逻辑计划结束时间。
    pub fn generate_with_end(&self, base_time_ns: u64, base_fid: u32) -> (Vec<FlowDesc>, u64) {
        match self.algorithm {
            CollectiveAlgorithm::Ring => {
                let n = self.nodes.len() as u64;
                self.generate_ring_phases(
                    base_time_ns,
                    base_fid,
                    &[n.saturating_sub(1), n.saturating_sub(1)],
                )
            }
            CollectiveAlgorithm::ReduceScatterThenAllGather => {
                self.generate_reduce_scatter_all_gather(base_time_ns, base_fid)
            }
            CollectiveAlgorithm::Tree => self.generate_tree(base_time_ns, base_fid),
        }
    }

    fn generate_ring_phases(
        &self,
        base_time_ns: u64,
        base_fid: u32,
        phase_steps: &[u64],
    ) -> (Vec<FlowDesc>, u64) {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return (Vec::new(), base_time_ns);
        }
        let payload_bytes = self.payload_bytes(n);
        let chunk_size = self.chunk_bytes(payload_bytes);
        let channels = self.channels();
        let chunks = self.chunks_per_edge(payload_bytes, chunk_size);
        let step_spacing = self.step_spacing_ns();
        let mut flows = Vec::new();
        let mut fid = base_fid;
        let mut step_offset = 0u64;
        let mut last_start = base_time_ns;

        for &steps in phase_steps {
            for step in 0..steps {
                for chunk_idx in 0..chunks {
                    let wave = chunk_idx / (channels as u64);
                    let start_time_ns = base_time_ns + (step_offset + step + wave) * step_spacing;
                    last_start = last_start.max(start_time_ns);
                    let bytes = self.chunk_bytes_for(chunk_idx, payload_bytes, chunk_size);
                    for i in 0..self.nodes.len() {
                        let src = self.nodes[i];
                        let dst = self.nodes[(i + 1) % self.nodes.len()];
                        flows.push(FlowDesc {
                            flow_id: fid,
                            src,
                            dst,
                            bytes,
                            start_time_ns,
                        });
                        fid += 1;
                    }
                }
            }
            step_offset += self.phase_advance_steps(steps, chunks);
        }

        (flows, last_start.saturating_add(step_spacing))
    }

    fn generate_reduce_scatter_all_gather(
        &self,
        base_time_ns: u64,
        base_fid: u32,
    ) -> (Vec<FlowDesc>, u64) {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return (Vec::new(), base_time_ns);
        }
        self.generate_ring_phases(base_time_ns, base_fid, &[n - 1, n - 1])
    }

    fn generate_tree(&self, base_time_ns: u64, base_fid: u32) -> (Vec<FlowDesc>, u64) {
        let n = self.nodes.len() as u64;
        if n < 2 {
            return (Vec::new(), base_time_ns);
        }
        let payload_bytes = self.payload_bytes(n);
        let chunk_size = self.chunk_bytes(payload_bytes);
        let channels = self.channels();
        let chunks = self.chunks_per_edge(payload_bytes, chunk_size);
        let step_spacing = self.step_spacing_ns();
        let mut flows = Vec::new();
        let mut fid = base_fid;
        let mut step = 0u64;
        let mut last_start = base_time_ns;

        // 简化 binary tree：假设节点数 = 2^k
        // Reduce 阶段：子节点 -> 父节点
        let mut stride = 1u64;
        while stride < n {
            for chunk_idx in 0..chunks {
                let wave = chunk_idx / (channels as u64);
                let start_time_ns = base_time_ns + (step + wave) * step_spacing;
                last_start = last_start.max(start_time_ns);
                let bytes = self.chunk_bytes_for(chunk_idx, payload_bytes, chunk_size);
                for i in (0..n).step_by((stride * 2) as usize) {
                    let child = self.nodes[(i + stride) as usize % self.nodes.len()];
                    let parent = self.nodes[i as usize];
                    flows.push(FlowDesc {
                        flow_id: fid,
                        src: child,
                        dst: parent,
                        bytes,
                        start_time_ns,
                    });
                    fid += 1;
                }
            }
            stride *= 2;
            step += self.phase_advance_steps(1, chunks);
        }

        // Broadcast 阶段：父节点 -> 子节点
        stride = n / 2;
        while stride >= 1 {
            for chunk_idx in 0..chunks {
                let wave = chunk_idx / (channels as u64);
                let start_time_ns = base_time_ns + (step + wave) * step_spacing;
                last_start = last_start.max(start_time_ns);
                let bytes = self.chunk_bytes_for(chunk_idx, payload_bytes, chunk_size);
                for i in (0..n).step_by((stride * 2) as usize) {
                    let child = self.nodes[(i + stride) as usize % self.nodes.len()];
                    let parent = self.nodes[i as usize];
                    flows.push(FlowDesc {
                        flow_id: fid,
                        src: parent,
                        dst: child,
                        bytes,
                        start_time_ns,
                    });
                    fid += 1;
                }
            }
            stride /= 2;
            step += self.phase_advance_steps(1, chunks);
        }

        (flows, last_start.saturating_add(step_spacing))
    }

    fn payload_bytes(&self, n: u64) -> u64 {
        self.message_bytes.div_ceil(n).max(1)
    }

    fn chunk_bytes(&self, payload_bytes: u64) -> u64 {
        if self.chunk_config.chunk_size_bytes > 0 {
            self.chunk_config.chunk_size_bytes.min(payload_bytes).max(1)
        } else {
            payload_bytes
        }
    }

    fn chunk_bytes_for(&self, chunk_idx: u64, payload_bytes: u64, chunk_size: u64) -> u64 {
        let remaining = payload_bytes.saturating_sub(chunk_idx.saturating_mul(chunk_size));
        remaining.min(chunk_size).max(1)
    }

    fn chunks_per_edge(&self, payload_bytes: u64, chunk_size: u64) -> u64 {
        if self.chunk_config.chunk_size_bytes == 0 {
            1
        } else {
            payload_bytes.div_ceil(chunk_size).max(1)
        }
    }

    fn channels(&self) -> u32 {
        self.chunk_config.num_channels.max(1)
    }

    fn pipeline_depth(&self) -> u32 {
        self.chunk_config.pipeline_depth.max(1)
    }

    fn step_spacing_ns(&self) -> u64 {
        (STEP_SPACING_NS / (self.pipeline_depth() as u64)).max(1)
    }

    fn phase_advance_steps(&self, steps: u64, chunks: u64) -> u64 {
        let waves = chunks.div_ceil(self.channels() as u64).max(1);
        if self.pipeline_depth() > 1 {
            steps + waves.saturating_sub(1)
        } else {
            steps.saturating_mul(waves)
        }
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
    /// 每个 collective 的计划持续时间（ns）
    pub collective_planned_ns: Vec<u64>,
    /// 每个 collective 是否全部 flow 完成
    pub collective_completed: Vec<bool>,
    /// 每个 collective 的算法和类型（用于诊断）
    pub collective_labels: Vec<String>,
}

impl TrainingMetrics {
    pub fn pretty_print(&self) {
        println!(
            "┌─────────── Training 指标 [{}] ────────────",
            self.job_name
        );
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
        for (i, (t, label)) in self
            .collective_completion_ns
            .iter()
            .zip(&self.collective_labels)
            .enumerate()
        {
            let planned = self.collective_planned_ns.get(i).copied().unwrap_or(0);
            let done = self.collective_completed.get(i).copied().unwrap_or(false);
            println!(
                "│ Collective {:>2}      {:>10.3} ms  planned={:>8.3} ms  done={}  {}",
                i,
                *t as f64 / 1e6,
                planned as f64 / 1e6,
                done,
                label
            );
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
            iterations: vec![Iteration {
                iter_id: 0,
                compute_delay_ns: 0,
                collectives: vec![CollectiveOp {
                    kind: CollectiveKind::AllReduce,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes: vec![0, 1, 2, 3],
                    message_bytes: 1024 * 1024,
                    chunk_config: ChunkConfig::default(),
                }],
            }],
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
            message_bytes: 12 * 1024,
            chunk_config: ChunkConfig {
                chunk_size_bytes: 4096,
                num_channels: 1,
                pipeline_depth: 1,
            },
        };
        let flows = op.generate(0, 0);
        assert!(flows.iter().all(|f| f.bytes == 4096));
    }

    #[test]
    fn explicit_chunking_increases_flow_count() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Ring,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 16 * 1024,
            chunk_config: ChunkConfig {
                chunk_size_bytes: 1024,
                num_channels: 1,
                pipeline_depth: 1,
            },
        };
        let flows = op.generate(0, 0);
        let n = 4;
        let steps = 2 * (n - 1);
        let chunks_per_edge = 4;
        assert_eq!(flows.len(), steps * n * chunks_per_edge);
    }

    #[test]
    fn channels_overlap_chunks_in_same_wave() {
        let op = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Ring,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 16 * 1024,
            chunk_config: ChunkConfig {
                chunk_size_bytes: 1024,
                num_channels: 2,
                pipeline_depth: 1,
            },
        };
        let flows = op.generate(10_000, 0);
        let starts: Vec<u64> = flows.iter().take(8).map(|f| f.start_time_ns).collect();
        assert_eq!(starts, vec![10_000; 8]);
        assert_eq!(flows[8].start_time_ns, 11_000);
    }

    #[test]
    fn pipeline_depth_shortens_planned_duration() {
        let base = CollectiveOp {
            kind: CollectiveKind::AllReduce,
            algorithm: CollectiveAlgorithm::Ring,
            nodes: vec![0, 1, 2, 3],
            message_bytes: 16 * 1024,
            chunk_config: ChunkConfig {
                chunk_size_bytes: 1024,
                num_channels: 1,
                pipeline_depth: 1,
            },
        };
        let mut pipelined = base.clone();
        pipelined.chunk_config.pipeline_depth = 2;

        let (_, end_base) = base.generate_with_end(0, 0);
        let (_, end_pipelined) = pipelined.generate_with_end(0, 0);
        assert!(end_pipelined < end_base);
    }

    #[test]
    fn compute_delay_moves_iteration_start() {
        let job = TrainingJob {
            name: "test_delay".to_string(),
            nodes: vec![0, 1, 2, 3],
            iterations: vec![Iteration {
                iter_id: 0,
                compute_delay_ns: 50_000,
                collectives: vec![CollectiveOp {
                    kind: CollectiveKind::AllReduce,
                    algorithm: CollectiveAlgorithm::Ring,
                    nodes: vec![0, 1, 2, 3],
                    message_bytes: 1024 * 1024,
                    chunk_config: ChunkConfig::default(),
                }],
            }],
        };
        let plan = job.plan();
        assert_eq!(plan.collectives[0].planned_start_ns, 50_000);
        assert!(plan.flows.iter().all(|f| f.start_time_ns >= 50_000));
    }
}
