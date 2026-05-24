//! 训练 DAG 与 Compute-Communication Overlap 模型
//!
//! P4 扩展：在 TrainingJob 基础上支持：
//! - 计算-通信重叠：Forward/Backward 计算与 AllReduce 通信并行
//! - Collective DAG：AllReduce → AllGather → Barrier 的依赖关系
//! - Pipeline Bubble：PP（Pipeline Parallelism）的空泡建模
//!
//! 模型简化：用时间偏移模拟重叠，不引入真实 GPU 计算模拟。

use crate::training::{CollectiveOp, Iteration};
use crate::EntityId;

/// 训练阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrainingPhase {
    Forward,
    Backward,
    AllReduce,
    AllGather,
    Barrier,
}

/// DAG 节点：一个计算或通信任务
#[derive(Debug, Clone)]
pub struct DagNode {
    pub id: u32,
    pub phase: TrainingPhase,
    /// 依赖的前驱节点 ID
    pub depends_on: Vec<u32>,
    /// 计算/通信持续时间（ns），0 表示由 collective 展开决定
    pub duration_ns: u64,
    /// 关联的 collective（仅通信节点）
    pub collective: Option<CollectiveOp>,
    /// 是否与下一个阶段重叠
    pub overlap_next: bool,
}

/// 训练 DAG
#[derive(Debug, Clone)]
pub struct TrainingDag {
    pub iteration_id: u32,
    pub nodes: Vec<DagNode>,
    /// 用于 overlap 的通信偏移时间（ns）
    pub comm_offset_ns: u64,
}

/// 将 Iteration 展开为带重叠的 DAG
pub fn build_dag(iter: &Iteration, compute_ns: u64, overlap_ratio: f64) -> TrainingDag {
    let mut nodes = Vec::new();
    let mut node_id = 0u32;

    // Forward pass（计算）
    let fwd_id = node_id;
    node_id += 1;
    nodes.push(DagNode {
        id: fwd_id,
        phase: TrainingPhase::Forward,
        depends_on: vec![],
        duration_ns: compute_ns / 2, // forward ≈ half of compute
        collective: None,
        overlap_next: overlap_ratio > 0.0,
    });

    // Backward pass（计算，依赖 forward）
    let bwd_id = node_id;
    node_id += 1;
    nodes.push(DagNode {
        id: bwd_id,
        phase: TrainingPhase::Backward,
        depends_on: vec![fwd_id],
        duration_ns: compute_ns / 2,
        collective: None,
        overlap_next: overlap_ratio > 0.0,
    });

    // Collectives（通信，可以与 backward 重叠）
    let comm_offset = if overlap_ratio > 0.0 {
        (compute_ns as f64 * (1.0 - overlap_ratio)) as u64
    } else {
        0
    };

    for (i, collective) in iter.collectives.iter().enumerate() {
        let prev_id = if i == 0 { bwd_id } else { node_id - 1 };
        let cid = node_id;
        node_id += 1;
        let phase = match collective.kind {
            crate::training::CollectiveKind::AllReduce => TrainingPhase::AllReduce,
            crate::training::CollectiveKind::AllGather => TrainingPhase::AllGather,
            crate::training::CollectiveKind::ReduceScatter => TrainingPhase::AllReduce,
            crate::training::CollectiveKind::AllToAll => TrainingPhase::AllGather,
        };
        nodes.push(DagNode {
            id: cid,
            phase,
            depends_on: vec![prev_id],
            duration_ns: 0, // 由 collective 展开决定
            collective: Some(collective.clone()),
            overlap_next: false,
        });
    }

    TrainingDag {
        iteration_id: iter.iter_id,
        nodes,
        comm_offset_ns: comm_offset,
    }
}

/// 计算 compute-communication overlap 后的流时间偏移
/// overlap_ratio=0 表示完全串行，=0.5 表示计算和通信重叠 50%
pub fn apply_overlap(flows: &mut [(u64, u64)], compute_ns: u64, overlap_ratio: f64) {
    if overlap_ratio <= 0.0 || compute_ns == 0 {
        return;
    }
    let offset = (compute_ns as f64 * (1.0 - overlap_ratio)) as u64;
    for (start_time, _size) in flows.iter_mut() {
        *start_time = start_time.saturating_sub(offset);
    }
}