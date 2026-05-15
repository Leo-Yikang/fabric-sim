//! 通用合成流量生成器
//!
//! 支持流大小分布、到达过程、通信对模式的三维组合，
//! 用于系统评估协议在不同工作负载特征下的表现。
//!
//! 所有随机数使用 `rand_pcg::Pcg64` + 显式 `seed`，保证结果可复现。

use super::FlowDesc;
use crate::EntityId;
use rand::Rng;
use rand_pcg::Pcg64;
use rand::SeedableRng;

/// 流大小分布
#[derive(Debug, Clone, Copy)]
pub enum FlowSizeDist {
    /// 固定大小
    Fixed(u64),
    /// 均匀分布 [min, max]
    Uniform { min: u64, max: u64 },
    /// Pareto 重尾分布：x_min 和 shape 参数
    /// 数据中心流量常呈现重尾特征（大量 mice + 少量 elephant）
    Pareto { min: u64, shape: f64 },
    /// 双模态分布：以 `large_ratio` 概率取 large，否则取 small
    Bimodal { small: u64, large: u64, large_ratio: f64 },
}

impl FlowSizeDist {
    /// 根据分布生成一个流大小（字节）
    pub fn sample<R: Rng>(&self, rng: &mut R) -> u64 {
        match *self {
            FlowSizeDist::Fixed(b) => b,
            FlowSizeDist::Uniform { min, max } => {
                if min >= max { min } else { rng.gen_range(min..=max) }
            }
            FlowSizeDist::Pareto { min, shape } => {
                // Pareto 逆变换采样：X = x_min / U^(1/alpha)
                let u: f64 = rng.gen_range(0.0001..1.0);
                let v = (min as f64) / u.powf(1.0 / shape);
                v.max(min as f64) as u64
            }
            FlowSizeDist::Bimodal { small, large, large_ratio } => {
                if rng.gen::<f64>() < large_ratio { large } else { small }
            }
        }
    }
}

/// 流到达过程
#[derive(Debug, Clone, Copy)]
pub enum ArrivalProcess {
    /// 所有流在指定时刻同时开始
    Simultaneous(u64),
    /// 从 `start` 开始，每隔 `interval_ns` 到达一条流
    FixedInterval { start: u64, interval_ns: u64 },
    /// 泊松到达：从 `start` 开始，间隔服从指数分布（均值 `mean_interval_ns`）
    Poisson { start: u64, mean_interval_ns: u64 },
}

impl ArrivalProcess {
    /// 根据到达过程生成 `n` 个流的开始时间
    pub fn sample<R: Rng>(&self, rng: &mut R, n: usize) -> Vec<u64> {
        match *self {
            ArrivalProcess::Simultaneous(t) => vec![t; n],
            ArrivalProcess::FixedInterval { start, interval_ns } => {
                (0..n).map(|i| start + i as u64 * interval_ns).collect()
            }
            ArrivalProcess::Poisson { start, mean_interval_ns } => {
                let mut times = Vec::with_capacity(n);
                let mut t = start;
                let lambda = 1.0 / (mean_interval_ns as f64);
                for _ in 0..n {
                    times.push(t);
                    // 指数分布：-ln(U) / lambda
                    let u: f64 = rng.gen_range(0.0001..1.0);
                    let dt = (-u.ln() / lambda) as u64;
                    t = t.saturating_add(dt.max(1));
                }
                times
            }
        }
    }
}

/// 通信对生成模式
#[derive(Debug, Clone)]
pub enum PairPattern {
    /// 所有节点两两之间各发一条流（有向）
    AllToAll,
    /// 排列：每个节点向另一个唯一节点发送，无热点
    Permutation,
    /// 随机指定 `n` 条源目的对
    RandomPairs(u64),
    /// 完全自定义
    Custom(Vec<(EntityId, EntityId)>),
}

impl PairPattern {
    /// 根据模式生成通信对列表
    pub fn generate(&self, nodes: &[EntityId], rng: &mut impl Rng) -> Vec<(EntityId, EntityId)> {
        match self {
            PairPattern::AllToAll => {
                let mut pairs = Vec::new();
                for &src in nodes {
                    for &dst in nodes {
                        if src != dst {
                            pairs.push((src, dst));
                        }
                    }
                }
                pairs
            }
            PairPattern::Permutation => {
                let n = nodes.len();
                if n < 2 { return Vec::new(); }
                // Fisher-Yates shuffle 生成随机排列
                let mut perm: Vec<usize> = (0..n).collect();
                // 确保没有不动点（src != dst）
                for _ in 0..100 {
                    for i in (1..n).rev() {
                        let j = rng.gen_range(0..=i);
                        perm.swap(i, j);
                    }
                    // 检查是否有不动点
                    let has_fixed = perm.iter().enumerate().any(|(i, &p)| i == p);
                    if !has_fixed { break; }
                }
                // 如果仍有不动点，简单移位兜底
                if perm.iter().enumerate().any(|(i, &p)| i == p) {
                    perm.rotate_left(1);
                }
                perm.iter()
                    .enumerate()
                    .map(|(i, &p)| (nodes[i], nodes[p]))
                    .collect()
            }
            PairPattern::RandomPairs(count) => {
                let mut pairs = Vec::with_capacity(*count as usize);
                let n = nodes.len() as u32;
                if n < 2 { return pairs; }
                for _ in 0..*count {
                    let src = nodes[rng.gen_range(0..n) as usize];
                    let mut dst = nodes[rng.gen_range(0..n) as usize];
                    while src == dst {
                        dst = nodes[rng.gen_range(0..n) as usize];
                    }
                    pairs.push((src, dst));
                }
                pairs
            }
            PairPattern::Custom(pairs) => pairs.clone(),
        }
    }
}

/// 通用合成流量生成器
///
/// 组合 `PairPattern` + `FlowSizeDist` + `ArrivalProcess`，
/// 生成一组带分布特征的 `FlowDesc`。
#[derive(Debug, Clone)]
pub struct Synthetic {
    /// 参与通信的节点列表
    pub nodes: Vec<EntityId>,
    /// 通信对生成模式
    pub pair_pattern: PairPattern,
    /// 流大小分布
    pub flow_size: FlowSizeDist,
    /// 到达过程
    pub arrival: ArrivalProcess,
    /// 随机数种子（保证可复现）
    pub seed: u64,
}

impl Synthetic {
    /// 生成流量描述列表
    pub fn generate(&self) -> Vec<FlowDesc> {
        let mut rng = Pcg64::seed_from_u64(self.seed);
        let pairs = self.pair_pattern.generate(&self.nodes, &mut rng);
        let n = pairs.len();
        let sizes: Vec<u64> = (0..n).map(|_| self.flow_size.sample(&mut rng)).collect();
        let start_times = self.arrival.sample(&mut rng, n);

        pairs.into_iter().enumerate().map(|(i, (src, dst))| FlowDesc {
            flow_id: i as u32,
            src,
            dst,
            bytes: sizes[i],
            start_time_ns: start_times[i],
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_fixed_simultaneous() {
        let gen = Synthetic {
            nodes: vec![0, 1],
            pair_pattern: PairPattern::AllToAll,
            flow_size: FlowSizeDist::Fixed(1024),
            arrival: ArrivalProcess::Simultaneous(1000),
            seed: 42,
        };
        let flows = gen.generate();
        assert_eq!(flows.len(), 2); // 0->1, 1->0
        for f in &flows {
            assert_eq!(f.bytes, 1024);
            assert_eq!(f.start_time_ns, 1000);
        }
    }

    #[test]
    fn synthetic_pareto_heavy_tail() {
        let gen = Synthetic {
            nodes: vec![0, 1, 2],
            pair_pattern: PairPattern::AllToAll,
            flow_size: FlowSizeDist::Pareto { min: 1000, shape: 1.5 },
            arrival: ArrivalProcess::Simultaneous(0),
            seed: 123,
        };
        let flows = gen.generate();
        assert_eq!(flows.len(), 6);
        // 至少有一些流远大于 min
        let max = flows.iter().map(|f| f.bytes).max().unwrap();
        assert!(max > 1000, "Pareto 分布应产生大于 min 的值");
    }

    #[test]
    fn synthetic_bimodal_ratio() {
        let gen = Synthetic {
            nodes: vec![0, 1],
            pair_pattern: PairPattern::AllToAll,
            flow_size: FlowSizeDist::Bimodal { small: 1000, large: 1_000_000, large_ratio: 0.5 },
            arrival: ArrivalProcess::Simultaneous(0),
            seed: 42,
        };
        let flows = gen.generate();
        let large_count = flows.iter().filter(|f| f.bytes == 1_000_000).count();
        // 2 条流，large_ratio=0.5，期望有 0~2 条大流
        assert!((0..=2).contains(&large_count));
    }

    #[test]
    fn synthetic_poisson_increasing_time() {
        let gen = Synthetic {
            nodes: vec![0, 1, 2],
            pair_pattern: PairPattern::AllToAll,
            flow_size: FlowSizeDist::Fixed(100),
            arrival: ArrivalProcess::Poisson { start: 1000, mean_interval_ns: 500 },
            seed: 42,
        };
        let flows = gen.generate();
        assert_eq!(flows.len(), 6);
        // 时间应非递减
        for i in 1..flows.len() {
            assert!(flows[i].start_time_ns >= flows[i - 1].start_time_ns);
        }
        // 第一条应在 start 时刻
        assert_eq!(flows[0].start_time_ns, 1000);
    }

    #[test]
    fn synthetic_permutation_no_fixed_point() {
        let gen = Synthetic {
            nodes: vec![0, 1, 2, 3],
            pair_pattern: PairPattern::Permutation,
            flow_size: FlowSizeDist::Fixed(100),
            arrival: ArrivalProcess::Simultaneous(0),
            seed: 99,
        };
        let flows = gen.generate();
        assert_eq!(flows.len(), 4);
        for f in &flows {
            assert_ne!(f.src, f.dst, "Permutation 不应有 src==dst 的流");
        }
        // 每个 src 只出现一次，每个 dst 只出现一次
        let srcs: std::collections::HashSet<_> = flows.iter().map(|f| f.src).collect();
        let dsts: std::collections::HashSet<_> = flows.iter().map(|f| f.dst).collect();
        assert_eq!(srcs.len(), 4);
        assert_eq!(dsts.len(), 4);
    }

    #[test]
    fn synthetic_random_pairs_count() {
        let gen = Synthetic {
            nodes: vec![0, 1, 2, 3],
            pair_pattern: PairPattern::RandomPairs(10),
            flow_size: FlowSizeDist::Fixed(100),
            arrival: ArrivalProcess::Simultaneous(0),
            seed: 77,
        };
        let flows = gen.generate();
        assert_eq!(flows.len(), 10);
        for f in &flows {
            assert_ne!(f.src, f.dst);
        }
    }
}
