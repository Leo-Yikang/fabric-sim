//! 排列流量（Permutation Traffic）
//!
//! 数据中心和 HPC 场景中常见的基准流量模式：
//! 每个节点向另一个固定节点发送数据，无热点、负载均衡。
//!
//! 支持多种排列构造方式：
//! - 随机排列（Fisher-Yates）
//! - 循环移位
//! - 位翻转（Bit Reversal，HPC 常用）

use super::FlowDesc;
use crate::EntityId;
use rand::Rng;
use rand_pcg::Pcg64;
use rand::SeedableRng;

/// 排列类型
#[derive(Debug, Clone, Copy)]
pub enum PermuteKind {
    /// 随机排列（Fisher-Yates shuffle，保证无不动点）
    Random,
    /// 循环移位：dst = (src + shift) % n
    Shift(u32),
    /// 位翻转：假设 n 是 2 的幂，dst = bit_reverse(src)
    BitReversal,
}

/// 排列流量生成器
#[derive(Debug, Clone)]
pub struct Permutation {
    pub nodes: Vec<EntityId>,
    pub bytes_per_flow: u64,
    pub start_time_ns: u64,
    pub kind: PermuteKind,
    /// 仅当 `kind == Random` 时使用
    pub seed: u64,
}

impl Permutation {
    /// 生成流量描述
    pub fn generate(&self) -> Vec<FlowDesc> {
        let n = self.nodes.len();
        if n < 2 {
            return Vec::new();
        }

        let mapping: Vec<usize> = match self.kind {
            PermuteKind::Random => {
                let mut rng = Pcg64::seed_from_u64(self.seed);
                let mut perm: Vec<usize> = (0..n).collect();
                // Fisher-Yates shuffle
                for i in (1..n).rev() {
                    let j = rng.gen_range(0..=i);
                    perm.swap(i, j);
                }
                // 确保无不动点
                if perm.iter().enumerate().any(|(i, &p)| i == p) {
                    // 尝试重新 shuffle 最多 100 次
                    let mut found = false;
                    for _ in 0..100 {
                        for i in (1..n).rev() {
                            let j = rng.gen_range(0..=i);
                            perm.swap(i, j);
                        }
                        if !perm.iter().enumerate().any(|(i, &p)| i == p) {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        // 兜底：循环移位
                        perm.rotate_left(1);
                    }
                }
                perm
            }
            PermuteKind::Shift(shift) => {
                let s = shift as usize % n;
                (0..n).map(|i| (i + s) % n).collect()
            }
            PermuteKind::BitReversal => {
                // 找到不小于 n 的最小 2 的幂
                let bits = (n as u32).next_power_of_two().trailing_zeros();
                let mut mapping: Vec<usize> = (0..n)
                    .map(|i| {
                        let reversed = i.reverse_bits() >> (usize::BITS - bits);
                        // 如果翻转后超出范围，回退到循环移位
                        if reversed >= n { (i + 1) % n } else { reversed }
                    })
                    .collect();
                // bit-reversal 在 0 等位置可能出现不动点，兜底做一次移位
                if mapping.iter().enumerate().any(|(i, &p)| i == p) {
                    mapping.rotate_left(1);
                }
                mapping
            }
        };

        mapping
            .into_iter()
            .enumerate()
            .map(|(i, dst_idx)| FlowDesc {
                flow_id: i as u32,
                src: self.nodes[i],
                dst: self.nodes[dst_idx],
                bytes: self.bytes_per_flow,
                start_time_ns: self.start_time_ns,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permute_shift_basic() {
        let p = Permutation {
            nodes: vec![10, 20, 30, 40],
            bytes_per_flow: 1024,
            start_time_ns: 1000,
            kind: PermuteKind::Shift(1),
            seed: 0,
        };
        let flows = p.generate();
        assert_eq!(flows.len(), 4);
        assert_eq!(flows[0].dst, 20);
        assert_eq!(flows[1].dst, 30);
        assert_eq!(flows[2].dst, 40);
        assert_eq!(flows[3].dst, 10);
    }

    #[test]
    fn permute_random_no_fixed_point() {
        let p = Permutation {
            nodes: vec![0, 1, 2, 3, 4, 5, 6, 7],
            bytes_per_flow: 100,
            start_time_ns: 0,
            kind: PermuteKind::Random,
            seed: 42,
        };
        let flows = p.generate();
        assert_eq!(flows.len(), 8);
        for f in &flows {
            assert_ne!(f.src, f.dst);
        }
        // 每个 dst 唯一
        let dsts: std::collections::HashSet<_> = flows.iter().map(|f| f.dst).collect();
        assert_eq!(dsts.len(), 8);
    }

    #[test]
    fn permute_bit_reversal_power_of_two() {
        let p = Permutation {
            nodes: vec![0, 1, 2, 3, 4, 5, 6, 7],
            bytes_per_flow: 100,
            start_time_ns: 0,
            kind: PermuteKind::BitReversal,
            seed: 0,
        };
        let flows = p.generate();
        assert_eq!(flows.len(), 8);
        for f in &flows {
            assert_ne!(f.src, f.dst);
        }
    }
}
