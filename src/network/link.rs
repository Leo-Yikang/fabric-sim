//! 链路模型
//!
//! 链路是单向的（实际网络中一条物理链路 = 两条单向链路）。
//! 给定包大小，可以计算"包发送完成 + 传播到达"的时刻。

use crate::EntityId;

pub type LinkId = u32;

/// 单向链路
#[derive(Debug, Clone, Copy)]
pub struct Link {
    pub id: LinkId,
    pub from: EntityId,         // 发送端 entity（主机或交换机）
    pub to: EntityId,            // 接收端 entity
    pub bandwidth_bps: u64,      // 带宽，bit/s
    pub prop_delay_ns: u64,      // 传播延迟，ns
}

impl Link {
    /// 序列化延迟（发送一个 size 字节的包需要的时间，ns）
    #[inline]
    pub fn serialization_ns(&self, size_bytes: u32) -> u64 {
        // size * 8 bits / bandwidth_bps * 1e9 ns
        // 用整数运算避免浮点误差
        (size_bytes as u64 * 8 * 1_000_000_000) / self.bandwidth_bps
    }

    /// 从当前时刻开始发送一个 size 字节的包，到达对端的时刻
    #[inline]
    pub fn arrive_time(&self, now_ns: u64, size_bytes: u32) -> u64 {
        now_ns + self.serialization_ns(size_bytes) + self.prop_delay_ns
    }
}

/// 链路注册表：通过 LinkId 快速查找
#[derive(Default)]
pub struct LinkRegistry {
    links: Vec<Link>,
}

impl LinkRegistry {
    pub fn new() -> Self {
        Self { links: Vec::new() }
    }

    pub fn add(&mut self, mut link: Link) -> LinkId {
        let id = self.links.len() as LinkId;
        link.id = id;
        self.links.push(link);
        id
    }

    pub fn get(&self, id: LinkId) -> &Link {
        &self.links[id as usize]
    }

    pub fn len(&self) -> usize {
        self.links.len()
    }

    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Link> {
        self.links.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_at_100gbps() {
        let l = Link { id: 0, from: 0, to: 1, bandwidth_bps: 100_000_000_000, prop_delay_ns: 1000 };
        // 1024 B = 8192 bit @ 100Gbps ≈ 81.92 ns
        assert!((l.serialization_ns(1024) as i64 - 81).abs() <= 1);
    }

    #[test]
    fn arrive_time_includes_propagation() {
        let l = Link { id: 0, from: 0, to: 1, bandwidth_bps: 100_000_000_000, prop_delay_ns: 1000 };
        let t = l.arrive_time(0, 1024);
        // ≈ 81 + 1000
        assert!(t >= 1080 && t <= 1090);
    }
}
