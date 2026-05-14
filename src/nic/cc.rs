//! 拥塞控制策略
//!
//! 支持两种模式：
//! - `Ecmp`：传统单路径流，基于流哈希走单一路径，DCQCN 风格降窗
//! - `Strack`：多路径 Packet Spraying，先切路再降窗

/// 拥塞控制模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionMode {
    /// ECMP baseline：每条流走单一哈希路径，全 ECN 时直接降窗
    Ecmp,
    /// STrack：多路径喷洒，遇 ECN 先尝试黑名单当前路径
    Strack,
}

/// 每条路径（在多路径模式下=每个出端口）的状态
#[derive(Debug, Clone, Copy)]
pub struct PathState {
    pub path_id: u8,
    pub blacklisted_until: u64,  // 仿真时间戳：< 此值则不选这条路
    pub ecn_recent: u32,          // 最近窗口内见到的 ECN 数
}

impl PathState {
    pub fn new(id: u8) -> Self {
        Self { path_id: id, blacklisted_until: 0, ecn_recent: 0 }
    }
    pub fn is_available(&self, now: u64) -> bool {
        now >= self.blacklisted_until
    }
}
