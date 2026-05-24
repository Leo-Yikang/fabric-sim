//! RDMA (Remote Direct Memory Access) 核心抽象
//!
//! 实现 InfiniBand/RoCEv2 协议栈的基础构件：
//! - QP (Queue Pair) 状态机：RESET → INIT → RTR → RTS
//! - PSN (Packet Sequence Number)：每 QP 独立的包序号空间
//! - 消息边界标记：Solo / First / Middle / Last
//! - WQE (Work Queue Entry) / CQE (Completion Queue Entry) 抽象
//!
//! 参考：InfiniBand Architecture Specification Vol.1, Chapter 10

use crate::EntityId;

/// QP 编号
pub type Qpn = u32;

/// PSN — 每 QP 独立的包序号（24-bit，符合 IB 规范）
pub type Psn = u32;

/// QP 状态（IB Spec §10.2）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QpState {
    /// 初始状态，QP 未配置
    Reset,
    /// 已初始化，本地参数已配置
    Init,
    /// Ready to Receive — 可以接收，还不能发送
    Rtr,
    /// Ready to Send — 可以收发
    Rts,
    /// 错误状态
    Error,
}

impl QpState {
    pub fn can_send(&self) -> bool {
        matches!(self, QpState::Rts)
    }
    pub fn can_recv(&self) -> bool {
        matches!(self, QpState::Rtr | QpState::Rts)
    }
}

/// 消息边界标志（编码在 2 bit 内）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgBoundary {
    /// 单包消息
    Solo = 0,
    /// 多包消息的第一包
    First = 1,
    /// 多包消息的中间包
    Middle = 2,
    /// 多包消息的最后一包
    Last = 3,
}

impl MsgBoundary {
    pub fn from_flags(flags: u8) -> Self {
        match flags & 0x03 {
            0 => MsgBoundary::Solo,
            1 => MsgBoundary::First,
            2 => MsgBoundary::Middle,
            3 => MsgBoundary::Last,
            _ => unreachable!(),
        }
    }
    pub fn to_flags(self) -> u8 {
        self as u8
    }
}

/// RDMA 操作类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaOpcode {
    Send,
    SendWithImm,
    Write,
    WriteWithImm,
    ReadRequest,
    ReadResponse,
    /// 仅 ACK（无 payload）
    Ack,
    /// RNR NAK
    RnrNak,
}

impl RdmaOpcode {
    /// 是否为单向操作（不需远端 CPU 参与）
    pub fn is_one_sided(&self) -> bool {
        matches!(self, RdmaOpcode::Write | RdmaOpcode::WriteWithImm | RdmaOpcode::ReadRequest | RdmaOpcode::ReadResponse)
    }
    /// 是否需要接收端预置 WQE
    pub fn needs_recv_wqe(&self) -> bool {
        matches!(self, RdmaOpcode::Send | RdmaOpcode::SendWithImm)
    }
}

// ------------------------------------------------------------------
// WQE / CQE
// ------------------------------------------------------------------

/// Work Queue Entry — 描述一个 RDMA 操作
#[derive(Debug, Clone)]
pub struct Wqe {
    /// 所属 QP
    pub qpn: Qpn,
    /// 操作类型
    pub opcode: RdmaOpcode,
    /// 远端实体
    pub remote_id: EntityId,
    /// 远端内存地址（简化：这里只存偏移量）
    pub remote_addr: u64,
    /// 本地内存地址（简化）
    pub local_addr: u64,
    /// 消息大小（bytes）
    pub length: u64,
    /// 提交时刻（ns）
    pub posted_ns: u64,
    /// 是否已发出 doorbell
    pub doorbell_rung: bool,
}

/// Completion Queue Entry — 操作完成通知
#[derive(Debug, Clone)]
pub struct Cqe {
    /// 所属 QP
    pub qpn: Qpn,
    /// 操作类型
    pub opcode: RdmaOpcode,
    /// 完成时刻（ns）
    pub completed_ns: u64,
    /// 完成状态
    pub status: CqStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqStatus {
    Success,
    /// 远端 RNR
    RnrRetry,
    /// 超时
    Timeout,
    /// 本地保护错误
    LocalProtection,
}

// ------------------------------------------------------------------
// QP 结构
// ------------------------------------------------------------------

/// Queue Pair 完整状态
#[derive(Debug, Clone)]
pub struct QueuePair {
    /// 本地 QP 编号
    pub qpn: Qpn,
    /// 远端 QP 编号
    pub remote_qpn: Qpn,
    /// QP 状态
    pub state: QpState,
    /// 下一个要发送的 PSN
    pub next_psn: Psn,
    /// 期望接收的下一个 PSN
    pub e_psn: Psn,
    /// 远端实体
    pub remote_id: EntityId,
    /// 创建时刻（ns）
    pub created_ns: u64,
    /// 发送队列
    pub send_queue: Vec<Wqe>,
    /// 接收队列（已 posted 的 receive WQE）
    pub recv_queue: Vec<Wqe>,
    /// 待处理的 read 请求（PSN → Wqe）
    pub pending_reads: std::collections::HashMap<Psn, Wqe>,
    /// 正在重组的接收消息（PSN → 数据包）
    pub reassembly: std::collections::HashMap<Psn, Vec<u8>>,
    /// 当前正在接收的消息 first PSN（用于重组追踪）
    pub current_msg_first_psn: Option<Psn>,
    /// 统计
    pub rnr_naks_sent: u64,
    pub rnr_naks_received: u64,
    pub retransmissions: u64,
}

impl QueuePair {
    pub fn new(qpn: Qpn, remote_qpn: Qpn, remote_id: EntityId, now: u64) -> Self {
        Self {
            qpn,
            remote_qpn,
            state: QpState::Reset,
            next_psn: 0,
            e_psn: 0,
            remote_id,
            created_ns: now,
            send_queue: Vec::new(),
            recv_queue: Vec::new(),
            pending_reads: std::collections::HashMap::new(),
            reassembly: std::collections::HashMap::new(),
            current_msg_first_psn: None,
            rnr_naks_sent: 0,
            rnr_naks_received: 0,
            retransmissions: 0,
        }
    }

    /// 状态转换（IB Spec §10.2 状态机）
    pub fn transition(&mut self, new_state: QpState) -> bool {
        let valid = match (self.state, new_state) {
            (QpState::Reset, QpState::Init) => true,
            (QpState::Init, QpState::Init) => true,
            (QpState::Init, QpState::Rtr) => true,
            (QpState::Rtr, QpState::Rts) => true,
            (_, QpState::Error) => true,
            (_, QpState::Reset) => true,
            _ => false,
        };
        if valid {
            self.state = new_state;
        }
        valid
    }

    /// 分配下一个 PSN（发送端）
    pub fn alloc_psn(&mut self) -> Psn {
        let psn = self.next_psn;
        self.next_psn = self.next_psn.wrapping_add(1);
        psn
    }

    /// 是否有可用的 receive WQE
    pub fn has_recv_wqe(&self) -> bool {
        self.recv_queue.iter().any(|w| !w.doorbell_rung)
    }
}