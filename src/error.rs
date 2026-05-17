//! 仿真错误类型
//!
//! 轻量级判错系统——不侵入 Protocol trait，仅对拓扑/初始化等"外部输入相关"的
//! 错误路径使用 `Result`；内部不变量违反（如 flow 查找失败）使用 `expect()` 即时
//! panic，因为那意味着代码 bug 而非运行时异常。
//!
//! ## 设计原则
//! - **对外 API 返回 `SimResult`**：让调用方有机会处理拓扑不一致等合法错误场景。
//! - **内部不变量使用 `expect()`**：不返回 `Result`，避免全项目级联修改 Protocol trait。
//! - **已守卫的 `unwrap()` 改写成模式匹配**：让控制流意图显式化。
//! - **测试代码允许 `unwrap()` / `expect()`**：不受此约束。

use thiserror::Error;

/// 仿真器可恢复错误
#[derive(Error, Debug)]
pub enum SimError {
    /// 拓扑数据不一致：缺少预期的主机上行链路、交换机缺失等
    #[error("拓扑不一致: {0}")]
    Topology(String),

    /// 仿真初始化阶段参数/状态异常
    #[error("初始化错误: {0}")]
    Init(String),
}

/// 仿真器常用 Result 别名
pub type SimResult<T> = Result<T, SimError>;
