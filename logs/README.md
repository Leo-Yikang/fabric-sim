# 日志目录

本目录存放运行时产生的日志文件，已加入 `.gitignore`。

## 日志文件说明

| 文件 | 来源 | 内容 |
|------|------|------|
| `des_demo.log` | `cargo run --release --example des_demo` | DES 引擎演示中每个事件的 `tracing::info` 记录 |
| `bench_*.log` | `cargo bench` (后续) | criterion 性能基准 |
| `sim_<timestamp>.log` | 后续阶段主程序 | 完整仿真过程，含 packet/ack/ecn 事件 |

## 日志级别控制

通过 `RUST_LOG` 环境变量：

```bash
RUST_LOG=info  cargo run --release --example des_demo   # 默认
RUST_LOG=debug cargo run --release --example des_demo   # 详细
RUST_LOG=trace cargo run --release --example des_demo   # 完整事件流（量大）
```

## 日志格式

使用 `tracing-subscriber` 默认结构化输出：

```
2026-05-14T07:43:21.123Z  INFO 事件触发 time=100 kind=Custom("t=100")
2026-05-14T07:43:21.124Z  INFO 事件触发 time=200 kind=Custom("t=200")
```

字段说明：
- `time`：事件的仿真时刻（ns）
- `kind`：事件种类
- 其他字段视事件而定（packet_id、src、dst 等）

## 性能注意

`tracing` 在禁用相应 level 时几乎零开销；启用 `info` 时百万级事件会写入约 100MB 日志，请按需开关。
