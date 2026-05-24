#!/usr/bin/env python3
"""
工作负载特征扫描结果可视化

用法：
    python3 scripts/plot_workloads.py
    # 或指定 CSV 路径：
    python3 scripts/plot_workloads.py output/workload_sweep.csv

输出：
    output/workload_fct_comparison.png
    output/workload_congestion_comparison.png
"""

import csv
import sys
from pathlib import Path

# ------------------------------------------------------------------
# 依赖检查
# ------------------------------------------------------------------
try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use("Agg")  # 无头环境（不弹窗）
    # macOS 中文字体支持
    plt.rcParams["font.sans-serif"] = ["PingFang HK", "Heiti TC", "Arial Unicode MS"]
    plt.rcParams["axes.unicode_minus"] = False
except ImportError:
    print("错误：缺少 matplotlib。请运行以下命令安装：")
    print("  python3 -m pip install matplotlib")
    sys.exit(1)

# ------------------------------------------------------------------
# 读取 CSV
# ------------------------------------------------------------------
def load_csv(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ------------------------------------------------------------------
# 图表 1：FCT 对比（P50 / P99 分组柱状图）
# ------------------------------------------------------------------
def plot_fct(rows: list[dict], out_dir: Path):
    labels = [r["label"] for r in rows]
    p50 = [float(r["fct_p50_us"]) for r in rows]
    p99 = [float(r["fct_p99_us"]) for r in rows]

    x = range(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    bars1 = ax.bar([i - width / 2 for i in x], p50, width, label="P50 FCT", color="steelblue")
    bars2 = ax.bar([i + width / 2 for i in x], p99, width, label="P99 FCT", color="coral")

    ax.set_ylabel("FCT (us)")
    ax.set_title("不同工作负载特征下的 FCT 对比 (LeafSpine × Fabric)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9)
    ax.legend()
    ax.set_yscale("log")  # P99 可能远大于 P50，用对数坐标
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    # 在柱子上方标注数值
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(f"{height:.1f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=7)
    for bar in bars2:
        height = bar.get_height()
        ax.annotate(f"{height:.1f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=7)

    fig.tight_layout()
    out_path = out_dir / "workload_fct_comparison.png"
    fig.savefig(out_path, dpi=150)
    print(f"[已保存] {out_path}")
    plt.close(fig)


# ------------------------------------------------------------------
# 图表 2：拥塞指标对比（ECN 标记数 + 丢包数）
# ------------------------------------------------------------------
def plot_congestion(rows: list[dict], out_dir: Path):
    labels = [r["label"] for r in rows]
    ecn = [int(r["ecn_marks"]) for r in rows]
    drops = [int(r["drops"]) for r in rows]

    x = range(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    bars1 = ax.bar([i - width / 2 for i in x], ecn, width, label="ECN 标记数", color="orange")
    bars2 = ax.bar([i + width / 2 for i in x], drops, width, label="丢包数", color="red")

    ax.set_ylabel("计数")
    ax.set_title("不同工作负载特征下的拥塞指标对比")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    fig.tight_layout()
    out_path = out_dir / "workload_congestion_comparison.png"
    fig.savefig(out_path, dpi=150)
    print(f"[已保存] {out_path}")
    plt.close(fig)


# ------------------------------------------------------------------
# 图表 3：散点图 —— 流数 vs 平均链路利用率
# ------------------------------------------------------------------
def plot_utilization(rows: list[dict], out_dir: Path):
    labels = [r["label"] for r in rows]
    flows = [int(r["total_flows"]) for r in rows]
    util = [float(r["avg_link_util_pct"]) for r in rows]

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = plt.cm.viridis([i / len(labels) for i in range(len(labels))])
    scatter = ax.scatter(flows, util, s=200, c=colors, edgecolors="black", zorder=3)

    for i, label in enumerate(labels):
        ax.annotate(label, (flows[i], util[i]), textcoords="offset points",
                    xytext=(8, 5), fontsize=8, ha="left")

    ax.set_xlabel("总流数")
    ax.set_ylabel("平均链路利用率 (%)")
    ax.set_title("工作负载规模与链路利用率关系")
    ax.grid(linestyle="--", alpha=0.5)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

    fig.tight_layout()
    out_path = out_dir / "workload_utilization_scatter.png"
    fig.savefig(out_path, dpi=150)
    print(f"[已保存] {out_path}")
    plt.close(fig)


# ------------------------------------------------------------------
# 主函数
# ------------------------------------------------------------------
def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "output/workload_sweep.csv"
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"错误：找不到 CSV 文件 {csv_path}")
        print("请先运行：cargo run --release --example workload_sweep")
        sys.exit(1)

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)

    rows = load_csv(str(csv_file))
    print(f"已读取 {len(rows)} 条记录")

    plot_fct(rows, out_dir)
    plot_congestion(rows, out_dir)
    plot_utilization(rows, out_dir)

    print("全部图表已保存到 output/ 目录")


if __name__ == "__main__":
    main()
