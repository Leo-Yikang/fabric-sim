#!/usr/bin/env python3
"""
多包丢失实验可视化

用法：
    python3 examples/tcp_limits/plot.py

输入：
    examples/tcp_limits/data/per_flow.csv
    examples/tcp_limits/data/aggregate.csv

输出：
    examples/tcp_limits/fig_fct_bars.png      — P50/P99 FCT 分组柱状图
    examples/tcp_limits/fig_fct_scatter.png   — 逐流 FCT 散点分布
    examples/tcp_limits/fig_packet_breakdown.png — 发包/重传/丢包/ECN 堆叠图
    examples/tcp_limits/fig_retx_ratio.png    — 重传率对比
"""

import csv
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use("Agg")
    plt.rcParams["font.sans-serif"] = ["PingFang HK", "Heiti TC", "Arial Unicode MS"]
    plt.rcParams["axes.unicode_minus"] = False
except ImportError:
    print("错误：缺少 matplotlib。请运行：")
    print("  scripts/.venv/bin/pip install matplotlib")
    raise SystemExit(1)

import numpy as np

# ------------------------------------------------------------------
# 路径
# ------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
OUT_DIR = Path(__file__).resolve().parent
PER_FLOW = DATA_DIR / "per_flow.csv"
AGGREGATE = DATA_DIR / "aggregate.csv"

# 配色
COLORS = {"SimpleTcp": "#E74C3C", "STrack": "#2980B9"}
HATCH = {"SimpleTcp": "//", "STrack": ".."}

# ------------------------------------------------------------------
# 数据加载
# ------------------------------------------------------------------
def load_aggregate(path: Path) -> dict[str, dict]:
    rows = {}
    with open(path, "r") as f:
        for r in csv.DictReader(f):
            rows[r["protocol"]] = r
    return rows


def load_per_flow(path: Path) -> dict[str, list[dict]]:
    rows: dict[str, list[dict]] = {}
    with open(path, "r") as f:
        for r in csv.DictReader(f):
            proto = r["protocol"]
            rows.setdefault(proto, []).append(r)
    return rows


# ------------------------------------------------------------------
# 图表 1：P50 / P99 FCT 分组柱状图
# ------------------------------------------------------------------
def plot_fct_bars(agg: dict[str, dict], out: Path):
    protocols = list(agg.keys())
    p50_vals = [float(agg[p]["p50_fct_us"]) for p in protocols]
    p99_vals = [float(agg[p]["p99_fct_us"]) for p in protocols]

    x = np.arange(len(protocols))
    width = 0.3

    fig, ax = plt.subplots(figsize=(7, 5))
    b1 = ax.bar(x - width / 2, p50_vals, width, label="P50 FCT",
                color=["#5DADE2", "#5DADE2"], edgecolor="black", linewidth=0.5)
    b2 = ax.bar(x + width / 2, p99_vals, width, label="P99 FCT",
                color=["#E74C3C", "#E74C3C"], edgecolor="black", linewidth=0.5)

    # 数值标注
    for bar in b1:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 2, f"{h:.0f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold")
    for bar in b2:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 2, f"{h:.0f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold")

    # 加速比标注
    if p99_vals[1] > 0:
        ratio = p99_vals[0] / p99_vals[1]
        mid_x = (x[0] + x[1]) / 2
        top_y = max(p99_vals) * 1.25
        ax.annotate(f"P99 加速比\n{ratio:.1f}×",
                    xy=(mid_x, max(p99_vals)),
                    xytext=(mid_x, top_y),
                    ha="center", fontsize=12, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", edgecolor="gray"),
                    arrowprops=dict(arrowstyle="->", color="gray"))

    ax.set_xticks(x)
    ax.set_xticklabels(protocols, fontsize=13, fontweight="bold")
    ax.set_ylabel("FCT (μs)", fontsize=12)
    ax.set_title("多包丢失场景：SimpleTcp vs STrack 的 FCT 对比", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_ylim(0, top_y * 1.05)

    fig.tight_layout()
    fig.savefig(out, dpi=180, bbox_inches="tight")
    print(f"[图] {out}")
    plt.close(fig)


# ------------------------------------------------------------------
# 图表 2：逐流 FCT 散点分布
# ------------------------------------------------------------------
def plot_fct_scatter(pf: dict[str, list[dict]], out: Path):
    fig, ax = plt.subplots(figsize=(8, 4.5))

    for proto, flows in pf.items():
        fct_vals = [float(f["fct_us"]) for f in flows]
        sizes = [float(f["bytes"]) / 1024 for f in flows]  # KB
        jitter = np.random.default_rng(42).uniform(-0.15, 0.15, len(fct_vals))
        x_pos = np.full(len(fct_vals), 0 if proto == "SimpleTcp" else 1) + jitter

        ax.scatter(x_pos, fct_vals, s=sizes, c=COLORS[proto],
                   alpha=0.65, edgecolors="black", linewidth=0.3,
                   label=proto, zorder=3)

        # 标注均值线
        mean_val = np.mean(fct_vals)
        xi = 0 if proto == "SimpleTcp" else 1
        ax.hlines(mean_val, xi - 0.35, xi + 0.35, colors=COLORS[proto],
                  linestyles="dashed", linewidth=2, zorder=4)
        ax.text(xi + 0.38, mean_val, f"均值 {mean_val:.0f}μs",
                va="center", fontsize=9, color=COLORS[proto], fontweight="bold")

    ax.set_xticks([0, 1])
    ax.set_xticklabels(["SimpleTcp", "STrack"], fontsize=13, fontweight="bold")
    ax.set_ylabel("FCT (μs)", fontsize=12)
    ax.set_title("逐流 FCT 散点分布（点大小 ∝ 流数据量）", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    fig.tight_layout()
    fig.savefig(out, dpi=180, bbox_inches="tight")
    print(f"[图] {out}")
    plt.close(fig)


# ------------------------------------------------------------------
# 图表 3：发包/重传/丢包/ECN 分组柱状图
# ------------------------------------------------------------------
def plot_packet_breakdown(agg: dict[str, dict], out: Path):
    protocols = list(agg.keys())
    metrics = [
        ("packets_sent", "总发包", "#3498DB"),
        ("retransmitted", "重传", "#E67E22"),
        ("drops", "丢包", "#E74C3C"),
        ("ecn", "ECN 标记", "#9B59B6"),
    ]

    x = np.arange(len(protocols))
    n_groups = len(metrics)
    width = 0.7 / n_groups

    fig, ax = plt.subplots(figsize=(8, 5))

    for i, (key, label, color) in enumerate(metrics):
        vals = [int(agg[p][key]) for p in protocols]
        offset = (i - (n_groups - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width, label=label, color=color,
                      edgecolor="black", linewidth=0.5)
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 3, str(int(h)),
                        ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(protocols, fontsize=13, fontweight="bold")
    ax.set_ylabel("数据包数", fontsize=12)
    ax.set_title("数据包构成对比", fontsize=14, fontweight="bold")
    ax.legend(fontsize=9, ncols=2)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    fig.tight_layout()
    fig.savefig(out, dpi=180, bbox_inches="tight")
    print(f"[图] {out}")
    plt.close(fig)


# ------------------------------------------------------------------
# 图表 4：重传率对比（双柱）
# ------------------------------------------------------------------
def plot_retx_ratio(agg: dict[str, dict], out: Path):
    protocols = list(agg.keys())
    retx = [int(agg[p]["retransmitted"]) for p in protocols]
    sent = [int(agg[p]["packets_sent"]) for p in protocols]
    orig = [s - r for s, r in zip(sent, retx)]  # 首次发送（非重传）
    ratios = [r / s * 100 if s > 0 else 0 for r, s in zip(retx, sent)]

    x = np.arange(len(protocols))
    width = 0.45

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))

    # 左图：堆叠柱状图
    b_orig = ax1.bar(x, orig, width, label="首次发送", color="#27AE60",
                     edgecolor="black", linewidth=0.5)
    b_retx = ax1.bar(x, retx, width, bottom=orig, label="重传", color="#E67E22",
                     edgecolor="black", linewidth=0.5, hatch="//")

    for i, (o, r) in enumerate(zip(orig, retx)):
        ax1.text(i, o / 2, str(o), ha="center", va="center", fontsize=10, fontweight="bold")
        ax1.text(i, o + r / 2, str(r), ha="center", va="center", fontsize=10,
                 fontweight="bold", color="white")

    ax1.set_xticks(x)
    ax1.set_xticklabels(protocols, fontsize=12, fontweight="bold")
    ax1.set_ylabel("数据包数", fontsize=11)
    ax1.set_title("发包构成（首次 vs 重传）", fontsize=13, fontweight="bold")
    ax1.legend(fontsize=9)

    # 右图：重传率
    colors_r = [COLORS[p] for p in protocols]
    bars = ax2.bar(x, ratios, width, color=colors_r, edgecolor="black", linewidth=0.5)
    for bar, r in zip(bars, ratios):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                 f"{r:.1f}%", ha="center", fontsize=13, fontweight="bold")

    ax2.set_xticks(x)
    ax2.set_xticklabels(protocols, fontsize=12, fontweight="bold")
    ax2.set_ylabel("重传率 (%)", fontsize=11)
    ax2.set_title("重传率对比", fontsize=13, fontweight="bold")
    ax2.set_ylim(0, max(ratios) * 1.3)
    ax2.grid(axis="y", linestyle="--", alpha=0.3)

    fig.tight_layout()
    fig.savefig(out, dpi=180, bbox_inches="tight")
    print(f"[图] {out}")
    plt.close(fig)


# ------------------------------------------------------------------
# 入口
# ------------------------------------------------------------------
def main():
    if not PER_FLOW.exists() or not AGGREGATE.exists():
        print("错误：找不到 CSV 数据文件。请先运行：")
        print("  cargo run --release --example multi_loss")
        raise SystemExit(1)

    agg = load_aggregate(AGGREGATE)
    pf = load_per_flow(PER_FLOW)

    print(f"已加载 {len(agg)} 组聚合数据, {sum(len(v) for v in pf.values())} 条逐流记录")

    plot_fct_bars(agg, OUT_DIR / "fig_fct_bars.png")
    plot_fct_scatter(pf, OUT_DIR / "fig_fct_scatter.png")
    plot_packet_breakdown(agg, OUT_DIR / "fig_packet_breakdown.png")
    plot_retx_ratio(agg, OUT_DIR / "fig_retx_ratio.png")

    print(f"\n全部图表已保存到 {OUT_DIR.resolve()}/")


if __name__ == "__main__":
    main()
