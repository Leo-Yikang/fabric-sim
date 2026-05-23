#!/usr/bin/env python3
"""3D 交互式网络拓扑可视化

读取仿真导出的 JSON，用 Plotly 渲染带时间滑块的 3D 拓扑动画图。
链路颜色从绿→黄→红随利用率渐变。

依赖: pip install plotly

用法:
    python3 scripts/visualize_3d.py output/viz_data.json
    python3 scripts/visualize_3d.py output/viz_data.json --output topo.html --no-open
"""

import argparse
import json
import math
import sys

import plotly.graph_objects as go


def load_data(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def utilization_color(u: float) -> str:
    """0→green, 0.5→yellow, 1.0→red"""
    u = max(0.0, min(1.0, u))
    if u < 0.5:
        r = int(255 * (u / 0.5))
        g = 255
        b = 0
    else:
        r = 255
        g = int(255 * (1.0 - (u - 0.5) / 0.5))
        b = 0
    return f"rgb({r},{g},{b})"


def link_width(u: float) -> float:
    """利用率越高线越粗"""
    return 2.0 + u * 6.0


def build_figure(data: dict) -> go.Figure:
    topo = data["topology"]
    frames_data = data["time_series"]

    # Downsample if too many frames
    if len(frames_data) > 500:
        step = len(frames_data) // 500
        frames_data = frames_data[::step]

    nodes = topo["nodes"]
    links = topo["links"]

    # Map node id → position
    pos = {n["id"]: (n["x"], n["y"], n["z"]) for n in nodes}

    # Build node traces (static)
    host_x, host_y, host_z = [], [], []
    host_labels = []
    sw_x, sw_y, sw_z = [], [], []
    sw_labels = []

    for n in nodes:
        if n["kind"] == "host":
            host_x.append(n["x"])
            host_y.append(n["y"])
            host_z.append(n["z"])
            host_labels.append(n["label"])
        else:
            sw_x.append(n["x"])
            sw_y.append(n["y"])
            sw_z.append(n["z"])
            sw_labels.append(n["label"])

    fig = go.Figure()

    # Hosts (trace 0)
    fig.add_trace(
        go.Scatter3d(
            x=host_x,
            y=host_y,
            z=host_z,
            mode="markers+text",
            marker=dict(size=8, color="deepskyblue", symbol="circle"),
            text=host_labels,
            textposition="top center",
            textfont=dict(size=10, color="white"),
            name="Hosts",
            showlegend=True,
        )
    )

    # Switches (trace 1)
    fig.add_trace(
        go.Scatter3d(
            x=sw_x,
            y=sw_y,
            z=sw_z,
            mode="markers+text",
            marker=dict(size=10, color="orange", symbol="diamond"),
            text=sw_labels,
            textposition="top center",
            textfont=dict(size=10, color="white"),
            name="Switches",
            showlegend=True,
        )
    )

    # Precompute link segment coordinates (one trace per link)
    # Each link trace: trace index = 2 + link_index
    n_links = len(links)
    first_snapshots = frames_data[0]["links"] if frames_data else []
    link_trace_indices = list(range(2, 2 + n_links))

    for i, link in enumerate(links):
        fx, fy, fz = pos[link["from"]]
        tx, ty, tz = pos[link["to"]]
        u = first_snapshots[i]["utilization"] if i < len(first_snapshots) else 0.0
        color = utilization_color(u)
        width = link_width(u)
        fig.add_trace(
            go.Scatter3d(
                x=[fx, tx],
                y=[fy, ty],
                z=[fz, tz],
                mode="lines",
                line=dict(color=color, width=width),
                name=f"L{link['id']}",
                showlegend=False,
                hoverinfo="none",
            )
        )

    # Build animation frames
    plotly_frames = []
    slider_steps = []

    for fi, frame in enumerate(frames_data):
        snapshots = frame["links"]
        frame_data = []

        for i in range(n_links):
            u = snapshots[i]["utilization"] if i < len(snapshots) else 0.0
            color = utilization_color(u)
            width = link_width(u)
            link = links[i]
            fx, fy, fz = pos[link["from"]]
            tx, ty, tz = pos[link["to"]]
            frame_data.append(
                go.Scatter3d(
                    x=[fx, tx],
                    y=[fy, ty],
                    z=[fz, tz],
                    mode="lines",
                    line=dict(color=color, width=width),
                )
            )

        time_us = frame["time_ns"] / 1000.0

        plotly_frames.append(
            go.Frame(
                data=frame_data,
                name=f"f{fi}",
                traces=link_trace_indices,
            )
        )

        slider_steps.append(
            dict(
                args=[
                    [f"f{fi}"],
                    {
                        "frame": {"duration": 50, "redraw": True},
                        "mode": "immediate",
                    },
                ],
                label=f"{time_us:.0f}",
                method="animate",
            )
        )

    fig.frames = plotly_frames

    # Slider
    sliders = [
        dict(
            active=0,
            steps=slider_steps,
            currentvalue=dict(prefix="Time: ", suffix=" us", font=dict(color="white")),
            pad=dict(t=50),
            len=0.9,
            x=0.05,
            bgcolor="#333",
            font=dict(color="white"),
        )
    ]

    # Play/pause buttons
    updatemenus = [
        dict(
            type="buttons",
            buttons=[
                dict(
                    label="Play",
                    method="animate",
                    args=[
                        None,
                        {
                            "frame": {"duration": 50, "redraw": True},
                            "fromcurrent": True,
                            "mode": "immediate",
                        },
                    ],
                ),
                dict(
                    label="Pause",
                    method="animate",
                    args=[
                        [None],
                        {
                            "frame": {"duration": 0, "redraw": True},
                            "mode": "immediate",
                        },
                    ],
                ),
            ],
            direction="left",
            pad=dict(r=10, t=70),
            x=0.1,
            xanchor="right",
            y=0.0,
            yanchor="top",
            font=dict(color="white"),
            bgcolor="#333",
        )
    ]

    # Color legend
    total_flows = data["summary"]["total_flows"]
    completed = data["summary"]["completed_flows"]
    fct_p99_us = data["summary"]["fct_p99_ns"] / 1000.0
    avg_util = data["summary"]["avg_link_util"] * 100
    title_text = (
        f"Network Topology — {completed}/{total_flows} flows, "
        f"FCT P99={fct_p99_us:.1f}us, Avg Util={avg_util:.1f}%"
    )

    fig.update_layout(
        title=dict(text=title_text, font=dict(color="white")),
        scene=dict(
            xaxis=dict(title="", showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(title="", showgrid=False, zeroline=False, showticklabels=False),
            zaxis=dict(title="", showgrid=False, zeroline=False, showticklabels=False),
            bgcolor="rgba(0,0,0,0)",
        ),
        paper_bgcolor="#111",
        plot_bgcolor="#111",
        font=dict(color="white"),
        sliders=sliders,
        updatemenus=updatemenus,
        margin=dict(l=0, r=0, t=60, b=100),
        legend=dict(x=0.8, y=0.95, font=dict(color="white")),
    )

    return fig


def main():
    parser = argparse.ArgumentParser(description="3D Network Topology Visualization")
    parser.add_argument("json_path", help="Path to viz_data.json")
    parser.add_argument("--output", "-o", default=None, help="Output HTML path")
    parser.add_argument("--no-open", action="store_true", help="Don't open in browser")
    args = parser.parse_args()

    data = load_data(args.json_path)
    fig = build_figure(data)

    import pathlib

    out_path = args.output or args.json_path.replace(".json", ".html")
    out_path = str(pathlib.Path(out_path).resolve())
    fig.write_html(out_path)
    print(f"Saved to {out_path}")

    if not args.no_open:
        import webbrowser
        webbrowser.open(f"file://{out_path}")


if __name__ == "__main__":
    main()
