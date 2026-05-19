#!/usr/bin/env python3
"""
Build a compact three-way comparison for:
1. no-PFC baseline,
2. same-queue PFC baseline,
3. MQ-RDMA.

The script intentionally keeps only the metrics that are defensible for the
current experiments: completion counts, FCT, PFC timing/counts, trace-active
cluster throughput, and busiest-link throughput.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from statistics import mean
from typing import Optional


PATTERNS = ("overall", "pipeline", "pipeline_non_hotspot", "pipeline_hotspot_touch", "alltoall")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare no-PFC, same-queue PFC, and MQ-RDMA runs.")
    parser.add_argument("--no-pfc-prefix", required=True, help="No-PFC baseline prefix.")
    parser.add_argument("--pfc-prefix", required=True, help="Same-queue PFC baseline prefix.")
    parser.add_argument("--mq-prefix", required=True, help="MQ-RDMA prefix.")
    parser.add_argument("--bin-us", type=float, default=10.0, help="Timeline bin width in microseconds.")
    parser.add_argument(
        "--plot-pattern",
        choices=PATTERNS,
        default="pipeline_non_hotspot",
        help="Pipeline subset to use for box plots. Use pipeline for total pipeline, or pipeline_non_hotspot for clean victim traffic.",
    )
    parser.add_argument(
        "--timeline",
        action="store_true",
        help="Also emit the trace-based timeline plot. The default output is bar plots only.",
    )
    parser.add_argument("--label", default="three_way", help="Output filename label.")
    return parser.parse_args()


def load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fmt(value: Optional[float], spec: str = ".2f") -> str:
    return "n/a" if value is None else format(value, spec)


def pct_change(new_value: Optional[float], old_value: Optional[float]) -> Optional[float]:
    if new_value is None or old_value is None or old_value == 0:
        return None
    return (new_value - old_value) * 100.0 / old_value


def percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * pct
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def ensure_timeline_csv(mix_dir: Path, prefix: str, bin_us: float) -> Path:
    csv_path = mix_dir / f"pfc_timeline_{prefix}.csv"
    script_path = mix_dir / "plot_pfc_event_timeline.py"
    cmd = [sys.executable, str(script_path), "--prefix", prefix, "--bin-us", str(bin_us)]
    subprocess.run(cmd, cwd=mix_dir.parent, check=True, stdout=subprocess.DEVNULL)
    if not csv_path.exists():
        raise SystemExit(f"timeline csv not generated: {csv_path}")
    return csv_path


def load_timeline_rows(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="ascii") as handle:
        return list(csv.DictReader(handle))


def load_pattern_samples(collector, fct_path: Path, flows: list[dict], hotspot_nodes: list[int]) -> dict[str, object]:
    lookup = collector.build_lookup(flows)
    hotspot_set = set(hotspot_nodes)
    expected = {pattern: 0 for pattern in PATTERNS}
    expected["overall"] = len(flows)
    for flow in flows:
        expected[flow["pattern"]] += 1
        if flow["pattern"] == "pipeline":
            if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                expected["pipeline_hotspot_touch"] += 1
            else:
                expected["pipeline_non_hotspot"] += 1

    samples = {
        "fct_us": {pattern: [] for pattern in PATTERNS},
        "goodput_gbps": {pattern: [] for pattern in PATTERNS},
        "expected": expected,
    }
    with fct_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 8:
                continue
            src = collector.ip_hex_to_node_id(parts[0])
            dst = collector.ip_hex_to_node_id(parts[1])
            dst_port = int(parts[3])
            size_bytes = int(parts[4])
            fct_us = int(parts[6]) / 1e3
            fct_ns = int(parts[6])
            goodput_gbps = size_bytes * 8.0 / max(fct_ns, 1)
            flow = None
            candidates = lookup.get((src, dst, dst_port, size_bytes))
            if candidates:
                flow = candidates.pop(0)

            samples["fct_us"]["overall"].append(fct_us)
            samples["goodput_gbps"]["overall"].append(goodput_gbps)
            if flow is None:
                continue
            samples["fct_us"][flow["pattern"]].append(fct_us)
            samples["goodput_gbps"][flow["pattern"]].append(goodput_gbps)
            if flow["pattern"] == "pipeline":
                if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                    samples["fct_us"]["pipeline_hotspot_touch"].append(fct_us)
                    samples["goodput_gbps"]["pipeline_hotspot_touch"].append(goodput_gbps)
                else:
                    samples["fct_us"]["pipeline_non_hotspot"].append(fct_us)
                    samples["goodput_gbps"]["pipeline_non_hotspot"].append(goodput_gbps)
    return samples


def sample_stats(run: dict, pattern: str) -> dict[str, object]:
    samples = run["plot_samples"]
    fcts = samples["fct_us"][pattern]
    goodputs = samples["goodput_gbps"][pattern]
    return {
        "completed": len(fcts),
        "expected": samples["expected"][pattern],
        "avg_fct_us": mean(fcts) if fcts else None,
        "p95_fct_us": percentile(fcts, 0.95),
        "avg_per_flow_goodput_gbps": mean(goodputs) if goodputs else None,
    }


def load_run(collector, mix_dir: Path, prefix: str) -> dict:
    manifest = collector.load_manifest(mix_dir, prefix)
    flows = manifest["flows"]
    hotspot_nodes = manifest["scenario"].get("alltoall_hotspot_nodes", [])
    files = manifest["files"]
    fct_path = mix_dir / Path(files["fct"]).name
    pfc_path = mix_dir / Path(files["pfc"]).name
    trace_path = mix_dir / Path(files["trace_output"]).name if "trace_output" in files else None
    if not fct_path.exists():
        raise SystemExit(f"missing fct file: {fct_path}")
    if not pfc_path.exists():
        raise SystemExit(f"missing pfc file: {pfc_path}")
    fct = collector.summarize_fct(fct_path, flows, hotspot_nodes)
    pfc = collector.summarize_pfc(pfc_path)
    trace_active = None
    if trace_path is not None and trace_path.exists():
        host_link_rate_gbps = float(
            manifest["scenario"].get("host_link_rate_gbps", manifest["scenario"].get("min_link_rate_gbps", 0.0))
        )
        tor_link_rate_gbps = float(
            manifest["scenario"].get("tor_link_rate_gbps", manifest["scenario"].get("min_link_rate_gbps", 0.0))
        )
        trace_active = collector.summarize_trace_activity(
            trace_path,
            flows,
            hotspot_nodes,
            host_link_rate_gbps,
            tor_link_rate_gbps,
        )
    return {
        "prefix": prefix,
        "manifest": manifest,
        "fct": fct,
        "pfc": pfc,
        "trace_active": trace_active,
        "flows": flows,
        "fct_path": fct_path,
        "samples": load_pattern_samples(collector, fct_path, flows, hotspot_nodes),
    }


def bottleneck_desc(collector, run: dict, pattern: str) -> str:
    trace_active = run.get("trace_active")
    if trace_active is None:
        return "n/a"
    return collector.describe_bottleneck(trace_active.get("link_bottleneck"), pattern)


def metric_row(collector, run: dict, pattern: str) -> dict[str, object]:
    fct = run["fct"]["patterns"][pattern]
    trace_pattern = (run.get("trace_active") or {}).get("patterns", {}).get(pattern, {})
    return {
        "completed": f"{fct['completed_flows']}/{fct['expected_flows']}",
        "avg_fct_us": fct.get("avg_fct_us"),
        "p95_fct_us": fct.get("p95_fct_us"),
        "aggregate_goodput_gbps": fct.get("aggregate_goodput_gbps"),
        "cluster_throughput_gbps": trace_pattern.get("throughput_gbps"),
        "busiest_link": bottleneck_desc(collector, run, pattern),
    }


def pipeline_busiest_link_gbps(run: dict) -> Optional[float]:
    trace_active = run.get("trace_active")
    if trace_active is None:
        return None
    bottleneck = trace_active.get("link_bottleneck", {}).get("pipeline", {}).get("bottleneck", {})
    return bottleneck.get("gbps")


def write_summary(collector, output_path: Path, runs: list[tuple[str, dict]]) -> None:
    baseline = runs[0][1]
    lines = ["Three-way comparison", ""]
    warnings = []
    for label, run in runs:
        pause_events = run["pfc"]["pause_events"]
        if label == "no_pfc" and pause_events != 0:
            warnings.append(
                f"{label} has {pause_events} PFC pause events; regenerate it with --disable-pfc before using this comparison."
            )
    if warnings:
        lines.append("Warnings")
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    lines.append("| Setting | Prefix | Pipeline PG | All-to-all PG | PFC pauses | Ports paused | First pause vs all-to-all |")
    lines.append("|---|---|---:|---:|---:|---:|---|")
    for label, run in runs:
        scenario = run["manifest"]["scenario"]
        trigger = collector.summarize_trigger_timing(run["pfc"], scenario)
        relation = "n/a"
        if trigger is not None:
            relation = (
                f"{collector.fmt(trigger['delta_vs_alltoall_us'], '.2f')} us, "
                f"{trigger['relation_to_alltoall']}"
            )
        lines.append(
            "| "
            f"{label} | {run['prefix']} | {scenario.get('pipeline_pg', 'n/a')} | {scenario.get('alltoall_pg', 'n/a')} | "
            f"{run['pfc']['pause_events']} | {run['pfc']['ports_with_pause']} | {relation} |"
        )

    for pattern in ("overall", "pipeline", "pipeline_non_hotspot", "pipeline_hotspot_touch", "alltoall"):
        lines.extend(["", f"{pattern} metrics"])
        lines.append("| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |")
        lines.append("|---|---:|---:|---:|---:|---:|---|---:|")
        ref = baseline["fct"]["patterns"][pattern]
        for label, run in runs:
            row = metric_row(collector, run, pattern)
            avg_delta = pct_change(row["avg_fct_us"], ref.get("avg_fct_us"))
            lines.append(
                "| "
                f"{label} | {row['completed']} | {fmt(row['avg_fct_us'])} | {fmt(row['p95_fct_us'])} | "
                f"{fmt(row['aggregate_goodput_gbps'])} | {fmt(row['cluster_throughput_gbps'])} | "
                f"{row['busiest_link']} | {fmt(avg_delta)}% |"
            )
    output_path.write_text("\n".join(lines) + "\n", encoding="ascii")


def plot_fct(output_path: Path, runs: list[tuple[str, dict]], pattern: str = "pipeline_non_hotspot") -> None:
    import matplotlib.pyplot as plt  # type: ignore

    labels = [label for label, _ in runs]
    data = [run["plot_samples"]["fct_us"][pattern] for _, run in runs]
    fig, ax = plt.subplots(figsize=(8, 5))
    bp = ax.boxplot(data, labels=labels, patch_artist=True, showmeans=True, showfliers=False, widths=0.55)
    colors = ["tab:blue", "tab:red", "tab:green"]
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.35)
    for median in bp["medians"]:
        median.set_color("black")
        median.set_linewidth(1.5)
    for idx, samples in enumerate(data, start=1):
        if samples:
            ax.text(idx, max(samples), f"n={len(samples)}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel(f"{pattern} FCT (us)")
    ax.set_title(f"FCT distribution across three settings ({pattern})")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_goodput_box(output_path: Path, runs: list[tuple[str, dict]], pattern: str = "pipeline_non_hotspot") -> None:
    import matplotlib.pyplot as plt  # type: ignore

    labels = [label for label, _ in runs]
    data = [run["plot_samples"]["goodput_gbps"][pattern] for _, run in runs]
    fig, ax = plt.subplots(figsize=(8, 5))
    bp = ax.boxplot(data, labels=labels, patch_artist=True, showmeans=True, showfliers=False, widths=0.55)
    colors = ["tab:blue", "tab:red", "tab:green"]
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.35)
    for median in bp["medians"]:
        median.set_color("black")
        median.set_linewidth(1.5)
    for idx, samples in enumerate(data, start=1):
        if samples:
            ax.text(idx, max(samples), f"n={len(samples)}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel(f"{pattern} per-flow goodput (Gbps)")
    ax.set_title(f"Throughput distribution across three settings ({pattern})")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_pipeline_fct_summary(output_path: Path, runs: list[tuple[str, dict]], pattern: str) -> None:
    import matplotlib.pyplot as plt  # type: ignore

    labels = [label for label, _ in runs]
    stats = [sample_stats(run, pattern) for _, run in runs]
    avg_fct = [row["avg_fct_us"] or 0.0 for row in stats]
    p95_fct = [row["p95_fct_us"] or 0.0 for row in stats]
    completed = [f"{row['completed']}/{row['expected']}" for row in stats]

    xs = list(range(len(labels)))
    width = 0.36
    fig, ax = plt.subplots(figsize=(8.5, 5))
    avg_bars = ax.bar([x - width / 2 for x in xs], avg_fct, width, label="avg FCT", color="tab:blue", alpha=0.75)
    p95_bars = ax.bar([x + width / 2 for x in xs], p95_fct, width, label="p95 FCT", color="tab:orange", alpha=0.75)

    for bars in (avg_bars, p95_bars):
        ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)
    for x, text in zip(xs, completed):
        ax.text(x, 0, f"done {text}", ha="center", va="bottom", rotation=90, fontsize=8, color="dimgray")

    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.set_ylabel(f"{pattern} FCT (us)")
    ax.set_title(f"{pattern} FCT across three settings")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_pipeline_throughput_summary(output_path: Path, runs: list[tuple[str, dict]], pattern: str) -> None:
    import matplotlib.pyplot as plt  # type: ignore

    labels = [label for label, _ in runs]
    stats = [sample_stats(run, pattern) for _, run in runs]
    goodput_gbps = [row["avg_per_flow_goodput_gbps"] or 0.0 for row in stats]
    completed = [f"{row['completed']}/{row['expected']}" for row in stats]

    xs = list(range(len(labels)))
    fig, ax = plt.subplots(figsize=(8.5, 5))
    bars = ax.bar(xs, goodput_gbps, width=0.52, color="tab:green", alpha=0.75, label="avg per-flow goodput")
    ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
    for x, text in zip(xs, completed):
        ax.text(x, 0, f"done {text}", ha="center", va="bottom", rotation=90, fontsize=8, color="dimgray")

    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, max(1.0, max(goodput_gbps, default=0.0) * 1.25))
    ax.set_ylabel(f"{pattern} avg per-flow goodput (Gbps)")
    ax.set_title(f"{pattern} throughput across three settings")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_pause_counts(output_path: Path, runs: list[tuple[str, dict]]) -> None:
    import matplotlib.pyplot as plt  # type: ignore

    labels = [label for label, _ in runs]
    pipeline_pg_pause_events = []
    pipeline_pg_ports_with_pause = []
    pg_labels = []
    for _, run in runs:
        scenario = run["manifest"]["scenario"]
        pipeline_pg = scenario.get("pipeline_pg")
        by_pg = run["pfc"].get("by_pg", {})
        pipeline_pg_stats = by_pg.get(str(pipeline_pg), {})
        pipeline_pg_pause_events.append(pipeline_pg_stats.get("pause_events", 0))
        pipeline_pg_ports_with_pause.append(pipeline_pg_stats.get("ports_with_pause", 0))
        pg_labels.append(f"pipeline PG {pipeline_pg}")
    xs = list(range(len(labels)))

    fig, ax1 = plt.subplots(figsize=(8.5, 5))
    pipeline_bars = ax1.bar(
        xs,
        pipeline_pg_pause_events,
        width=0.52,
        color="tab:orange",
        alpha=0.80,
        label="pipeline-PG pause events",
    )
    ax1.bar_label(pipeline_bars, fmt="%d", padding=3, fontsize=8)
    ax1.set_xticks(xs)
    ax1.set_xticklabels(labels)
    ax1.set_ylabel("pipeline-PG PFC pause events")
    ax1.grid(True, axis="y", alpha=0.25)
    for x, text in zip(xs, pg_labels):
        ax1.text(x, 0, text, ha="center", va="bottom", rotation=90, fontsize=7, color="dimgray")

    ax2 = ax1.twinx()
    line = ax2.plot(
        xs,
        pipeline_pg_ports_with_pause,
        color="tab:blue",
        marker="o",
        linewidth=2.0,
        label="pipeline-PG ports with pause",
    )
    ax2.set_ylabel("pipeline-PG ports with pause")
    ax2.set_ylim(0, max(1.0, max(pipeline_pg_ports_with_pause, default=0) * 1.25))
    for x, value in zip(xs, pipeline_pg_ports_with_pause):
        ax2.text(x, value, str(value), ha="center", va="bottom", fontsize=8, color="tab:blue")

    handles = [pipeline_bars] + line
    legend_labels = [
        "pipeline-PG pause events",
        "pipeline-PG ports with pause",
    ]
    ax1.legend(handles, legend_labels, loc="upper left")
    ax1.set_title("Pipeline-queue PFC pause counts across three settings")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_timeline(output_path: Path, timeline_data: list[tuple[str, list[dict]]], bin_us: float) -> None:
    import matplotlib.pyplot as plt  # type: ignore

    fig, ax1 = plt.subplots(figsize=(12, 5.5))
    styles = {
        "no_pfc": {"color": "tab:blue", "linestyle": "--", "linewidth": 2.0},
        "same_queue_pfc": {"color": "tab:red", "linestyle": "-", "linewidth": 2.0},
        "mq_rdma": {"color": "tab:green", "linestyle": "-", "linewidth": 2.0},
    }
    for label, rows in timeline_data:
        xs = [float(row["bin_mid_us"]) for row in rows]
        overall = [float(row["overall_cluster_throughput_gbps"]) for row in rows]
        ax1.plot(xs, overall, label=f"{label} overall", **styles[label])
    ax1.set_xlabel("time (us)")
    ax1.set_ylabel("cluster throughput (Gbps)")
    ax1.grid(True, axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    for label, rows in timeline_data:
        if label == "no_pfc":
            continue
        xs = [float(row["bin_mid_us"]) for row in rows]
        pauses = [int(row["pause_events"]) for row in rows]
        alpha = 0.18 if label == "same_queue_pfc" else 0.10
        color = "tab:red" if label == "same_queue_pfc" else "tab:green"
        ax2.bar(xs, pauses, width=bin_us * 0.35, alpha=alpha, color=color, label=f"{label} pause events/bin")
    ax2.set_ylabel(f"pause events per {bin_us:g} us bin")

    handles = ax1.get_lines() + [container for container in ax2.containers]
    labels = [line.get_label() for line in ax1.get_lines()] + [container.get_label() for container in ax2.containers]
    ax1.legend(handles, labels, loc="upper right")
    ax1.set_title("Overall throughput and PFC events across three settings")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main() -> int:
    args = parse_args()
    mix_dir = Path(__file__).resolve().parent
    collector = load_module(mix_dir / "collect_2tor_pfc_hotspot_metrics.py", "collect_metrics")
    runs = [
        ("no_pfc", load_run(collector, mix_dir, args.no_pfc_prefix)),
        ("same_queue_pfc", load_run(collector, mix_dir, args.pfc_prefix)),
        ("mq_rdma", load_run(collector, mix_dir, args.mq_prefix)),
    ]
    common_hotspot_nodes = runs[1][1]["manifest"]["scenario"].get("alltoall_hotspot_nodes", [])
    if not common_hotspot_nodes:
        common_hotspot_nodes = runs[2][1]["manifest"]["scenario"].get("alltoall_hotspot_nodes", [])
    for _, run in runs:
        run["plot_samples"] = load_pattern_samples(
            collector,
            run["fct_path"],
            run["flows"],
            common_hotspot_nodes,
        )

    summary_path = mix_dir / f"comparison_{args.label}.md"
    json_path = mix_dir / f"comparison_{args.label}.json"
    fct_png = mix_dir / f"figure_three_fct_{args.label}.png"
    throughput_box_png = mix_dir / f"figure_three_throughput_{args.label}.png"
    pause_counts_png = mix_dir / f"figure_pause_counts_{args.label}.png"
    timeline_png = mix_dir / f"figure_three_timeline_{args.label}.png"
    pipeline_fct_png = mix_dir / f"figure_pipeline_fct_summary_{args.label}.png"
    pipeline_throughput_png = mix_dir / f"figure_pipeline_throughput_summary_{args.label}.png"

    write_summary(collector, summary_path, runs)
    json_path.write_text(
        json.dumps(
            {
                label: {
                    "prefix": run["prefix"],
                    "scenario": run["manifest"]["scenario"],
                    "pfc": run["pfc"],
                    "table": {pattern: metric_row(collector, run, pattern) for pattern in PATTERNS},
                }
                for label, run in runs
            },
            indent=2,
        )
        + "\n",
        encoding="ascii",
    )

    plot_fct(fct_png, runs, args.plot_pattern)
    plot_goodput_box(throughput_box_png, runs, args.plot_pattern)
    plot_pipeline_fct_summary(pipeline_fct_png, runs, args.plot_pattern)
    plot_pipeline_throughput_summary(pipeline_throughput_png, runs, args.plot_pattern)
    plot_pause_counts(pause_counts_png, runs)
    if args.timeline:
        timeline_data = []
        for label, run in runs:
            timeline_csv = ensure_timeline_csv(mix_dir, run["prefix"], args.bin_us)
            timeline_data.append((label, load_timeline_rows(timeline_csv)))
        plot_timeline(timeline_png, timeline_data, args.bin_us)

    print(summary_path.name)
    print(json_path.name)
    print(fct_png.name)
    print(throughput_box_png.name)
    print(pause_counts_png.name)
    print(pipeline_fct_png.name)
    print(pipeline_throughput_png.name)
    if args.timeline:
        print(timeline_png.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
