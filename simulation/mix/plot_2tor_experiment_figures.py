#!/usr/bin/env python3
"""
Generate two figures for the 2-ToR experiment:

1. Overall FCT comparison across a PFC run and a no-PFC baseline.
2. Time series of throughput and PFC event counts for the PFC run.

Typical usage:
  python3 mix/plot_2tor_experiment_figures.py \
    --prefix 2tor_alltoall_pfc_trigger \
    --baseline-prefix 2tor_alltoall_no_pfc_baseline \
    --bin-us 10
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate FCT and throughput/PFC plots for the 2-ToR experiment.")
    parser.add_argument("--prefix", required=True, help="Primary experiment prefix under mix/.")
    parser.add_argument("--baseline-prefix", required=True, help="Baseline experiment prefix under mix/.")
    parser.add_argument("--bin-us", type=float, default=10.0, help="Bin width in microseconds for the timeline.")
    return parser.parse_args()


def load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def render_timeline_figure(
    timeline_rows: list[dict],
    prefix: str,
    bin_us: float,
    output_path: Path,
    baseline_rows: list[dict] | None = None,
    baseline_prefix: str | None = None,
) -> None:
    import matplotlib.pyplot as plt  # type: ignore

    xs = [float(row["bin_mid_us"]) for row in timeline_rows]
    pause_counts = [int(row["pause_events"]) for row in timeline_rows]
    overall_tput = [float(row["overall_cluster_throughput_gbps"]) for row in timeline_rows]

    fig, ax1 = plt.subplots(figsize=(12, 5.5))
    ax1.plot(xs, overall_tput, color="tab:gray", linewidth=2.0, label=f"{prefix} overall")
    if baseline_rows is not None:
        baseline_xs = [float(row["bin_mid_us"]) for row in baseline_rows]
        baseline_overall_tput = [float(row["overall_cluster_throughput_gbps"]) for row in baseline_rows]
        baseline_label = baseline_prefix or "baseline"
        ax1.plot(
            baseline_xs,
            baseline_overall_tput,
            color="tab:gray",
            linewidth=1.8,
            linestyle="--",
            label=f"{baseline_label} overall",
        )
    ax1.set_xlabel("time (us)")
    ax1.set_ylabel("cluster throughput (Gbps)")
    ax1.grid(True, axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    ax2.bar(xs, pause_counts, width=bin_us * 0.45, color="tab:red", alpha=0.22, label="pause events/bin")
    ax2.set_ylabel(f"pause events per {bin_us:g} us bin")

    lines = ax1.get_lines() + [ax2.containers[0]]
    labels = [line.get_label() for line in ax1.get_lines()] + ["pause events/bin"]
    ax1.legend(lines, labels, loc="upper right")
    title = f"Overall/all-to-all throughput and pause events: {prefix}"
    if baseline_prefix:
        title += f" vs {baseline_prefix}"
    ax1.set_title(title.replace("Overall/all-to-all", "Overall"))

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def load_overall_fct_samples(fct_path: Path) -> list[float]:
    samples = []
    with fct_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 8:
                continue
            fct_ns = int(parts[6])
            samples.append(fct_ns / 1e3)
    return samples


def main() -> int:
    args = parse_args()
    mix_dir = Path(__file__).resolve().parent
    collector = load_module(mix_dir / "collect_2tor_pfc_hotspot_metrics.py", "collect_metrics")

    primary_manifest = collector.load_manifest(mix_dir, args.prefix)
    baseline_manifest = collector.load_manifest(mix_dir, args.baseline_prefix)

    primary_fct = mix_dir / Path(primary_manifest["files"]["fct"]).name
    baseline_fct = mix_dir / Path(baseline_manifest["files"]["fct"]).name
    if not primary_fct.exists():
        raise SystemExit(f"missing fct file: {primary_fct}")
    if not baseline_fct.exists():
        raise SystemExit(f"missing fct file: {baseline_fct}")

    timeline_csv = ensure_timeline_csv(mix_dir, args.prefix, args.bin_us)
    timeline_rows = load_timeline_rows(timeline_csv)
    baseline_timeline_csv = ensure_timeline_csv(mix_dir, args.baseline_prefix, args.bin_us)
    baseline_timeline_rows = load_timeline_rows(baseline_timeline_csv)

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        raise SystemExit(f"matplotlib is required: {exc}")

    fct_png = mix_dir / f"figure_fct_compare_{args.prefix}_vs_{args.baseline_prefix}.png"
    primary_fct_samples = load_overall_fct_samples(primary_fct)
    baseline_fct_samples = load_overall_fct_samples(baseline_fct)
    fig, ax = plt.subplots(figsize=(7, 5))
    bp = ax.boxplot(
        [baseline_fct_samples, primary_fct_samples],
        labels=[args.baseline_prefix, args.prefix],
        patch_artist=True,
        showfliers=False,
        widths=0.55,
    )
    colors = ["tab:blue", "tab:red"]
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.35)
    for median in bp["medians"]:
        median.set_color("black")
        median.set_linewidth(1.5)
    ax.set_ylabel("overall FCT (us)")
    ax.set_title("Overall FCT distribution: PFC vs no-PFC")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(fct_png, dpi=150)
    plt.close(fig)

    timeline_png = mix_dir / f"figure_timeline_{args.prefix}.png"
    render_timeline_figure(
        timeline_rows,
        args.prefix,
        args.bin_us,
        timeline_png,
        baseline_rows=baseline_timeline_rows,
        baseline_prefix=args.baseline_prefix,
    )
    baseline_timeline_png = mix_dir / f"figure_timeline_{args.baseline_prefix}.png"
    render_timeline_figure(baseline_timeline_rows, args.baseline_prefix, args.bin_us, baseline_timeline_png)

    print(fct_png.name)
    print(timeline_png.name)
    print(baseline_timeline_png.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
