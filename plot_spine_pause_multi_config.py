#!/usr/bin/env python3
"""
Overlay PAUSE-on-Spine-P1 vs N curves across multiple network configurations.

Each configuration is one rank_sweep_<tag>.json produced by
run_rank_sweep.py with --scenario 3tier-spine-incast. For each config we
plot:

    X-axis: rank count N (number of incast senders on ToR 1)
    Y-axis: total PAUSE frames observed on Spine P1
            (i.e., pause_tor_to_spine in the per-hop classifier, which
             corresponds to ToR 1 emitting PAUSE on its uplink port; the
             Spine "observes" these as PAUSE arrivals on P1)
    one curve per config, one dashed vertical line per config marking
    that config's predicted N* from n_threshold_model.

The visual claim is the same as the user-specified updated experiment:
each curve rises from zero exactly at its dashed line, validating that
the fluid model predicts the right propagation threshold per config.

Usage:

    python3 mix/plot_spine_pause_multi_config.py \\
        --configs baseline,big-buffer,small-q,low-eta \\
        --etas 0.75,0.75,0.75,0.50 \\
        --core-gbps 100,100,100,100 \\
        --buffer-mb 2,8,2,2

Each "config" is a tag whose rank_sweep_<tag>.json must exist. The
matching --etas / --core-gbps / --buffer-mb lists tell the model what
parameters were used so per-config N* lines can be plotted.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import n_threshold_model  # noqa: E402


DEFAULT_COLORS = (
    "#1f77b4", "#d62728", "#2ca02c", "#9467bd",
    "#ff7f0e", "#8c564b", "#e377c2", "#7f7f7f",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Overlay Spine-P1 PAUSE vs N for multiple configs.")
    parser.add_argument("--configs", required=True,
                        help="Comma-separated rank-sweep tags. Each must have a rank_sweep_<tag>.json.")
    parser.add_argument("--labels", default=None,
                        help="Comma-separated curve labels. Defaults to the tag names.")
    parser.add_argument("--etas", default=None,
                        help="Per-config eta values. Single value broadcasts to all. Default 0.75.")
    parser.add_argument("--core-gbps", default=None,
                        help="Per-config core (ToR uplink/spine) Gbps. Single value broadcasts.")
    parser.add_argument("--buffer-mb", default=None,
                        help="Per-config switch buffer MB. Single value broadcasts. Default 2.")
    parser.add_argument("--headroom-kb", default=None,
                        help="Per-config per-ingress headroom KB. Single value broadcasts. Default 30.")
    parser.add_argument("--pfc-response-us", type=float, default=2.0)
    parser.add_argument("--num-ingress", type=int, default=17)
    parser.add_argument("--metric", default="pause_tor_to_spine",
                        choices=("pause_tor_to_spine", "pause_inter_tor",
                                 "pause_spine_to_tor", "pause_intra_tor",
                                 "pause_switch_total", "pause_rank_total"),
                        help="Which PFC bucket to plot. Default pause_tor_to_spine "
                             "(= PAUSE on Spine P1 in 3-tier mode). pause_switch_total "
                             "is all inter-switch-link pauses excluding host/rank ports.")
    parser.add_argument("--threshold-kind", default="auto",
                        choices=("auto", "n_star", "n_double_star", "both"),
                        help="Which model threshold to draw as the dashed vertical. "
                             "'auto' picks N* for local-pause metrics and N** for "
                             "propagation metrics. 'both' draws both lines per config.")
    parser.add_argument("--y-log", action="store_true",
                        help="Use symlog y-axis (helps when one config explodes).")
    parser.add_argument("--out", default=None)
    parser.add_argument("--mix-dir", default=None)
    return parser.parse_args()


def select_threshold_kind(metric: str, requested: str) -> str:
    if requested != "auto":
        return requested
    # Local-pause metrics (rank/host-facing ports) are what the single-port
    # fluid model N* predicts. Inter-switch propagation metrics use N**.
    local_metrics = ("pause_intra_tor", "pause_rank_total")
    return "n_star" if metric in local_metrics else "n_double_star"


def split_list(spec: Optional[str], count: int, default: float, kind: type = float) -> List[float]:
    if spec is None:
        return [kind(default)] * count
    items = [kind(x.strip()) for x in spec.split(",") if x.strip()]
    if len(items) == 1:
        return items * count
    if len(items) != count:
        raise SystemExit(f"expected {count} values, got {len(items)} from {spec!r}")
    return items


def load_curve(mix_dir: Path, tag: str) -> Tuple[List[int], List[int], Dict[str, object]]:
    path = mix_dir / f"rank_sweep_{tag}.json"
    if not path.exists():
        raise SystemExit(f"missing rank-sweep aggregate: {path}")
    bundle = json.loads(path.read_text(encoding="ascii"))
    rows = sorted(bundle.get("rows", []), key=lambda r: r["ranks"])
    return rows, bundle  # type: ignore[return-value]


def main() -> int:
    args = parse_args()
    mix_dir = Path(args.mix_dir).resolve() if args.mix_dir else Path(__file__).resolve().parent
    tags = [s.strip() for s in args.configs.split(",") if s.strip()]
    if not tags:
        raise SystemExit("--configs must list at least one tag")
    labels = (
        [s.strip() for s in args.labels.split(",") if s.strip()]
        if args.labels
        else list(tags)
    )
    if len(labels) != len(tags):
        raise SystemExit("--labels must have the same number of entries as --configs")

    etas = split_list(args.etas, len(tags), 0.75)
    core_gbps = split_list(args.core_gbps, len(tags), 100.0)
    buffers = split_list(args.buffer_mb, len(tags), 2.0)
    headrooms = split_list(args.headroom_kb, len(tags), 30.0)

    threshold_kind = select_threshold_kind(args.metric, args.threshold_kind)
    fig, ax = plt.subplots(figsize=(9.0, 5.0))
    summary_lines: List[Dict[str, object]] = []
    for idx, tag in enumerate(tags):
        rows, _bundle = load_curve(mix_dir, tag)
        if not rows:
            print(f"[warn] tag={tag}: no rows in rank-sweep aggregate", flush=True)
            continue
        ranks = [int(row["ranks"]) for row in rows]
        ys = [int(row.get(args.metric) or 0) for row in rows]
        host_gbps = float(rows[0].get("host_link_gbps") or 40.0)
        q_pfc = int(rows[0].get("pfc_xoff_bytes") or 320_000)
        inputs = n_threshold_model.default_inputs(
            host_gbps=host_gbps,
            core_gbps=core_gbps[idx],
            q_pfc_bytes=q_pfc,
            eta=etas[idx],
            buffer_mb=buffers[idx],
            headroom_kb=headrooms[idx],
            num_ingress=args.num_ingress,
            pfc_response_us=args.pfc_response_us,
        )
        model = n_threshold_model.compute(inputs)
        n_star = model["n_star_real"]
        n_double_star = model["n_double_star_real"]
        # First N where the metric is > 0 -- the empirical onset.
        n_empirical = next((int(r["ranks"]) for r, y in zip(rows, ys) if y > 0), None)

        color = DEFAULT_COLORS[idx % len(DEFAULT_COLORS)]
        ax.plot(ranks, ys, color=color, marker="o", linewidth=1.8,
                label=f"{labels[idx]} (eta={etas[idx]}, buf={buffers[idx]}MB, Q={q_pfc/1024:.0f}KB)")
        if threshold_kind in ("n_star", "both"):
            ax.axvline(n_star, color=color, linestyle="--", linewidth=1.2,
                       alpha=0.7,
                       label=f"  N* = {n_star:.2f}")
        if threshold_kind in ("n_double_star", "both"):
            ax.axvline(n_double_star, color=color, linestyle=":", linewidth=1.4,
                       alpha=0.85,
                       label=f"  N** = {n_double_star:.2f}")
        summary_lines.append({
            "tag": tag,
            "label": labels[idx],
            "eta": etas[idx],
            "core_gbps": core_gbps[idx],
            "buffer_mb": buffers[idx],
            "n_star_real": n_star,
            "n_star": model["n_star"],
            "n_double_star_real": model["n_double_star_real"],
            "n_double_star": model["n_double_star"],
            "n_empirical_onset": n_empirical,
        })

    ax.set_xlabel("Rank count N (incast senders on ToR 1)")
    ax.set_ylabel(f"PAUSE frames on Spine P1  ({args.metric})")
    if args.y_log:
        ax.set_yscale("symlog", linthresh=1)
    ax.set_title("Spine-P1 PAUSE onset vs N — model N* dashed line should align with each curve")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()

    out_path = (
        Path(args.out).resolve()
        if args.out
        else (mix_dir / "spine_pause_multi_config.png")
    )
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(out_path.name)
    print(json.dumps({"configs": summary_lines}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
