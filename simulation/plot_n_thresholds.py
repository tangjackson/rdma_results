#!/usr/bin/env python3
"""
Plot N* vs N** vs the empirical propagation threshold for the rank sweep.

Reads mix/rank_sweep_<tag>.json produced by run_rank_sweep.py, computes
model predictions via n_threshold_model.py, and writes a single PNG that
overlays:

  - Bars: pause_intra_tor and pause_inter_tor (= pause_tor_to_spine) per N
  - Vertical lines: model N*, model N**, and empirical N_propagate
  - Secondary axis line: victim pipeline FCT inflation vs the smallest-N
    baseline (where pause_inter_tor is still zero)

The intent is the figure described in Step 3/5 of the plan: N** matches
N_propagate while N* fires too early.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import n_threshold_model  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot rank sweep with N*, N**, empirical threshold.")
    parser.add_argument("--tag", default="default",
                        help="Reads mix/rank_sweep_{tag}.json.")
    parser.add_argument("--out", default=None,
                        help="Output PNG path. Default: alltoall_n_star_vs_n_double_star_{tag}.png")
    parser.add_argument("--mix-dir", default=None)
    parser.add_argument("--eta", type=float, default=0.75,
                        help="Burst factor used in the analytic model.")
    parser.add_argument("--core-gbps", type=float, default=100.0)
    parser.add_argument("--buffer-mb", type=float, default=2.0,
                        help="Total switch buffer (MMU pool) in MB used for N**.")
    parser.add_argument("--headroom-kb", type=float, default=30.0)
    parser.add_argument("--num-ingress", type=int, default=17)
    parser.add_argument("--pfc-response-us", type=float, default=2.0)
    return parser.parse_args()


def empirical_n_propagate(rows: Sequence[Dict[str, object]]) -> Optional[int]:
    """Smallest N at which pause_inter_tor first becomes > 0."""
    for row in sorted(rows, key=lambda r: r["ranks"]):
        if (row.get("pause_inter_tor") or 0) > 0:
            return int(row["ranks"])  # type: ignore[arg-type]
    return None


def fct_inflation(rows: Sequence[Dict[str, object]], field: str) -> List[Optional[float]]:
    """Percent inflation of `field` vs the smallest-N (assumed clean) baseline."""
    sorted_rows = sorted(rows, key=lambda r: r["ranks"])
    if not sorted_rows:
        return []
    baseline = sorted_rows[0].get(field)
    out: List[Optional[float]] = []
    for row in sorted_rows:
        value = row.get(field)
        if baseline is None or value is None or baseline == 0:
            out.append(None)
        else:
            out.append((float(value) - float(baseline)) * 100.0 / float(baseline))
    return out


def main() -> int:
    args = parse_args()
    mix_dir = Path(args.mix_dir).resolve() if args.mix_dir else Path(__file__).resolve().parent
    data_path = mix_dir / f"rank_sweep_{args.tag}.json"
    if not data_path.exists():
        raise SystemExit(f"missing rank-sweep aggregate: {data_path}")
    bundle = json.loads(data_path.read_text(encoding="ascii"))
    rows = sorted(bundle.get("rows", []), key=lambda r: r["ranks"])
    if not rows:
        raise SystemExit("rank-sweep aggregate has no rows")

    host_gbps = float(rows[0].get("host_link_gbps") or 40.0)
    pfc_xoff = int(rows[0].get("pfc_xoff_bytes") or 320_000)

    inputs = n_threshold_model.default_inputs(
        host_gbps=host_gbps,
        core_gbps=args.core_gbps,
        q_pfc_bytes=pfc_xoff,
        eta=args.eta,
        buffer_mb=args.buffer_mb,
        headroom_kb=args.headroom_kb,
        num_ingress=args.num_ingress,
        pfc_response_us=args.pfc_response_us,
    )
    model = n_threshold_model.compute(inputs)
    n_star = model["n_star_real"]
    n_double_star = model["n_double_star_real"]
    n_emp = empirical_n_propagate(rows)

    ranks = [int(row["ranks"]) for row in rows]
    intra = [int(row.get("pause_intra_tor") or 0) for row in rows]
    inter = [int(row.get("pause_inter_tor") or 0) for row in rows]
    pipeline_inflation = fct_inflation(rows, "pipeline_non_hotspot_avg_fct_us")
    if all(value is None for value in pipeline_inflation):
        # Fall back to overall pipeline FCT if no non-hotspot pattern.
        pipeline_inflation = fct_inflation(rows, "pipeline_avg_fct_us")

    fig, ax_bar = plt.subplots(figsize=(8.5, 4.6))
    width = 0.4
    xs = list(range(len(ranks)))
    ax_bar.bar([x - width / 2 for x in xs], intra, width=width,
               color="#6c8ebf", label="pause_intra_tor")
    ax_bar.bar([x + width / 2 for x in xs], inter, width=width,
               color="#b85450", label="pause_inter_tor (propagated)")
    ax_bar.set_xticks(xs)
    ax_bar.set_xticklabels([str(n) for n in ranks])
    ax_bar.set_xlabel("All-to-all ranks N on the hotspot ToR")
    ax_bar.set_ylabel("PFC PAUSE events (count)")
    ax_bar.set_yscale("symlog", linthresh=1)

    def vline(value: float, color: str, label: str) -> None:
        if value is None or not math.isfinite(value):
            return
        # Translate N value to bar x-coordinate by linear interpolation
        # between the discrete ranks shown.
        x_lo = None
        x_hi = None
        for idx, n in enumerate(ranks):
            if n <= value:
                x_lo = (idx, n)
            if n >= value and x_hi is None:
                x_hi = (idx, n)
        if x_lo is None:
            x_pos = -0.5
        elif x_hi is None or x_lo[1] == value:
            x_pos = x_lo[0]
        elif x_lo == x_hi:
            x_pos = x_lo[0]
        else:
            x_pos = x_lo[0] + (value - x_lo[1]) / (x_hi[1] - x_lo[1]) * (x_hi[0] - x_lo[0])
        ax_bar.axvline(x_pos, color=color, linestyle="--", linewidth=1.5,
                       label=f"{label} = {value:.2f}")

    vline(n_star, "#1f77b4", "N* (local fluid)")
    vline(n_double_star, "#d62728", "N** (two-hop)")
    if n_emp is not None:
        vline(float(n_emp), "#2ca02c", "N_propagate (empirical)")

    ax_fct = ax_bar.twinx()
    valid_inflation = [v for v in pipeline_inflation if v is not None]
    if valid_inflation:
        ax_fct.plot(xs, [v if v is not None else 0 for v in pipeline_inflation],
                    color="#7f7f7f", marker="o", linewidth=1.6,
                    label="victim pipeline FCT inflation %")
        ax_fct.set_ylabel("Victim pipeline FCT inflation (% vs smallest N)")
    else:
        ax_fct.set_yticks([])

    # Combine legends
    h1, l1 = ax_bar.get_legend_handles_labels()
    h2, l2 = ax_fct.get_legend_handles_labels()
    ax_bar.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8, framealpha=0.9)

    title = (f"Rank sweep: pause_intra_tor vs pause_inter_tor vs N\n"
             f"host={host_gbps:g} Gbps  core={args.core_gbps:g} Gbps  "
             f"Q_pfc={pfc_xoff/1024:.0f} KB  eta={args.eta}")
    ax_bar.set_title(title, fontsize=10)
    fig.tight_layout()

    out_path = (
        Path(args.out).resolve()
        if args.out
        else (mix_dir / f"alltoall_n_star_vs_n_double_star_{args.tag}.png")
    )
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(out_path.name)
    print(json.dumps({
        "n_star": model["n_star"],
        "n_star_real": n_star,
        "n_double_star": model["n_double_star"],
        "n_double_star_real": n_double_star,
        "n_propagate_empirical": n_emp,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
