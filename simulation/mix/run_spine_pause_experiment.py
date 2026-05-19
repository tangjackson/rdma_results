#!/usr/bin/env python3
"""
One-shot driver for the Spine-P1 PAUSE-onset-vs-N experiment.

This is the single command that produces the final figure described in
the experiment spec:

    X-axis: rank count N (e.g., 2, 4, 6, 8, 12, 16, 24, 32)
    Y-axis: total PAUSE frames observed on Spine P1
    one curve per network configuration
    one matching-color dashed vertical at each config's N* prediction

Pipeline:
    1. For each config in --configs-json (or the built-in CONFIG_PRESETS),
       call run_rank_sweep.py with --scenario 3tier-spine-incast,
       --ranks <ranks>, --tag <config-label>, and the config's network
       parameters. That runs gen + simulator + collector for every N and
       writes rank_sweep_<label>.{json,csv} into mix/.
    2. Once all sweeps are done, call plot_spine_pause_multi_config.py
       with --configs <comma-separated labels> and matching per-config
       --etas / --buffer-mb / --core-gbps lists.

Invoke from the parent of mix/ (i.e., the simulation/ directory) so that
`./waf --run 'scratch/mp-rdma-simulator {config}'` finds waf:

    cd ~/RDMA_PFC_Simulation_with_MP_RDMA/simulation
    python3 mix/run_spine_pause_experiment.py \\
        --ranks 2,4,6,8,12,16,24,32 \\
        --simulator-cmd "./waf --run 'scratch/mp-rdma-simulator {config}'"

By default this runs four configs (eta sweep + buffer sweep). Use
--configs-json to override with your own list.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


# Reasonable defaults for "one curve per network configuration". Each
# entry sets the parameters that the fluid model uses to compute N*, so
# distinct N* values per config make the dashed lines actually fall at
# different X positions in the final figure.
CONFIG_PRESETS: List[Dict[str, object]] = [
    {"label": "eta075_buf2",  "eta": 0.75, "host_gbps": 40, "core_gbps": 100, "buffer_mb": 2,  "q_pfc_bytes": 320_000},
    {"label": "eta050_buf2",  "eta": 0.50, "host_gbps": 40, "core_gbps": 100, "buffer_mb": 2,  "q_pfc_bytes": 320_000},
    {"label": "eta075_buf8",  "eta": 0.75, "host_gbps": 40, "core_gbps": 100, "buffer_mb": 8,  "q_pfc_bytes": 320_000},
    {"label": "eta075_buf2_smallQ", "eta": 0.75, "host_gbps": 40, "core_gbps": 100, "buffer_mb": 2, "q_pfc_bytes": 80_000},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Spine-P1 PAUSE multi-config experiment.")
    parser.add_argument("--ranks", default="2,4,6,8,12,16,24,32",
                        help="Comma-separated N values to sweep per config.")
    parser.add_argument("--configs-json", default=None,
                        help="Optional path to a JSON file listing configs (same shape as the built-in "
                             "CONFIG_PRESETS). When omitted the built-in preset is used.")
    parser.add_argument("--simulator-cmd", required=True,
                        help="Shell template for the NS-3 run; {config} is replaced with the absolute "
                             "path to each per-N config file. Typically: "
                             "\"./waf --run 'scratch/mp-rdma-simulator {config}'\"")
    parser.add_argument("--mix-dir", default=None,
                        help="Path to the mix/ dir holding run_rank_sweep.py and plot_*.py. "
                             "Defaults to this script's directory.")
    parser.add_argument("--out-png", default=None,
                        help="Output PNG path. Default: mix/spine_pause_multi_config.png")
    parser.add_argument("--skip-sweep", action="store_true",
                        help="Skip running rank sweeps; only re-aggregate existing rank_sweep_<tag>.json "
                             "files and re-render the plot.")
    parser.add_argument("--no-victim-flow", action="store_true")
    parser.add_argument("--no-normal-flow", action="store_true")
    # Defaults chosen so the receiver-side egress queue stays loaded long
    # enough for shared-buffer pressure to reach the uplink ingress at
    # high N. A single 64KB incast empties before propagation can occur.
    parser.add_argument("--victim-flow-bytes", type=int, default=16 * 1024 * 1024,
                        help="Sender 0 -> Receiver 0 cross-ToR flow bytes (default 16 MB so the "
                             "victim is active across the entire incast window).")
    parser.add_argument("--normal-flow-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--incast-flow-bytes", type=int, default=1024 * 1024,
                        help="Per-sender incast bytes (default 1 MB so the egress queue builds "
                             "past Q_pfc and shared buffer pressure can reach the uplink).")
    parser.add_argument("--incast-rounds", type=int, default=4,
                        help="Repeat the incast burst this many times (default 4) so the queue "
                             "stays charged across PFC OFF/ON cycles.")
    parser.add_argument("--threshold-kind", default="auto",
                        choices=("auto", "n_star", "n_double_star", "both"),
                        help="Threshold to draw in the overlay plot. Default 'auto' uses N** for "
                             "propagation metrics (the right model for Spine-P1 PAUSE).")
    return parser.parse_args()


def load_configs(path: Optional[str]) -> List[Dict[str, object]]:
    if path is None:
        return CONFIG_PRESETS
    payload = json.loads(Path(path).read_text(encoding="ascii"))
    if isinstance(payload, dict) and "configs" in payload:
        payload = payload["configs"]
    if not isinstance(payload, list):
        raise SystemExit(f"{path}: expected a JSON list of config dicts")
    return payload  # trust the caller's keys to match CONFIG_PRESETS shape


def run_sweep(mix_dir: Path, ranks: str, args: argparse.Namespace, cfg: Dict[str, object]) -> None:
    label = str(cfg["label"])
    cmd = [
        sys.executable, str(mix_dir / "run_rank_sweep.py"),
        "--scenario", "3tier-spine-incast",
        "--ranks", ranks,
        "--prefix-base", f"3tier_{label}",
        "--tag", label,
        "--host-gbps", str(cfg["host_gbps"]),
        "--core-gbps", str(cfg["core_gbps"]),
        "--buffer-mb", str(cfg["buffer_mb"]),
        "--pfc-xoff-bytes", str(cfg["q_pfc_bytes"]),
        "--pfc-xon-bytes", str(int(int(cfg["q_pfc_bytes"]) / 2)),
        "--victim-flow-bytes", str(args.victim_flow_bytes),
        "--normal-flow-bytes", str(args.normal_flow_bytes),
        "--incast-flow-bytes", str(args.incast_flow_bytes),
        "--incast-rounds", str(args.incast_rounds),
        "--simulator-cmd", args.simulator_cmd,
    ]
    if args.no_victim_flow:
        cmd.append("--no-victim-flow")
    if args.no_normal_flow:
        cmd.append("--no-normal-flow")
    print(f"\n=== sweeping config {label}: {cfg} ===\n", flush=True)
    subprocess.run(cmd, check=True, cwd=mix_dir.parent)  # run from simulation/ so ./waf is found


def render_plot(mix_dir: Path, configs: List[Dict[str, object]], out_png: Optional[str],
                threshold_kind: str = "auto") -> Path:
    labels = [str(cfg["label"]) for cfg in configs]
    etas = ",".join(f"{cfg['eta']}" for cfg in configs)
    buffers = ",".join(f"{cfg['buffer_mb']}" for cfg in configs)
    cores = ",".join(f"{cfg['core_gbps']}" for cfg in configs)
    out = Path(out_png).resolve() if out_png else (mix_dir / "spine_pause_multi_config.png")
    cmd = [
        sys.executable, str(mix_dir / "plot_spine_pause_multi_config.py"),
        "--configs", ",".join(labels),
        "--labels", ",".join(labels),
        "--etas", etas,
        "--buffer-mb", buffers,
        "--core-gbps", cores,
        "--threshold-kind", threshold_kind,
        "--out", str(out),
        "--mix-dir", str(mix_dir),
    ]
    print(f"\n=== rendering overlay plot ===\n", flush=True)
    subprocess.run(cmd, check=True)
    return out


def main() -> int:
    args = parse_args()
    mix_dir = Path(args.mix_dir).resolve() if args.mix_dir else Path(__file__).resolve().parent
    configs = load_configs(args.configs_json)
    if not configs:
        raise SystemExit("no configs to run")
    if not args.skip_sweep:
        for cfg in configs:
            run_sweep(mix_dir, args.ranks, args, cfg)
    out_png = render_plot(mix_dir, configs, args.out_png, args.threshold_kind)
    print(f"\n=== done: {out_png} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
