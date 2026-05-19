#!/usr/bin/env python3
"""
Rank sweep driver for the 2-ToR PFC propagation study (Step 2 of the plan).

For each N in --ranks, this script:
  1. Calls gen_2tor_pfc_hotspot.py to write a fresh mix/* config with
     --alltoall-nodes N and a concurrent victim pipeline flow.
  2. (Optionally) invokes the simulator with --simulator-cmd CMD where
     {config} expands to the path of the generated config file. Skip with
     --no-run to only stage configs and parse pre-existing outputs.
  3. Runs collect_2tor_pfc_hotspot_metrics.py against the prefix.
  4. Reads summary_<prefix>.json and pulls out the per-hop PFC counts and
     pipeline_non_hotspot FCT — the metrics that matter for distinguishing
     pause_intra_tor onset from pause_inter_tor (=pause_tor_to_spine)
     onset, and for measuring victim-flow FCT inflation.

Aggregate output is written to mix/rank_sweep_<tag>.json and a flat
mix/rank_sweep_<tag>.csv that downstream plotting scripts consume.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence


HOSTS_PER_TOR = 16

SCENARIO_LEGACY = "2tor-alltoall-pipeline"
SCENARIO_3TIER = "3tier-spine-incast"

# Per-N metrics pulled from each summary_<prefix>.json. Keep in sync with
# plot_n_thresholds.py and plot_spine_pause_multi_config.py.
SWEEP_FIELDS = (
    "ranks",
    "prefix",
    "pause_events",
    "pause_intra_tor",
    "pause_inter_tor",
    "pause_tor_to_spine",      # = "PAUSE on Spine P1" in 3-tier mode
    "pause_spine_to_tor",
    "pause_host_to_tor",
    "intra_tor_first_us",
    "inter_tor_first_us",
    "tor_to_spine_first_us",
    "spine_to_tor_first_us",
    "pipeline_avg_fct_us",
    "pipeline_p95_fct_us",
    "pipeline_non_hotspot_avg_fct_us",
    "pipeline_non_hotspot_p95_fct_us",
    "alltoall_avg_fct_us",
    "alltoall_p95_fct_us",
    "victim_avg_fct_us",
    "victim_p95_fct_us",
    "normal_avg_fct_us",
    "normal_p95_fct_us",
    "incast_avg_fct_us",
    "incast_p95_fct_us",
    "host_link_gbps",
    "tor_link_gbps",
    "pfc_xoff_bytes",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep ranks N and aggregate per-N PFC/FCT metrics.")
    parser.add_argument("--scenario", choices=(SCENARIO_LEGACY, SCENARIO_3TIER),
                        default=SCENARIO_LEGACY,
                        help="Which gen-script scenario to drive. In 3tier mode --ranks is "
                             "interpreted as the number of incast senders (not all-to-all rank).")
    parser.add_argument("--ranks", default="2,4,8,12,16",
                        help="Comma-separated list of N. In legacy mode N = all-to-all "
                             "rank count; in 3tier-spine-incast mode N = incast sender count.")
    parser.add_argument("--prefix-base", default="2tor_rank_sweep",
                        help="Per-N config prefix is {prefix_base}_N{N}.")
    parser.add_argument("--tag", default="default",
                        help="Aggregate output is mix/rank_sweep_{tag}.{json,csv}.")
    parser.add_argument("--alltoall-rack", type=int, choices=(0, 1), default=1,
                        help="Which rack the all-to-all hotspot lives on (default 1 = hosts 16-31).")
    parser.add_argument("--alltoall-flow-bytes", type=int, default=64 * 1024)
    parser.add_argument("--alltoall-rounds", type=int, default=1)
    parser.add_argument("--pipeline-nodes-per-rack", type=int, default=8,
                        help="Number of pipeline hosts per rack (1 cross-ToR victim flow needs >= 1).")
    parser.add_argument("--pipeline-rounds", type=int, default=4)
    parser.add_argument("--pipeline-flow-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--host-gbps", type=float, default=40.0)
    parser.add_argument("--core-gbps", type=float, default=100.0)
    parser.add_argument("--buffer-mb", type=int, default=2)
    parser.add_argument("--pfc-xoff-bytes", type=int, default=320_000)
    parser.add_argument("--pfc-xon-bytes", type=int, default=160_000)
    # 3-tier-specific knobs (passed through to gen)
    parser.add_argument("--victim-flow-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--normal-flow-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--incast-flow-bytes", type=int, default=64 * 1024)
    parser.add_argument("--incast-rounds", type=int, default=1)
    parser.add_argument("--no-victim-flow", action="store_true")
    parser.add_argument("--no-normal-flow", action="store_true")
    parser.add_argument("--simulator-cmd", default=None,
                        help="Shell command template for the NS-3 run. {config} is replaced "
                             "with the absolute path to the generated config. Skipped if --no-run.")
    parser.add_argument("--no-run", action="store_true",
                        help="Only stage configs; do not invoke simulator. Use this if you "
                             "are running NS-3 manually or only re-aggregating existing outputs.")
    parser.add_argument("--mix-dir", default=None,
                        help="Directory holding gen_*.py / collect_*.py (defaults to script dir).")
    return parser.parse_args()


def parse_rank_list(spec: str, scenario: str) -> List[int]:
    out: List[int] = []
    for chunk in spec.split(","):
        item = chunk.strip()
        if not item:
            continue
        out.append(int(item))
    if not out:
        raise SystemExit("--ranks must contain at least one integer")
    # In 3tier-spine-incast the ToR1 host count grows with N, so we don't
    # cap it at HOSTS_PER_TOR. In legacy mode keep the original [2, 16] cap.
    if scenario == SCENARIO_LEGACY:
        for n in out:
            if n < 2 or n > HOSTS_PER_TOR:
                raise SystemExit(f"rank {n} out of valid range [2, {HOSTS_PER_TOR}] for legacy scenario")
    else:
        for n in out:
            if n < 1:
                raise SystemExit(f"rank {n} out of valid range (>=1) for 3tier scenario")
    return out


def gen_config(mix_dir: Path, args: argparse.Namespace, ranks: int) -> str:
    """Invoke gen_2tor_pfc_hotspot.py for a single N. Returns the prefix."""
    prefix = f"{args.prefix_base}_N{ranks:02d}"
    if args.scenario == SCENARIO_3TIER:
        cmd = [
            sys.executable,
            str(mix_dir / "gen_2tor_pfc_hotspot.py"),
            "--scenario", SCENARIO_3TIER,
            "--prefix", prefix,
            "--incast-senders", str(ranks),
            "--incast-flow-bytes", str(args.incast_flow_bytes),
            "--incast-rounds", str(args.incast_rounds),
            "--victim-flow-bytes", str(args.victim_flow_bytes),
            "--normal-flow-bytes", str(args.normal_flow_bytes),
            "--host-link-rate-gbps", str(args.host_gbps),
            "--tor-link-rate-gbps", str(args.core_gbps),
            "--link-rate-gbps", str(args.core_gbps),
            "--buffer-size-mb", str(args.buffer_mb),
            "--pfc-xoff-bytes", str(args.pfc_xoff_bytes),
            "--pfc-xon-bytes", str(args.pfc_xon_bytes),
        ]
        if args.no_victim_flow:
            cmd.append("--no-victim-flow")
        if args.no_normal_flow:
            cmd.append("--no-normal-flow")
    else:
        # Legacy 2-tor alltoall + pipeline path.
        base = args.alltoall_rack * HOSTS_PER_TOR
        node_list = f"{base}-{base + ranks - 1}"
        cmd = [
            sys.executable,
            str(mix_dir / "gen_2tor_pfc_hotspot.py"),
            "--prefix", prefix,
            "--alltoall-node-list", node_list,
            "--alltoall-rack", str(args.alltoall_rack),
            "--alltoall-nodes", str(ranks),
            "--alltoall-flow-bytes", str(args.alltoall_flow_bytes),
            "--alltoall-rounds", str(args.alltoall_rounds),
            "--pipeline-nodes-per-rack", str(args.pipeline_nodes_per_rack),
            "--pipeline-rounds", str(args.pipeline_rounds),
            "--pipeline-flow-bytes", str(args.pipeline_flow_bytes),
            "--host-link-rate-gbps", str(args.host_gbps),
            "--tor-link-rate-gbps", str(args.core_gbps),
            "--link-rate-gbps", str(args.core_gbps),
            "--buffer-size-mb", str(args.buffer_mb),
            "--pfc-xoff-bytes", str(args.pfc_xoff_bytes),
            "--pfc-xon-bytes", str(args.pfc_xon_bytes),
        ]
    print(f"[gen] scenario={args.scenario} N={ranks} prefix={prefix}", flush=True)
    subprocess.run(cmd, check=True, cwd=mix_dir)
    return prefix


def run_simulator(mix_dir: Path, prefix: str, template: str) -> None:
    # Run from the parent of mix/ (typically simulation/) so that the user's
    # ./waf is on the working directory. The config file path is absolute
    # and the config's TOPOLOGY_FILE / FLOW_FILE / ... entries are relative
    # like 'mix/topo_X.txt' -- which resolve correctly when cwd is the parent
    # of mix/.
    config = (mix_dir / f"config_{prefix}.txt").resolve()
    cmd = template.format(config=str(config))
    print(f"[sim] {cmd}", flush=True)
    subprocess.run(shlex.split(cmd), check=True, cwd=mix_dir.parent)


def run_collector(mix_dir: Path, prefix: str) -> Path:
    cmd = [sys.executable, str(mix_dir / "collect_2tor_pfc_hotspot_metrics.py"),
           "--prefix", prefix]
    print(f"[collect] prefix={prefix}", flush=True)
    subprocess.run(cmd, check=True, cwd=mix_dir)
    return mix_dir / f"summary_{prefix}.json"


def _safe(summary: dict, *path, default=None):
    cur = summary
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def extract_row(prefix: str, ranks: int, summary_json: Path) -> Dict[str, object]:
    summary = json.loads(summary_json.read_text(encoding="ascii"))
    pfc = summary.get("pfc", {})
    fct = summary.get("fct", {}).get("patterns", {})
    by_hop = pfc.get("by_hop", {}) or {}
    scenario = summary.get("scenario", {})

    def hop(name: str, field: str, default=0):
        return _safe(by_hop, name, field, default=default)

    def first_us(bucket: str) -> Optional[float]:
        ns = hop(bucket, "first_pause_ns", default=None)
        return ns / 1e3 if ns is not None else None

    return {
        "ranks": ranks,
        "prefix": prefix,
        "pause_events": pfc.get("pause_events", 0),
        "pause_intra_tor": hop("pause_intra_tor", "pause_events"),
        "pause_inter_tor": hop("pause_inter_tor", "pause_events"),
        "pause_tor_to_spine": hop("pause_tor_to_spine", "pause_events"),
        "pause_spine_to_tor": hop("pause_spine_to_tor", "pause_events"),
        "pause_host_to_tor": hop("pause_host_to_tor", "pause_events"),
        "intra_tor_first_us": first_us("pause_intra_tor"),
        "inter_tor_first_us": first_us("pause_inter_tor"),
        "tor_to_spine_first_us": first_us("pause_tor_to_spine"),
        "spine_to_tor_first_us": first_us("pause_spine_to_tor"),
        "pipeline_avg_fct_us": _safe(fct, "pipeline", "avg_fct_us"),
        "pipeline_p95_fct_us": _safe(fct, "pipeline", "p95_fct_us"),
        "pipeline_non_hotspot_avg_fct_us": _safe(fct, "pipeline_non_hotspot", "avg_fct_us"),
        "pipeline_non_hotspot_p95_fct_us": _safe(fct, "pipeline_non_hotspot", "p95_fct_us"),
        "alltoall_avg_fct_us": _safe(fct, "alltoall", "avg_fct_us"),
        "alltoall_p95_fct_us": _safe(fct, "alltoall", "p95_fct_us"),
        "victim_avg_fct_us": _safe(fct, "victim", "avg_fct_us"),
        "victim_p95_fct_us": _safe(fct, "victim", "p95_fct_us"),
        "normal_avg_fct_us": _safe(fct, "normal", "avg_fct_us"),
        "normal_p95_fct_us": _safe(fct, "normal", "p95_fct_us"),
        "incast_avg_fct_us": _safe(fct, "incast", "avg_fct_us"),
        "incast_p95_fct_us": _safe(fct, "incast", "p95_fct_us"),
        "host_link_gbps": scenario.get("host_link_rate_gbps"),
        "tor_link_gbps": scenario.get("tor_link_rate_gbps"),
        "pfc_xoff_bytes": scenario.get("pfc_xoff_bytes"),
    }


def write_outputs(mix_dir: Path, tag: str, rows: Sequence[Dict[str, object]]) -> None:
    json_path = mix_dir / f"rank_sweep_{tag}.json"
    csv_path = mix_dir / f"rank_sweep_{tag}.csv"
    json_path.write_text(json.dumps({"tag": tag, "rows": rows}, indent=2) + "\n", encoding="ascii")
    header = ",".join(SWEEP_FIELDS)
    lines = [header]
    for row in rows:
        cells = []
        for key in SWEEP_FIELDS:
            value = row.get(key)
            if value is None:
                cells.append("")
            elif isinstance(value, float):
                cells.append(f"{value:.6f}")
            else:
                cells.append(str(value))
        lines.append(",".join(cells))
    csv_path.write_text("\n".join(lines) + "\n", encoding="ascii")
    print(f"[out] {json_path.name}", flush=True)
    print(f"[out] {csv_path.name}", flush=True)


def main() -> int:
    args = parse_args()
    mix_dir = Path(args.mix_dir).resolve() if args.mix_dir else Path(__file__).resolve().parent
    ranks_list = parse_rank_list(args.ranks, args.scenario)

    if not args.no_run and not args.simulator_cmd:
        raise SystemExit("must provide --simulator-cmd or pass --no-run "
                         "(then run NS-3 yourself between gen and collect)")

    rows: List[Dict[str, object]] = []
    for n in ranks_list:
        prefix = gen_config(mix_dir, args, n)
        if not args.no_run:
            run_simulator(mix_dir, prefix, args.simulator_cmd)
        summary_path = mix_dir / f"summary_{prefix}.json"
        if not summary_path.exists():
            if args.no_run:
                print(f"[warn] N={n}: missing {summary_path.name}; "
                      f"running simulator + collector required before aggregation",
                      flush=True)
                continue
        else:
            # If user ran sim externally and summary already exists, skip re-collect.
            pass
        # Always (re-)run the collector when we have the FCT/PFC files.
        if (mix_dir / f"fct_{prefix}.txt").exists() and (mix_dir / f"pfc_{prefix}.txt").exists():
            summary_path = run_collector(mix_dir, prefix)
        if not summary_path.exists():
            continue
        rows.append(extract_row(prefix, n, summary_path))

    write_outputs(mix_dir, args.tag, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
