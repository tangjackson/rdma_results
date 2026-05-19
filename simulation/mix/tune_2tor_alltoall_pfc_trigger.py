#!/usr/bin/env python3
"""
Search PFC thresholds that keep pipeline-only clean but let hotspot all-to-all
trigger PFC after the hotspot starts.

The search evaluates each xoff/xon pair in two stages:
1. pipeline-only, PFC enabled, no hotspot
2. pipeline + hotspot, same thresholds

A candidate is considered successful when:
1. the pipeline-only run reports zero pause events
2. the hotspot run reports pause events
3. the hotspot run reports relation_to_alltoall=after_alltoall_start
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence


def parse_int_list(spec: str) -> List[int]:
    values = []
    for item in spec.split(","):
        token = item.strip()
        if token:
            values.append(int(token))
    if not values:
        raise ValueError("expected at least one integer value")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tune PFC thresholds so hotspot all-to-all is the first PFC trigger."
    )
    parser.add_argument("--search-tag", default="2tor_pfc_trigger_threshold")
    parser.add_argument("--xoff-list", default="4096,8192,16384,32768,65536,131072,262144")
    parser.add_argument(
        "--xon-ratio",
        type=float,
        default=0.5,
        help="Set xon = floor(xoff * xon_ratio). Must be in [0, 1).",
    )
    parser.add_argument("--pipeline-nodes-per-rack", type=int, default=16)
    parser.add_argument(
        "--pipeline-ring-layout",
        choices=("interleaved", "rack_contiguous"),
        default="rack_contiguous",
    )
    parser.add_argument("--pipeline-rounds", type=int, default=4)
    parser.add_argument("--pipeline-flow-bytes", type=int, default=1 * 1024 * 1024)
    parser.add_argument("--alltoall-flow-bytes", type=int, default=128 * 1024)
    parser.add_argument("--alltoall-rounds", type=int, default=8)
    parser.add_argument("--alltoall-base-us", type=float, default=10.0)
    parser.add_argument("--alltoall-round-gap-us", type=float, default=80.0)
    parser.add_argument("--alltoall-src-stagger-us", type=float, default=0.0)
    parser.add_argument("--alltoall-node-list", default="24-31")
    parser.add_argument("--link-rate-gbps", type=float, default=100.0)
    parser.add_argument("--host-link-rate-gbps", type=float, default=None)
    parser.add_argument("--tor-link-rate-gbps", type=float, default=None)
    parser.add_argument("--link-delay-us", type=float, default=1.0)
    parser.add_argument("--buffer-size-mb", type=int, default=2)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--force-rerun", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def run_and_capture(cmd: Sequence[str], cwd: Path, log_path: Path) -> int:
    with log_path.open("w", encoding="utf-8") as handle:
        proc = subprocess.run(list(cmd), cwd=str(cwd), stdout=handle, stderr=subprocess.STDOUT, text=True)
    return proc.returncode


def load_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="ascii"))


def run_case(
    simulation_dir: Path,
    prefix: str,
    generator_cmd: Sequence[str],
    force_rerun: bool,
) -> Optional[dict]:
    mix_dir = simulation_dir / "mix"
    summary_json_path = mix_dir / f"summary_{prefix}.json"
    if summary_json_path.exists() and not force_rerun:
        return load_summary(summary_json_path)

    if run_and_capture(generator_cmd, simulation_dir, mix_dir / f"{prefix}.generate.log") != 0:
        return None
    if (
        run_and_capture(
            ["./waf", "--run", f"scratch/mp-rdma-simulator mix/config_{prefix}.txt"],
            simulation_dir,
            mix_dir / f"{prefix}.run.log",
        )
        != 0
    ):
        return None
    if (
        run_and_capture(
            [sys.executable, "mix/collect_2tor_pfc_hotspot_metrics.py", "--prefix", prefix],
            simulation_dir,
            mix_dir / f"{prefix}.collect.log",
        )
        != 0
    ):
        return None
    if not summary_json_path.exists():
        return None
    return load_summary(summary_json_path)


def main() -> int:
    args = parse_args()
    if not (0.0 <= args.xon_ratio < 1.0):
        raise SystemExit("--xon-ratio must be in [0, 1)")

    simulation_dir = Path(__file__).resolve().parent.parent
    mix_dir = simulation_dir / "mix"
    report_path = mix_dir / f"tuning_{args.search_tag}.json"

    xoff_values = parse_int_list(args.xoff_list)
    report = {"search_tag": args.search_tag, "results": [], "winner": None}

    if args.dry_run:
        for xoff in xoff_values:
            xon = max(0, int(xoff * args.xon_ratio))
            print(f"xoff={xoff} xon={xon}")
        return 0

    if args.build:
        rc = run_and_capture(["./waf", "build", "-j1"], simulation_dir, mix_dir / f"{args.search_tag}.build.log")
        if rc != 0:
            raise SystemExit(f"build failed, see {mix_dir / f'{args.search_tag}.build.log'}")

    for xoff in xoff_values:
        xon = max(0, int(xoff * args.xon_ratio))
        base_prefix = f"{args.search_tag}_pipeline_xoff{xoff}"
        hot_prefix = f"{args.search_tag}_hotspot_xoff{xoff}"

        common_args = [
            "--pipeline-nodes-per-rack",
            str(args.pipeline_nodes_per_rack),
            "--pipeline-ring-layout",
            args.pipeline_ring_layout,
            "--pipeline-rounds",
            str(args.pipeline_rounds),
            "--pipeline-flow-bytes",
            str(args.pipeline_flow_bytes),
            "--link-rate-gbps",
            str(args.link_rate_gbps),
            "--link-delay-us",
            str(args.link_delay_us),
            "--buffer-size-mb",
            str(args.buffer_size_mb),
            "--pfc-xoff-bytes",
            str(xoff),
            "--pfc-xon-bytes",
            str(xon),
            "--enable-trace",
            "--pipeline-auto-gap",
        ]
        if args.host_link_rate_gbps is not None:
            common_args.extend(["--host-link-rate-gbps", str(args.host_link_rate_gbps)])
        if args.tor_link_rate_gbps is not None:
            common_args.extend(["--tor-link-rate-gbps", str(args.tor_link_rate_gbps)])

        base_cmd = [
            sys.executable,
            "mix/gen_2tor_pfc_hotspot.py",
            "--prefix",
            base_prefix,
            "--no-alltoall",
        ] + common_args

        hot_cmd = [
            sys.executable,
            "mix/gen_2tor_pfc_hotspot.py",
            "--prefix",
            hot_prefix,
            "--alltoall-node-list",
            args.alltoall_node_list,
            "--alltoall-flow-bytes",
            str(args.alltoall_flow_bytes),
            "--alltoall-rounds",
            str(args.alltoall_rounds),
            "--alltoall-base-us",
            str(args.alltoall_base_us),
            "--alltoall-round-gap-us",
            str(args.alltoall_round_gap_us),
            "--alltoall-src-stagger-us",
            str(args.alltoall_src_stagger_us),
        ] + common_args

        base_summary = run_case(simulation_dir, base_prefix, base_cmd, args.force_rerun)
        hot_summary = run_case(simulation_dir, hot_prefix, hot_cmd, args.force_rerun)

        result: Dict[str, object] = {
            "xoff": xoff,
            "xon": xon,
            "pipeline_prefix": base_prefix,
            "hotspot_prefix": hot_prefix,
            "pipeline_pause_events": None,
            "hotspot_pause_events": None,
            "trigger_relation": None,
            "success": False,
        }
        if base_summary is not None:
            result["pipeline_pause_events"] = base_summary["pfc"]["pause_events"]
        if hot_summary is not None:
            result["hotspot_pause_events"] = hot_summary["pfc"]["pause_events"]
            result["trigger_relation"] = hot_summary.get("trigger_timing", {}).get("relation_to_alltoall")
            result["trigger_delta_vs_alltoall_us"] = hot_summary.get("trigger_timing", {}).get("delta_vs_alltoall_us")
            result["trace_active_pipeline_throughput_gbps"] = (
                hot_summary.get("trace_active", {}).get("patterns", {}).get("pipeline", {}).get("throughput_gbps")
            )

        if (
            base_summary is not None
            and hot_summary is not None
            and base_summary["pfc"]["pause_events"] == 0
            and hot_summary["pfc"]["pause_events"] > 0
            and hot_summary.get("trigger_timing", {}).get("relation_to_alltoall") == "after_alltoall_start"
        ):
            result["success"] = True
            report["winner"] = result
            report["results"].append(result)
            break

        report["results"].append(result)

    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="ascii")
    print(f"report={report_path.name}")
    if report["winner"] is not None:
        print(
            f"winner_xoff={report['winner']['xoff']} "
            f"winner_xon={report['winner']['xon']} "
            f"hotspot_prefix={report['winner']['hotspot_prefix']}"
        )
        return 0
    print("no successful threshold found")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
