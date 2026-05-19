#!/usr/bin/env python3
"""
Search for the strongest hotspot workload and evaluate its impact.

The tuner varies only the all-to-all hotspot intensity while keeping the
topology and reduce-scatter pipeline fixed. By default a candidate is accepted
only when all expected flows complete and PFC is observed, but this can be
relaxed with `--allow-incomplete`.

Typical usage from simulation/:
  python3 mix/tune_2tor_pfc_hotspot.py --search-tag 2tor_bw_search
"""

from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def parse_int_list(spec: str) -> List[int]:
    values = []
    for item in spec.split(","):
        token = item.strip()
        if token:
            values.append(int(token))
    if not values:
        raise ValueError("expected at least one integer value")
    return values


def parse_float_list(spec: str) -> List[float]:
    values = []
    for item in spec.split(","):
        token = item.strip()
        if token:
            values.append(float(token))
    if not values:
        raise ValueError("expected at least one float value")
    return values


def fmt_token(value: float) -> str:
    text = f"{value:g}"
    return text.replace("-", "m").replace(".", "p")


@dataclass(frozen=True)
class Candidate:
    alltoall_flow_bytes: int
    alltoall_rounds: int
    alltoall_round_gap_us: float
    alltoall_src_stagger_us: float
    offered_hotspot_gbps: float
    hotspot_total_bytes: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tune the hotspot workload to maximize load while preserving clean completion."
    )
    parser.add_argument("--search-tag", default="2tor_pfc_tune", help="Prefix for generated candidate files.")
    parser.add_argument("--alltoall-node-list", default="24-31")
    parser.add_argument("--pipeline-nodes-per-rack", type=int, default=16)
    parser.add_argument("--pipeline-rounds", type=int, default=8)
    parser.add_argument("--pipeline-flow-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--pipeline-base-us", type=float, default=0.0)
    parser.add_argument("--pipeline-gap-us", type=float, default=1.0)
    parser.add_argument("--alltoall-base-us", type=float, default=2.0)
    parser.add_argument("--link-rate-gbps", type=float, default=400.0)
    parser.add_argument("--host-link-rate-gbps", type=float, default=None)
    parser.add_argument("--tor-link-rate-gbps", type=float, default=None)
    parser.add_argument("--link-delay-us", type=float, default=1.0)
    parser.add_argument("--buffer-size-mb", type=int, default=2)
    parser.add_argument("--pfc-xoff-bytes", type=int, default=1000)
    parser.add_argument("--pfc-xon-bytes", type=int, default=300)
    parser.add_argument("--allow-incomplete", action="store_true", help="Accept candidates even if not all flows complete.")
    parser.add_argument(
        "--search-alltoall-flow-bytes",
        default="65536,49152,32768,24576,16384,12288,8192",
        help="Comma-separated hotspot flow sizes to try, strongest to weakest.",
    )
    parser.add_argument(
        "--search-alltoall-rounds",
        default="4,3,2,1",
        help="Comma-separated hotspot burst counts to try, strongest to weakest.",
    )
    parser.add_argument(
        "--search-alltoall-round-gap-us",
        default="0.5,1,2,4",
        help="Comma-separated round gaps in microseconds to try, smallest gap is strongest.",
    )
    parser.add_argument(
        "--search-alltoall-src-stagger-us",
        default="0",
        help="Comma-separated per-source staggers in microseconds to try.",
    )
    parser.add_argument("--min-pause-events", type=int, default=1, help="Minimum PFC pause events required.")
    parser.add_argument("--max-runs", type=int, default=0, help="Optional cap on attempted candidates. 0 means unlimited.")
    parser.add_argument("--build", action="store_true", help="Run `./waf build -j1` once before the search.")
    parser.add_argument("--force-rerun", action="store_true", help="Ignore existing simulator outputs and rerun candidates.")
    parser.add_argument(
        "--exhaustive",
        action="store_true",
        help="Try all candidates instead of stopping at the strongest clean one.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the ordered search plan without running simulations.")
    return parser.parse_args()


def hotspot_node_count(node_list_spec: str) -> int:
    count = 0
    seen = set()
    for chunk in node_list_spec.split(","):
        token = chunk.strip()
        if not token:
            continue
        if "-" in token:
            left, right = token.split("-", 1)
            start = int(left)
            end = int(right)
            step = 1 if end >= start else -1
            for value in range(start, end + step, step):
                seen.add(value)
        else:
            seen.add(int(token))
    count = len(seen)
    if count < 2:
        raise ValueError("all-to-all hotspot needs at least two nodes")
    return count


def offered_hotspot_gbps(
    flow_bytes: int,
    rounds: int,
    round_gap_us: float,
    src_stagger_us: float,
    node_count: int,
    host_link_rate_gbps: float,
) -> Tuple[float, int]:
    flow_count_per_round = node_count * (node_count - 1)
    total_bytes = flow_count_per_round * rounds * flow_bytes
    overlap_window_us = (rounds - 1) * round_gap_us + (node_count - 1) * src_stagger_us
    per_source_bytes = rounds * (node_count - 1) * flow_bytes
    host_serialization_us = per_source_bytes * 8.0 / (host_link_rate_gbps * 1e9) * 1e6
    burst_window_us = max(overlap_window_us, host_serialization_us, 0.001)
    offered = total_bytes * 8.0 / (burst_window_us * 1e-6) / 1e9
    return offered, total_bytes


def build_candidates(args: argparse.Namespace) -> List[Candidate]:
    node_count = hotspot_node_count(args.alltoall_node_list)
    host_link_rate_gbps = args.host_link_rate_gbps or args.link_rate_gbps
    candidates = []
    for flow_bytes, rounds, round_gap_us, src_stagger_us in itertools.product(
        parse_int_list(args.search_alltoall_flow_bytes),
        parse_int_list(args.search_alltoall_rounds),
        parse_float_list(args.search_alltoall_round_gap_us),
        parse_float_list(args.search_alltoall_src_stagger_us),
    ):
        offered, total_bytes = offered_hotspot_gbps(
            flow_bytes=flow_bytes,
            rounds=rounds,
            round_gap_us=round_gap_us,
            src_stagger_us=src_stagger_us,
            node_count=node_count,
            host_link_rate_gbps=host_link_rate_gbps,
        )
        candidates.append(
            Candidate(
                alltoall_flow_bytes=flow_bytes,
                alltoall_rounds=rounds,
                alltoall_round_gap_us=round_gap_us,
                alltoall_src_stagger_us=src_stagger_us,
                offered_hotspot_gbps=offered,
                hotspot_total_bytes=total_bytes,
            )
        )
    candidates.sort(
        key=lambda item: (
            item.offered_hotspot_gbps,
            item.hotspot_total_bytes,
            item.alltoall_rounds,
            item.alltoall_flow_bytes,
            -item.alltoall_round_gap_us,
            -item.alltoall_src_stagger_us,
        ),
        reverse=True,
    )
    return candidates


def candidate_prefix(search_tag: str, candidate: Candidate) -> str:
    return (
        f"{search_tag}"
        f"_b{candidate.alltoall_flow_bytes}"
        f"_r{candidate.alltoall_rounds}"
        f"_g{fmt_token(candidate.alltoall_round_gap_us)}"
        f"_s{fmt_token(candidate.alltoall_src_stagger_us)}"
    )


def run_and_log(cmd: Sequence[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess:
    with log_path.open("w", encoding="utf-8") as handle:
        proc = subprocess.run(
            list(cmd),
            cwd=str(cwd),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return proc


def tail_text(path: Path, lines: int = 20) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])


def ratio(completed: Optional[int], expected: Optional[int]) -> float:
    if expected in (None, 0) or completed is None:
        return 0.0
    return completed / expected


def is_clean(summary: dict, min_pause_events: int) -> bool:
    overall = summary["fct"]["patterns"]["overall"]
    pipeline = summary["fct"]["patterns"]["pipeline"]
    alltoall = summary["fct"]["patterns"]["alltoall"]
    if overall["completed_flows"] != overall["expected_flows"]:
        return False
    if pipeline["completed_flows"] != pipeline["expected_flows"]:
        return False
    if alltoall["expected_flows"] > 0 and alltoall["completed_flows"] != alltoall["expected_flows"]:
        return False
    if summary["pfc"]["pause_events"] < min_pause_events:
        return False
    return True


def is_accepted(summary: dict, min_pause_events: int, allow_incomplete: bool) -> bool:
    if summary["pfc"]["pause_events"] < min_pause_events:
        return False
    if allow_incomplete:
        return True
    return is_clean(summary, min_pause_events)


def summary_snapshot(summary: dict) -> Dict[str, object]:
    patterns = summary["fct"]["patterns"]
    return {
        "overall_completion_ratio": ratio(patterns["overall"]["completed_flows"], patterns["overall"]["expected_flows"]),
        "pipeline_completion_ratio": ratio(patterns["pipeline"]["completed_flows"], patterns["pipeline"]["expected_flows"]),
        "alltoall_completion_ratio": ratio(patterns["alltoall"]["completed_flows"], patterns["alltoall"]["expected_flows"]),
        "pause_events": summary["pfc"]["pause_events"],
        "resume_events": summary["pfc"]["resume_events"],
        "pipeline_avg_fct_us": patterns["pipeline"]["avg_fct_us"],
        "pipeline_p95_fct_us": patterns["pipeline"]["p95_fct_us"],
        "pipeline_aggregate_goodput_gbps": patterns["pipeline"]["aggregate_goodput_gbps"],
    }


def make_generator_cmd(prefix: str, candidate: Candidate, args: argparse.Namespace) -> List[str]:
    cmd = [
        sys.executable,
        "mix/gen_2tor_pfc_hotspot.py",
        "--prefix",
        prefix,
        "--alltoall-node-list",
        args.alltoall_node_list,
        "--alltoall-flow-bytes",
        str(candidate.alltoall_flow_bytes),
        "--alltoall-rounds",
        str(candidate.alltoall_rounds),
        "--alltoall-round-gap-us",
        str(candidate.alltoall_round_gap_us),
        "--alltoall-src-stagger-us",
        str(candidate.alltoall_src_stagger_us),
        "--alltoall-base-us",
        str(args.alltoall_base_us),
        "--pipeline-nodes-per-rack",
        str(args.pipeline_nodes_per_rack),
        "--pipeline-rounds",
        str(args.pipeline_rounds),
        "--pipeline-flow-bytes",
        str(args.pipeline_flow_bytes),
        "--pipeline-base-us",
        str(args.pipeline_base_us),
        "--pipeline-gap-us",
        str(args.pipeline_gap_us),
        "--link-rate-gbps",
        str(args.link_rate_gbps),
        "--link-delay-us",
        str(args.link_delay_us),
        "--buffer-size-mb",
        str(args.buffer_size_mb),
        "--pfc-xoff-bytes",
        str(args.pfc_xoff_bytes),
        "--pfc-xon-bytes",
        str(args.pfc_xon_bytes),
        "--enable-trace",
    ]
    if args.host_link_rate_gbps is not None:
        cmd.extend(["--host-link-rate-gbps", str(args.host_link_rate_gbps)])
    if args.tor_link_rate_gbps is not None:
        cmd.extend(["--tor-link-rate-gbps", str(args.tor_link_rate_gbps)])
    return cmd


def main() -> int:
    args = parse_args()
    simulation_dir = Path(__file__).resolve().parent.parent
    mix_dir = simulation_dir / "mix"
    tuning_json_path = mix_dir / f"tuning_{args.search_tag}.json"

    candidates = build_candidates(args)
    if args.max_runs > 0:
        candidates = candidates[: args.max_runs]

    if args.dry_run:
        print(f"search_tag={args.search_tag}")
        print(f"candidate_count={len(candidates)}")
        for idx, candidate in enumerate(candidates, start=1):
            print(
                f"{idx:03d} "
                f"prefix={candidate_prefix(args.search_tag, candidate)} "
                f"offered_hotspot_gbps={candidate.offered_hotspot_gbps:.2f} "
                f"alltoall_flow_bytes={candidate.alltoall_flow_bytes} "
                f"alltoall_rounds={candidate.alltoall_rounds} "
                f"alltoall_round_gap_us={candidate.alltoall_round_gap_us:g} "
                f"alltoall_src_stagger_us={candidate.alltoall_src_stagger_us:g}"
            )
        return 0

    if args.build:
        build_log = mix_dir / f"tuning_{args.search_tag}_build.log"
        print("building simulator once before search")
        proc = run_and_log(["./waf", "build", "-j1"], simulation_dir, build_log)
        if proc.returncode != 0:
            print(tail_text(build_log), file=sys.stderr)
            raise SystemExit(f"build failed, see {build_log}")

    tried = []
    best_candidate = None

    for idx, candidate in enumerate(candidates, start=1):
        prefix = candidate_prefix(args.search_tag, candidate)
        generator_log = mix_dir / f"{prefix}.generate.log"
        run_log = mix_dir / f"{prefix}.run.log"
        collect_log = mix_dir / f"{prefix}.collect.log"
        summary_json_path = mix_dir / f"summary_{prefix}.json"

        print(
            f"[{idx}/{len(candidates)}] "
            f"prefix={prefix} "
            f"offered_hotspot_gbps={candidate.offered_hotspot_gbps:.2f}"
        )

        if summary_json_path.exists() and not args.force_rerun:
            summary = json.loads(summary_json_path.read_text(encoding="ascii"))
            accepted = is_accepted(summary, args.min_pause_events, args.allow_incomplete)
            result = {
                "prefix": prefix,
                "candidate": asdict(candidate),
                "status": "reused_accepted" if accepted else "reused_rejected",
                "accepted": accepted,
                "summary": summary_snapshot(summary),
            }
        else:
            proc = run_and_log(make_generator_cmd(prefix, candidate, args), simulation_dir, generator_log)
            if proc.returncode != 0:
                result = {
                    "prefix": prefix,
                    "candidate": asdict(candidate),
                    "status": "generate_failed",
                    "accepted": False,
                    "error_tail": tail_text(generator_log),
                }
                tried.append(result)
                continue

            proc = run_and_log(
                ["./waf", "--run", f"scratch/mp-rdma-simulator mix/config_{prefix}.txt"],
                simulation_dir,
                run_log,
            )
            if proc.returncode != 0:
                result = {
                    "prefix": prefix,
                    "candidate": asdict(candidate),
                    "status": "run_failed",
                    "accepted": False,
                    "error_tail": tail_text(run_log),
                }
                tried.append(result)
                continue

            proc = run_and_log(
                [sys.executable, "mix/collect_2tor_pfc_hotspot_metrics.py", "--prefix", prefix],
                simulation_dir,
                collect_log,
            )
            if proc.returncode != 0 or not summary_json_path.exists():
                result = {
                    "prefix": prefix,
                    "candidate": asdict(candidate),
                    "status": "collect_failed",
                    "accepted": False,
                    "error_tail": tail_text(collect_log),
                }
                tried.append(result)
                continue

            summary = json.loads(summary_json_path.read_text(encoding="ascii"))
            accepted = is_accepted(summary, args.min_pause_events, args.allow_incomplete)
            result = {
                "prefix": prefix,
                "candidate": asdict(candidate),
                "status": "accepted" if accepted else "rejected",
                "accepted": accepted,
                "summary": summary_snapshot(summary),
            }

        tried.append(result)
        if result["accepted"]:
            best_candidate = result
            if not args.exhaustive:
                break

    tuning_report = {
        "search_tag": args.search_tag,
        "best_candidate": best_candidate,
        "tried": tried,
    }
    tuning_json_path.write_text(json.dumps(tuning_report, indent=2) + "\n", encoding="ascii")

    if best_candidate is None:
        print(f"no acceptable candidate found, see {tuning_json_path}")
        return 1

    print(f"best_prefix={best_candidate['prefix']}")
    print(f"tuning_report={tuning_json_path.name}")
    print(
        f"best_offered_hotspot_gbps={best_candidate['candidate']['offered_hotspot_gbps']:.2f} "
        f"pause_events={best_candidate['summary']['pause_events']} "
        f"pipeline_avg_fct_us={best_candidate['summary']['pipeline_avg_fct_us']}"
    )
    print(f"./waf --run 'scratch/mp-rdma-simulator mix/config_{best_candidate['prefix']}.txt'")
    print(f"python3 mix/collect_2tor_pfc_hotspot_metrics.py --prefix {best_candidate['prefix']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
