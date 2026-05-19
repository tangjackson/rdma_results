#!/usr/bin/env python3
"""
Generate and run a 2-ToR PFC hotspot experiment.

Traffic mix:
1. Intra-ToR all-to-all among a configurable subset of hosts in one rack.
2. Cross-ToR reduce-scatter pipeline on an interleaved ring so every hop
   traverses the inter-ToR link.

The script writes topology, flow, config, and summary files under:
  simulation/mix/generated/<tag>/
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


HOSTS_PER_TOR = 16
TOR_COUNT = 2
TOTAL_HOSTS = HOSTS_PER_TOR * TOR_COUNT
TOR_IDS = (TOTAL_HOSTS, TOTAL_HOSTS + 1)
DEFAULT_TAG = "2tor_pfc_hotspot"
DEFAULT_PACKET_PAYLOAD = 1000
DEFAULT_PRIORITY_GROUP = 3
PIPELINE_DST_PORT_BASE = 10000
ALLTOALL_DST_PORT_BASE = 20000


CONFIG_TEMPLATE = """ENABLE_QCN 1
USE_DYNAMIC_PFC_THRESHOLD 1

PACKET_PAYLOAD_SIZE {packet_payload_size}

TOPOLOGY_FILE {topology_file}
FLOW_FILE {flow_file}
TRACE_FILE mix/trace.txt
TRACE_OUTPUT_FILE {trace_output_file}
FCT_OUTPUT_FILE {fct_output_file}
PFC_OUTPUT_FILE {pfc_output_file}

SIMULATOR_STOP_TIME {stop_time_s:.6f}

CC_MODE {cc_mode}
ALPHA_RESUME_INTERVAL 1
RATE_DECREASE_INTERVAL 4
CLAMP_TARGET_RATE 0
RP_TIMER 900
EWMA_GAIN 0.00390625
FAST_RECOVERY_TIMES 1
RATE_AI 50Mb/s
RATE_HAI 100Mb/s
MIN_RATE 100Mb/s
DCTCP_RATE_AI 1000Mb/s

ERROR_RATE_PER_LINK 0.0000
L2_CHUNK_SIZE 4000
L2_ACK_INTERVAL 1
L2_BACK_TO_ZERO 0

HAS_WIN 1
GLOBAL_T 1
VAR_WIN 1
FAST_REACT 1
U_TARGET 0.95
MI_THRESH 0
INT_MULTI 1
MULTI_RATE 0
SAMPLE_FEEDBACK 0
PINT_LOG_BASE 1.05
PINT_PROB 1.0

RATE_BOUND 1

ACK_HIGH_PRIO 0

LINK_DOWN 0 0 0

ENABLE_TRACE {enable_trace}

KMAX_MAP 1 {link_rate_bps} {kmax}
KMIN_MAP 1 {link_rate_bps} {kmin}
PMAX_MAP 1 {link_rate_bps} 0.2
BUFFER_SIZE {buffer_size_mb}
QLEN_MON_FILE {qlen_output_file}
QLEN_MON_START 0
QLEN_MON_END {qlen_mon_end_ns}
"""

WAF_COMPAT_BOOTSTRAP = """
import builtins
import runpy
import sys

_open = builtins.open

def compat_open(file, mode='r', *args, **kwargs):
    if isinstance(mode, str) and 'U' in mode:
        mode = mode.replace('U', '')
    return _open(file, mode, *args, **kwargs)

builtins.open = compat_open
sys.argv = ['./waf'] + sys.argv[1:]
runpy.run_path('./waf', run_name='__main__')
"""


@dataclass(frozen=True)
class FlowSpec:
    flow_id: str
    pattern: str
    src: int
    dst: int
    size_bytes: int
    start_time_s: float
    pg: int = DEFAULT_PRIORITY_GROUP
    dst_port: int = PIPELINE_DST_PORT_BASE

    @property
    def start_time_ns(self) -> int:
        return int(round(self.start_time_s * 1e9))

    def to_flow_line(self) -> str:
        return (
            f"{self.src} {self.dst} {self.pg} {self.dst_port} "
            f"{self.size_bytes} {self.start_time_s:.9f}"
        )


@dataclass
class OutputPaths:
    root: Path
    topology: Path
    flow: Path
    config: Path
    manifest: Path
    summary_json: Path
    summary_txt: Path
    pfc: Path
    fct: Path
    qlen: Path
    trace_output: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a 2-ToR hotspot scenario and summarize PFC/FCT metrics."
    )
    parser.add_argument("--tag", default=DEFAULT_TAG, help="Output directory name.")
    parser.add_argument(
        "--alltoall-rack",
        type=int,
        choices=(0, 1),
        default=0,
        help="Rack that carries the intra-ToR all-to-all hotspot.",
    )
    parser.add_argument(
        "--alltoall-nodes",
        type=int,
        default=8,
        help="Number of hosts in the hotspot all-to-all subset within one ToR.",
    )
    parser.add_argument(
        "--alltoall-flow-bytes",
        type=int,
        default=256 * 1024,
        help="Payload bytes per all-to-all flow.",
    )
    parser.add_argument(
        "--alltoall-base-us",
        type=float,
        default=2.0,
        help="Hotspot launch offset in microseconds.",
    )
    parser.add_argument(
        "--alltoall-src-stagger-us",
        type=float,
        default=0.1,
        help="Per-source start stagger for the all-to-all hotspot.",
    )
    parser.add_argument(
        "--pipeline-nodes-per-rack",
        type=int,
        default=HOSTS_PER_TOR,
        help="Number of hosts per rack participating in reduce-scatter.",
    )
    parser.add_argument(
        "--pipeline-rounds",
        type=int,
        default=8,
        help="Number of reduce-scatter pipeline waves to inject.",
    )
    parser.add_argument(
        "--pipeline-flow-bytes",
        type=int,
        default=1 * 1024 * 1024,
        help="Payload bytes per pipeline flow.",
    )
    parser.add_argument(
        "--pipeline-base-us",
        type=float,
        default=0.0,
        help="Reduce-scatter pipeline launch time in microseconds.",
    )
    parser.add_argument(
        "--pipeline-gap-us",
        type=float,
        default=1.0,
        help="Gap between successive pipeline waves in microseconds.",
    )
    parser.add_argument(
        "--link-rate-gbps",
        type=float,
        default=100.0,
        help="Link rate for host-ToR and ToR-ToR links.",
    )
    parser.add_argument(
        "--link-delay-us",
        type=float,
        default=1.0,
        help="One-way link delay in microseconds.",
    )
    parser.add_argument(
        "--buffer-size-mb",
        type=int,
        default=2,
        help="Shared switch buffer size in MB.",
    )
    parser.add_argument(
        "--cc-mode",
        type=int,
        default=1,
        help="Simulator congestion control mode. 1 follows the PFC example config.",
    )
    parser.add_argument(
        "--packet-payload-size",
        type=int,
        default=DEFAULT_PACKET_PAYLOAD,
        help="MTU payload configured in the simulator.",
    )
    parser.add_argument(
        "--simulator",
        default="mp-rdma-simulator",
        help="Scratch program to launch with waf.",
    )
    parser.add_argument(
        "--launcher",
        choices=("auto", "binary", "waf"),
        default="auto",
        help="How to launch the simulator: built binary, waf, or auto-detect.",
    )
    parser.add_argument(
        "--waf-python",
        default=None,
        help="Python interpreter to use for the waf compatibility launcher.",
    )
    parser.add_argument(
        "--build-first",
        action="store_true",
        help="Run './waf build' before the simulation.",
    )
    parser.add_argument(
        "--enable-trace",
        action="store_true",
        help="Enable packet-level trace dumping.",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Only generate topology/flow/config files without launching waf.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Skip generation and rerun parsing on an existing output directory.",
    )
    return parser.parse_args()


def ensure_valid_args(args: argparse.Namespace) -> None:
    if not 1 <= args.alltoall_nodes <= HOSTS_PER_TOR:
        raise ValueError("--alltoall-nodes must be between 1 and 16")
    if not 1 <= args.pipeline_nodes_per_rack <= HOSTS_PER_TOR:
        raise ValueError("--pipeline-nodes-per-rack must be between 1 and 16")
    if args.pipeline_rounds < 1:
        raise ValueError("--pipeline-rounds must be >= 1")
    if args.link_rate_gbps <= 0:
        raise ValueError("--link-rate-gbps must be > 0")
    if args.link_delay_us <= 0:
        raise ValueError("--link-delay-us must be > 0")


def simulation_dir_from_script() -> Path:
    return Path(__file__).resolve().parents[1]


def rack_nodes(rack_index: int, count: int) -> List[int]:
    base = rack_index * HOSTS_PER_TOR
    return list(range(base, base + count))


def interleaved_pipeline_ring(nodes_per_rack: int) -> List[int]:
    left = rack_nodes(0, nodes_per_rack)
    right = rack_nodes(1, nodes_per_rack)
    ring: List[int] = []
    for left_node, right_node in zip(left, right):
        ring.append(left_node)
        ring.append(right_node)
    return ring


def generate_flows(args: argparse.Namespace) -> List[FlowSpec]:
    flows: List[FlowSpec] = []

    pipeline_ring = interleaved_pipeline_ring(args.pipeline_nodes_per_rack)
    pipeline_base_s = args.pipeline_base_us * 1e-6
    pipeline_gap_s = args.pipeline_gap_us * 1e-6
    for round_idx in range(args.pipeline_rounds):
        start_time_s = pipeline_base_s + round_idx * pipeline_gap_s
        for idx, src in enumerate(pipeline_ring):
            dst = pipeline_ring[(idx + 1) % len(pipeline_ring)]
            flows.append(
                FlowSpec(
                    flow_id=f"pipeline-r{round_idx:03d}-{src:02d}-{dst:02d}",
                    pattern="pipeline",
                    src=src,
                    dst=dst,
                    size_bytes=args.pipeline_flow_bytes,
                    start_time_s=start_time_s,
                    dst_port=PIPELINE_DST_PORT_BASE + round_idx * len(pipeline_ring) + idx,
                )
            )

    hotspot_nodes = rack_nodes(args.alltoall_rack, args.alltoall_nodes)
    hotspot_base_s = args.alltoall_base_us * 1e-6
    src_stagger_s = args.alltoall_src_stagger_us * 1e-6
    for src_offset, src in enumerate(hotspot_nodes):
        start_time_s = hotspot_base_s + src_offset * src_stagger_s
        dst_rank = 0
        for dst in hotspot_nodes:
            if dst == src:
                continue
            flows.append(
                FlowSpec(
                    flow_id=f"alltoall-{src:02d}-{dst:02d}",
                    pattern="alltoall",
                    src=src,
                    dst=dst,
                    size_bytes=args.alltoall_flow_bytes,
                    start_time_s=start_time_s,
                    dst_port=ALLTOALL_DST_PORT_BASE + src_offset * args.alltoall_nodes + dst_rank,
                )
            )
            dst_rank += 1

    flows.sort(key=lambda flow: (flow.start_time_s, flow.pattern, flow.src, flow.dst))
    return flows


def write_topology(path: Path, args: argparse.Namespace) -> None:
    total_nodes = TOTAL_HOSTS + len(TOR_IDS)
    link_count = TOTAL_HOSTS + 1
    rate_str = f"{args.link_rate_gbps:g}Gbps"
    delay_ms = args.link_delay_us / 1000.0
    delay_str = f"{delay_ms:.6f}ms"

    lines = [f"{total_nodes} 2 {link_count}", f"{TOR_IDS[0]} {TOR_IDS[1]}"]
    for host in range(HOSTS_PER_TOR):
        lines.append(f"{host} {TOR_IDS[0]} {rate_str} {delay_str} 0")
    for host in range(HOSTS_PER_TOR, TOTAL_HOSTS):
        lines.append(f"{host} {TOR_IDS[1]} {rate_str} {delay_str} 0")
    lines.append(f"{TOR_IDS[0]} {TOR_IDS[1]} {rate_str} {delay_str} 0")
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def write_flow_file(path: Path, flows: Sequence[FlowSpec]) -> None:
    lines = [str(len(flows))]
    lines.extend(flow.to_flow_line() for flow in flows)
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def estimate_stop_time_s(flows: Sequence[FlowSpec], args: argparse.Namespace) -> float:
    last_start_s = max(flow.start_time_s for flow in flows)
    total_bytes = sum(flow.size_bytes for flow in flows)
    bottleneck_seconds = (total_bytes * 8.0) / (args.link_rate_gbps * 1e9)
    safety_tail_s = max(0.01, 2.0 * bottleneck_seconds)
    return last_start_s + safety_tail_s


def write_config(
    path: Path,
    stop_time_s: float,
    args: argparse.Namespace,
    output_paths: OutputPaths,
    simulation_dir: Path,
) -> None:
    link_rate_bps = int(round(args.link_rate_gbps * 1e9))
    kmin = max(1, int(round(400 * args.link_rate_gbps / 100.0)))
    kmax = max(kmin + 1, int(round(1600 * args.link_rate_gbps / 100.0)))

    config = CONFIG_TEMPLATE.format(
        packet_payload_size=args.packet_payload_size,
        topology_file=output_paths.topology.relative_to(simulation_dir).as_posix(),
        flow_file=output_paths.flow.relative_to(simulation_dir).as_posix(),
        trace_output_file=output_paths.trace_output.relative_to(simulation_dir).as_posix(),
        fct_output_file=output_paths.fct.relative_to(simulation_dir).as_posix(),
        pfc_output_file=output_paths.pfc.relative_to(simulation_dir).as_posix(),
        stop_time_s=stop_time_s,
        cc_mode=args.cc_mode,
        enable_trace=1 if args.enable_trace else 0,
        link_rate_bps=link_rate_bps,
        kmax=kmax,
        kmin=kmin,
        buffer_size_mb=args.buffer_size_mb,
        qlen_output_file=output_paths.qlen.relative_to(simulation_dir).as_posix(),
        qlen_mon_end_ns=int(math.ceil(stop_time_s * 1e9)),
    )
    path.write_text(config, encoding="ascii")


def write_manifest(
    path: Path,
    args: argparse.Namespace,
    flows: Sequence[FlowSpec],
    stop_time_s: float,
    output_paths: OutputPaths,
    simulation_dir: Path,
) -> None:
    data = {
        "scenario": {
            "tag": args.tag,
            "alltoall_rack": args.alltoall_rack,
            "alltoall_nodes": args.alltoall_nodes,
            "alltoall_flow_bytes": args.alltoall_flow_bytes,
            "pipeline_nodes_per_rack": args.pipeline_nodes_per_rack,
            "pipeline_rounds": args.pipeline_rounds,
            "pipeline_flow_bytes": args.pipeline_flow_bytes,
            "pipeline_ring": interleaved_pipeline_ring(args.pipeline_nodes_per_rack),
            "link_rate_gbps": args.link_rate_gbps,
            "link_delay_us": args.link_delay_us,
            "buffer_size_mb": args.buffer_size_mb,
            "cc_mode": args.cc_mode,
            "stop_time_s": stop_time_s,
            "simulator": args.simulator,
        },
        "files": {
            "topology": output_paths.topology.relative_to(simulation_dir).as_posix(),
            "flow": output_paths.flow.relative_to(simulation_dir).as_posix(),
            "config": output_paths.config.relative_to(simulation_dir).as_posix(),
            "fct": output_paths.fct.relative_to(simulation_dir).as_posix(),
            "pfc": output_paths.pfc.relative_to(simulation_dir).as_posix(),
            "qlen": output_paths.qlen.relative_to(simulation_dir).as_posix(),
        },
        "flows": [asdict(flow) for flow in flows],
    }
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="ascii")


def ip_hex_to_node_id(token: str) -> int:
    if len(token) == 8 or any(c in "abcdefABCDEF" for c in token):
        return (int(token, 16) >> 8) & 0xFFFF
    return int(token)


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * pct
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def build_flow_lookup(flows: Sequence[FlowSpec]) -> Dict[Tuple[int, int, int, int], List[FlowSpec]]:
    lookup: Dict[Tuple[int, int, int, int], List[FlowSpec]] = {}
    for flow in flows:
        key = (flow.src, flow.dst, flow.size_bytes, flow.start_time_ns)
        lookup.setdefault(key, []).append(flow)
    return lookup


def summarize_fct(fct_path: Path, flows: Sequence[FlowSpec]) -> Dict[str, object]:
    lookup = build_flow_lookup(flows)
    flow_counts: Dict[str, int] = {}
    for flow in flows:
        flow_counts[flow.pattern] = flow_counts.get(flow.pattern, 0) + 1
    flow_counts["overall"] = len(flows)

    records_by_pattern: Dict[str, List[Dict[str, float]]] = {"overall": []}
    unmatched: List[Dict[str, int]] = []

    if not fct_path.exists():
        return {
            "completed_flows": 0,
            "expected_flows": len(flows),
            "unmatched_rows": 0,
            "patterns": {},
        }

    with fct_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 8:
                continue
            src = ip_hex_to_node_id(parts[0])
            dst = ip_hex_to_node_id(parts[1])
            size_bytes = int(parts[4])
            start_time_ns = int(parts[5])
            fct_ns = int(parts[6])
            standalone_fct_ns = int(parts[7])
            key = (src, dst, size_bytes, start_time_ns)

            matched_flow: Optional[FlowSpec] = None
            candidates = lookup.get(key)
            if candidates:
                matched_flow = candidates.pop(0)

            record = {
                "src": src,
                "dst": dst,
                "size_bytes": size_bytes,
                "start_time_ns": start_time_ns,
                "fct_ns": fct_ns,
                "standalone_fct_ns": standalone_fct_ns,
                "goodput_gbps": size_bytes * 8.0 / max(fct_ns, 1),
                "slowdown": fct_ns / max(standalone_fct_ns, 1),
                "end_time_ns": start_time_ns + fct_ns,
            }
            records_by_pattern["overall"].append(record)

            if matched_flow is None:
                unmatched.append(
                    {
                        "src": src,
                        "dst": dst,
                        "size_bytes": size_bytes,
                        "start_time_ns": start_time_ns,
                    }
                )
                continue

            records_by_pattern.setdefault(matched_flow.pattern, []).append(record)

    def summarize_records(pattern: str) -> Dict[str, object]:
        records = records_by_pattern.get(pattern, [])
        fcts_us = [item["fct_ns"] / 1e3 for item in records]
        throughputs = [item["goodput_gbps"] for item in records]
        slowdowns = [item["slowdown"] for item in records]
        total_bytes = sum(item["size_bytes"] for item in records)
        if records:
            first_start = min(item["start_time_ns"] for item in records)
            last_end = max(item["end_time_ns"] for item in records)
            active_ns = max(last_end - first_start, 1)
            aggregate_goodput_gbps = total_bytes * 8.0 / active_ns
        else:
            first_start = None
            last_end = None
            active_ns = None
            aggregate_goodput_gbps = None
        return {
            "expected_flows": flow_counts.get(pattern, 0),
            "completed_flows": len(records),
            "total_bytes": total_bytes,
            "avg_fct_us": mean(fcts_us) if fcts_us else None,
            "p50_fct_us": percentile(fcts_us, 0.50),
            "p95_fct_us": percentile(fcts_us, 0.95),
            "p99_fct_us": percentile(fcts_us, 0.99),
            "avg_slowdown": mean(slowdowns) if slowdowns else None,
            "avg_per_flow_goodput_gbps": mean(throughputs) if throughputs else None,
            "aggregate_goodput_gbps": aggregate_goodput_gbps,
            "first_start_ns": first_start,
            "last_end_ns": last_end,
            "active_window_us": (active_ns / 1e3) if active_ns is not None else None,
        }

    patterns = {
        "overall": summarize_records("overall"),
        "pipeline": summarize_records("pipeline"),
        "alltoall": summarize_records("alltoall"),
    }
    return {
        "completed_flows": len(records_by_pattern["overall"]),
        "expected_flows": len(flows),
        "unmatched_rows": len(unmatched),
        "patterns": patterns,
    }


def summarize_pfc(pfc_path: Path) -> Dict[str, object]:
    if not pfc_path.exists():
        return {
            "pause_events": 0,
            "resume_events": 0,
            "ports_with_pause": 0,
            "top_paused_ports": [],
        }

    active_pause: Dict[Tuple[int, int], int] = {}
    by_port: Dict[Tuple[int, int], Dict[str, int]] = {}
    pause_events = 0
    resume_events = 0
    earliest_pause_ns: Optional[int] = None
    latest_pause_ns: Optional[int] = None

    with pfc_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            time_ns, node_id, node_type, if_index, event_type = map(int, line.split())
            key = (node_id, if_index)
            port_stats = by_port.setdefault(
                key,
                {
                    "node_id": node_id,
                    "node_type": node_type,
                    "if_index": if_index,
                    "pause_events": 0,
                    "resume_events": 0,
                    "paused_time_ns": 0,
                    "max_pause_ns": 0,
                    "open_pause": 0,
                },
            )
            if event_type == 1:
                pause_events += 1
                port_stats["pause_events"] += 1
                earliest_pause_ns = time_ns if earliest_pause_ns is None else min(earliest_pause_ns, time_ns)
                latest_pause_ns = time_ns if latest_pause_ns is None else max(latest_pause_ns, time_ns)
                if key not in active_pause:
                    active_pause[key] = time_ns
            else:
                resume_events += 1
                port_stats["resume_events"] += 1
                start_ns = active_pause.pop(key, None)
                if start_ns is not None:
                    duration_ns = time_ns - start_ns
                    port_stats["paused_time_ns"] += duration_ns
                    port_stats["max_pause_ns"] = max(port_stats["max_pause_ns"], duration_ns)

    for key, start_ns in active_pause.items():
        by_port[key]["open_pause"] = start_ns

    top_ports = sorted(
        by_port.values(),
        key=lambda item: (item["pause_events"], item["paused_time_ns"], item["max_pause_ns"]),
        reverse=True,
    )[:5]
    return {
        "pause_events": pause_events,
        "resume_events": resume_events,
        "ports_with_pause": sum(1 for item in by_port.values() if item["pause_events"] > 0),
        "earliest_pause_ns": earliest_pause_ns,
        "latest_pause_ns": latest_pause_ns,
        "top_paused_ports": top_ports,
    }


def summarize_qlen(qlen_path: Path) -> Dict[str, object]:
    if not qlen_path.exists():
        return {"max_queue_kb": None, "switch_id": None, "if_index": None, "sample_time_ns": None}

    max_queue_kb = -1
    switch_id = None
    if_index = None
    sample_time_ns = None
    current_time_ns: Optional[int] = None

    with qlen_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("time:"):
                current_time_ns = int(line.split()[1])
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            port_switch = int(parts[0])
            port_if = int(parts[1])
            histogram = [int(value) for value in parts[2:]]
            highest_non_zero = -1
            for kb in range(len(histogram) - 1, -1, -1):
                if histogram[kb] > 0:
                    highest_non_zero = kb
                    break
            if highest_non_zero > max_queue_kb:
                max_queue_kb = highest_non_zero
                switch_id = port_switch
                if_index = port_if
                sample_time_ns = current_time_ns

    return {
        "max_queue_kb": None if max_queue_kb < 0 else max_queue_kb,
        "switch_id": switch_id,
        "if_index": if_index,
        "sample_time_ns": sample_time_ns,
    }


def format_optional(value: Optional[float], fmt: str) -> str:
    if value is None:
        return "n/a"
    return format(value, fmt)


def write_text_summary(
    path: Path,
    args: argparse.Namespace,
    flows: Sequence[FlowSpec],
    fct_summary: Dict[str, object],
    pfc_summary: Dict[str, object],
    qlen_summary: Dict[str, object],
    output_paths: OutputPaths,
    simulation_dir: Path,
) -> None:
    pipeline_ring = interleaved_pipeline_ring(args.pipeline_nodes_per_rack)
    patterns = fct_summary.get("patterns", {})
    overall = patterns.get("overall", {})
    pipeline = patterns.get("pipeline", {})
    alltoall = patterns.get("alltoall", {})

    lines = [
        f"Scenario tag: {args.tag}",
        f"Output root: {output_paths.root.relative_to(simulation_dir).as_posix()}",
        (
            "Traffic mix: "
            f"hotspot all-to-all on ToR {args.alltoall_rack} over {args.alltoall_nodes} hosts, "
            f"cross-ToR pipeline with {args.pipeline_nodes_per_rack * 2} hosts and "
            f"{args.pipeline_rounds} waves"
        ),
        f"Pipeline ring: {' '.join(map(str, pipeline_ring))}",
        f"Generated flows: {len(flows)}",
        "",
        "Latency / throughput",
        (
            "  overall: "
            f"{overall.get('completed_flows', 0)}/{overall.get('expected_flows', len(flows))} completed, "
            f"avg_fct={format_optional(overall.get('avg_fct_us'), '.2f')} us, "
            f"p95={format_optional(overall.get('p95_fct_us'), '.2f')} us, "
            f"aggregate_goodput={format_optional(overall.get('aggregate_goodput_gbps'), '.2f')} Gbps"
        ),
        (
            "  pipeline: "
            f"{pipeline.get('completed_flows', 0)}/{pipeline.get('expected_flows', 0)} completed, "
            f"avg_fct={format_optional(pipeline.get('avg_fct_us'), '.2f')} us, "
            f"p95={format_optional(pipeline.get('p95_fct_us'), '.2f')} us, "
            f"aggregate_goodput={format_optional(pipeline.get('aggregate_goodput_gbps'), '.2f')} Gbps"
        ),
        (
            "  alltoall: "
            f"{alltoall.get('completed_flows', 0)}/{alltoall.get('expected_flows', 0)} completed, "
            f"avg_fct={format_optional(alltoall.get('avg_fct_us'), '.2f')} us, "
            f"p95={format_optional(alltoall.get('p95_fct_us'), '.2f')} us, "
            f"aggregate_goodput={format_optional(alltoall.get('aggregate_goodput_gbps'), '.2f')} Gbps"
        ),
        "",
        "PFC",
        (
            f"  pause_events={pfc_summary.get('pause_events', 0)}, "
            f"resume_events={pfc_summary.get('resume_events', 0)}, "
            f"ports_with_pause={pfc_summary.get('ports_with_pause', 0)}"
        ),
    ]

    for port in pfc_summary.get("top_paused_ports", []):
        lines.append(
            "  port "
            f"node={port['node_id']} if={port['if_index']} pauses={port['pause_events']} "
            f"paused_time={port['paused_time_ns'] / 1e3:.2f} us "
            f"max_pause={port['max_pause_ns'] / 1e3:.2f} us"
        )

    lines.extend(
        [
            "",
            "Queue occupancy",
            (
                "  max_queue="
                f"{qlen_summary.get('max_queue_kb') if qlen_summary.get('max_queue_kb') is not None else 'n/a'} KB "
                f"at switch={qlen_summary.get('switch_id')} if={qlen_summary.get('if_index')} "
                f"time_ns={qlen_summary.get('sample_time_ns')}"
            ),
            "",
            "Files",
            f"  topology: {output_paths.topology.relative_to(simulation_dir).as_posix()}",
            f"  flow: {output_paths.flow.relative_to(simulation_dir).as_posix()}",
            f"  config: {output_paths.config.relative_to(simulation_dir).as_posix()}",
            f"  pfc: {output_paths.pfc.relative_to(simulation_dir).as_posix()}",
            f"  fct: {output_paths.fct.relative_to(simulation_dir).as_posix()}",
            f"  qlen: {output_paths.qlen.relative_to(simulation_dir).as_posix()}",
            f"  summary_json: {output_paths.summary_json.relative_to(simulation_dir).as_posix()}",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def run_command(cmd: Sequence[str], cwd: Path) -> None:
    try:
        completed = subprocess.run(cmd, cwd=cwd)
    except OSError as exc:
        joined = " ".join(map(str, cmd))
        raise RuntimeError(f"Failed to start command: {joined} ({exc})") from exc
    if completed.returncode != 0:
        joined = " ".join(map(str, cmd))
        raise RuntimeError(f"Command failed ({completed.returncode}): {joined}")


def built_binary_path(simulation_dir: Path, simulator: str) -> Path:
    return simulation_dir / "build" / "scratch" / simulator


def should_use_binary(simulation_dir: Path, args: argparse.Namespace) -> bool:
    if args.launcher == "binary":
        return True
    if args.launcher == "waf":
        return False
    return platform.system() == "Linux" and built_binary_path(simulation_dir, args.simulator).exists()


def interpreter_version(executable: str) -> Optional[Tuple[int, int]]:
    try:
        completed = subprocess.run(
            [executable, "-c", "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    try:
        major_str, minor_str = completed.stdout.strip().split(".", 1)
        return int(major_str), int(minor_str)
    except ValueError:
        return None


def select_waf_python(args: argparse.Namespace) -> str:
    candidates: List[str] = []
    if args.waf_python:
        candidates.append(args.waf_python)

    for name in (sys.executable, "python", "python3.11", "python3", "python3.10"):
        resolved = shutil.which(name) if not Path(name).is_absolute() else name
        if resolved and resolved not in candidates:
            candidates.append(resolved)

    fallback = candidates[0]
    for candidate in candidates:
        version = interpreter_version(candidate)
        if version is not None and version < (3, 12):
            return candidate
    return fallback


def run_waf_compat(simulation_dir: Path, waf_args: Sequence[str], args: argparse.Namespace) -> None:
    python_executable = select_waf_python(args)
    run_command([python_executable, "-c", WAF_COMPAT_BOOTSTRAP, *waf_args], simulation_dir)


def run_simulation(simulation_dir: Path, config_rel: str, args: argparse.Namespace) -> None:
    if should_use_binary(simulation_dir, args):
        binary = built_binary_path(simulation_dir, args.simulator)
        if not binary.exists():
            raise RuntimeError(f"Missing built simulator binary: {binary}")
        run_command([str(binary), config_rel], simulation_dir)
        return

    run_waf_compat(simulation_dir, ["--run", f"scratch/{args.simulator} {config_rel}"], args)


def build_output_paths(simulation_dir: Path, tag: str) -> OutputPaths:
    root = simulation_dir / "mix" / "generated" / tag
    root.mkdir(parents=True, exist_ok=True)
    return OutputPaths(
        root=root,
        topology=root / "topology.txt",
        flow=root / "flow.txt",
        config=root / "config.txt",
        manifest=root / "manifest.json",
        summary_json=root / "summary.json",
        summary_txt=root / "summary.txt",
        pfc=root / "pfc.txt",
        fct=root / "fct.txt",
        qlen=root / "qlen.txt",
        trace_output=root / "trace.tr",
    )


def generate_inputs(
    args: argparse.Namespace,
    output_paths: OutputPaths,
    simulation_dir: Path,
) -> Tuple[List[FlowSpec], float]:
    flows = generate_flows(args)
    stop_time_s = estimate_stop_time_s(flows, args)
    write_topology(output_paths.topology, args)
    write_flow_file(output_paths.flow, flows)
    write_config(output_paths.config, stop_time_s, args, output_paths, simulation_dir)
    write_manifest(output_paths.manifest, args, flows, stop_time_s, output_paths, simulation_dir)
    return flows, stop_time_s


def load_flows_from_manifest(manifest_path: Path) -> List[FlowSpec]:
    data = json.loads(manifest_path.read_text(encoding="ascii"))
    return [FlowSpec(**item) for item in data["flows"]]


def parse_existing_outputs(
    args: argparse.Namespace,
    output_paths: OutputPaths,
    simulation_dir: Path,
) -> Dict[str, object]:
    flows = load_flows_from_manifest(output_paths.manifest)
    fct_summary = summarize_fct(output_paths.fct, flows)
    pfc_summary = summarize_pfc(output_paths.pfc)
    qlen_summary = summarize_qlen(output_paths.qlen)
    summary = {
        "scenario": json.loads(output_paths.manifest.read_text(encoding="ascii"))["scenario"],
        "fct": fct_summary,
        "pfc": pfc_summary,
        "qlen": qlen_summary,
        "files": {
            "root": output_paths.root.relative_to(simulation_dir).as_posix(),
            "topology": output_paths.topology.relative_to(simulation_dir).as_posix(),
            "flow": output_paths.flow.relative_to(simulation_dir).as_posix(),
            "config": output_paths.config.relative_to(simulation_dir).as_posix(),
            "fct": output_paths.fct.relative_to(simulation_dir).as_posix(),
            "pfc": output_paths.pfc.relative_to(simulation_dir).as_posix(),
            "qlen": output_paths.qlen.relative_to(simulation_dir).as_posix(),
            "summary_txt": output_paths.summary_txt.relative_to(simulation_dir).as_posix(),
        },
    }
    output_paths.summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="ascii")
    write_text_summary(
        output_paths.summary_txt,
        args,
        flows,
        fct_summary,
        pfc_summary,
        qlen_summary,
        output_paths,
        simulation_dir,
    )
    return summary


def main() -> int:
    try:
        args = parse_args()
        ensure_valid_args(args)

        simulation_dir = simulation_dir_from_script()
        output_paths = build_output_paths(simulation_dir, args.tag)

        if args.summary_only:
            if not output_paths.manifest.exists():
                print(f"Missing manifest: {output_paths.manifest}", file=sys.stderr)
                return 1
            summary = parse_existing_outputs(args, output_paths, simulation_dir)
            print(output_paths.summary_txt.relative_to(simulation_dir).as_posix())
            print(json.dumps(summary["fct"]["patterns"]["overall"], indent=2))
            return 0

        flows, _ = generate_inputs(args, output_paths, simulation_dir)

        if args.skip_run:
            print(output_paths.config.relative_to(simulation_dir).as_posix())
            print(output_paths.manifest.relative_to(simulation_dir).as_posix())
            print(f"generated_flows={len(flows)}")
            return 0

        if args.build_first:
            run_waf_compat(simulation_dir, ["build"], args)

        config_rel = output_paths.config.relative_to(simulation_dir).as_posix()
        run_simulation(simulation_dir, config_rel, args)

        summary = parse_existing_outputs(args, output_paths, simulation_dir)
        print(output_paths.summary_txt.relative_to(simulation_dir).as_posix())
        print(
            "completed_flows="
            f"{summary['fct']['completed_flows']}/{summary['fct']['expected_flows']} "
            f"pause_events={summary['pfc']['pause_events']} "
            f"resume_events={summary['pfc']['resume_events']}"
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
