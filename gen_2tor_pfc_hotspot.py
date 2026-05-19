#!/usr/bin/env python3
"""
Generate a native mix/ experiment for:
1. Intra-ToR all-to-all on a small host subset.
2. Cross-ToR reduce-scatter pipeline on an interleaved 2-rack ring.

The generated files are meant to be run with:
  ./waf --run 'scratch/mp-rdma-simulator mix/config_<prefix>.txt'
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Sequence, Tuple


HOSTS_PER_TOR = 16
TOTAL_HOSTS = 32
TOR_A = 32
TOR_B = 33
DEFAULT_PREFIX = "2tor_pfc_hotspot"
DEFAULT_PG = 3
PIPELINE_DST_PORT_BASE = 10000
ALLTOALL_DST_PORT_BASE = 20000


CONFIG_TEMPLATE = """ENABLE_QCN 1
USE_DYNAMIC_PFC_THRESHOLD 1
ENABLE_PFC {enable_pfc}

PACKET_PAYLOAD_SIZE 1000

TOPOLOGY_FILE mix/topo_{prefix}.txt
FLOW_FILE mix/flow_{prefix}.txt
TRACE_FILE mix/trace_{prefix}.txt
TRACE_OUTPUT_FILE mix/mix_{prefix}.tr
FCT_OUTPUT_FILE mix/fct_{prefix}.txt
PFC_OUTPUT_FILE mix/pfc_{prefix}.txt

SIMULATOR_STOP_TIME {stop_time_s:.6f}

CC_MODE 1
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

{kmax_map_line}
{kmin_map_line}
{pmax_map_line}
BUFFER_SIZE {buffer_size_mb}
PFC_XOFF {pfc_xoff}
PFC_XON {pfc_xon}
QLEN_MON_FILE mix/qlen_{prefix}.txt
QLEN_MON_START 0
QLEN_MON_END {qlen_mon_end_ns}
"""


@dataclass(frozen=True)
class FlowSpec:
    flow_id: str
    pattern: str
    src: int
    dst: int
    size_bytes: int
    start_time_s: float
    pg: int = DEFAULT_PG
    dst_port: int = PIPELINE_DST_PORT_BASE

    @property
    def start_time_ns(self) -> int:
        return int(round(self.start_time_s * 1e9))

    def to_line(self) -> str:
        return (
            f"{self.src} {self.dst} {self.pg} {self.dst_port} "
            f"{self.size_bytes} {self.start_time_s:.9f}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mix/* files for the 2-ToR PFC hotspot experiment.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="File prefix under mix/.")
    parser.add_argument(
        "--alltoall-node-list",
        default="24-31",
        help="Comma-separated list and/or ranges for hotspot all-to-all nodes, e.g. '24-31' or '24,25,26,27'.",
    )
    parser.add_argument("--alltoall-rack", type=int, choices=(0, 1), default=0)
    parser.add_argument("--alltoall-nodes", type=int, default=8)
    parser.add_argument("--alltoall-flow-bytes", type=int, default=64 * 1024)
    parser.add_argument("--alltoall-rounds", type=int, default=1)
    parser.add_argument("--alltoall-round-gap-us", type=float, default=2.0)
    parser.add_argument("--alltoall-base-us", type=float, default=2.0)
    parser.add_argument("--alltoall-src-stagger-us", type=float, default=0.0)
    parser.add_argument("--alltoall-pg", type=int, default=DEFAULT_PG, help="Priority group / queue index for all-to-all flows.")
    parser.add_argument("--no-alltoall", action="store_true", help="Generate reduce-scatter only for baseline comparison.")
    parser.add_argument("--pipeline-nodes-per-rack", type=int, default=16)
    parser.add_argument(
        "--pipeline-ring-layout",
        choices=("interleaved", "rack_contiguous"),
        default="interleaved",
        help=(
            "How to order the cross-rack pipeline ring. "
            "'interleaved' alternates racks on every hop; "
            "'rack_contiguous' keeps all rack-0 hosts together, then all rack-1 hosts."
        ),
    )
    parser.add_argument("--pipeline-rounds", type=int, default=8)
    parser.add_argument("--pipeline-flow-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--pipeline-base-us", type=float, default=0.0)
    parser.add_argument("--pipeline-gap-us", type=float, default=1.0)
    parser.add_argument("--pipeline-pg", type=int, default=DEFAULT_PG, help="Priority group / queue index for pipeline flows.")
    parser.add_argument("--no-pipeline", action="store_true", help="Generate all-to-all only, without reduce-scatter pipeline flows.")
    parser.add_argument(
        "--pipeline-auto-gap",
        action="store_true",
        help="Choose a round gap that keeps successive pipeline waves approximately back-to-back without overlap.",
    )
    parser.add_argument("--link-rate-gbps", type=float, default=400.0)
    parser.add_argument(
        "--host-link-rate-gbps",
        type=float,
        default=None,
        help="Optional host-to-ToR rate override. Defaults to --link-rate-gbps.",
    )
    parser.add_argument(
        "--tor-link-rate-gbps",
        type=float,
        default=None,
        help="Optional inter-ToR rate override. Defaults to --link-rate-gbps.",
    )
    parser.add_argument("--link-delay-us", type=float, default=1.0)
    parser.add_argument("--buffer-size-mb", type=int, default=2)
    parser.add_argument("--disable-pfc", action="store_true", help="Generate config with PFC disabled.")
    parser.add_argument("--enable-trace", action="store_true", help="Enable binary packet tracing for this experiment.")
    parser.add_argument("--pfc-xoff-bytes", type=int, default=1000)
    parser.add_argument("--pfc-xon-bytes", type=int, default=300)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not 1 <= args.alltoall_nodes <= HOSTS_PER_TOR:
        raise ValueError("--alltoall-nodes must be in [1, 16]")
    if not 1 <= args.pipeline_nodes_per_rack <= HOSTS_PER_TOR:
        raise ValueError("--pipeline-nodes-per-rack must be in [1, 16]")
    if not args.no_pipeline and args.pipeline_rounds < 1:
        raise ValueError("--pipeline-rounds must be >= 1")
    if args.alltoall_rounds < 1:
        raise ValueError("--alltoall-rounds must be >= 1")
    if args.pipeline_gap_us < 0:
        raise ValueError("--pipeline-gap-us must be >= 0")
    if args.link_rate_gbps <= 0:
        raise ValueError("--link-rate-gbps must be > 0")
    if args.host_link_rate_gbps is not None and args.host_link_rate_gbps <= 0:
        raise ValueError("--host-link-rate-gbps must be > 0")
    if args.tor_link_rate_gbps is not None and args.tor_link_rate_gbps <= 0:
        raise ValueError("--tor-link-rate-gbps must be > 0")
    if args.pfc_xon_bytes < 0 or args.pfc_xoff_bytes <= 0:
        raise ValueError("PFC thresholds must be non-negative, and xoff must be > 0")
    if args.pfc_xon_bytes >= args.pfc_xoff_bytes:
        raise ValueError("--pfc-xon-bytes must be smaller than --pfc-xoff-bytes")
    if not 0 <= args.pipeline_pg < 8:
        raise ValueError("--pipeline-pg must be in [0, 7]")
    if not 0 <= args.alltoall_pg < 8:
        raise ValueError("--alltoall-pg must be in [0, 7]")


def parse_node_list(spec: str) -> List[int]:
    nodes: List[int] = []
    for chunk in spec.split(","):
        item = chunk.strip()
        if not item:
            continue
        if "-" in item:
            start_str, end_str = item.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            step = 1 if end >= start else -1
            nodes.extend(range(start, end + step, step))
        else:
            nodes.append(int(item))
    deduped = []
    seen = set()
    for node in nodes:
        if node not in seen:
            deduped.append(node)
            seen.add(node)
    return deduped


def rack_nodes(rack_index: int, count: int) -> List[int]:
    base = rack_index * HOSTS_PER_TOR
    return list(range(base, base + count))


def get_hotspot_nodes(args: argparse.Namespace) -> List[int]:
    if args.alltoall_node_list:
        nodes = parse_node_list(args.alltoall_node_list)
    else:
        nodes = rack_nodes(args.alltoall_rack, args.alltoall_nodes)

    if not nodes:
        return nodes
    if any(node < 0 or node >= TOTAL_HOSTS for node in nodes):
        raise ValueError("all-to-all hotspot nodes must be in [0, 31]")
    racks = {node // HOSTS_PER_TOR for node in nodes}
    if len(racks) != 1:
        raise ValueError("all-to-all hotspot nodes must stay within a single ToR")
    return nodes


def pipeline_ring(nodes_per_rack: int, layout: str) -> List[int]:
    left = rack_nodes(0, nodes_per_rack)
    right = rack_nodes(1, nodes_per_rack)
    if layout == "interleaved":
        ring: List[int] = []
        for left_node, right_node in zip(left, right):
            ring.append(left_node)
            ring.append(right_node)
        return ring
    if layout == "rack_contiguous":
        return left + right
    raise ValueError(f"unknown pipeline ring layout: {layout}")


def resolve_link_rates(args: argparse.Namespace) -> Tuple[float, float]:
    host_link_rate_gbps = args.host_link_rate_gbps or args.link_rate_gbps
    tor_link_rate_gbps = args.tor_link_rate_gbps or args.link_rate_gbps
    return host_link_rate_gbps, tor_link_rate_gbps


def ecn_thresholds_for_rate_gbps(rate_gbps: float) -> Tuple[int, int]:
    kmin = max(1, int(round(400 * rate_gbps / 100.0)))
    kmax = max(kmin + 1, int(round(1600 * rate_gbps / 100.0)))
    return kmin, kmax


def pipeline_expectation(args: argparse.Namespace) -> dict:
    if getattr(args, "no_pipeline", False):
        return {
            "concurrent_flows_per_round": 0,
            "cross_tor_flows_per_round": 0,
            "tor_0_to_1_bytes_per_round": 0,
            "tor_1_to_0_bytes_per_round": 0,
            "host_max_tx_bytes_per_round": 0,
            "host_max_rx_bytes_per_round": 0,
            "aggregate_bottleneck_gbps": None,
            "per_flow_bottleneck_gbps": None,
            "critical_tor_serialize_us": 0.0,
            "critical_host_serialize_us": 0.0,
            "critical_serialize_us": 0.0,
            "one_way_path_delay_us": 0.0,
            "ideal_round_duration_us": None,
            "configured_gap_us": args.pipeline_gap_us,
            "rounds_overlap_expected": False,
        }

    ring = pipeline_ring(args.pipeline_nodes_per_rack, args.pipeline_ring_layout)
    concurrent_flows = len(ring)
    host_link_rate_gbps, tor_link_rate_gbps = resolve_link_rates(args)
    tor_dir_bytes = {"0_to_1": 0, "1_to_0": 0}
    host_tx_bytes = {}
    host_rx_bytes = {}
    max_hops = 0

    for idx, src in enumerate(ring):
        dst = ring[(idx + 1) % len(ring)]
        src_rack = src // HOSTS_PER_TOR
        dst_rack = dst // HOSTS_PER_TOR
        host_tx_bytes[src] = host_tx_bytes.get(src, 0) + args.pipeline_flow_bytes
        host_rx_bytes[dst] = host_rx_bytes.get(dst, 0) + args.pipeline_flow_bytes
        if src_rack != dst_rack:
            if src_rack == 0:
                tor_dir_bytes["0_to_1"] += args.pipeline_flow_bytes
            else:
                tor_dir_bytes["1_to_0"] += args.pipeline_flow_bytes
            max_hops = max(max_hops, 3)
        else:
            max_hops = max(max_hops, 2)

    critical_tor_serialize_us = (
        max(tor_dir_bytes.values()) * 8.0 / (tor_link_rate_gbps * 1e9) * 1e6
        if concurrent_flows > 0 and max(tor_dir_bytes.values()) > 0
        else 0.0
    )
    critical_host_serialize_us = (
        max(
            max(host_tx_bytes.values(), default=0),
            max(host_rx_bytes.values(), default=0),
        )
        * 8.0
        / (host_link_rate_gbps * 1e9)
        * 1e6
        if concurrent_flows > 0
        else 0.0
    )
    critical_serialize_us = max(critical_tor_serialize_us, critical_host_serialize_us)
    one_way_path_delay_us = max_hops * args.link_delay_us
    ideal_round_duration_us = critical_serialize_us + one_way_path_delay_us if concurrent_flows > 0 else None
    total_round_bytes = concurrent_flows * args.pipeline_flow_bytes
    aggregate_bottleneck_gbps = (
        total_round_bytes * 8.0 / (ideal_round_duration_us * 1e3)
        if ideal_round_duration_us and ideal_round_duration_us > 0
        else None
    )
    per_flow_bottleneck_gbps = (
        args.pipeline_flow_bytes * 8.0 / (ideal_round_duration_us * 1e3)
        if ideal_round_duration_us and ideal_round_duration_us > 0
        else None
    )
    return {
        "concurrent_flows_per_round": concurrent_flows,
        "cross_tor_flows_per_round": (tor_dir_bytes["0_to_1"] + tor_dir_bytes["1_to_0"]) // max(args.pipeline_flow_bytes, 1),
        "tor_0_to_1_bytes_per_round": tor_dir_bytes["0_to_1"],
        "tor_1_to_0_bytes_per_round": tor_dir_bytes["1_to_0"],
        "host_max_tx_bytes_per_round": max(host_tx_bytes.values(), default=0),
        "host_max_rx_bytes_per_round": max(host_rx_bytes.values(), default=0),
        "aggregate_bottleneck_gbps": aggregate_bottleneck_gbps,
        "per_flow_bottleneck_gbps": per_flow_bottleneck_gbps,
        "critical_tor_serialize_us": critical_tor_serialize_us,
        "critical_host_serialize_us": critical_host_serialize_us,
        "critical_serialize_us": critical_serialize_us,
        "one_way_path_delay_us": one_way_path_delay_us,
        "ideal_round_duration_us": ideal_round_duration_us,
        "configured_gap_us": args.pipeline_gap_us,
        "rounds_overlap_expected": (
            ideal_round_duration_us is not None and args.pipeline_gap_us < ideal_round_duration_us
        ),
    }


def pattern_time_bounds_ns(flows: Sequence[FlowSpec], pattern: str) -> dict:
    starts = [flow.start_time_ns for flow in flows if flow.pattern == pattern]
    if not starts:
        return {
            "first_start_ns": None,
            "last_start_ns": None,
        }
    return {
        "first_start_ns": min(starts),
        "last_start_ns": max(starts),
    }


def generate_flows(args: argparse.Namespace) -> List[FlowSpec]:
    flows: List[FlowSpec] = []

    if not args.no_pipeline:
        ring = pipeline_ring(args.pipeline_nodes_per_rack, args.pipeline_ring_layout)
        pipeline_base_s = args.pipeline_base_us * 1e-6
        pipeline_gap_s = args.pipeline_gap_us * 1e-6
        for round_idx in range(args.pipeline_rounds):
            start_time_s = pipeline_base_s + round_idx * pipeline_gap_s
            for idx, src in enumerate(ring):
                dst = ring[(idx + 1) % len(ring)]
                flows.append(
                    FlowSpec(
                        flow_id=f"pipeline-r{round_idx:03d}-{src:02d}-{dst:02d}",
                        pattern="pipeline",
                        src=src,
                        dst=dst,
                        size_bytes=args.pipeline_flow_bytes,
                        start_time_s=start_time_s,
                        pg=args.pipeline_pg,
                        dst_port=PIPELINE_DST_PORT_BASE + round_idx * len(ring) + idx,
                    )
                )

    hotspot_nodes = get_hotspot_nodes(args)
    if not args.no_alltoall:
        alltoall_base_s = args.alltoall_base_us * 1e-6
        round_gap_s = args.alltoall_round_gap_us * 1e-6
        src_stagger_s = args.alltoall_src_stagger_us * 1e-6
        flows_per_round = len(hotspot_nodes) * max(len(hotspot_nodes) - 1, 0)
        for round_idx in range(args.alltoall_rounds):
            round_start_s = alltoall_base_s + round_idx * round_gap_s
            for src_idx, src in enumerate(hotspot_nodes):
                start_time_s = round_start_s + src_idx * src_stagger_s
                dst_rank = 0
                for dst in hotspot_nodes:
                    if src == dst:
                        continue
                    flows.append(
                        FlowSpec(
                            flow_id=f"alltoall-r{round_idx:03d}-{src:02d}-{dst:02d}",
                            pattern="alltoall",
                            src=src,
                            dst=dst,
                            size_bytes=args.alltoall_flow_bytes,
                            start_time_s=start_time_s,
                            pg=args.alltoall_pg,
                            dst_port=ALLTOALL_DST_PORT_BASE + round_idx * flows_per_round + src_idx * len(hotspot_nodes) + dst_rank,
                        )
                    )
                    dst_rank += 1

    flows.sort(key=lambda flow: (flow.start_time_s, flow.pattern, flow.src, flow.dst))
    return flows


def write_topology(path: Path, args: argparse.Namespace) -> None:
    host_link_rate_gbps, tor_link_rate_gbps = resolve_link_rates(args)
    host_rate = f"{host_link_rate_gbps:g}Gbps"
    tor_rate = f"{tor_link_rate_gbps:g}Gbps"
    delay_ms = args.link_delay_us / 1000.0
    delay = f"{delay_ms:.6f}ms"
    lines = ["34 2 33", f"{TOR_A} {TOR_B}"]

    for host in range(0, 16):
        lines.append(f"{host} {TOR_A} {host_rate} {delay} 0")
    for host in range(16, 32):
        lines.append(f"{host} {TOR_B} {host_rate} {delay} 0")
    lines.append(f"{TOR_A} {TOR_B} {tor_rate} {delay} 0")
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def write_flow_file(path: Path, flows: Sequence[FlowSpec]) -> None:
    lines = [str(len(flows))]
    lines.extend(flow.to_line() for flow in flows)
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def write_trace_file(path: Path) -> None:
    node_ids = list(range(TOTAL_HOSTS + 2))
    lines = [str(len(node_ids)), " ".join(str(node_id) for node_id in node_ids)]
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def estimate_stop_time_s(flows: Sequence[FlowSpec], link_rate_gbps: float) -> float:
    last_start_s = max(flow.start_time_s for flow in flows)
    total_bytes = sum(flow.size_bytes for flow in flows)
    bottleneck_s = total_bytes * 8.0 / (link_rate_gbps * 1e9)
    return last_start_s + max(0.02, 3.0 * bottleneck_s)


def write_config(path: Path, args: argparse.Namespace, stop_time_s: float) -> None:
    host_link_rate_gbps, tor_link_rate_gbps = resolve_link_rates(args)
    unique_rates_bps = sorted({int(round(host_link_rate_gbps * 1e9)), int(round(tor_link_rate_gbps * 1e9))})

    def render_map_line(name: str, value_builder) -> str:
        entries = []
        for rate_bps in unique_rates_bps:
            rate_gbps = rate_bps / 1e9
            entries.append(f"{rate_bps} {value_builder(rate_gbps)}")
        return f"{name} {len(unique_rates_bps)} " + " ".join(entries)

    path.write_text(
        CONFIG_TEMPLATE.format(
            prefix=args.prefix,
            enable_pfc=0 if args.disable_pfc else 1,
            enable_trace=1 if args.enable_trace else 0,
            stop_time_s=stop_time_s,
            kmax_map_line=render_map_line("KMAX_MAP", lambda rate_gbps: ecn_thresholds_for_rate_gbps(rate_gbps)[1]),
            kmin_map_line=render_map_line("KMIN_MAP", lambda rate_gbps: ecn_thresholds_for_rate_gbps(rate_gbps)[0]),
            pmax_map_line=render_map_line("PMAX_MAP", lambda _rate_gbps: "0.2"),
            buffer_size_mb=args.buffer_size_mb,
            pfc_xoff=args.pfc_xoff_bytes,
            pfc_xon=args.pfc_xon_bytes,
            qlen_mon_end_ns=int(math.ceil(stop_time_s * 1e9)),
        ),
        encoding="ascii",
    )


def write_manifest(
    path: Path,
    args: argparse.Namespace,
    flows: Sequence[FlowSpec],
    stop_time_s: float,
    pipeline_expected: dict,
) -> None:
    hotspot_nodes = [] if args.no_alltoall else get_hotspot_nodes(args)
    host_link_rate_gbps, tor_link_rate_gbps = resolve_link_rates(args)
    data = {
        "prefix": args.prefix,
        "scenario": {
            "alltoall_enabled": not args.no_alltoall,
            "alltoall_rack": hotspot_nodes[0] // HOSTS_PER_TOR if hotspot_nodes else None,
            "alltoall_nodes": len(hotspot_nodes),
            "alltoall_hotspot_nodes": hotspot_nodes,
            "alltoall_flow_bytes": args.alltoall_flow_bytes,
            "alltoall_pg": args.alltoall_pg,
            "alltoall_rounds": args.alltoall_rounds,
            "alltoall_base_us": args.alltoall_base_us,
            "alltoall_round_gap_us": args.alltoall_round_gap_us,
            "alltoall_src_stagger_us": args.alltoall_src_stagger_us,
            "pipeline_nodes_per_rack": args.pipeline_nodes_per_rack,
            "pipeline_enabled": not args.no_pipeline,
            "pipeline_rounds": args.pipeline_rounds,
            "pipeline_flow_bytes": args.pipeline_flow_bytes,
            "pipeline_pg": args.pipeline_pg,
            "pipeline_base_us": args.pipeline_base_us,
            "pipeline_gap_us": args.pipeline_gap_us,
            "pipeline_auto_gap": args.pipeline_auto_gap,
            "pipeline_ring_layout": args.pipeline_ring_layout,
            "pipeline_ring": [] if args.no_pipeline else pipeline_ring(args.pipeline_nodes_per_rack, args.pipeline_ring_layout),
            "pipeline_expected": pipeline_expected,
            "timeline": {
                "pipeline": pattern_time_bounds_ns(flows, "pipeline"),
                "alltoall": pattern_time_bounds_ns(flows, "alltoall"),
            },
            "host_link_rate_gbps": host_link_rate_gbps,
            "tor_link_rate_gbps": tor_link_rate_gbps,
            "min_link_rate_gbps": min(host_link_rate_gbps, tor_link_rate_gbps),
            "link_delay_us": args.link_delay_us,
            "buffer_size_mb": args.buffer_size_mb,
            "enable_pfc": not args.disable_pfc,
            "enable_trace": args.enable_trace,
            "pfc_xoff_bytes": args.pfc_xoff_bytes,
            "pfc_xon_bytes": args.pfc_xon_bytes,
            "stop_time_s": stop_time_s,
        },
        "files": {
            "topology": f"mix/topo_{args.prefix}.txt",
            "flow": f"mix/flow_{args.prefix}.txt",
            "config": f"mix/config_{args.prefix}.txt",
            "trace_input": f"mix/trace_{args.prefix}.txt",
            "trace_output": f"mix/mix_{args.prefix}.tr",
            "fct": f"mix/fct_{args.prefix}.txt",
            "pfc": f"mix/pfc_{args.prefix}.txt",
            "qlen": f"mix/qlen_{args.prefix}.txt",
            "summary_txt": f"mix/summary_{args.prefix}.txt",
            "summary_json": f"mix/summary_{args.prefix}.json",
        },
        "flows": [asdict(flow) for flow in flows],
    }
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="ascii")


def main() -> int:
    args = parse_args()
    validate_args(args)

    if args.pipeline_auto_gap and not args.no_pipeline:
        auto_gap = pipeline_expectation(args)["ideal_round_duration_us"]
        if auto_gap is None:
            raise SystemExit("failed to derive pipeline auto-gap")
        args.pipeline_gap_us = auto_gap

    mix_dir = Path(__file__).resolve().parent
    flows = generate_flows(args)
    host_link_rate_gbps, tor_link_rate_gbps = resolve_link_rates(args)
    stop_time_s = estimate_stop_time_s(flows, min(host_link_rate_gbps, tor_link_rate_gbps))
    pipeline_expected = pipeline_expectation(args)

    topo_path = mix_dir / f"topo_{args.prefix}.txt"
    flow_path = mix_dir / f"flow_{args.prefix}.txt"
    trace_path = mix_dir / f"trace_{args.prefix}.txt"
    config_path = mix_dir / f"config_{args.prefix}.txt"
    manifest_path = mix_dir / f"manifest_{args.prefix}.json"

    write_topology(topo_path, args)
    write_flow_file(flow_path, flows)
    write_trace_file(trace_path)
    write_config(config_path, args, stop_time_s)
    write_manifest(manifest_path, args, flows, stop_time_s, pipeline_expected)

    print(config_path.name)
    print(flow_path.name)
    print(topo_path.name)
    print(trace_path.name)
    print(manifest_path.name)
    print(f"generated_flows={len(flows)}")
    def fmt(value: object) -> str:
        return "n/a" if value is None else f"{float(value):.2f}"

    print(
        "pipeline_expected: "
        f"concurrent_flows_per_round={pipeline_expected['concurrent_flows_per_round']} "
        f"cross_tor_flows_per_round={pipeline_expected['cross_tor_flows_per_round']} "
        f"aggregate_bottleneck_gbps={fmt(pipeline_expected['aggregate_bottleneck_gbps'])} "
        f"per_flow_bottleneck_gbps={fmt(pipeline_expected['per_flow_bottleneck_gbps'])} "
        f"critical_tor_serialize_us={fmt(pipeline_expected['critical_tor_serialize_us'])} "
        f"critical_host_serialize_us={fmt(pipeline_expected['critical_host_serialize_us'])} "
        f"ideal_round_duration_us={fmt(pipeline_expected['ideal_round_duration_us'])} "
        f"configured_gap_us={fmt(pipeline_expected['configured_gap_us'])}"
    )
    print(f"./waf --run 'scratch/mp-rdma-simulator mix/{config_path.name}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
