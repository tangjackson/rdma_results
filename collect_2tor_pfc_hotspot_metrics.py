#!/usr/bin/env python3
"""
Summarize PFC, queue, throughput, and latency for the 2-ToR hotspot experiment.

Typical usage after the simulator run:
  python3 mix/collect_2tor_pfc_hotspot_metrics.py --prefix 2tor_pfc_hotspot
"""

from __future__ import annotations

import argparse
import json
import math
import struct
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional, Sequence, Tuple


HOSTS_PER_TOR = 16
TRACE_PATTERNS = ("overall", "pipeline", "pipeline_hotspot_touch", "pipeline_non_hotspot", "alltoall")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize outputs from the 2-ToR PFC hotspot experiment.")
    parser.add_argument("--prefix", default="2tor_pfc_hotspot", help="Experiment file prefix under mix/.")
    parser.add_argument(
        "--baseline-prefix",
        default=None,
        help="Optional baseline prefix for comparison. Same-window trace comparison uses the primary run's PFC window.",
    )
    return parser.parse_args()


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


def ip_hex_to_node_id(token: str) -> int:
    if len(token) == 8 or any(c in "abcdefABCDEF" for c in token):
        return (int(token, 16) >> 8) & 0xFFFF
    return int(token)


def ip_int_to_node_id(value: int) -> int:
    return (value >> 8) & 0xFFFF


def build_lookup(flows: Sequence[dict]) -> Dict[Tuple[int, int, int, int], List[dict]]:
    lookup: Dict[Tuple[int, int, int, int], List[dict]] = {}
    for flow in flows:
        key = (flow["src"], flow["dst"], flow["dst_port"], flow["size_bytes"])
        lookup.setdefault(key, []).append(flow)
    return lookup


def build_trace_lookup(flows: Sequence[dict]) -> Dict[Tuple[int, int, int], dict]:
    lookup: Dict[Tuple[int, int, int], dict] = {}
    for flow in flows:
        lookup[(flow["src"], flow["dst"], flow["dst_port"])] = flow
    return lookup


TRACE_RECORD_STRUCT = struct.Struct("<QHBBIIIHBBBB2x24s")
TRACE_DATA_STRUCT = struct.Struct("<HHIQHH4x")


def rollup_trace_counters(
    recv_bytes: Dict[str, int],
    recv_packets: Dict[str, int],
    start_ns: int,
    end_ns: int,
    unknown_packets: int,
    trace_diag: Dict[str, int],
    link_bottleneck_summary: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    duration_ns = max(end_ns - start_ns, 0)
    patterns = {}
    for pattern, total_bytes in recv_bytes.items():
        patterns[pattern] = {
            "recv_bytes": total_bytes,
            "recv_packets": recv_packets[pattern],
            "throughput_gbps": total_bytes * 8.0 / max(duration_ns, 1),
        }
    return {
        "start_ns": start_ns,
        "end_ns": end_ns,
        "duration_us": duration_ns / 1e3,
        "unknown_packets": unknown_packets,
        "trace_diag": trace_diag,
        "patterns": patterns,
        "link_bottleneck": link_bottleneck_summary,
    }


def init_trace_pattern_bytes() -> Dict[str, int]:
    return {key: 0 for key in TRACE_PATTERNS}


def init_link_counters() -> Dict[str, dict]:
    return {
        pattern: {
            "host_rx_bytes": {},
            "host_tx_bytes": {},
            "tor_0_to_1_bytes": 0,
            "tor_1_to_0_bytes": 0,
        }
        for pattern in TRACE_PATTERNS
    }


def update_link_counters(
    link_counters: Dict[str, dict],
    patterns: Sequence[str],
    src: int,
    dst: int,
    bytes_delivered: int,
) -> None:
    src_rack = src // HOSTS_PER_TOR
    dst_rack = dst // HOSTS_PER_TOR
    for pattern in patterns:
        counters = link_counters[pattern]
        counters["host_tx_bytes"][src] = counters["host_tx_bytes"].get(src, 0) + bytes_delivered
        counters["host_rx_bytes"][dst] = counters["host_rx_bytes"].get(dst, 0) + bytes_delivered
        if src_rack != dst_rack:
            if src_rack == 0:
                counters["tor_0_to_1_bytes"] += bytes_delivered
            else:
                counters["tor_1_to_0_bytes"] += bytes_delivered


def summarize_link_bottlenecks(
    link_counters: Dict[str, dict],
    duration_ns: int,
    host_link_rate_gbps: float,
    tor_link_rate_gbps: float,
) -> Dict[str, object]:
    duration_ns = max(duration_ns, 1)
    result: Dict[str, object] = {}

    def link_metric(
        gbps: float,
        rate_gbps: float,
        kind: str,
        ident: Optional[object],
    ) -> dict:
        return {
            "kind": kind,
            "id": ident,
            "gbps": gbps,
            "util_pct": (gbps * 100.0 / rate_gbps) if rate_gbps > 0 else None,
        }

    for pattern, counters in link_counters.items():
        max_host_rx_node, max_host_rx_bytes = max(
            counters["host_rx_bytes"].items(),
            key=lambda item: item[1],
            default=(None, 0),
        )
        max_host_tx_node, max_host_tx_bytes = max(
            counters["host_tx_bytes"].items(),
            key=lambda item: item[1],
            default=(None, 0),
        )
        max_host_rx_gbps = max_host_rx_bytes * 8.0 / duration_ns
        max_host_tx_gbps = max_host_tx_bytes * 8.0 / duration_ns
        tor_0_to_1_gbps = counters["tor_0_to_1_bytes"] * 8.0 / duration_ns
        tor_1_to_0_gbps = counters["tor_1_to_0_bytes"] * 8.0 / duration_ns

        candidates = [
            link_metric(max_host_rx_gbps, host_link_rate_gbps, "host_rx", max_host_rx_node),
            link_metric(max_host_tx_gbps, host_link_rate_gbps, "host_tx", max_host_tx_node),
            link_metric(tor_0_to_1_gbps, tor_link_rate_gbps, "tor_0_to_1", "32->33"),
            link_metric(tor_1_to_0_gbps, tor_link_rate_gbps, "tor_1_to_0", "33->32"),
        ]
        bottleneck = max(candidates, key=lambda item: item["gbps"])
        result[pattern] = {
            "max_host_rx_node": max_host_rx_node,
            "max_host_rx_gbps": max_host_rx_gbps,
            "max_host_rx_util_pct": (max_host_rx_gbps * 100.0 / host_link_rate_gbps)
            if host_link_rate_gbps > 0
            else None,
            "max_host_tx_node": max_host_tx_node,
            "max_host_tx_gbps": max_host_tx_gbps,
            "max_host_tx_util_pct": (max_host_tx_gbps * 100.0 / host_link_rate_gbps)
            if host_link_rate_gbps > 0
            else None,
            "tor_0_to_1_gbps": tor_0_to_1_gbps,
            "tor_0_to_1_util_pct": (tor_0_to_1_gbps * 100.0 / tor_link_rate_gbps)
            if tor_link_rate_gbps > 0
            else None,
            "tor_1_to_0_gbps": tor_1_to_0_gbps,
            "tor_1_to_0_util_pct": (tor_1_to_0_gbps * 100.0 / tor_link_rate_gbps)
            if tor_link_rate_gbps > 0
            else None,
            "bottleneck": bottleneck,
        }

    return result


def summarize_fct(fct_path: Path, flows: Sequence[dict], hotspot_nodes: Sequence[int]) -> Dict[str, object]:
    lookup = build_lookup(flows)
    hotspot_set = set(hotspot_nodes)
    expected = {
        "overall": len(flows),
        "pipeline": 0,
        "pipeline_hotspot_touch": 0,
        "pipeline_non_hotspot": 0,
        "alltoall": 0,
    }
    for flow in flows:
        expected[flow["pattern"]] += 1
        if flow["pattern"] == "pipeline":
            if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                expected["pipeline_hotspot_touch"] += 1
            else:
                expected["pipeline_non_hotspot"] += 1

    by_pattern: Dict[str, List[dict]] = {
        "overall": [],
        "pipeline": [],
        "pipeline_hotspot_touch": [],
        "pipeline_non_hotspot": [],
        "alltoall": [],
    }
    unmatched = 0

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
            dst_port = int(parts[3])
            size_bytes = int(parts[4])
            start_ns = int(parts[5])
            fct_ns = int(parts[6])
            standalone_ns = int(parts[7])
            key = (src, dst, dst_port, size_bytes)

            flow = None
            candidates = lookup.get(key)
            if candidates:
                flow = candidates.pop(0)
            else:
                unmatched += 1

            record = {
                "size_bytes": size_bytes,
                "start_ns": start_ns,
                "end_ns": start_ns + fct_ns,
                "fct_ns": fct_ns,
                "standalone_ns": standalone_ns,
                "goodput_gbps": size_bytes * 8.0 / max(fct_ns, 1),
                "slowdown": fct_ns / max(standalone_ns, 1),
            }
            by_pattern["overall"].append(record)
            if flow is not None:
                by_pattern[flow["pattern"]].append(record)
                if flow["pattern"] == "pipeline":
                    if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                        by_pattern["pipeline_hotspot_touch"].append(record)
                    else:
                        by_pattern["pipeline_non_hotspot"].append(record)

    def rollup(records: Sequence[dict], pattern: str) -> Dict[str, object]:
        fcts_us = [row["fct_ns"] / 1e3 for row in records]
        slowdowns = [row["slowdown"] for row in records]
        per_flow_goodput = [row["goodput_gbps"] for row in records]
        total_bytes = sum(row["size_bytes"] for row in records)
        if records:
            active_ns = max(row["end_ns"] for row in records) - min(row["start_ns"] for row in records)
            aggregate_goodput = total_bytes * 8.0 / max(active_ns, 1)
        else:
            active_ns = None
            aggregate_goodput = None
        return {
            "expected_flows": expected[pattern],
            "completed_flows": len(records),
            "avg_fct_us": mean(fcts_us) if fcts_us else None,
            "p50_fct_us": percentile(fcts_us, 0.50),
            "p95_fct_us": percentile(fcts_us, 0.95),
            "p99_fct_us": percentile(fcts_us, 0.99),
            "avg_slowdown": mean(slowdowns) if slowdowns else None,
            "avg_per_flow_goodput_gbps": mean(per_flow_goodput) if per_flow_goodput else None,
            "aggregate_goodput_gbps": aggregate_goodput,
            "active_window_us": (active_ns / 1e3) if active_ns is not None else None,
        }

    return {
        "expected_flows": len(flows),
        "completed_flows": len(by_pattern["overall"]),
        "unmatched_rows": unmatched,
        "patterns": {
            "overall": rollup(by_pattern["overall"], "overall"),
            "pipeline": rollup(by_pattern["pipeline"], "pipeline"),
            "pipeline_hotspot_touch": rollup(by_pattern["pipeline_hotspot_touch"], "pipeline_hotspot_touch"),
            "pipeline_non_hotspot": rollup(by_pattern["pipeline_non_hotspot"], "pipeline_non_hotspot"),
            "alltoall": rollup(by_pattern["alltoall"], "alltoall"),
        },
    }


def summarize_pfc(pfc_path: Path) -> Dict[str, object]:
    active_pause: Dict[Tuple[int, int, int], int] = {}
    by_port: Dict[Tuple[int, int], dict] = {}
    by_pg: Dict[int, dict] = {}
    pause_events = 0
    resume_events = 0
    first_pause_ns = None
    last_resume_ns = None
    has_qindex = False

    with pfc_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = list(map(int, line.split()))
            if len(parts) < 5:
                continue
            time_ns, node_id, node_type, if_index, event_type = parts[:5]
            qindex = parts[5] if len(parts) >= 6 else -1
            has_qindex = has_qindex or qindex >= 0
            key = (node_id, if_index)
            active_key = (node_id, if_index, qindex)
            stats = by_port.setdefault(
                key,
                {
                    "node_id": node_id,
                    "node_type": node_type,
                    "if_index": if_index,
                    "pause_events": 0,
                    "resume_events": 0,
                    "paused_time_ns": 0,
                    "max_pause_ns": 0,
                },
            )
            pg_stats = by_pg.setdefault(
                qindex,
                {
                    "qindex": qindex,
                    "pause_events": 0,
                    "resume_events": 0,
                    "paused_time_ns": 0,
                    "max_pause_ns": 0,
                    "ports_with_pause": set(),
                },
            )
            if event_type == 1:
                pause_events += 1
                stats["pause_events"] += 1
                pg_stats["pause_events"] += 1
                pg_stats["ports_with_pause"].add(key)
                if first_pause_ns is None:
                    first_pause_ns = time_ns
                active_pause.setdefault(active_key, time_ns)
            else:
                resume_events += 1
                stats["resume_events"] += 1
                pg_stats["resume_events"] += 1
                last_resume_ns = time_ns
                start_ns = active_pause.pop(active_key, None)
                if start_ns is not None:
                    duration = time_ns - start_ns
                    stats["paused_time_ns"] += duration
                    stats["max_pause_ns"] = max(stats["max_pause_ns"], duration)
                    pg_stats["paused_time_ns"] += duration
                    pg_stats["max_pause_ns"] = max(pg_stats["max_pause_ns"], duration)

    top_ports = sorted(
        by_port.values(),
        key=lambda item: (item["pause_events"], item["paused_time_ns"], item["max_pause_ns"]),
        reverse=True,
    )[:5]
    by_pg_output = {
        str(qindex): {
            "qindex": qindex,
            "pause_events": item["pause_events"],
            "resume_events": item["resume_events"],
            "paused_time_ns": item["paused_time_ns"],
            "max_pause_ns": item["max_pause_ns"],
            "ports_with_pause": len(item["ports_with_pause"]),
        }
        for qindex, item in sorted(by_pg.items())
    }
    return {
        "pause_events": pause_events,
        "resume_events": resume_events,
        "ports_with_pause": sum(1 for item in by_port.values() if item["pause_events"] > 0),
        "has_qindex": has_qindex,
        "by_pg": by_pg_output,
        "first_pause_ns": first_pause_ns,
        "last_resume_ns": last_resume_ns,
        "active_window_ns": (last_resume_ns - first_pause_ns) if first_pause_ns is not None and last_resume_ns is not None else None,
        "top_paused_ports": top_ports,
    }


def summarize_qlen(qlen_path: Path) -> Dict[str, object]:
    max_queue_kb = -1
    best_switch = None
    best_if = None
    best_time = None
    current_time = None
    sample_blocks = 0
    ports = []

    with qlen_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("time:"):
                current_time = int(line.split()[1])
                sample_blocks += 1
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            switch_id = int(parts[0])
            if_index = int(parts[1])
            histogram = [int(item) for item in parts[2:]]
            sample_count = sum(histogram)
            peak_queue_kb = -1
            for idx in range(len(histogram) - 1, -1, -1):
                if histogram[idx] > 0:
                    peak_queue_kb = idx
                    break
            avg_queue_kb = (
                sum(idx * count for idx, count in enumerate(histogram)) / sample_count
                if sample_count > 0
                else None
            )
            port_stats = {
                "switch_id": switch_id,
                "if_index": if_index,
                "sample_time_ns": current_time,
                "sample_count": sample_count,
                "peak_queue_kb": None if peak_queue_kb < 0 else peak_queue_kb,
                "avg_queue_kb": avg_queue_kb,
                "nonzero_samples": sum(count for idx, count in enumerate(histogram) if idx > 0),
                "histogram": histogram,
            }
            ports.append(port_stats)
            if peak_queue_kb > max_queue_kb:
                max_queue_kb = peak_queue_kb
                best_switch = switch_id
                best_if = if_index
                best_time = current_time

    busiest_peak = None
    busiest_avg = None
    if ports:
        busiest_peak = max(
            ports,
            key=lambda item: (
                item["peak_queue_kb"] if item["peak_queue_kb"] is not None else -1,
                item["avg_queue_kb"] if item["avg_queue_kb"] is not None else -1.0,
                item["nonzero_samples"],
            ),
        )
        busiest_avg = max(
            ports,
            key=lambda item: (
                item["avg_queue_kb"] if item["avg_queue_kb"] is not None else -1.0,
                item["peak_queue_kb"] if item["peak_queue_kb"] is not None else -1,
                item["nonzero_samples"],
            ),
        )

    return {
        "max_queue_kb": None if max_queue_kb < 0 else max_queue_kb,
        "switch_id": best_switch,
        "if_index": best_if,
        "sample_time_ns": best_time,
        "sample_blocks": sample_blocks,
        "ports_observed": len(ports),
        "busiest_peak_port": busiest_peak,
        "busiest_avg_port": busiest_avg,
    }


def summarize_trace_window(
    trace_path: Path,
    flows: Sequence[dict],
    hotspot_nodes: Sequence[int],
    start_ns: int,
    end_ns: int,
    host_link_rate_gbps: float,
    tor_link_rate_gbps: float,
) -> Dict[str, object]:
    if end_ns <= start_ns:
        return rollup_trace_counters(
            init_trace_pattern_bytes(),
            init_trace_pattern_bytes(),
            start_ns,
            end_ns,
            0,
            {
                "total_records": 0,
                "recv_records": 0,
                "udp_host_recv_records": 0,
                "udp_host_recv_records_in_window": 0,
                "matched_udp_host_recv_records_in_window": 0,
            },
            summarize_link_bottlenecks(init_link_counters(), end_ns - start_ns, host_link_rate_gbps, tor_link_rate_gbps),
        )

    lookup = build_trace_lookup(flows)
    hotspot_set = set(hotspot_nodes)
    recv_bytes = init_trace_pattern_bytes()
    recv_packets = init_trace_pattern_bytes()
    link_counters = init_link_counters()
    unknown_packets = 0
    total_records = 0
    recv_records = 0
    udp_host_recv_records = 0
    udp_host_recv_records_in_window = 0
    matched_udp_host_recv_records_in_window = 0

    with trace_path.open("rb") as handle:
        header = handle.read(4)
        if len(header) != 4:
            raise SystemExit(f"trace file too short: {trace_path}")
        (entry_count,) = struct.unpack("<I", header)
        handle.seek(entry_count * 11 + 4, 1)

        while True:
            raw = handle.read(TRACE_RECORD_STRUCT.size)
            if not raw:
                break
            if len(raw) != TRACE_RECORD_STRUCT.size:
                break
            total_records += 1
            (
                time_ns,
                node_id,
                if_index,
                qidx,
                qlen,
                sip,
                dip,
                size,
                l3_prot,
                event,
                ecn,
                node_type,
                extra,
            ) = TRACE_RECORD_STRUCT.unpack(raw)

            if event == 0:
                recv_records += 1
            if event == 0 and node_type == 0 and l3_prot == 0x11:
                udp_host_recv_records += 1
            if time_ns < start_ns or time_ns > end_ns:
                continue
            if event != 0 or node_type != 0 or l3_prot != 0x11:
                continue
            udp_host_recv_records_in_window += 1

            sport, dport, seq, ts, pg, payload = TRACE_DATA_STRUCT.unpack(extra)
            src = ip_int_to_node_id(sip)
            dst = ip_int_to_node_id(dip)
            flow = lookup.get((src, dst, dport))
            if flow is None:
                unknown_packets += 1
                continue
            matched_udp_host_recv_records_in_window += 1

            bytes_delivered = payload
            applied_patterns = ["overall", flow["pattern"]]
            recv_bytes["overall"] += bytes_delivered
            recv_packets["overall"] += 1
            recv_bytes[flow["pattern"]] += bytes_delivered
            recv_packets[flow["pattern"]] += 1
            if flow["pattern"] == "pipeline":
                if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                    recv_bytes["pipeline_hotspot_touch"] += bytes_delivered
                    recv_packets["pipeline_hotspot_touch"] += 1
                    applied_patterns.append("pipeline_hotspot_touch")
                else:
                    recv_bytes["pipeline_non_hotspot"] += bytes_delivered
                    recv_packets["pipeline_non_hotspot"] += 1
                    applied_patterns.append("pipeline_non_hotspot")
            update_link_counters(link_counters, applied_patterns, src, dst, bytes_delivered)

    return rollup_trace_counters(
        recv_bytes,
        recv_packets,
        start_ns,
        end_ns,
        unknown_packets,
        {
            "total_records": total_records,
            "recv_records": recv_records,
            "udp_host_recv_records": udp_host_recv_records,
            "udp_host_recv_records_in_window": udp_host_recv_records_in_window,
            "matched_udp_host_recv_records_in_window": matched_udp_host_recv_records_in_window,
        },
        summarize_link_bottlenecks(link_counters, end_ns - start_ns, host_link_rate_gbps, tor_link_rate_gbps),
    )


def summarize_trace_activity(
    trace_path: Path,
    flows: Sequence[dict],
    hotspot_nodes: Sequence[int],
    host_link_rate_gbps: float,
    tor_link_rate_gbps: float,
) -> Optional[Dict[str, object]]:
    lookup = build_trace_lookup(flows)
    hotspot_set = set(hotspot_nodes)
    recv_bytes = init_trace_pattern_bytes()
    recv_packets = init_trace_pattern_bytes()
    link_counters = init_link_counters()
    unknown_packets = 0
    total_records = 0
    recv_records = 0
    udp_host_recv_records = 0
    first_match_ns = None
    last_match_ns = None

    with trace_path.open("rb") as handle:
        header = handle.read(4)
        if len(header) != 4:
            raise SystemExit(f"trace file too short: {trace_path}")
        (entry_count,) = struct.unpack("<I", header)
        handle.seek(entry_count * 11 + 4, 1)

        while True:
            raw = handle.read(TRACE_RECORD_STRUCT.size)
            if not raw:
                break
            if len(raw) != TRACE_RECORD_STRUCT.size:
                break
            total_records += 1
            (
                time_ns,
                node_id,
                if_index,
                qidx,
                qlen,
                sip,
                dip,
                size,
                l3_prot,
                event,
                ecn,
                node_type,
                extra,
            ) = TRACE_RECORD_STRUCT.unpack(raw)

            if event == 0:
                recv_records += 1
            if event != 0 or node_type != 0 or l3_prot != 0x11:
                continue
            udp_host_recv_records += 1

            sport, dport, seq, ts, pg, payload = TRACE_DATA_STRUCT.unpack(extra)
            src = ip_int_to_node_id(sip)
            dst = ip_int_to_node_id(dip)
            flow = lookup.get((src, dst, dport))
            if flow is None:
                unknown_packets += 1
                continue

            first_match_ns = time_ns if first_match_ns is None else min(first_match_ns, time_ns)
            last_match_ns = time_ns if last_match_ns is None else max(last_match_ns, time_ns)
            bytes_delivered = payload
            applied_patterns = ["overall", flow["pattern"]]
            recv_bytes["overall"] += bytes_delivered
            recv_packets["overall"] += 1
            recv_bytes[flow["pattern"]] += bytes_delivered
            recv_packets[flow["pattern"]] += 1
            if flow["pattern"] == "pipeline":
                if flow["src"] in hotspot_set or flow["dst"] in hotspot_set:
                    recv_bytes["pipeline_hotspot_touch"] += bytes_delivered
                    recv_packets["pipeline_hotspot_touch"] += 1
                    applied_patterns.append("pipeline_hotspot_touch")
                else:
                    recv_bytes["pipeline_non_hotspot"] += bytes_delivered
                    recv_packets["pipeline_non_hotspot"] += 1
                    applied_patterns.append("pipeline_non_hotspot")
            update_link_counters(link_counters, applied_patterns, src, dst, bytes_delivered)

    if first_match_ns is None or last_match_ns is None:
        return None

    return rollup_trace_counters(
        recv_bytes,
        recv_packets,
        first_match_ns,
        last_match_ns,
        unknown_packets,
        {
            "total_records": total_records,
            "recv_records": recv_records,
            "udp_host_recv_records": udp_host_recv_records,
            "matched_udp_host_recv_records": recv_packets["overall"],
        },
        summarize_link_bottlenecks(
            link_counters,
            last_match_ns - first_match_ns,
            host_link_rate_gbps,
            tor_link_rate_gbps,
        ),
    )


def fmt(value: Optional[float], spec: str) -> str:
    if value is None:
        return "n/a"
    return format(value, spec)


def describe_bottleneck(link_bottleneck: Optional[Dict[str, object]], pattern: str) -> str:
    if not link_bottleneck:
        return "n/a"
    item = link_bottleneck.get(pattern, {})
    bottleneck = item.get("bottleneck", {})
    gbps = bottleneck.get("gbps")
    util_pct = bottleneck.get("util_pct")
    kind = bottleneck.get("kind")
    ident = bottleneck.get("id")
    if gbps is None or util_pct is None or kind is None:
        return "n/a"
    suffix = f"@{ident}" if ident is not None else ""
    return f"{kind}{suffix}:{gbps:.2f}Gbps({util_pct:.2f}%)"


def pct_change(new_value: Optional[float], old_value: Optional[float]) -> Optional[float]:
    if new_value is None or old_value is None or old_value == 0:
        return None
    return (new_value - old_value) * 100.0 / old_value


def load_manifest(mix_dir: Path, prefix: str) -> dict:
    manifest_path = mix_dir / f"manifest_{prefix}.json"
    if not manifest_path.exists():
        raise SystemExit(f"missing manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="ascii"))


def compare_pipeline(primary: dict, baseline: dict) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for pattern in ("pipeline", "pipeline_hotspot_touch", "pipeline_non_hotspot"):
        cur = primary["fct"]["patterns"].get(pattern, {})
        ref = baseline["fct"]["patterns"].get(pattern, {})
        result[pattern] = {
            "avg_fct_us_delta_pct": pct_change(cur.get("avg_fct_us"), ref.get("avg_fct_us")),
            "p95_fct_us_delta_pct": pct_change(cur.get("p95_fct_us"), ref.get("p95_fct_us")),
            "avg_slowdown_delta_pct": pct_change(cur.get("avg_slowdown"), ref.get("avg_slowdown")),
            "aggregate_goodput_gbps_delta_pct": pct_change(
                cur.get("aggregate_goodput_gbps"), ref.get("aggregate_goodput_gbps")
            ),
        }
    return result


def compare_trace_window(primary: dict, baseline: dict) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for pattern in TRACE_PATTERNS:
        cur = primary.get("patterns", {}).get(pattern, {})
        ref = baseline.get("patterns", {}).get(pattern, {})
        result[pattern] = {
            "throughput_gbps_delta_pct": pct_change(cur.get("throughput_gbps"), ref.get("throughput_gbps")),
            "recv_bytes_delta_pct": pct_change(cur.get("recv_bytes"), ref.get("recv_bytes")),
            "recv_packets_delta_pct": pct_change(cur.get("recv_packets"), ref.get("recv_packets")),
        }
    return result


def compare_link_bottleneck_window(primary: dict, baseline: dict) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for pattern in TRACE_PATTERNS:
        cur = primary.get("link_bottleneck", {}).get(pattern, {})
        ref = baseline.get("link_bottleneck", {}).get(pattern, {})
        cur_b = cur.get("bottleneck", {})
        ref_b = ref.get("bottleneck", {})
        result[pattern] = {
            "bottleneck_gbps_delta_pct": pct_change(cur_b.get("gbps"), ref_b.get("gbps")),
            "bottleneck_util_pct_delta_pct": pct_change(cur_b.get("util_pct"), ref_b.get("util_pct")),
            "primary_desc": describe_bottleneck(primary.get("link_bottleneck"), pattern),
            "baseline_desc": describe_bottleneck(baseline.get("link_bottleneck"), pattern),
        }
    return result


def compare_pipeline_expectation(measured: dict, expected: dict) -> Dict[str, Optional[float]]:
    return {
        "avg_fct_us_delta_pct": pct_change(measured.get("avg_fct_us"), expected.get("ideal_round_duration_us")),
        "avg_per_flow_goodput_gbps_delta_pct": pct_change(
            measured.get("avg_per_flow_goodput_gbps"),
            expected.get("per_flow_bottleneck_gbps"),
        ),
        "aggregate_goodput_gbps_delta_pct": pct_change(
            measured.get("aggregate_goodput_gbps"),
            expected.get("aggregate_bottleneck_gbps"),
        ),
    }


def compare_trace_active_expectation(trace_active: dict, expected: dict) -> Dict[str, Optional[float]]:
    pipeline = trace_active.get("patterns", {}).get("pipeline", {})
    overall = trace_active.get("patterns", {}).get("overall", {})
    return {
        "pipeline_throughput_gbps_delta_pct": pct_change(
            pipeline.get("throughput_gbps"),
            expected.get("aggregate_bottleneck_gbps"),
        ),
        "pipeline_per_flow_goodput_gbps_delta_pct": pct_change(
            (
                pipeline.get("throughput_gbps") / expected.get("concurrent_flows_per_round")
                if pipeline.get("throughput_gbps") is not None and expected.get("concurrent_flows_per_round")
                else None
            ),
            expected.get("per_flow_bottleneck_gbps"),
        ),
        "overall_throughput_gbps_delta_pct": pct_change(
            overall.get("throughput_gbps"),
            expected.get("aggregate_bottleneck_gbps"),
        ),
    }


def make_table_row(
    fct_patterns: Dict[str, dict],
    trace_active: Optional[dict],
    pattern: str,
) -> Dict[str, object]:
    fct_metrics = fct_patterns.get(pattern, {})
    trace_metrics = (trace_active or {}).get("patterns", {}).get(pattern, {})
    bottleneck = ((trace_active or {}).get("link_bottleneck", {}).get(pattern, {}) or {}).get("bottleneck", {})
    return {
        "completed_flows": fct_metrics.get("completed_flows"),
        "expected_flows": fct_metrics.get("expected_flows"),
        "avg_fct_us": fct_metrics.get("avg_fct_us"),
        "p95_fct_us": fct_metrics.get("p95_fct_us"),
        "aggregate_goodput_gbps": fct_metrics.get("aggregate_goodput_gbps"),
        "cluster_throughput_gbps": trace_metrics.get("throughput_gbps"),
        "busiest_link_kind": bottleneck.get("kind"),
        "busiest_link_id": bottleneck.get("id"),
        "busiest_link_gbps": bottleneck.get("gbps"),
        "busiest_link_util_pct": bottleneck.get("util_pct"),
    }


def build_table_metrics(summary: dict) -> Dict[str, dict]:
    trace_active = summary.get("trace_active")
    fct_patterns = summary["fct"]["patterns"]
    return {
        pattern: make_table_row(fct_patterns, trace_active, pattern)
        for pattern in TRACE_PATTERNS
    }


def format_table_metric_row(pattern: str, row: dict) -> str:
    if row.get("busiest_link_kind") is None:
        busiest_link = "n/a"
    else:
        ident = row.get("busiest_link_id")
        suffix = f"@{ident}" if ident is not None else ""
        busiest_link = (
            f"{row['busiest_link_kind']}{suffix}:"
            f"{fmt(row.get('busiest_link_gbps'), '.2f')}Gbps"
            f"({fmt(row.get('busiest_link_util_pct'), '.2f')}%)"
        )
    return (
        f"table_metric_{pattern}: "
        f"completed={row.get('completed_flows')}/{row.get('expected_flows')} "
        f"avg_fct_us={fmt(row.get('avg_fct_us'), '.2f')} "
        f"p95_fct_us={fmt(row.get('p95_fct_us'), '.2f')} "
        f"aggregate_goodput_gbps={fmt(row.get('aggregate_goodput_gbps'), '.2f')} "
        f"cluster_throughput_gbps={fmt(row.get('cluster_throughput_gbps'), '.2f')} "
        f"busiest_link={busiest_link}"
    )


def format_queue_port(prefix: str, port: Optional[dict]) -> str:
    if not port:
        return f"{prefix}: n/a"
    return (
        f"{prefix}: "
        f"switch={port['switch_id']} "
        f"if={port['if_index']} "
        f"peak_queue_kb={fmt(port.get('peak_queue_kb'), '.0f')} "
        f"avg_queue_kb={fmt(port.get('avg_queue_kb'), '.2f')} "
        f"nonzero_samples={port.get('nonzero_samples')} "
        f"sample_count={port.get('sample_count')} "
        f"sample_time_ns={port.get('sample_time_ns')}"
    )


def summarize_trigger_timing(pfc_summary: dict, scenario: dict) -> Optional[Dict[str, object]]:
    first_pause_ns = pfc_summary.get("first_pause_ns")
    if first_pause_ns is None:
        return None

    timeline = scenario.get("timeline", {})
    pipeline_timing = timeline.get("pipeline", {})
    alltoall_timing = timeline.get("alltoall", {})
    pipeline_first_start_ns = pipeline_timing.get("first_start_ns")
    alltoall_first_start_ns = alltoall_timing.get("first_start_ns")

    delta_vs_pipeline_us = (
        (first_pause_ns - pipeline_first_start_ns) / 1e3
        if pipeline_first_start_ns is not None
        else None
    )
    delta_vs_alltoall_us = (
        (first_pause_ns - alltoall_first_start_ns) / 1e3
        if alltoall_first_start_ns is not None
        else None
    )

    if alltoall_first_start_ns is None:
        relation_to_alltoall = "not_applicable"
    elif first_pause_ns < alltoall_first_start_ns:
        relation_to_alltoall = "before_alltoall_start"
    else:
        relation_to_alltoall = "after_alltoall_start"

    return {
        "first_pause_ns": first_pause_ns,
        "pipeline_first_start_ns": pipeline_first_start_ns,
        "alltoall_first_start_ns": alltoall_first_start_ns,
        "delta_vs_pipeline_us": delta_vs_pipeline_us,
        "delta_vs_alltoall_us": delta_vs_alltoall_us,
        "relation_to_alltoall": relation_to_alltoall,
    }


def main() -> int:
    args = parse_args()
    mix_dir = Path(__file__).resolve().parent

    summary_txt_path = mix_dir / f"summary_{args.prefix}.txt"
    summary_json_path = mix_dir / f"summary_{args.prefix}.json"

    manifest = load_manifest(mix_dir, args.prefix)
    flows = manifest["flows"]
    files = manifest["files"]
    hotspot_nodes = manifest["scenario"].get("alltoall_hotspot_nodes", [])
    host_link_rate_gbps = float(manifest["scenario"].get("host_link_rate_gbps", manifest["scenario"].get("min_link_rate_gbps", 0.0)))
    tor_link_rate_gbps = float(manifest["scenario"].get("tor_link_rate_gbps", manifest["scenario"].get("min_link_rate_gbps", 0.0)))

    fct_path = mix_dir / Path(files["fct"]).name
    pfc_path = mix_dir / Path(files["pfc"]).name
    qlen_path = mix_dir / Path(files["qlen"]).name
    trace_path = mix_dir / Path(files["trace_output"]).name if "trace_output" in files else None

    for path in (fct_path, pfc_path, qlen_path):
        if not path.exists():
            raise SystemExit(f"missing simulator output: {path}")

    fct_summary = summarize_fct(fct_path, flows, hotspot_nodes)
    pfc_summary = summarize_pfc(pfc_path)
    qlen_summary = summarize_qlen(qlen_path)

    summary = {
        "prefix": args.prefix,
        "scenario": manifest["scenario"],
        "fct": fct_summary,
        "pfc": pfc_summary,
        "qlen": qlen_summary,
    }
    trigger_timing = summarize_trigger_timing(pfc_summary, manifest["scenario"])
    if trigger_timing is not None:
        summary["trigger_timing"] = trigger_timing
    pipeline_expected = manifest["scenario"].get("pipeline_expected")
    if pipeline_expected is not None:
        summary["pipeline_expected"] = pipeline_expected
        summary["pipeline_vs_expected"] = compare_pipeline_expectation(
            fct_summary["patterns"]["pipeline"],
            pipeline_expected,
        )
    trace_active_summary = None
    if trace_path is not None and trace_path.exists():
        trace_active_summary = summarize_trace_activity(
            trace_path,
            flows,
            hotspot_nodes,
            host_link_rate_gbps,
            tor_link_rate_gbps,
        )
        if trace_active_summary is not None:
            summary["trace_active"] = trace_active_summary
            if pipeline_expected is not None:
                summary["trace_active_pipeline_vs_expected"] = compare_trace_active_expectation(
                    trace_active_summary,
                    pipeline_expected,
                )
    summary["table_metrics"] = build_table_metrics(summary)
    trace_window_summary = None
    if (
        trace_path is not None
        and trace_path.exists()
        and pfc_summary["first_pause_ns"] is not None
        and pfc_summary["last_resume_ns"] is not None
    ):
        trace_window_summary = summarize_trace_window(
            trace_path,
            flows,
            hotspot_nodes,
            pfc_summary["first_pause_ns"],
            pfc_summary["last_resume_ns"],
            host_link_rate_gbps,
            tor_link_rate_gbps,
        )
        summary["pfc_window_trace"] = trace_window_summary

    if args.baseline_prefix:
        baseline_manifest = load_manifest(mix_dir, args.baseline_prefix)
        baseline_files = baseline_manifest["files"]
        baseline_fct_path = mix_dir / Path(baseline_files["fct"]).name
        if not baseline_fct_path.exists():
            raise SystemExit(f"missing baseline simulator output: {baseline_fct_path}")
        baseline_summary = {
            "prefix": args.baseline_prefix,
            "scenario": baseline_manifest["scenario"],
            "fct": summarize_fct(
                baseline_fct_path,
                baseline_manifest["flows"],
                baseline_manifest["scenario"].get("alltoall_hotspot_nodes", []),
            ),
        }
        baseline_trace_path = (
            mix_dir / Path(baseline_files["trace_output"]).name
            if "trace_output" in baseline_files
            else None
        )
        baseline_trace_active_summary = None
        if baseline_trace_path is not None and baseline_trace_path.exists():
            baseline_trace_active_summary = summarize_trace_activity(
                baseline_trace_path,
                baseline_manifest["flows"],
                baseline_manifest["scenario"].get("alltoall_hotspot_nodes", []),
                float(
                    baseline_manifest["scenario"].get(
                        "host_link_rate_gbps",
                        baseline_manifest["scenario"].get("min_link_rate_gbps", 0.0),
                    )
                ),
                float(
                    baseline_manifest["scenario"].get(
                        "tor_link_rate_gbps",
                        baseline_manifest["scenario"].get("min_link_rate_gbps", 0.0),
                    )
                ),
            )
            if baseline_trace_active_summary is not None:
                baseline_summary["trace_active"] = baseline_trace_active_summary
        baseline_summary["table_metrics"] = build_table_metrics(baseline_summary)
        baseline_trace_window_summary = None
        if (
            trace_window_summary is not None
            and baseline_trace_path is not None
            and baseline_trace_path.exists()
        ):
            baseline_trace_window_summary = summarize_trace_window(
                baseline_trace_path,
                baseline_manifest["flows"],
                baseline_manifest["scenario"].get("alltoall_hotspot_nodes", []),
                trace_window_summary["start_ns"],
                trace_window_summary["end_ns"],
                float(
                    baseline_manifest["scenario"].get(
                        "host_link_rate_gbps",
                        baseline_manifest["scenario"].get("min_link_rate_gbps", 0.0),
                    )
                ),
                float(
                    baseline_manifest["scenario"].get(
                        "tor_link_rate_gbps",
                        baseline_manifest["scenario"].get("min_link_rate_gbps", 0.0),
                    )
                ),
            )
            baseline_summary["same_window_trace"] = baseline_trace_window_summary
        summary["baseline"] = baseline_summary
        summary["pipeline_impact_vs_baseline"] = compare_pipeline(summary, baseline_summary)
        if trace_active_summary is not None and baseline_trace_active_summary is not None:
            summary["trace_active_cluster_throughput_impact_vs_baseline"] = compare_trace_window(
                trace_active_summary,
                baseline_trace_active_summary,
            )
            summary["trace_active_link_bottleneck_impact_vs_baseline"] = compare_link_bottleneck_window(
                trace_active_summary,
                baseline_trace_active_summary,
            )
        if trace_window_summary is not None and baseline_trace_window_summary is not None:
            summary["pfc_window_throughput_impact_vs_baseline"] = compare_trace_window(
                trace_window_summary,
                baseline_trace_window_summary,
            )
            summary["pfc_window_link_bottleneck_impact_vs_baseline"] = compare_link_bottleneck_window(
                trace_window_summary,
                baseline_trace_window_summary,
            )
    summary_json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="ascii")

    overall = fct_summary["patterns"]["overall"]
    pipeline = fct_summary["patterns"]["pipeline"]
    pipeline_hotspot = fct_summary["patterns"]["pipeline_hotspot_touch"]
    pipeline_non_hotspot = fct_summary["patterns"]["pipeline_non_hotspot"]
    alltoall = fct_summary["patterns"]["alltoall"]
    lines = [
        f"Experiment: {args.prefix}",
        f"alltoall_hotspot_nodes: {' '.join(map(str, hotspot_nodes)) if hotspot_nodes else 'none'}",
        (
            "flow_queues: "
            f"pipeline_pg={manifest['scenario'].get('pipeline_pg', 'n/a')} "
            f"alltoall_pg={manifest['scenario'].get('alltoall_pg', 'n/a')}"
        ),
        (
            "overall: "
            f"{overall['completed_flows']}/{overall['expected_flows']} completed, "
            f"avg_fct={fmt(overall['avg_fct_us'], '.2f')} us, "
            f"p95_fct={fmt(overall['p95_fct_us'], '.2f')} us, "
            f"aggregate_goodput={fmt(overall['aggregate_goodput_gbps'], '.2f')} Gbps"
        ),
        (
            "pipeline: "
            f"{pipeline['completed_flows']}/{pipeline['expected_flows']} completed, "
            f"avg_fct={fmt(pipeline['avg_fct_us'], '.2f')} us, "
            f"p95_fct={fmt(pipeline['p95_fct_us'], '.2f')} us, "
            f"aggregate_goodput={fmt(pipeline['aggregate_goodput_gbps'], '.2f')} Gbps"
        ),
        (
            "pipeline_hotspot_touch: "
            f"{pipeline_hotspot['completed_flows']}/{pipeline_hotspot['expected_flows']} completed, "
            f"avg_fct={fmt(pipeline_hotspot['avg_fct_us'], '.2f')} us, "
            f"p95_fct={fmt(pipeline_hotspot['p95_fct_us'], '.2f')} us, "
            f"aggregate_goodput={fmt(pipeline_hotspot['aggregate_goodput_gbps'], '.2f')} Gbps"
        ),
        (
            "pipeline_non_hotspot: "
            f"{pipeline_non_hotspot['completed_flows']}/{pipeline_non_hotspot['expected_flows']} completed, "
            f"avg_fct={fmt(pipeline_non_hotspot['avg_fct_us'], '.2f')} us, "
            f"p95_fct={fmt(pipeline_non_hotspot['p95_fct_us'], '.2f')} us, "
            f"aggregate_goodput={fmt(pipeline_non_hotspot['aggregate_goodput_gbps'], '.2f')} Gbps"
        ),
        (
            "alltoall: "
            f"{alltoall['completed_flows']}/{alltoall['expected_flows']} completed, "
            f"avg_fct={fmt(alltoall['avg_fct_us'], '.2f')} us, "
            f"p95_fct={fmt(alltoall['p95_fct_us'], '.2f')} us, "
            f"aggregate_goodput={fmt(alltoall['aggregate_goodput_gbps'], '.2f')} Gbps"
        ),
        (
            "pfc: "
            f"pause_events={pfc_summary['pause_events']}, "
            f"resume_events={pfc_summary['resume_events']}, "
            f"ports_with_pause={pfc_summary['ports_with_pause']}"
        ),
    ]
    if pfc_summary.get("has_qindex"):
        by_pg = pfc_summary.get("by_pg", {})
        pg_parts = []
        for qindex, metrics in sorted(by_pg.items(), key=lambda item: int(item[0])):
            if metrics["pause_events"] > 0:
                pg_parts.append(
                    f"pg{qindex}={metrics['pause_events']} pauses/{metrics['ports_with_pause']} ports"
                )
        lines.append("pfc_by_pg: " + (" ".join(pg_parts) if pg_parts else "none"))
    if pipeline_expected is not None:
        lines.append(
            "pipeline_expected: "
            f"concurrent_flows_per_round={pipeline_expected['concurrent_flows_per_round']} "
            f"cross_tor_flows_per_round={pipeline_expected['cross_tor_flows_per_round']} "
            f"aggregate_bottleneck_gbps={fmt(pipeline_expected['aggregate_bottleneck_gbps'], '.2f')} "
            f"per_flow_bottleneck_gbps={fmt(pipeline_expected['per_flow_bottleneck_gbps'], '.2f')} "
            f"critical_tor_serialize_us={fmt(pipeline_expected['critical_tor_serialize_us'], '.2f')} "
            f"critical_host_serialize_us={fmt(pipeline_expected['critical_host_serialize_us'], '.2f')} "
            f"critical_serialize_us={fmt(pipeline_expected['critical_serialize_us'], '.2f')} "
            f"one_way_path_delay_us={fmt(pipeline_expected['one_way_path_delay_us'], '.2f')} "
            f"ideal_round_duration_us={fmt(pipeline_expected['ideal_round_duration_us'], '.2f')} "
            f"configured_gap_us={fmt(pipeline_expected['configured_gap_us'], '.2f')} "
            f"rounds_overlap_expected={'yes' if pipeline_expected['rounds_overlap_expected'] else 'no'}"
        )
        if "pipeline_vs_expected" in summary:
            metrics = summary["pipeline_vs_expected"]
            lines.append(
                "pipeline_vs_expected: "
                f"avg_fct_pct={fmt(metrics['avg_fct_us_delta_pct'], '.2f')} "
                f"avg_per_flow_goodput_pct={fmt(metrics['avg_per_flow_goodput_gbps_delta_pct'], '.2f')} "
                f"aggregate_goodput_pct={fmt(metrics['aggregate_goodput_gbps_delta_pct'], '.2f')}"
            )
    if trigger_timing is not None:
        lines.append(
            "trigger_timing: "
            f"first_pause_ns={trigger_timing['first_pause_ns']} "
            f"pipeline_first_start_ns={trigger_timing['pipeline_first_start_ns']} "
            f"alltoall_first_start_ns={trigger_timing['alltoall_first_start_ns']} "
            f"delta_vs_pipeline_us={fmt(trigger_timing['delta_vs_pipeline_us'], '.2f')} "
            f"delta_vs_alltoall_us={fmt(trigger_timing['delta_vs_alltoall_us'], '.2f')} "
            f"relation_to_alltoall={trigger_timing['relation_to_alltoall']}"
        )
    if trace_active_summary is not None:
        ta_overall = trace_active_summary["patterns"]["overall"]
        ta_pipeline = trace_active_summary["patterns"]["pipeline"]
        ta_pipeline_hotspot = trace_active_summary["patterns"]["pipeline_hotspot_touch"]
        ta_pipeline_non_hotspot = trace_active_summary["patterns"]["pipeline_non_hotspot"]
        ta_alltoall = trace_active_summary["patterns"]["alltoall"]
        ta_diag = trace_active_summary["trace_diag"]
        lines.extend(
            [
                (
                    "trace_active_window: "
                    f"start_ns={trace_active_summary['start_ns']} "
                    f"end_ns={trace_active_summary['end_ns']} "
                    f"duration_us={trace_active_summary['duration_us']:.2f} "
                    f"unknown_packets={trace_active_summary['unknown_packets']}"
                ),
                (
                    "trace_active_cluster_throughput: "
                    f"overall={fmt(ta_overall['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline={fmt(ta_pipeline['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline_hotspot_touch={fmt(ta_pipeline_hotspot['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline_non_hotspot={fmt(ta_pipeline_non_hotspot['throughput_gbps'], '.2f')} Gbps "
                    f"alltoall={fmt(ta_alltoall['throughput_gbps'], '.2f')} Gbps"
                ),
                (
                    "trace_active_link_bottleneck: "
                    f"overall={describe_bottleneck(trace_active_summary.get('link_bottleneck'), 'overall')} "
                    f"pipeline={describe_bottleneck(trace_active_summary.get('link_bottleneck'), 'pipeline')} "
                    f"pipeline_hotspot_touch={describe_bottleneck(trace_active_summary.get('link_bottleneck'), 'pipeline_hotspot_touch')} "
                    f"pipeline_non_hotspot={describe_bottleneck(trace_active_summary.get('link_bottleneck'), 'pipeline_non_hotspot')} "
                    f"alltoall={describe_bottleneck(trace_active_summary.get('link_bottleneck'), 'alltoall')}"
                ),
                (
                    "trace_active_bytes: "
                    f"overall={ta_overall['recv_bytes']} "
                    f"pipeline={ta_pipeline['recv_bytes']} "
                    f"pipeline_hotspot_touch={ta_pipeline_hotspot['recv_bytes']} "
                    f"pipeline_non_hotspot={ta_pipeline_non_hotspot['recv_bytes']} "
                    f"alltoall={ta_alltoall['recv_bytes']}"
                ),
                (
                    "trace_active_diag: "
                    f"total_records={ta_diag['total_records']} "
                    f"recv_records={ta_diag['recv_records']} "
                    f"udp_host_recv_records={ta_diag['udp_host_recv_records']} "
                    f"matched_udp_host_recv_records={ta_diag['matched_udp_host_recv_records']}"
                ),
            ]
        )
        if "trace_active_pipeline_vs_expected" in summary:
            metrics = summary["trace_active_pipeline_vs_expected"]
            lines.append(
                "trace_active_pipeline_vs_expected: "
                f"pipeline_throughput_pct={fmt(metrics['pipeline_throughput_gbps_delta_pct'], '.2f')} "
                f"pipeline_per_flow_goodput_pct={fmt(metrics['pipeline_per_flow_goodput_gbps_delta_pct'], '.2f')} "
                f"overall_throughput_pct={fmt(metrics['overall_throughput_gbps_delta_pct'], '.2f')}"
            )
    for pattern in TRACE_PATTERNS:
        lines.append(format_table_metric_row(pattern, summary["table_metrics"][pattern]))
    if trace_window_summary is not None:
        tw_overall = trace_window_summary["patterns"]["overall"]
        tw_pipeline = trace_window_summary["patterns"]["pipeline"]
        tw_pipeline_hotspot = trace_window_summary["patterns"]["pipeline_hotspot_touch"]
        tw_pipeline_non_hotspot = trace_window_summary["patterns"]["pipeline_non_hotspot"]
        tw_alltoall = trace_window_summary["patterns"]["alltoall"]
        tw_diag = trace_window_summary["trace_diag"]
        lines.extend(
            [
                (
                    "pfc_window: "
                    f"start_ns={trace_window_summary['start_ns']} "
                    f"end_ns={trace_window_summary['end_ns']} "
                    f"duration_us={trace_window_summary['duration_us']:.2f} "
                    f"unknown_packets={trace_window_summary['unknown_packets']}"
                ),
                (
                    "pfc_window_cluster_throughput: "
                    f"overall={fmt(tw_overall['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline={fmt(tw_pipeline['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline_hotspot_touch={fmt(tw_pipeline_hotspot['throughput_gbps'], '.2f')} Gbps "
                    f"pipeline_non_hotspot={fmt(tw_pipeline_non_hotspot['throughput_gbps'], '.2f')} Gbps "
                    f"alltoall={fmt(tw_alltoall['throughput_gbps'], '.2f')} Gbps"
                ),
                (
                    "pfc_window_link_bottleneck: "
                    f"overall={describe_bottleneck(trace_window_summary.get('link_bottleneck'), 'overall')} "
                    f"pipeline={describe_bottleneck(trace_window_summary.get('link_bottleneck'), 'pipeline')} "
                    f"pipeline_hotspot_touch={describe_bottleneck(trace_window_summary.get('link_bottleneck'), 'pipeline_hotspot_touch')} "
                    f"pipeline_non_hotspot={describe_bottleneck(trace_window_summary.get('link_bottleneck'), 'pipeline_non_hotspot')} "
                    f"alltoall={describe_bottleneck(trace_window_summary.get('link_bottleneck'), 'alltoall')}"
                ),
                (
                    "pfc_window_bytes: "
                    f"overall={tw_overall['recv_bytes']} "
                    f"pipeline={tw_pipeline['recv_bytes']} "
                    f"pipeline_hotspot_touch={tw_pipeline_hotspot['recv_bytes']} "
                    f"pipeline_non_hotspot={tw_pipeline_non_hotspot['recv_bytes']} "
                    f"alltoall={tw_alltoall['recv_bytes']}"
                ),
                (
                    "trace_diag: "
                    f"total_records={tw_diag['total_records']} "
                    f"recv_records={tw_diag['recv_records']} "
                    f"udp_host_recv_records={tw_diag['udp_host_recv_records']} "
                    f"udp_host_recv_records_in_window={tw_diag['udp_host_recv_records_in_window']} "
                    f"matched_udp_host_recv_records_in_window={tw_diag['matched_udp_host_recv_records_in_window']}"
                ),
            ]
        )
    for port in pfc_summary["top_paused_ports"]:
        lines.append(
            f"top_pfc_port: node={port['node_id']} if={port['if_index']} "
            f"pauses={port['pause_events']} paused_time_us={port['paused_time_ns'] / 1e3:.2f} "
            f"max_pause_us={port['max_pause_ns'] / 1e3:.2f}"
        )
    lines.append(
        "queue: "
        f"max_queue_kb={qlen_summary['max_queue_kb']} "
        f"switch={qlen_summary['switch_id']} if={qlen_summary['if_index']} "
        f"time_ns={qlen_summary['sample_time_ns']} "
        f"sample_blocks={qlen_summary['sample_blocks']} "
        f"ports_observed={qlen_summary['ports_observed']}"
    )
    lines.append(format_queue_port("queue_peak_port", qlen_summary.get("busiest_peak_port")))
    lines.append(format_queue_port("queue_avg_port", qlen_summary.get("busiest_avg_port")))
    if args.baseline_prefix and "pipeline_impact_vs_baseline" in summary:
        lines.append(f"baseline_prefix: {args.baseline_prefix}")
        for pattern, metrics in summary["pipeline_impact_vs_baseline"].items():
            lines.append(
                f"{pattern}_impact_vs_baseline: "
                f"avg_fct_pct={fmt(metrics['avg_fct_us_delta_pct'], '.2f')} "
                f"p95_fct_pct={fmt(metrics['p95_fct_us_delta_pct'], '.2f')} "
                f"avg_slowdown_pct={fmt(metrics['avg_slowdown_delta_pct'], '.2f')} "
                f"aggregate_goodput_pct={fmt(metrics['aggregate_goodput_gbps_delta_pct'], '.2f')}"
            )
        baseline_window = summary["baseline"].get("same_window_trace")
        if baseline_window is not None:
            bw_overall = baseline_window["patterns"]["overall"]
            bw_pipeline = baseline_window["patterns"]["pipeline"]
            bw_pipeline_hotspot = baseline_window["patterns"]["pipeline_hotspot_touch"]
            bw_pipeline_non_hotspot = baseline_window["patterns"]["pipeline_non_hotspot"]
            bw_alltoall = baseline_window["patterns"]["alltoall"]
            lines.append(
                "baseline_same_pfc_window_cluster_throughput: "
                f"overall={fmt(bw_overall['throughput_gbps'], '.2f')} Gbps "
                f"pipeline={fmt(bw_pipeline['throughput_gbps'], '.2f')} Gbps "
                f"pipeline_hotspot_touch={fmt(bw_pipeline_hotspot['throughput_gbps'], '.2f')} Gbps "
                f"pipeline_non_hotspot={fmt(bw_pipeline_non_hotspot['throughput_gbps'], '.2f')} Gbps "
                f"alltoall={fmt(bw_alltoall['throughput_gbps'], '.2f')} Gbps"
            )
            lines.append(
                "baseline_same_pfc_window_link_bottleneck: "
                f"overall={describe_bottleneck(baseline_window.get('link_bottleneck'), 'overall')} "
                f"pipeline={describe_bottleneck(baseline_window.get('link_bottleneck'), 'pipeline')} "
                f"pipeline_hotspot_touch={describe_bottleneck(baseline_window.get('link_bottleneck'), 'pipeline_hotspot_touch')} "
                f"pipeline_non_hotspot={describe_bottleneck(baseline_window.get('link_bottleneck'), 'pipeline_non_hotspot')} "
                f"alltoall={describe_bottleneck(baseline_window.get('link_bottleneck'), 'alltoall')}"
            )
        baseline_trace_active = summary["baseline"].get("trace_active")
        if baseline_trace_active is not None:
            bta_overall = baseline_trace_active["patterns"]["overall"]
            bta_pipeline = baseline_trace_active["patterns"]["pipeline"]
            bta_pipeline_hotspot = baseline_trace_active["patterns"]["pipeline_hotspot_touch"]
            bta_pipeline_non_hotspot = baseline_trace_active["patterns"]["pipeline_non_hotspot"]
            bta_alltoall = baseline_trace_active["patterns"]["alltoall"]
            lines.append(
                "baseline_trace_active_cluster_throughput: "
                f"overall={fmt(bta_overall['throughput_gbps'], '.2f')} Gbps "
                f"pipeline={fmt(bta_pipeline['throughput_gbps'], '.2f')} Gbps "
                f"pipeline_hotspot_touch={fmt(bta_pipeline_hotspot['throughput_gbps'], '.2f')} Gbps "
                f"pipeline_non_hotspot={fmt(bta_pipeline_non_hotspot['throughput_gbps'], '.2f')} Gbps "
                f"alltoall={fmt(bta_alltoall['throughput_gbps'], '.2f')} Gbps"
            )
            lines.append(
                "baseline_trace_active_link_bottleneck: "
                f"overall={describe_bottleneck(baseline_trace_active.get('link_bottleneck'), 'overall')} "
                f"pipeline={describe_bottleneck(baseline_trace_active.get('link_bottleneck'), 'pipeline')} "
                f"pipeline_hotspot_touch={describe_bottleneck(baseline_trace_active.get('link_bottleneck'), 'pipeline_hotspot_touch')} "
                f"pipeline_non_hotspot={describe_bottleneck(baseline_trace_active.get('link_bottleneck'), 'pipeline_non_hotspot')} "
                f"alltoall={describe_bottleneck(baseline_trace_active.get('link_bottleneck'), 'alltoall')}"
            )
        for pattern in TRACE_PATTERNS:
            lines.append(
                "baseline_" + format_table_metric_row(pattern, summary["baseline"]["table_metrics"][pattern])
            )
        if "trace_active_cluster_throughput_impact_vs_baseline" in summary:
            for pattern, metrics in summary["trace_active_cluster_throughput_impact_vs_baseline"].items():
                lines.append(
                    f"{pattern}_trace_active_cluster_throughput_impact_vs_baseline: "
                    f"throughput_pct={fmt(metrics['throughput_gbps_delta_pct'], '.2f')} "
                    f"recv_bytes_pct={fmt(metrics['recv_bytes_delta_pct'], '.2f')} "
                    f"recv_packets_pct={fmt(metrics['recv_packets_delta_pct'], '.2f')}"
                )
        if "trace_active_link_bottleneck_impact_vs_baseline" in summary:
            for pattern, metrics in summary["trace_active_link_bottleneck_impact_vs_baseline"].items():
                lines.append(
                    f"{pattern}_trace_active_link_bottleneck_impact_vs_baseline: "
                    f"gbps_pct={fmt(metrics['bottleneck_gbps_delta_pct'], '.2f')} "
                    f"util_pct={fmt(metrics['bottleneck_util_pct_delta_pct'], '.2f')} "
                    f"primary={metrics['primary_desc']} "
                    f"baseline={metrics['baseline_desc']}"
                )
        if "pfc_window_throughput_impact_vs_baseline" in summary:
            for pattern, metrics in summary["pfc_window_throughput_impact_vs_baseline"].items():
                lines.append(
                    f"{pattern}_pfc_window_cluster_throughput_impact_vs_baseline: "
                    f"throughput_pct={fmt(metrics['throughput_gbps_delta_pct'], '.2f')} "
                    f"recv_bytes_pct={fmt(metrics['recv_bytes_delta_pct'], '.2f')} "
                    f"recv_packets_pct={fmt(metrics['recv_packets_delta_pct'], '.2f')}"
                )
        if "pfc_window_link_bottleneck_impact_vs_baseline" in summary:
            for pattern, metrics in summary["pfc_window_link_bottleneck_impact_vs_baseline"].items():
                lines.append(
                    f"{pattern}_pfc_window_link_bottleneck_impact_vs_baseline: "
                    f"gbps_pct={fmt(metrics['bottleneck_gbps_delta_pct'], '.2f')} "
                    f"util_pct={fmt(metrics['bottleneck_util_pct_delta_pct'], '.2f')} "
                    f"primary={metrics['primary_desc']} "
                    f"baseline={metrics['baseline_desc']}"
                )
    summary_txt_path.write_text("\n".join(lines) + "\n", encoding="ascii")

    print(summary_txt_path.name)
    print(summary_json_path.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
