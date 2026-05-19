#!/usr/bin/env python3
"""
Sweep all-to-all rank counts and summarize PFC pause counts.

Typical usage from simulation/:
  python3 mix/sweep_alltoall_pause_counts.py --ranks 2,4,8,12,16

By default this generates a strong all-to-all-only burst on one ToR, runs
the mp-rdma simulator, and writes:
  mix/alltoall_pause_rank_sweep.csv
  mix/alltoall_pause_rank_sweep.json
  mix/alltoall_pause_rank_sweep.png
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from collect_2tor_pfc_hotspot_metrics import load_manifest, summarize_fct, summarize_pfc


DEFAULT_PREFIX_BASE = "2tor_alltoall_rank_sweep"


def parse_rank_list(spec: str) -> List[int]:
    ranks: List[int] = []
    for chunk in spec.split(","):
        item = chunk.strip()
        if not item:
            continue
        if "-" in item:
            start_s, end_s = item.split("-", 1)
            start = int(start_s)
            end = int(end_s)
            step = 1 if end >= start else -1
            ranks.extend(range(start, end + step, step))
        else:
            ranks.append(int(item))
    deduped: List[int] = []
    seen = set()
    for rank in ranks:
        if rank not in seen:
            deduped.append(rank)
            seen.add(rank)
    return deduped


def parse_float_list(spec: str) -> List[float]:
    values: List[float] = []
    for chunk in spec.split(","):
        item = chunk.strip()
        if item:
            values.append(float(item))
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep all-to-all rank count and collect PFC pause statistics.")
    parser.add_argument("--ranks", default="2,4,8,12,16", help="Comma-separated rank counts or ranges, e.g. 2,4,8,16.")
    parser.add_argument("--rank-base", type=int, default=16, help="First host id used for the all-to-all rank list.")
    parser.add_argument("--prefix-base", default=DEFAULT_PREFIX_BASE, help="Prefix base for generated mix files.")
    parser.add_argument("--skip-run", action="store_true", help="Only generate configs and collect existing outputs.")
    parser.add_argument("--skip-generate", action="store_true", help="Do not regenerate configs before running/collecting.")
    parser.add_argument("--no-plot", action="store_true", help="Do not write the PNG pause-count plot.")
    parser.add_argument("--with-pipeline", action="store_true", help="Keep the reduce-scatter pipeline background enabled.")
    parser.add_argument("--link-rate-gbps", type=float, default=100.0)
    parser.add_argument("--host-link-rate-gbps", type=float, default=None)
    parser.add_argument("--tor-link-rate-gbps", type=float, default=None)
    parser.add_argument("--link-delay-us", type=float, default=1.0)
    parser.add_argument("--buffer-size-mb", type=int, default=2)
    parser.add_argument("--pfc-xoff-bytes", type=int, default=4096)
    parser.add_argument("--pfc-xon-bytes", type=int, default=1024)
    parser.add_argument(
        "--fluid-threshold",
        choices=("xon", "xoff"),
        default="xon",
        help="Queue threshold used by the fluid-model detector and plot. Default uses PFC_XON for conservative early isolation.",
    )
    parser.add_argument("--alltoall-flow-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--alltoall-rounds", type=int, default=64)
    parser.add_argument("--alltoall-round-gap-us", type=float, default=1.0)
    parser.add_argument("--alltoall-base-us", type=float, default=1.0)
    parser.add_argument("--alltoall-src-stagger-us", type=float, default=0.0)
    parser.add_argument("--alltoall-pg", type=int, default=3)
    parser.add_argument("--pipeline-pg", type=int, default=3)
    parser.add_argument("--pipeline-flow-bytes", type=int, default=256 * 1024)
    parser.add_argument("--pipeline-rounds", type=int, default=8)
    parser.add_argument("--pipeline-gap-us", type=float, default=250.0)
    parser.add_argument("--pipeline-ring-layout", choices=("interleaved", "rack_contiguous"), default="rack_contiguous")
    parser.add_argument("--pipeline-nodes-per-rack", type=int, default=16)
    parser.add_argument("--output-csv", default="alltoall_pause_rank_sweep.csv")
    parser.add_argument("--output-json", default="alltoall_pause_rank_sweep.json")
    parser.add_argument("--output-png", default="alltoall_pause_rank_sweep.png")
    parser.add_argument(
        "--prediction-model",
        choices=("fluid_nstar", "burst_headroom", "incast_worst", "per_source_fair"),
        default="fluid_nstar",
        help=(
            "PFC predictor. fluid_nstar uses the paper-style RTT fluid model; "
            "burst_headroom estimates the synchronized packet burst at the receiver queue; "
            "incast_worst assumes each receiver can see simultaneous line-rate fan-in; "
            "per_source_fair assumes each source link is evenly shared across its all-to-all destinations."
        ),
    )
    parser.add_argument("--fluid-eta", type=float, default=1.0, help="Eta used for CSV/JSON fluid-model prediction.")
    parser.add_argument(
        "--fluid-etas",
        default="1.0,0.8,0.6,0.4",
        help="Comma-separated eta values to draw on the plot.",
    )
    parser.add_argument(
        "--fluid-rtt-us",
        type=float,
        default=4.0,
        help="Fluid-model control interval T in us. Default assumes two 1us-hop paths plus return path.",
    )
    parser.add_argument(
        "--fluid-sender-rate-gbps",
        type=float,
        default=None,
        help="Fluid-model sender rate R. Defaults to host link rate.",
    )
    parser.add_argument(
        "--fluid-receiver-capacity-gbps",
        type=float,
        default=None,
        help="Fluid-model receiver drain capacity C. Defaults to host link rate.",
    )
    parser.add_argument(
        "--prediction-packet-bytes",
        type=int,
        default=1000,
        help="Packet payload bytes used by the burst_headroom predictor.",
    )
    parser.add_argument(
        "--prediction-burst-packets",
        type=int,
        default=1,
        help="Synchronized packet burst depth per sender used by the burst_headroom predictor.",
    )
    return parser.parse_args()


def require_valid_ranks(ranks: Sequence[int], rank_base: int) -> None:
    if not ranks:
        raise SystemExit("no ranks requested")
    for rank_count in ranks:
        if rank_count < 2:
            raise SystemExit(f"rank count must be >= 2: {rank_count}")
        last_node = rank_base + rank_count - 1
        if rank_base < 0 or last_node >= 32:
            raise SystemExit(f"rank count {rank_count} with --rank-base {rank_base} exceeds host id range [0, 31]")
        if rank_base // 16 != last_node // 16:
            raise SystemExit(
                f"rank count {rank_count} crosses ToRs; choose a rank base/count contained in one 16-host ToR"
            )


def run_cmd(cmd: Sequence[str], cwd: Path) -> None:
    print("+ " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)


def maybe_float(value: Optional[float]) -> Optional[float]:
    return None if value is None else float(value)


def pg_stats(pfc_summary: Dict[str, object], pg: int) -> Dict[str, object]:
    by_pg = pfc_summary.get("by_pg", {})
    if not isinstance(by_pg, dict):
        return {}
    return by_pg.get(str(pg), {})


def predict_alltoall_pfc(
    *,
    rank_count: int,
    flow_bytes: int,
    round_gap_us: float,
    rounds: int,
    host_link_rate_gbps: float,
    pfc_threshold_bytes: int,
    pfc_threshold_name: str,
    model: str,
    packet_bytes: int,
    burst_packets: int,
    eta: float,
    rtt_us: float,
    sender_rate_gbps: Optional[float],
    receiver_capacity_gbps: Optional[float],
) -> Dict[str, object]:
    """Predict PFC with a one-bottleneck fluid queue model.

    The bottleneck is the receiver-facing host link in the all-to-all ToR.
    PFC/isolation is predicted when max receiver queue occupancy exceeds
    the selected fluid threshold, XON by default.
    """
    fan_in = max(rank_count - 1, 0)
    if fan_in == 0 or flow_bytes <= 0 or host_link_rate_gbps <= 0:
        return {
            "prediction_model": model,
            "predicted_pfc": False,
            "predicted_queue_bytes": 0.0,
            "predicted_queue_over_threshold_bytes": -float(pfc_threshold_bytes),
            "predicted_queue_over_xon_bytes": -float(pfc_threshold_bytes) if pfc_threshold_name == "xon" else None,
            "predicted_queue_over_xoff_bytes": -float(pfc_threshold_bytes) if pfc_threshold_name == "xoff" else None,
            "predicted_input_rate_gbps": 0.0,
            "predicted_drain_rate_gbps": host_link_rate_gbps,
            "predicted_burst_duration_us": 0.0,
            "predicted_round_drain_time_us": 0.0,
            "predicted_rounds_overlap": False,
            "predicted_packet_bytes": packet_bytes,
            "predicted_burst_packets": burst_packets,
        }

    drain_rate_gbps = host_link_rate_gbps
    if model == "fluid_nstar":
        effective_sender_rate_gbps = eta * (sender_rate_gbps if sender_rate_gbps is not None else host_link_rate_gbps)
        receiver_capacity = receiver_capacity_gbps if receiver_capacity_gbps is not None else host_link_rate_gbps
        excess_rate_gbps = max(fan_in * effective_sender_rate_gbps - receiver_capacity, 0.0)
        predicted_queue_bytes = excess_rate_gbps * 1e3 * max(rtt_us, 0.0) / 8.0
        sender_rate_bytes_per_s = effective_sender_rate_gbps * 1e9 / 8.0
        receiver_capacity_bytes_per_s = receiver_capacity * 1e9 / 8.0
        t_s = max(rtt_us, 0.0) * 1e-6
        if sender_rate_bytes_per_s > 0 and t_s > 0:
            nstar = int(
                pfc_threshold_bytes / (sender_rate_bytes_per_s * t_s)
                + receiver_capacity_bytes_per_s / sender_rate_bytes_per_s
            ) + 2
        else:
            nstar = None
        return {
            "prediction_model": model,
            "predicted_pfc": predicted_queue_bytes >= pfc_threshold_bytes,
            "predicted_queue_bytes": predicted_queue_bytes,
            "predicted_queue_over_threshold_bytes": predicted_queue_bytes - pfc_threshold_bytes,
            "predicted_queue_over_xon_bytes": predicted_queue_bytes - pfc_threshold_bytes
            if pfc_threshold_name == "xon"
            else None,
            "predicted_queue_over_xoff_bytes": predicted_queue_bytes - pfc_threshold_bytes
            if pfc_threshold_name == "xoff"
            else None,
            "predicted_input_rate_gbps": fan_in * effective_sender_rate_gbps,
            "predicted_drain_rate_gbps": receiver_capacity,
            "predicted_excess_rate_gbps": excess_rate_gbps,
            "predicted_burst_duration_us": rtt_us,
            "predicted_round_drain_time_us": predicted_queue_bytes * 8.0 / (receiver_capacity * 1e3) if receiver_capacity > 0 else None,
            "predicted_rounds_overlap": False,
            "predicted_packet_bytes": packet_bytes,
            "predicted_burst_packets": burst_packets,
            "fluid_eta": eta,
            "fluid_rtt_us": rtt_us,
            "fluid_sender_rate_gbps": effective_sender_rate_gbps,
            "fluid_receiver_capacity_gbps": receiver_capacity,
            "fluid_nstar": nstar,
            "fluid_threshold_name": pfc_threshold_name,
            "fluid_threshold_bytes": pfc_threshold_bytes,
        }

    if model == "burst_headroom":
        # One receiver output queue can drain one packet while synchronized fan-in
        # contributes roughly fan_in packets. This captures the PFC-triggering
        # queue headroom better than charging the whole flow into the fluid burst.
        input_rate_gbps = fan_in * host_link_rate_gbps
        burst_duration_us = max(packet_bytes, 0) * max(burst_packets, 0) * 8.0 / (host_link_rate_gbps * 1e3)
        queue_growth_bytes = max(fan_in - 1, 0) * max(packet_bytes, 0) * max(burst_packets, 0)
        drain_time_us = queue_growth_bytes * 8.0 / (drain_rate_gbps * 1e3) if drain_rate_gbps > 0 else None
        max_queue = float(queue_growth_bytes)
        return {
            "prediction_model": model,
            "predicted_pfc": max_queue >= pfc_threshold_bytes,
            "predicted_queue_bytes": max_queue,
            "predicted_queue_over_threshold_bytes": max_queue - pfc_threshold_bytes,
            "predicted_queue_over_xon_bytes": max_queue - pfc_threshold_bytes if pfc_threshold_name == "xon" else None,
            "predicted_queue_over_xoff_bytes": max_queue - pfc_threshold_bytes if pfc_threshold_name == "xoff" else None,
            "predicted_input_rate_gbps": input_rate_gbps,
            "predicted_drain_rate_gbps": drain_rate_gbps,
            "predicted_excess_rate_gbps": max(input_rate_gbps - drain_rate_gbps, 0.0),
            "predicted_burst_duration_us": burst_duration_us,
            "predicted_round_drain_time_us": drain_time_us,
            "predicted_rounds_overlap": drain_time_us is not None and drain_time_us > max(round_gap_us, 0.0),
            "predicted_packet_bytes": packet_bytes,
            "predicted_burst_packets": burst_packets,
            "fluid_eta": eta,
            "fluid_rtt_us": rtt_us,
            "fluid_sender_rate_gbps": sender_rate_gbps if sender_rate_gbps is not None else host_link_rate_gbps,
            "fluid_receiver_capacity_gbps": receiver_capacity_gbps if receiver_capacity_gbps is not None else host_link_rate_gbps,
            "fluid_nstar": None,
            "fluid_threshold_name": pfc_threshold_name,
            "fluid_threshold_bytes": pfc_threshold_bytes,
        }

    if model == "per_source_fair":
        # Each source NIC spreads its line rate evenly over all destinations.
        input_rate_gbps = host_link_rate_gbps
        burst_duration_us = flow_bytes * 8.0 * fan_in / (host_link_rate_gbps * 1e3)
    else:
        # Conservative synchronized incast model: all fan-in ports can feed one receiver at line rate.
        input_rate_gbps = fan_in * host_link_rate_gbps
        burst_duration_us = flow_bytes * 8.0 / (host_link_rate_gbps * 1e3)

    excess_rate_gbps = max(input_rate_gbps - drain_rate_gbps, 0.0)
    queue_growth_bytes = excess_rate_gbps * 1e3 * burst_duration_us / 8.0
    drain_time_us = queue_growth_bytes * 8.0 / (drain_rate_gbps * 1e3) if drain_rate_gbps > 0 else None

    # If rounds overlap, residual queue can accumulate. This is a bounded fluid estimate
    # over the configured number of rounds, not a packet-level simulator.
    residual_queue = 0.0
    max_queue = 0.0
    gap_us = max(round_gap_us, 0.0)
    for _ in range(max(rounds, 1)):
        residual_queue += queue_growth_bytes
        max_queue = max(max_queue, residual_queue)
        drained = drain_rate_gbps * 1e3 * gap_us / 8.0
        residual_queue = max(residual_queue - drained, 0.0)

    return {
        "prediction_model": model,
        "predicted_pfc": max_queue >= pfc_threshold_bytes,
        "predicted_queue_bytes": max_queue,
        "predicted_queue_over_threshold_bytes": max_queue - pfc_threshold_bytes,
        "predicted_queue_over_xon_bytes": max_queue - pfc_threshold_bytes if pfc_threshold_name == "xon" else None,
        "predicted_queue_over_xoff_bytes": max_queue - pfc_threshold_bytes if pfc_threshold_name == "xoff" else None,
        "predicted_input_rate_gbps": input_rate_gbps,
        "predicted_drain_rate_gbps": drain_rate_gbps,
        "predicted_excess_rate_gbps": excess_rate_gbps,
        "predicted_burst_duration_us": burst_duration_us,
        "predicted_round_drain_time_us": drain_time_us,
        "predicted_rounds_overlap": drain_time_us is not None and drain_time_us > gap_us,
        "predicted_packet_bytes": packet_bytes,
        "predicted_burst_packets": burst_packets,
        "fluid_eta": eta,
        "fluid_rtt_us": rtt_us,
        "fluid_sender_rate_gbps": sender_rate_gbps if sender_rate_gbps is not None else host_link_rate_gbps,
        "fluid_receiver_capacity_gbps": receiver_capacity_gbps if receiver_capacity_gbps is not None else host_link_rate_gbps,
        "fluid_nstar": None,
        "fluid_threshold_name": pfc_threshold_name,
        "fluid_threshold_bytes": pfc_threshold_bytes,
    }


def collect_row(
    mix_dir: Path,
    prefix: str,
    rank_count: int,
    nodes: Sequence[int],
    prediction_model: str,
    prediction_packet_bytes: int,
    prediction_burst_packets: int,
    fluid_eta: float,
    fluid_rtt_us: float,
    fluid_sender_rate_gbps: Optional[float],
    fluid_receiver_capacity_gbps: Optional[float],
    fluid_threshold: str,
) -> Dict[str, object]:
    manifest = load_manifest(mix_dir, prefix)
    scenario = manifest["scenario"]
    files = manifest["files"]
    flows = manifest["flows"]
    pfc_path = mix_dir / Path(files["pfc"]).name
    fct_path = mix_dir / Path(files["fct"]).name

    row: Dict[str, object] = {
        "prefix": prefix,
        "ranks": rank_count,
        "nodes": " ".join(str(node) for node in nodes),
        "alltoall_pg": scenario["alltoall_pg"],
        "pipeline_pg": scenario["pipeline_pg"],
        "pipeline_enabled": scenario.get("pipeline_enabled", True),
        "alltoall_flow_bytes": scenario["alltoall_flow_bytes"],
        "alltoall_rounds": scenario["alltoall_rounds"],
        "alltoall_round_gap_us": scenario["alltoall_round_gap_us"],
        "alltoall_flows": sum(1 for flow in flows if flow["pattern"] == "alltoall"),
        "alltoall_total_bytes": sum(flow["size_bytes"] for flow in flows if flow["pattern"] == "alltoall"),
        "status": "ok",
    }
    bytes_per_round = rank_count * max(rank_count - 1, 0) * int(scenario["alltoall_flow_bytes"])
    per_receiver_bytes_per_round = max(rank_count - 1, 0) * int(scenario["alltoall_flow_bytes"])
    gap_us = max(float(scenario["alltoall_round_gap_us"]), 1e-9)
    row.update(
        {
            "alltoall_bytes_per_round": bytes_per_round,
            "offered_alltoall_cluster_gbps": bytes_per_round * 8.0 / (gap_us * 1000.0),
            "offered_alltoall_per_receiver_gbps": per_receiver_bytes_per_round * 8.0 / (gap_us * 1000.0),
        }
    )
    pfc_threshold_name = fluid_threshold
    pfc_threshold_bytes = int(scenario["pfc_xon_bytes"] if fluid_threshold == "xon" else scenario["pfc_xoff_bytes"])
    prediction = predict_alltoall_pfc(
        rank_count=rank_count,
        flow_bytes=int(scenario["alltoall_flow_bytes"]),
        round_gap_us=float(scenario["alltoall_round_gap_us"]),
        rounds=int(scenario["alltoall_rounds"]),
        host_link_rate_gbps=float(scenario["host_link_rate_gbps"]),
        pfc_threshold_bytes=pfc_threshold_bytes,
        pfc_threshold_name=pfc_threshold_name,
        model=prediction_model,
        packet_bytes=prediction_packet_bytes,
        burst_packets=prediction_burst_packets,
        eta=fluid_eta,
        rtt_us=fluid_rtt_us,
        sender_rate_gbps=fluid_sender_rate_gbps,
        receiver_capacity_gbps=fluid_receiver_capacity_gbps,
    )
    row.update(prediction)

    if not pfc_path.exists():
        row["status"] = "missing_pfc"
        return row

    pfc_summary = summarize_pfc(pfc_path)
    alltoall_pg_stats = pg_stats(pfc_summary, int(scenario["alltoall_pg"]))
    pipeline_pg_stats = pg_stats(pfc_summary, int(scenario["pipeline_pg"]))
    row.update(
        {
            "pause_events": pfc_summary["pause_events"],
            "resume_events": pfc_summary["resume_events"],
            "ports_with_pause": pfc_summary["ports_with_pause"],
            "has_qindex": pfc_summary["has_qindex"],
            "first_pause_ns": pfc_summary["first_pause_ns"],
            "last_resume_ns": pfc_summary["last_resume_ns"],
            "active_window_us": maybe_float(pfc_summary["active_window_ns"]) / 1000.0
            if pfc_summary["active_window_ns"] is not None
            else None,
            "alltoall_pg_pause_events": alltoall_pg_stats.get("pause_events", 0),
            "alltoall_pg_resume_events": alltoall_pg_stats.get("resume_events", 0),
            "alltoall_pg_ports_with_pause": alltoall_pg_stats.get("ports_with_pause", 0),
            "alltoall_pg_paused_time_us": maybe_float(alltoall_pg_stats.get("paused_time_ns", 0)) / 1000.0,
            "pipeline_pg_pause_events": pipeline_pg_stats.get("pause_events", 0),
            "pfc_by_pg_json": json.dumps(pfc_summary.get("by_pg", {}), sort_keys=True),
            "prediction_correct": bool(prediction["predicted_pfc"]) == (pfc_summary["pause_events"] > 0),
        }
    )

    if fct_path.exists():
        hotspot_nodes = scenario.get("alltoall_hotspot_nodes", [])
        fct_summary = summarize_fct(fct_path, flows, hotspot_nodes)
        alltoall = fct_summary["patterns"]["alltoall"]
        overall = fct_summary["patterns"]["overall"]
        row.update(
            {
                "overall_completed": overall["completed_flows"],
                "overall_expected": overall["expected_flows"],
                "alltoall_completed": alltoall["completed_flows"],
                "alltoall_expected": alltoall["expected_flows"],
                "alltoall_avg_fct_us": alltoall["avg_fct_us"],
                "alltoall_p95_fct_us": alltoall["p95_fct_us"],
                "alltoall_aggregate_goodput_gbps": alltoall["aggregate_goodput_gbps"],
            }
        )

    return row


def write_csv(path: Path, rows: Sequence[Dict[str, object]]) -> None:
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def fluid_queue_bytes_for_rank(
    rank_count: int,
    *,
    eta: float,
    sender_rate_gbps: float,
    receiver_capacity_gbps: float,
    rtt_us: float,
) -> float:
    fan_in = max(rank_count - 1, 0)
    excess_rate_gbps = max(fan_in * eta * sender_rate_gbps - receiver_capacity_gbps, 0.0)
    return excess_rate_gbps * 1e3 * max(rtt_us, 0.0) / 8.0


def fluid_nstar(
    *,
    eta: float,
    sender_rate_gbps: float,
    receiver_capacity_gbps: float,
    rtt_us: float,
    pfc_xoff_bytes: float,
) -> Optional[int]:
    effective_sender_bytes_per_s = eta * sender_rate_gbps * 1e9 / 8.0
    receiver_bytes_per_s = receiver_capacity_gbps * 1e9 / 8.0
    t_s = max(rtt_us, 0.0) * 1e-6
    if effective_sender_bytes_per_s <= 0 or t_s <= 0:
        return None
    return int(pfc_xoff_bytes / (effective_sender_bytes_per_s * t_s) + receiver_bytes_per_s / effective_sender_bytes_per_s) + 2


def write_plot(path: Path, rows: Sequence[Dict[str, object]], fluid_etas: Sequence[float]) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - optional local dependency
        print(f"skip plot: matplotlib unavailable: {exc}")
        return

    ok_rows = [row for row in rows if row.get("status") == "ok"]
    if not ok_rows:
        print("skip plot: no completed rows")
        return
    ranks = [int(row["ranks"]) for row in ok_rows]
    pause_events = [int(row.get("pause_events", 0) or 0) for row in ok_rows]
    first = ok_rows[0]
    sender_rate_gbps = float(first.get("fluid_sender_rate_gbps") or first.get("predicted_drain_rate_gbps") or 100.0)
    receiver_capacity_gbps = float(first.get("fluid_receiver_capacity_gbps") or first.get("predicted_drain_rate_gbps") or 100.0)
    rtt_us = float(first.get("fluid_rtt_us") or 4.0)
    predicted_queue = [float(row.get("predicted_queue_bytes", 0.0) or 0.0) for row in ok_rows]
    threshold_values = [
        queue - float(row.get("predicted_queue_over_threshold_bytes", queue) or 0.0)
        for row, queue in zip(ok_rows, predicted_queue)
    ]
    threshold = threshold_values[0] if threshold_values else 0.0
    threshold_name = str(first.get("fluid_threshold_name") or "xon").upper()

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "axes.titlesize": 22,
            "axes.labelsize": 20,
            "xtick.labelsize": 18,
            "ytick.labelsize": 18,
            "legend.fontsize": 16,
            "axes.linewidth": 1.6,
            "lines.linewidth": 2.6,
            "lines.markersize": 8,
        }
    )

    fig, ax1 = plt.subplots(figsize=(10.5, 5.6))
    x = list(range(len(ranks)))
    ax1.plot(x, pause_events, color="#1f77b4", marker="o", linestyle="-", label="PAUSE count")
    ax1.set_xlabel("Number of Ranks")
    ax1.set_ylabel("PFC pause events")
    ax1.set_xticks(x)
    ax1.set_xticklabels([str(rank) for rank in ranks])
    ax1.grid(True, axis="both", linestyle="--", color="#bdbdbd", alpha=0.85, linewidth=1.1)

    ax2 = ax1.twinx()
    colors = ["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]
    markers = ["s", "^", "D", "v", "*"]
    linestyles = ["--", "-.", ":", (0, (3, 1, 1, 1)), "--"]
    for idx, eta in enumerate(fluid_etas):
        queues = [
            fluid_queue_bytes_for_rank(
                rank,
                eta=eta,
                sender_rate_gbps=sender_rate_gbps,
                receiver_capacity_gbps=receiver_capacity_gbps,
                rtt_us=rtt_us,
            )
            for rank in ranks
        ]
        nstar = fluid_nstar(
            eta=eta,
            sender_rate_gbps=sender_rate_gbps,
            receiver_capacity_gbps=receiver_capacity_gbps,
            rtt_us=rtt_us,
            pfc_xoff_bytes=threshold,
        )
        label = f"fluid queue eta={eta:g}" + (f" (N*={nstar})" if nstar is not None else "")
        ax2.plot(
            x,
            queues,
            marker=markers[idx % len(markers)],
            color=colors[idx % len(colors)],
            linestyle=linestyles[idx % len(linestyles)],
            label=label,
        )
    ax2.axhline(threshold, color="#8c564b", linestyle="-.", linewidth=2.0, label=f"PFC {threshold_name}")
    ax2.set_ylabel("fluid model queue bytes")

    for axis in (ax1, ax2):
        axis.tick_params(width=1.4, length=6)
        for spine in axis.spines.values():
            spine.set_linewidth(1.6)
            spine.set_color("black")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    fig.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc="lower center",
        ncol=min(len(labels1) + len(labels2), 3),
        frameon=True,
        bbox_to_anchor=(0.5, 0.01),
        columnspacing=1.5,
        handlelength=2.8,
    )
    ax1.set_title("All-to-All")
    fig.tight_layout(rect=(0, 0.28, 1, 1))
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    args = parse_args()
    ranks = parse_rank_list(args.ranks)
    fluid_etas = parse_float_list(args.fluid_etas)
    require_valid_ranks(ranks, args.rank_base)

    mix_dir = Path(__file__).resolve().parent
    sim_dir = mix_dir.parent
    rows: List[Dict[str, object]] = []

    for rank_count in ranks:
        nodes = list(range(args.rank_base, args.rank_base + rank_count))
        node_list = ",".join(str(node) for node in nodes)
        prefix = f"{args.prefix_base}_r{rank_count}"

        if not args.skip_generate:
            gen_cmd = [
                sys.executable,
                "mix/gen_2tor_pfc_hotspot.py",
                "--prefix",
                prefix,
                "--alltoall-node-list",
                node_list,
                "--alltoall-flow-bytes",
                str(args.alltoall_flow_bytes),
                "--alltoall-rounds",
                str(args.alltoall_rounds),
                "--alltoall-round-gap-us",
                str(args.alltoall_round_gap_us),
                "--alltoall-base-us",
                str(args.alltoall_base_us),
                "--alltoall-src-stagger-us",
                str(args.alltoall_src_stagger_us),
                "--alltoall-pg",
                str(args.alltoall_pg),
                "--pipeline-pg",
                str(args.pipeline_pg),
                "--pipeline-flow-bytes",
                str(args.pipeline_flow_bytes),
                "--pipeline-rounds",
                str(args.pipeline_rounds),
                "--pipeline-gap-us",
                str(args.pipeline_gap_us),
                "--pipeline-ring-layout",
                args.pipeline_ring_layout,
                "--pipeline-nodes-per-rack",
                str(args.pipeline_nodes_per_rack),
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
            ]
            if args.host_link_rate_gbps is not None:
                gen_cmd.extend(["--host-link-rate-gbps", str(args.host_link_rate_gbps)])
            if args.tor_link_rate_gbps is not None:
                gen_cmd.extend(["--tor-link-rate-gbps", str(args.tor_link_rate_gbps)])
            if not args.with_pipeline:
                gen_cmd.append("--no-pipeline")
            run_cmd(gen_cmd, sim_dir)

        if not args.skip_run:
            run_cmd(["./waf", "--run", f"scratch/mp-rdma-simulator mix/config_{prefix}.txt"], sim_dir)

        row = collect_row(
            mix_dir,
            prefix,
            rank_count,
            nodes,
            args.prediction_model,
            args.prediction_packet_bytes,
            args.prediction_burst_packets,
            args.fluid_eta,
            args.fluid_rtt_us,
            args.fluid_sender_rate_gbps,
            args.fluid_receiver_capacity_gbps,
            args.fluid_threshold,
        )
        rows.append(row)
        print(
            f"rank={rank_count} status={row.get('status')} "
            f"predicted_pfc={row.get('predicted_pfc')} "
            f"predicted_queue_bytes={row.get('predicted_queue_bytes', 0):.0f} "
            f"pause_events={row.get('pause_events', 'n/a')} "
            f"alltoall_pg_pause_events={row.get('alltoall_pg_pause_events', 'n/a')} "
            f"ports_with_pause={row.get('ports_with_pause', 'n/a')}"
        )

    csv_path = mix_dir / args.output_csv
    json_path = mix_dir / args.output_json
    write_csv(csv_path, rows)
    json_path.write_text(json.dumps(rows, indent=2) + "\n", encoding="ascii")
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")

    if not args.no_plot:
        png_path = mix_dir / args.output_png
        write_plot(png_path, rows, fluid_etas)
        if png_path.exists():
            print(f"wrote {png_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
