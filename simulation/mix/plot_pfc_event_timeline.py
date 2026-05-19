#!/usr/bin/env python3
"""
Build a time-sequence view of PFC events from pfc.txt.

Input format per line in pfc.txt:
  time_ns node_id node_type if_index event_type

Where event_type is:
  1 = pause
  0 = resume

Outputs:
  - CSV with one row per time bin
  - Optional PNG if matplotlib is available

Typical usage:
  python3 mix/plot_pfc_event_timeline.py --prefix 2tor_alltoall_pfc_trigger
"""

from __future__ import annotations

import argparse
import csv
import json
import struct
from pathlib import Path


TRACE_RECORD_STRUCT = struct.Struct("<QHBBIIIHBBBB2x24s")
TRACE_DATA_STRUCT = struct.Struct("<HHIQHH4x")
HOSTS_PER_TOR = 16


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot/bin PFC pause and resume events over time.")
    parser.add_argument("--prefix", required=True, help="Experiment prefix under mix/.")
    parser.add_argument(
        "--bin-us",
        type=float,
        default=10.0,
        help="Time-bin width in microseconds for grouping events.",
    )
    parser.add_argument(
        "--per-port",
        action="store_true",
        help="Also emit per-port active pause counts in the CSV.",
    )
    return parser.parse_args()


def load_manifest(mix_dir: Path, prefix: str) -> dict:
    manifest_path = mix_dir / f"manifest_{prefix}.json"
    if not manifest_path.exists():
        raise SystemExit(f"missing manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="ascii"))


def ip_int_to_node_id(value: int) -> int:
    return (value >> 8) & 0xFFFF


def build_trace_lookup(flows: list[dict]) -> dict[tuple[int, int, int], dict]:
    lookup: dict[tuple[int, int, int], dict] = {}
    for flow in flows:
        lookup[(flow["src"], flow["dst"], flow["dst_port"])] = flow
    return lookup


def main() -> int:
    args = parse_args()
    mix_dir = Path(__file__).resolve().parent
    manifest = load_manifest(mix_dir, args.prefix)
    pfc_name = Path(manifest["files"]["pfc"]).name
    pfc_path = mix_dir / pfc_name
    if not pfc_path.exists():
        raise SystemExit(f"missing pfc file: {pfc_path}")
    trace_name = Path(manifest["files"]["trace_output"]).name if "trace_output" in manifest["files"] else None
    trace_path = mix_dir / trace_name if trace_name else None
    if trace_path is not None and not trace_path.exists():
        raise SystemExit(f"missing trace file: {trace_path}")

    bin_ns = int(args.bin_us * 1000.0)
    if bin_ns <= 0:
        raise SystemExit("--bin-us must be positive")

    events = []
    active_ports = set()
    first_time_ns = None
    last_time_ns = None

    with pfc_path.open(encoding="ascii") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = list(map(int, line.split()))
            if len(parts) < 5:
                continue
            time_ns, node_id, node_type, if_index, event_type = parts[:5]
            events.append((time_ns, node_id, if_index, event_type))
            first_time_ns = time_ns if first_time_ns is None else min(first_time_ns, time_ns)
            last_time_ns = time_ns if last_time_ns is None else max(last_time_ns, time_ns)

    throughput_lookup = build_trace_lookup(manifest.get("flows", []))
    trace_bins: dict[int, dict[str, int]] = {}
    trace_first_ns = None
    trace_last_ns = None
    if trace_path is not None:
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
                if event != 0 or node_type != 0 or l3_prot != 0x11:
                    continue
                sport, dport, seq, ts, pg, payload = TRACE_DATA_STRUCT.unpack(extra)
                src = ip_int_to_node_id(sip)
                dst = ip_int_to_node_id(dip)
                flow = throughput_lookup.get((src, dst, dport))
                if flow is None:
                    continue
                trace_first_ns = time_ns if trace_first_ns is None else min(trace_first_ns, time_ns)
                trace_last_ns = time_ns if trace_last_ns is None else max(trace_last_ns, time_ns)
                bin_idx = time_ns // bin_ns
                bucket = trace_bins.setdefault(
                    bin_idx,
                    {
                        "overall_bytes": 0,
                        "pipeline_bytes": 0,
                        "pipeline_hotspot_touch_bytes": 0,
                        "pipeline_non_hotspot_bytes": 0,
                        "alltoall_bytes": 0,
                    },
                )
                bucket["overall_bytes"] += payload
                if flow["pattern"] == "pipeline":
                    bucket["pipeline_bytes"] += payload
                    hotspot_nodes = set(manifest.get("scenario", {}).get("alltoall_hotspot_nodes", []))
                    if flow["src"] in hotspot_nodes or flow["dst"] in hotspot_nodes:
                        bucket["pipeline_hotspot_touch_bytes"] += payload
                    else:
                        bucket["pipeline_non_hotspot_bytes"] += payload
                elif flow["pattern"] == "alltoall":
                    bucket["alltoall_bytes"] += payload

    if not events and trace_first_ns is None:
        raise SystemExit(f"no PFC events and no trace activity for {args.prefix}")

    if first_time_ns is None:
        first_time_ns = trace_first_ns
    if last_time_ns is None:
        last_time_ns = trace_last_ns

    events.sort()
    first_bin = min(
        first_time_ns // bin_ns,
        trace_first_ns // bin_ns if trace_first_ns is not None else first_time_ns // bin_ns,
    )
    last_bin = max(
        last_time_ns // bin_ns,
        trace_last_ns // bin_ns if trace_last_ns is not None else last_time_ns // bin_ns,
    )
    bins = []
    event_idx = 0

    while first_bin <= last_bin:
        bin_start_ns = first_bin * bin_ns
        bin_end_ns = bin_start_ns + bin_ns
        pause_count = 0
        resume_count = 0
        changed_ports = set()

        while event_idx < len(events) and events[event_idx][0] < bin_end_ns:
            time_ns, node_id, if_index, event_type = events[event_idx]
            port = (node_id, if_index)
            changed_ports.add(port)
            if event_type == 1:
                pause_count += 1
                active_ports.add(port)
            else:
                resume_count += 1
                active_ports.discard(port)
            event_idx += 1

        trace_bucket = trace_bins.get(
            first_bin,
            {
                "overall_bytes": 0,
                "pipeline_bytes": 0,
                "pipeline_hotspot_touch_bytes": 0,
                "pipeline_non_hotspot_bytes": 0,
                "alltoall_bytes": 0,
            },
        )
        row = {
            "bin_start_ns": bin_start_ns,
            "bin_end_ns": bin_end_ns,
            "bin_mid_us": (bin_start_ns + bin_end_ns) / 2.0 / 1e3,
            "pause_events": pause_count,
            "resume_events": resume_count,
            "net_pause_events": pause_count - resume_count,
            "active_paused_ports": len(active_ports),
            "ports_with_event": len(changed_ports),
            "overall_cluster_throughput_gbps": trace_bucket["overall_bytes"] * 8.0 / bin_ns,
            "pipeline_cluster_throughput_gbps": trace_bucket["pipeline_bytes"] * 8.0 / bin_ns,
            "pipeline_hotspot_touch_cluster_throughput_gbps": trace_bucket["pipeline_hotspot_touch_bytes"] * 8.0 / bin_ns,
            "pipeline_non_hotspot_cluster_throughput_gbps": trace_bucket["pipeline_non_hotspot_bytes"] * 8.0 / bin_ns,
            "alltoall_cluster_throughput_gbps": trace_bucket["alltoall_bytes"] * 8.0 / bin_ns,
        }
        if args.per_port:
            row["active_port_list"] = " ".join(f"{node}:{iface}" for node, iface in sorted(active_ports))
        bins.append(row)
        first_bin += 1

    csv_path = mix_dir / f"pfc_timeline_{args.prefix}.csv"
    fieldnames = list(bins[0].keys())
    with csv_path.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(bins)

    png_path = mix_dir / f"pfc_timeline_{args.prefix}.png"
    png_written = False
    try:
        import matplotlib.pyplot as plt  # type: ignore

        xs = [row["bin_mid_us"] for row in bins]
        pauses = [row["pause_events"] for row in bins]
        resumes = [row["resume_events"] for row in bins]
        active = [row["active_paused_ports"] for row in bins]
        pipeline_tput = [row["pipeline_cluster_throughput_gbps"] for row in bins]

        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax1.plot(xs, pauses, label="pause events/bin", color="tab:red")
        ax1.plot(xs, resumes, label="resume events/bin", color="tab:blue")
        ax1.set_xlabel("time (us)")
        ax1.set_ylabel(f"events per {args.bin_us:g} us bin")

        ax2 = ax1.twinx()
        ax2.plot(xs, active, label="active paused ports", color="tab:green", linestyle="--")
        ax2.plot(xs, pipeline_tput, label="pipeline throughput", color="tab:purple")
        ax2.set_ylabel("active paused ports / throughput (Gbps)")

        lines = ax1.get_lines() + ax2.get_lines()
        labels = [line.get_label() for line in lines]
        ax1.legend(lines, labels, loc="upper right")
        ax1.set_title(f"PFC event timeline: {args.prefix}")
        fig.tight_layout()
        fig.savefig(png_path, dpi=150)
        plt.close(fig)
        png_written = True
    except Exception:
        png_written = False

    print(csv_path.name)
    if png_written:
        print(png_path.name)
    else:
        print("png_not_written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
