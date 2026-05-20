#!/usr/bin/env python3
"""
Diagnostic for the per-hop PFC classifier.

Given a prefix (e.g. 3tier_buf512_q40_N32), this script:
  1. Loads mix/manifest_<prefix>.json to recover the topology (which
     node ids are ToRs vs the Spine, which port index is each ToR's
     uplink).
  2. Reads mix/pfc_<prefix>.txt line by line, validates the format,
     and tallies events by (node_id, if_index, event_type).
  3. Runs each event through the exact same classify_pfc_hop function
     the collector uses, and prints per-bucket totals.
  4. Highlights mismatches: any (node_id, if_index) that *should* be
     pause_tor_to_spine (ToR -> Spine uplink) but lands in a different
     bucket, or vice versa.

Usage from the simulation/ folder:

    python3 mix/verify_pfc_classification.py --prefix 3tier_buf512_q40_N32

Add --show-unmatched to dump rows that don't match any expected port.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Import the *same* classifier used by the collector so this diagnostic
# can't drift from production behavior.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from collect_2tor_pfc_hotspot_metrics import (  # type: ignore  # noqa: E402
    PFC_HOP_BUCKETS,
    _build_classifier_index,
    classify_pfc_hop,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose where PFC events land in the per-hop classifier.")
    parser.add_argument("--prefix", required=True,
                        help="Experiment prefix; reads mix/manifest_<prefix>.json + mix/pfc_<prefix>.txt.")
    parser.add_argument("--mix-dir", default=None,
                        help="Directory holding manifest_<prefix>.json and pfc_<prefix>.txt.")
    parser.add_argument("--show-unmatched", action="store_true",
                        help="Print the first N raw lines that fall into 'pause_other' so you can "
                             "spot a format or port mismatch.")
    parser.add_argument("--unmatched-limit", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mix_dir = Path(args.mix_dir).resolve() if args.mix_dir else Path(__file__).resolve().parent
    manifest_path = mix_dir / f"manifest_{args.prefix}.json"
    pfc_path = mix_dir / f"pfc_{args.prefix}.txt"

    if not manifest_path.exists():
        raise SystemExit(f"missing manifest: {manifest_path}")
    if not pfc_path.exists():
        raise SystemExit(f"missing pfc log: {pfc_path}")

    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    topology = manifest.get("topology")
    classifier = _build_classifier_index(topology)

    print("--- topology ---")
    if topology:
        for k, v in topology.items():
            print(f"  {k}: {v}")
    else:
        print("  (no topology block; classifier is using the legacy 2-tor default)")
    print(f"  classifier kind: {classifier['kind']}")
    print(f"  classifier tor_uplink_by_id: {classifier['tor_uplink_by_id']}")
    print(f"  classifier spine_ids: {classifier['spine_ids']}")
    print()

    raw_size = pfc_path.stat().st_size
    line_count = sum(1 for _ in pfc_path.open())
    print(f"--- pfc log ---")
    print(f"  path: {pfc_path}")
    print(f"  size: {raw_size} bytes  lines: {line_count}")
    if line_count == 0:
        print("  >>> EMPTY. The simulator's PFC-trace callback never wrote to this file.")
        print("      Either the get_pfc callback isn't wired to QbbPfc, or it failed to fopen.")
        return 0
    print()

    per_port_counts: Counter = Counter()  # (node_id, node_type, if_index) -> count
    per_event: Counter = Counter()        # event_type -> count
    bucket_counts: Counter = {b: 0 for b in PFC_HOP_BUCKETS}
    sample_other_lines: list = []
    bad_lines = 0

    with pfc_path.open() as handle:
        for lineno, raw in enumerate(handle, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                parts = list(map(int, line.split()))
            except ValueError:
                bad_lines += 1
                continue
            if len(parts) < 5:
                bad_lines += 1
                continue
            time_ns, node_id, node_type, if_index, event_type = parts[:5]
            per_event[event_type] += 1
            bucket = classify_pfc_hop(node_id, node_type, if_index, classifier)
            # Count only PAUSE events (event_type == 1) into the per-port and
            # per-bucket tallies, so this diagnostic matches exactly what the
            # collector reports. RESUME events are tracked in per_event only.
            if event_type == 1:
                per_port_counts[(node_id, node_type, if_index)] += 1
                bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
                if bucket == "pause_other" and len(sample_other_lines) < args.unmatched_limit:
                    sample_other_lines.append((lineno, line))

    print("--- per (node, node_type, if_index) ---")
    for key, count in sorted(per_port_counts.items(), key=lambda kv: -kv[1])[:20]:
        node_id, node_type, if_index = key
        bucket = classify_pfc_hop(node_id, node_type, if_index, classifier)
        print(f"  node={node_id:>3} type={node_type} if={if_index:>3}  events={count:>5}  -> {bucket}")
    print()

    print("--- per event_type ---")
    for et, count in sorted(per_event.items()):
        kind = "PAUSE" if et == 1 else ("RESUME" if et == 0 else f"type={et}")
        print(f"  event_type={et} ({kind}): {count}")
    if bad_lines:
        print(f"  malformed lines: {bad_lines}")
    print()

    print("--- per hop bucket (what the collector would report) ---")
    for bucket in PFC_HOP_BUCKETS:
        print(f"  {bucket:25s} {bucket_counts.get(bucket, 0)}")
    print()

    # The switch-vs-rank split the Spine-P1 study cares about.
    switch_total = (
        bucket_counts.get("pause_inter_tor", 0)
        + bucket_counts.get("pause_tor_to_spine", 0)
        + bucket_counts.get("pause_spine_to_tor", 0)
    )
    rank_total = bucket_counts.get("pause_intra_tor", 0)
    host_total = bucket_counts.get("pause_host_to_tor", 0)
    print("--- switch ports vs rank ports ---")
    print(f"  SWITCH ports (inter-switch links, EXCLUDING rank ports): {switch_total}")
    print(f"    of which ToR->Spine uplink (pause_tor_to_spine): {bucket_counts.get('pause_tor_to_spine', 0)}")
    print(f"    of which Spine->ToR        (pause_spine_to_tor): {bucket_counts.get('pause_spine_to_tor', 0)}")
    print(f"    of which ToR<->ToR direct  (pause_inter_tor):    {bucket_counts.get('pause_inter_tor', 0)}")
    print(f"  RANK ports (switch->host, pause_intra_tor):              {rank_total}")
    print(f"  HOST-emitted PFC (NIC->switch, pause_host_to_tor):       {host_total}")
    print()

    if args.show_unmatched and sample_other_lines:
        print("--- sample 'pause_other' lines (first N) ---")
        for lineno, line in sample_other_lines:
            print(f"  {lineno}: {line}")
        print()

    # Diagnostic conclusion
    tor_uplink_ids = set(classifier["tor_uplink_by_id"].items())
    uplink_events = 0
    for (node_id, node_type, if_index), count in per_port_counts.items():
        if node_type == 1 and classifier["tor_uplink_by_id"].get(node_id) == if_index:
            uplink_events += count
    print("--- summary ---")
    print(f"  Expected (ToR, uplink_if) pairs: {tor_uplink_ids}")
    print(f"  Total PFC events on those uplinks: {uplink_events}")
    if uplink_events == 0 and bucket_counts.get("pause_intra_tor", 0) > 0:
        print("  >>> All PFC events were intra-ToR. The receiver-side egress queue triggered")
        print("      local PAUSE to senders but pressure never reached ToR1's uplink ingress.")
        print("      Try raising Q_pfc (so the queue grows larger before local PAUSE caps it)")
        print("      AND/OR shrinking the switch buffer further.")
    if uplink_events > 0 and bucket_counts.get("pause_tor_to_spine", 0) == 0:
        print("  >>> Uplink events exist but classifier didn't tag them as pause_tor_to_spine.")
        print("      The classifier's tor_uplink_by_id mapping is off. Check manifest's")
        print("      topology.tor_uplink_if_index against the actual log node/if values above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
