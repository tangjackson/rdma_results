#!/usr/bin/env python3
"""
Generate a low-pipeline + bursty all-to-all experiment intended to trigger PFC.

This preset is designed to separate the two roles:
1. the pipeline is a light victim background that should not trigger PFC alone,
2. the hotspot all-to-all is the burst that should push queues over the PFC threshold.

Default design:
- pipeline:
  - 32-host rack-contiguous ring
  - 256 KiB per flow
  - 8 rounds
  - 250 us round gap
  - ~8.39 Gbps offered per active link
- all-to-all:
  - nodes 24-31
  - 256 KiB per flow
  - 8 rounds
  - 20 us round gap
  - starts at 500 us
- PFC thresholds:
  - xoff = 32768 B
  - xon  = 16384 B
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    mix_dir = Path(__file__).resolve().parent
    generator = mix_dir / "gen_2tor_pfc_hotspot.py"
    cmd = [
        sys.executable,
        str(generator),
        "--prefix",
        "2tor_alltoall_pfc_trigger_low_pipeline",
        "--enable-trace",
        "--link-rate-gbps",
        "100",
        "--pipeline-nodes-per-rack",
        "16",
        "--pipeline-ring-layout",
        "rack_contiguous",
        "--pipeline-rounds",
        "8",
        "--pipeline-flow-bytes",
        str(256 * 1024),
        "--pipeline-gap-us",
        "250",
        "--pipeline-base-us",
        "0",
        "--alltoall-node-list",
        "24-31",
        "--alltoall-flow-bytes",
        str(256 * 1024),
        "--alltoall-rounds",
        "8",
        "--alltoall-round-gap-us",
        "20",
        "--alltoall-base-us",
        "500",
        "--alltoall-src-stagger-us",
        "0",
        "--pfc-xoff-bytes",
        "32768",
        "--pfc-xon-bytes",
        "16384",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
