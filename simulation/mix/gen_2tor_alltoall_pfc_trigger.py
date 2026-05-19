#!/usr/bin/env python3
"""
Generate a pipeline + hotspot all-to-all experiment intended to trigger PFC.

This wrapper keeps the validated pipeline baseline shape and adds a bursty
all-to-all hotspot on a small subset of one ToR. Tracing is enabled by default
so the collector can verify whether the first PFC event occurs only after the
hotspot starts.
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
        "2tor_alltoall_pfc_trigger",
        "--enable-trace",
        "--link-rate-gbps",
        "100",
        "--pipeline-nodes-per-rack",
        "16",
        "--pipeline-ring-layout",
        "rack_contiguous",
        "--pipeline-auto-gap",
        "--pipeline-rounds",
        "4",
        "--pipeline-flow-bytes",
        str(1 * 1024 * 1024),
        "--alltoall-node-list",
        "24-31",
        "--alltoall-flow-bytes",
        str(128 * 1024),
        "--alltoall-rounds",
        "8",
        "--alltoall-round-gap-us",
        "80",
        "--alltoall-base-us",
        "10",
        "--alltoall-src-stagger-us",
        "0",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
