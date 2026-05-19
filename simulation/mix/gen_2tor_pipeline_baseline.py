#!/usr/bin/env python3
"""
Generate a no-PFC, pipeline-only 2-ToR baseline.

This is a thin wrapper around gen_2tor_pfc_hotspot.py that:
1. disables the hotspot all-to-all workload,
2. disables PFC,
3. enables tracing, and
4. auto-selects a non-overlapping pipeline round gap.
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
        "2tor_pipeline_baseline",
        "--no-alltoall",
        "--disable-pfc",
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
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
