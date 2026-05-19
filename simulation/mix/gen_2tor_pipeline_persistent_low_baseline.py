#!/usr/bin/env python3
"""
Generate the pipeline-only baseline for the persistent low-pipeline comparison.

This is the clean baseline for MQ-RDMA/PFC comparisons:
- same pipeline shape as the persistent mixed workload,
- no all-to-all hotspot traffic,
- PFC disabled so the baseline has no pause mechanism.
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
        "2tor_pipeline_persistent_low_baseline",
        "--disable-pfc",
        "--no-alltoall",
        "--enable-trace",
        "--link-rate-gbps",
        "100",
        "--pipeline-nodes-per-rack",
        "16",
        "--pipeline-ring-layout",
        "rack_contiguous",
        "--pipeline-rounds",
        "12",
        "--pipeline-flow-bytes",
        str(256 * 1024),
        "--pipeline-gap-us",
        "250",
        "--pipeline-base-us",
        "0",
        "--pipeline-pg",
        "3",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
