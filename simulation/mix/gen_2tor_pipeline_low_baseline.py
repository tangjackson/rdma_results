#!/usr/bin/env python3
"""
Generate a low-load, pipeline-only 2-ToR baseline.

This preset is intended to keep the pipeline safely below the 100 Gbps
per-link capacity so it does not trigger PFC by itself and yields a steadier
throughput baseline before adding the hotspot all-to-all workload.

Design choices:
1. keep the same 32-host rack-contiguous ring shape,
2. reduce per-flow bytes to 256 KiB, and
3. use a fixed 250 us round gap instead of auto-gap.

That gives an offered rate of roughly:
  256 KiB * 8 / 250 us ~= 8.39 Gbps
per active link, which is far below the 100 Gbps link rate.
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
        "2tor_pipeline_low_baseline",
        "--no-alltoall",
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
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
