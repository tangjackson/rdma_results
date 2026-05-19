#!/usr/bin/env python3
"""
Generate a stronger one-ToR all-to-all PFC stress run.

This keeps the same low pipeline as the persistent baseline, but makes the
all-to-all hotspot cover the entire second ToR (hosts 16-31). That causes many
pipeline ring flows to touch the hotspot hosts, making the PFC effect visible
in total pipeline and pipeline_hotspot_touch metrics.
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
        "2tor_alltoall_pfc_strong_low_pipeline",
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
        "--alltoall-node-list",
        "16-31",
        "--alltoall-flow-bytes",
        str(1024 * 1024),
        "--alltoall-rounds",
        "32",
        "--alltoall-round-gap-us",
        "5",
        "--alltoall-base-us",
        "250",
        "--alltoall-src-stagger-us",
        "0",
        "--alltoall-pg",
        "3",
        "--pfc-xoff-bytes",
        "16384",
        "--pfc-xon-bytes",
        "8192",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
