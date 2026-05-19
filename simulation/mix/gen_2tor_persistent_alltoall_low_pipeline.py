#!/usr/bin/env python3
"""
Generate a low-pipeline experiment with a more persistent all-to-all PFC trigger.

This keeps the victim pipeline light enough to avoid PFC by itself, but makes
the hotspot all-to-all run longer and more continuously than the default
low-pipeline trigger preset.

Defaults:
- pipeline:
  - 32-host rack-contiguous ring
  - 256 KiB per flow
  - 12 rounds
  - 250 us round gap
  - ~8.39 Gbps offered per active link
- all-to-all:
  - nodes 24-31
  - 512 KiB per flow
  - 24 rounds
  - 10 us round gap
  - starts at 500 us
- queues:
  - same-queue PFC baseline by default: pipeline PG 3, all-to-all PG 3
  - MQ-RDMA variant: use gen_2tor_mq_rdma_persistent_low_pipeline.py, which
    keeps PFC enabled and uses pipeline PG 4 / all-to-all PG 3
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
        "2tor_alltoall_pfc_persistent_low_pipeline",
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
        "24-31",
        "--alltoall-flow-bytes",
        str(512 * 1024),
        "--alltoall-rounds",
        "24",
        "--alltoall-round-gap-us",
        "10",
        "--alltoall-base-us",
        "500",
        "--alltoall-src-stagger-us",
        "0",
        "--alltoall-pg",
        "3",
        "--pfc-xoff-bytes",
        "32768",
        "--pfc-xon-bytes",
        "16384",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
