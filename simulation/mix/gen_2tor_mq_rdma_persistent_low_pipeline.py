#!/usr/bin/env python3
"""
Generate the MQ-RDMA variant of the persistent all-to-all low-pipeline run.

This uses the same workload shape as gen_2tor_persistent_alltoall_low_pipeline.py
with PFC enabled, but places pipeline/all-to-all traffic on separate priority
groups. The expected behavior is that all-to-all may still trigger PFC on PG 3,
while the pipeline runs on PG 4.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    mix_dir = Path(__file__).resolve().parent
    generator = mix_dir / "gen_2tor_persistent_alltoall_low_pipeline.py"
    cmd = [
        sys.executable,
        str(generator),
        "--prefix",
        "2tor_mq_rdma_persistent_low_pipeline",
        "--pipeline-pg",
        "4",
        "--alltoall-pg",
        "3",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
