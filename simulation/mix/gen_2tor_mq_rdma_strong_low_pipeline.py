#!/usr/bin/env python3
"""
Generate the MQ-RDMA variant of the stronger all-to-all PFC stress run.

PFC remains enabled. The only intended difference from
gen_2tor_alltoall_pfc_strong_low_pipeline.py is queue assignment:
- pipeline victim traffic: PG 4
- all-to-all hotspot traffic: PG 3
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    mix_dir = Path(__file__).resolve().parent
    generator = mix_dir / "gen_2tor_alltoall_pfc_strong_low_pipeline.py"
    cmd = [
        sys.executable,
        str(generator),
        "--prefix",
        "2tor_mq_rdma_strong_low_pipeline",
        "--pipeline-pg",
        "4",
        "--alltoall-pg",
        "3",
    ]
    cmd.extend(sys.argv[1:])
    return subprocess.run(cmd, cwd=str(mix_dir.parent)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
