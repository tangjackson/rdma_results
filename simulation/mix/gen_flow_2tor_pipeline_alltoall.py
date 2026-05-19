#!/usr/bin/env python
"""
Generate combined pipeline-allreduce + all-to-all flow files for a 2-ToR testbed.

By default this reproduces the existing 32-node workload, but it can also emit
smaller smoke-test traces such as 8 nodes:

  python gen_flow_2tor_pipeline_alltoall.py flow.txt
  python gen_flow_2tor_pipeline_alltoall.py --num-nodes 8 flow_8.txt

Output format (HPCC simulator):
  <num_flows>
  <src> <dst> <pg> <dst_port> <size_bytes> <start_time_ns>
  ...  (sorted by start_time_ns, ascending)
"""

import argparse
import io
import sys

# ---------- tunables ---------------------------------------------------

PG        = 3    # RDMA QoS priority group (matches existing flow files)
DST_PORT  = 100  # destination port used in HPCC examples

# Pipeline allreduce – ring reduce-scatter phase
# Each node carries 1/N of the total message; 2 MB matches the NSDI example.
PIPELINE_SIZE_BYTES = 2 * 1024 * 1024   # 2 MB per ring step

# All-to-all
# 256 KB is a typical mid-size all-to-all message that stresses the fabric.
ALLTOALL_SIZE_BYTES     = 256 * 1024    # 256 KB per flow
ALLTOALL_BASE_NS        = 2000          # 2 us after t=0 (let pipeline launch first)
ALLTOALL_SRC_STAGGER_NS = 100           # 100 ns between successive source starts

# -----------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate pipeline-allreduce + all-to-all flows for a 2-ToR workload."
    )
    parser.add_argument(
        "output_file",
        nargs="?",
        help="Output flow file path. If omitted, writes to stdout.",
    )
    parser.add_argument(
        "--num-nodes",
        dest="num_nodes",
        type=int,
        default=32,
        help="Number of end hosts participating in the workload.",
    )
    parser.add_argument(
        "--pipeline-size-bytes",
        dest="pipeline_size_bytes",
        type=int,
        default=PIPELINE_SIZE_BYTES,
        help="Payload size in bytes for each ring step.",
    )
    parser.add_argument(
        "--alltoall-size-bytes",
        dest="alltoall_size_bytes",
        type=int,
        default=ALLTOALL_SIZE_BYTES,
        help="Payload size in bytes for each all-to-all flow.",
    )
    parser.add_argument(
        "--alltoall-base-ns",
        dest="alltoall_base_ns",
        type=int,
        default=ALLTOALL_BASE_NS,
        help="Base start time in ns for all-to-all traffic.",
    )
    parser.add_argument(
        "--alltoall-src-stagger-ns",
        dest="alltoall_src_stagger_ns",
        type=int,
        default=ALLTOALL_SRC_STAGGER_NS,
        help="Source-level start-time stagger in ns for all-to-all traffic.",
    )
    args = parser.parse_args()
    if args.num_nodes < 2:
        parser.error("--num-nodes must be at least 2")
    return args


def generate_flows(num_nodes, pipeline_size_bytes, alltoall_size_bytes,
                   alltoall_base_ns, alltoall_src_stagger_ns):
    flows = []  # list of (start_ns, src, dst, pg, port, size_bytes)

    # --- 1. Pipeline allreduce: ring pattern ----------------------------
    # Reduce-scatter: node i sends to (i+1) % N simultaneously.
    for i in range(num_nodes):
        src = i
        dst = (i + 1) % num_nodes
        flows.append((0, src, dst, PG, DST_PORT, pipeline_size_bytes))

    # --- 2. All-to-all --------------------------------------------------
    # Each source starts slightly later than the previous one to prevent
    # a perfectly synchronised N^2 burst from overwhelming switch buffers.
    for src in range(num_nodes):
        start_ns = alltoall_base_ns + src * alltoall_src_stagger_ns
        for dst in range(num_nodes):
            if dst == src:
                continue
            flows.append((start_ns, src, dst, PG, DST_PORT, alltoall_size_bytes))

    # Sort ascending by start time (required by simulator)
    flows.sort(key=lambda x: x[0])
    return flows


def write_flows(flows, out, num_nodes, pipeline_size_bytes,
                alltoall_size_bytes, alltoall_base_ns, alltoall_src_stagger_ns):
    out.write("%d\n" % len(flows))
    for (start_ns, src, dst, pg, port, size) in flows:
        out.write("%d %d %d %d %d %d\n" % (src, dst, pg, port, size, start_ns))
    out.write("\n")
    out.write("# First line: number of flows\n")
    out.write("# src dst pg dst_port size_bytes start_time_ns\n")
    out.write("# Flows sorted in ascending start_time order\n")
    out.write("#\n")
    out.write("# Pattern 1 – pipeline allreduce (ring reduce-scatter)\n")
    out.write("#   flows : %d  (ring 0->1->...->%d->0)\n" % (num_nodes, num_nodes - 1))
    out.write("#   size  : %d bytes (%.1f MB) per flow\n" % (
              pipeline_size_bytes, pipeline_size_bytes / 1024.0 / 1024.0))
    out.write("#   start : t=0 ns\n")
    out.write("#\n")
    out.write("# Pattern 2 – all-to-all\n")
    out.write("#   flows : %d  (%d x %d)\n" % (
              num_nodes * (num_nodes - 1), num_nodes, num_nodes - 1))
    out.write("#   size  : %d bytes (%.0f KB) per flow\n" % (
              alltoall_size_bytes, alltoall_size_bytes / 1024.0))
    out.write("#   start : base=%d ns, stagger=%d ns per source\n" % (
              alltoall_base_ns, alltoall_src_stagger_ns))


if __name__ == "__main__":
    args = parse_args()
    flows = generate_flows(
        args.num_nodes,
        args.pipeline_size_bytes,
        args.alltoall_size_bytes,
        args.alltoall_base_ns,
        args.alltoall_src_stagger_ns,
    )

    if args.output_file:
        with open(args.output_file, "w") as f:
            write_flows(
                flows,
                f,
                args.num_nodes,
                args.pipeline_size_bytes,
                args.alltoall_size_bytes,
                args.alltoall_base_ns,
                args.alltoall_src_stagger_ns,
            )
        sys.stderr.write("Wrote %d flows to %s\n" % (len(flows), args.output_file))
    else:
        buf = io.StringIO()
        write_flows(
            flows,
            buf,
            args.num_nodes,
            args.pipeline_size_bytes,
            args.alltoall_size_bytes,
            args.alltoall_base_ns,
            args.alltoall_src_stagger_ns,
        )
        sys.stdout.write(buf.getvalue())
        sys.stderr.write("Generated %d flows (pipeline=%d, alltoall=%d)\n" % (
            len(flows), args.num_nodes, args.num_nodes * (args.num_nodes - 1)))
