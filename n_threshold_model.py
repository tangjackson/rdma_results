#!/usr/bin/env python3
"""
Fluid-model thresholds N* and N** for the 2-ToR PFC propagation study.

Background
----------
N* is the *local* PAUSE threshold from the existing Eq. 3 single-port fluid
model: above N* the receiver-side ToR host port's offered load exceeds its
drain rate by enough that the per-port queue grows past Xoff within the
PAUSE response time.

N** is the *propagation* (two-hop) threshold: above N** local PAUSEs are
sustained on enough of the N receiver ports simultaneously that the
shared buffer's reserved headroom for the inter-ToR (uplink) ingress port
is exhausted, so the ToR ALSO emits PAUSE upstream on its uplink. That
is the PAUSE-frame "escaping the pod" event the App Monitor should
detect.

This module exposes pure functions so it can be unit-tested and called
from the rank-sweep driver and plotting scripts without simulator side
effects.

Usage as a script:

    python3 mix/n_threshold_model.py --host-gbps 40 --core-gbps 100 \\
        --q-pfc-bytes 320000 --eta 0.75 --buffer-mb 2

Prints N*, N** and the input assumptions in a single JSON object.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ModelInputs:
    """All inputs to the analytic N*/N** model."""

    # Offered-load parameters
    eta: float                       # Burst factor / utilization of each sender's NIC, in (0, 1].
    host_link_gbps: float            # Per-host NIC rate (sender == receiver == this).
    core_link_gbps: float            # Inter-ToR / spine uplink rate.

    # PFC parameters
    q_pfc_xoff_bytes: int            # Xoff (PAUSE) threshold per (port, priority).
    pfc_response_us: float           # One-way PAUSE response time (link delay + processing).

    # Shared-buffer parameters
    buffer_total_bytes: int          # Total switch buffer (MMU pool).
    buffer_alpha: float              # Dynamic threshold alpha for the contributing queues
                                     # (queue cap = alpha * remaining_pool).
    headroom_per_port_bytes: int     # Reserved headroom per ingress (per-port PFC headroom).
    num_ingress_ports: int           # Number of ingress ports sharing the pool (incl. uplink).


def n_star(inputs: ModelInputs) -> float:
    """Local PAUSE threshold from the single-port fluid model (Eq. 3).

    Receiver-side ToR host port has offered load (N-1)*eta*C_host from
    the other ranks, and drain rate C_host (one host NIC). The egress
    queue grows whenever offered exceeds drain:

        (N - 1) * eta * C_host > C_host
        N > 1 + 1 / eta

    For long enough collectives the queue is unbounded once this holds,
    so the queue inevitably crosses Q_pfc and local PAUSE fires. This is
    why N* "fires too early": it predicts local pause even at very small
    N where the actual queue never lasts long enough to harm a cross-ToR
    victim.

    The Q_pfc and PFC response time govern *how soon* the pause fires
    (time_to_pause), which we expose separately as
    `time_to_local_pause_us` for completeness.
    """
    if inputs.eta <= 0:
        raise ValueError("eta must be > 0")
    return 1.0 + 1.0 / inputs.eta


def time_to_local_pause_us(inputs: ModelInputs, n_ranks: int) -> Optional[float]:
    """Time for the receiver-side egress queue to fill from empty to Q_pfc
    at N ranks, in microseconds. None if queue does not fill (N <= N*)."""
    if n_ranks <= 1:
        return None
    excess = (n_ranks - 1) * inputs.eta - 1.0
    if excess <= 0:
        return None
    c_host_bps = inputs.host_link_gbps * 1e9
    fill_bps = excess * c_host_bps
    q_bits = inputs.q_pfc_xoff_bytes * 8.0
    return (q_bits / fill_bps) * 1e6


def n_double_star(inputs: ModelInputs) -> float:
    """Two-hop propagation threshold (shared-buffer model).

    Once local PAUSE is in steady state at K of the N receiver ports, each
    such port's egress queue is parked at ~ Q_pfc bytes. Additional buffer
    is consumed by the all-to-all senders' in-flight bytes that arrive
    during the PAUSE response interval -- approximated by
    eta * C_host * T_response per saturated port.

    The shared-buffer dynamic threshold for an ingress kicks PFC when the
    pool's free space drops below the per-ingress reserved headroom. For
    the inter-ToR ingress to PAUSE upstream the condition is

        K * (Q_pfc + eta * C_host * T_response)
            >= B_total - num_ingress_ports * headroom_per_port

    In the all-to-all on N hosts every rank is a receiver (each receives
    N - 1 flows), so K == N. Solving for N gives N**.

    Returns a real-valued threshold; caller can ceil.
    """
    if inputs.q_pfc_xoff_bytes <= 0:
        raise ValueError("Q_pfc must be > 0")
    c_host_bps = inputs.host_link_gbps * 1e9
    t_resp_s = inputs.pfc_response_us * 1e-6
    in_flight_bytes = inputs.eta * c_host_bps * t_resp_s / 8.0
    per_saturated_port = inputs.q_pfc_xoff_bytes + in_flight_bytes
    free_pool = max(
        inputs.buffer_total_bytes
        - inputs.num_ingress_ports * inputs.headroom_per_port_bytes,
        1.0,
    )
    return free_pool / per_saturated_port


def default_inputs(
    host_gbps: float = 40.0,
    core_gbps: float = 100.0,
    q_pfc_bytes: int = 320_000,
    eta: float = 0.75,
    buffer_mb: float = 2.0,
    buffer_alpha: float = 0.25,
    headroom_kb: float = 30.0,
    num_ingress: int = 17,           # 16 host ports + 1 uplink for a 16-host ToR
    pfc_response_us: float = 2.0,    # ~1 us link delay each way
) -> ModelInputs:
    return ModelInputs(
        eta=eta,
        host_link_gbps=host_gbps,
        core_link_gbps=core_gbps,
        q_pfc_xoff_bytes=int(q_pfc_bytes),
        pfc_response_us=pfc_response_us,
        buffer_total_bytes=int(buffer_mb * 1024 * 1024),
        buffer_alpha=buffer_alpha,
        headroom_per_port_bytes=int(headroom_kb * 1024),
        num_ingress_ports=int(num_ingress),
    )


def compute(inputs: Optional[ModelInputs] = None) -> dict:
    inputs = inputs or default_inputs()
    ns = n_star(inputs)
    nss = n_double_star(inputs)
    return {
        "inputs": {
            "eta": inputs.eta,
            "host_link_gbps": inputs.host_link_gbps,
            "core_link_gbps": inputs.core_link_gbps,
            "q_pfc_xoff_bytes": inputs.q_pfc_xoff_bytes,
            "pfc_response_us": inputs.pfc_response_us,
            "buffer_total_bytes": inputs.buffer_total_bytes,
            "buffer_alpha": inputs.buffer_alpha,
            "headroom_per_port_bytes": inputs.headroom_per_port_bytes,
            "num_ingress_ports": inputs.num_ingress_ports,
        },
        "n_star_real": ns,
        "n_star": math.ceil(ns),
        "n_double_star_real": nss,
        "n_double_star": math.ceil(nss),
        "time_to_local_pause_us_at_n_double_star": time_to_local_pause_us(
            inputs, max(2, math.ceil(nss))
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute N* and N** for the 2-ToR PFC study.")
    parser.add_argument("--eta", type=float, default=0.75)
    parser.add_argument("--host-gbps", type=float, default=40.0)
    parser.add_argument("--core-gbps", type=float, default=100.0)
    parser.add_argument("--q-pfc-bytes", type=int, default=320_000)
    parser.add_argument("--pfc-response-us", type=float, default=2.0)
    parser.add_argument("--buffer-mb", type=float, default=2.0)
    parser.add_argument("--buffer-alpha", type=float, default=0.25)
    parser.add_argument("--headroom-kb", type=float, default=30.0)
    parser.add_argument("--num-ingress", type=int, default=17)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = default_inputs(
        host_gbps=args.host_gbps,
        core_gbps=args.core_gbps,
        q_pfc_bytes=args.q_pfc_bytes,
        eta=args.eta,
        buffer_mb=args.buffer_mb,
        buffer_alpha=args.buffer_alpha,
        headroom_kb=args.headroom_kb,
        num_ingress=args.num_ingress,
        pfc_response_us=args.pfc_response_us,
    )
    result = compute(inputs)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
