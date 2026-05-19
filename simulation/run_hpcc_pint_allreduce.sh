#!/usr/bin/env bash
# =============================================================================
# run_hpcc_pint_allreduce.sh
#
# End-to-end experiment runner for HPCC-PINT on a 32-node, 2-ToR topology
# with a combined pipeline-allreduce + all-to-all traffic pattern.
#
# Topology : mix/topo_2tor_32nodes.txt
#            Rack A: nodes  0-15  -> ToR_A (switch 32)
#            Rack B: nodes 16-31  -> ToR_B (switch 33)
#            ToR_A <-> ToR_B: 1 x 100 Gbps inter-rack link
#
# Traffic  : mix/flow_2tor_32nodes_pipeline_alltoall.txt  (auto-generated)
#            - ring allreduce (reduce-scatter): 32 flows x 2 MB
#            - all-to-all:                     992 flows x 256 KB
#
# CC modes tested:
#   hpccPint  – HPCC-PINT (CC_MODE 10); sweeps over log_base and prob
#   hp        – vanilla HPCC (CC_MODE 3); used as comparison baseline
#
# Usage:
#   cd <repo>/simulation
#   bash run_hpcc_pint_allreduce.sh [--dry-run] [--enable-trace]
#
# Outputs (all in mix/):
#   fct_2tor_32nodes_2tor_32nodes_pipeline_alltoall_<cc>.txt
#   pfc_2tor_32nodes_2tor_32nodes_pipeline_alltoall_<cc>.txt
#   qlen_2tor_32nodes_2tor_32nodes_pipeline_alltoall_<cc>.txt
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
DRY_RUN=0
ENABLE_TRACE=0
for arg in "$@"; do
  case $arg in
    --dry-run)      DRY_RUN=1 ;;
    --enable-trace) ENABLE_TRACE=1 ;;
    *)  echo "Unknown arg: $arg  (valid: --dry-run  --enable-trace)"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Paths – run script from the simulation/ directory
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TOPO="2tor_32nodes"
TRACE="2tor_32nodes_pipeline_alltoall"
BW=100                  # NIC bandwidth in Gbps (matches 100Gbps links in topo)
FLOW_FILE="mix/flow_${TRACE}.txt"
TOPO_FILE="mix/topo_${TOPO}.txt"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
sep()  { echo "------------------------------------------------------------------------"; }

run_sim() {
  local label="$1"; shift
  local cmd="python run.py $* --enable_tr ${ENABLE_TRACE}"
  log "START: ${label}"
  log "CMD  : ${cmd}"
  if [[ $DRY_RUN -eq 0 ]]; then
    $cmd
    log "DONE : ${label}"
  else
    log "DRY-RUN – skipped"
  fi
  sep
}

# ---------------------------------------------------------------------------
# Step 1: Sanity-check required files
# ---------------------------------------------------------------------------
sep
log "Checking required input files..."
for f in "$TOPO_FILE" "mix/gen_flow_2tor_pipeline_alltoall.py" "run.py" "waf"; do
  if [[ -f "$f" ]]; then
    log "  OK  $f"
  else
    log "  MISSING: $f"
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# Step 2: Generate flow file
# ---------------------------------------------------------------------------
sep
log "Generating flow file: ${FLOW_FILE}"
python3 mix/gen_flow_2tor_pipeline_alltoall.py "${FLOW_FILE}"
NUM_FLOWS=$(head -1 "${FLOW_FILE}")
log "Flow file ready: ${NUM_FLOWS} flows"
log "  pipeline (ring allreduce) : 32 flows  x 2 MB"
log "  all-to-all                : 992 flows x 256 KB"
sep

# ---------------------------------------------------------------------------
# Step 3: Build the simulator (waf build) – skip if already built
# ---------------------------------------------------------------------------
log "Checking/building simulator..."
if [[ $DRY_RUN -eq 0 ]]; then
  ./waf build 2>&1 | tail -5
fi
sep

# ---------------------------------------------------------------------------
# Step 4: Run experiments
# ---------------------------------------------------------------------------
log "Running HPCC-PINT and baseline experiments  (BW=${BW}Gbps)"
sep

# --- HPCC-PINT sweep -------------------------------------------------------
# PINT encodes utilisation into log_base-quantised values carried on packets.
#   log_base closer to 1.0  => finer resolution, more bits => more overhead
#   pint_prob < 1.0         => sampling fraction (reduces per-packet overhead)

# (A) Full precision, all packets: reference PINT configuration
run_sim "hpccPint | log_base=1.05 | prob=1.0 | utgt=95" \
  --cc hpccPint --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" \
  --pint_log_base 1.05 --pint_prob 1.0 --utgt 95

# (B) Coarser quantisation (faster per-packet processing)
run_sim "hpccPint | log_base=1.10 | prob=1.0 | utgt=95" \
  --cc hpccPint --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" \
  --pint_log_base 1.10 --pint_prob 1.0 --utgt 95

# (C) 50% sampling – halves INT bandwidth overhead
run_sim "hpccPint | log_base=1.05 | prob=0.5 | utgt=95" \
  --cc hpccPint --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" \
  --pint_log_base 1.05 --pint_prob 0.5 --utgt 95

# (D) Coarse + 50% sampling – minimum overhead config
run_sim "hpccPint | log_base=1.10 | prob=0.5 | utgt=95" \
  --cc hpccPint --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" \
  --pint_log_base 1.10 --pint_prob 0.5 --utgt 95

# (E) Higher target utilisation (less headroom for CC reaction)
run_sim "hpccPint | log_base=1.05 | prob=1.0 | utgt=99" \
  --cc hpccPint --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" \
  --pint_log_base 1.05 --pint_prob 1.0 --utgt 99

# --- Baseline: vanilla HPCC (full INT, no PINT compression) ----------------
run_sim "hp (vanilla HPCC) | utgt=95" \
  --cc hp --topo "${TOPO}" --trace "${TRACE}" --bw "${BW}" --utgt 95

# ---------------------------------------------------------------------------
# Step 5: Summarise FCT results
# ---------------------------------------------------------------------------
sep
log "Experiment complete.  FCT result files:"
for fct in mix/fct_${TOPO}_${TRACE}_*.txt; do
  if [[ -f "$fct" ]]; then
    count=$(grep -c "^" "$fct" 2>/dev/null || echo "0")
    log "  ${fct}  (${count} lines)"
  fi
done

sep
log "To analyse FCT distributions run (from simulation/):"
log "  python ../analysis/fct_analysis.py mix/fct_${TOPO}_${TRACE}_<cc>.txt"
sep
