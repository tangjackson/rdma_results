Three-way comparison

| Setting | Prefix | Pipeline PG | All-to-all PG | PFC pauses | Ports paused | First pause vs all-to-all |
|---|---|---:|---:|---:|---:|---|
| no_pfc | 2tor_pipeline_persistent_low_baseline | 3 | 3 | 0 | 0 | n/a |
| same_queue_pfc | 2tor_alltoall_pfc_persistent_low_pipeline | 3 | 3 | 108 | 7 | 178.36 us, after_alltoall_start |
| mq_rdma | 2tor_mq_rdma_persistent_low_pipeline | 4 | 3 | 300 | 8 | 98.34 us, after_alltoall_start |

overall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 1106/1728 | 7148.14 | 7897.53 | 508.81 | 752.19 | host_rx@24:85.21Gbps(85.21%) | 5774.04% |
| mq_rdma | 1068/1728 | 7213.63 | 7858.17 | 505.11 | 752.65 | host_rx@24:85.26Gbps(85.26%) | 5827.86% |

pipeline metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 133/384 | 2486.39 | 4145.00 | 41.07 | 94.02 | host_rx@1:2.94Gbps(2.94%) | 1943.21% |
| mq_rdma | 74/384 | 713.46 | 3081.44 | 23.64 | 94.08 | host_rx@1:2.94Gbps(2.94%) | 486.29% |

pipeline_non_hotspot metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 45/276 | 118.21 | 156.65 | 32.47 | 67.58 | host_rx@1:2.94Gbps(2.94%) | -2.86% |
| mq_rdma | 51/276 | 115.58 | 156.79 | 36.80 | 67.62 | host_rx@1:2.94Gbps(2.94%) | -5.02% |

pipeline_hotspot_touch metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 0/0 | n/a | n/a | n/a | 0.00 | host_rx:0.00Gbps(0.00%) | n/a% |
| same_queue_pfc | 88/108 | 3697.39 | 4156.31 | 27.17 | 26.44 | host_rx@24:2.94Gbps(2.94%) | n/a% |
| mq_rdma | 23/108 | 2039.20 | 3661.01 | 7.35 | 26.46 | host_rx@24:2.94Gbps(2.94%) | n/a% |

alltoall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 0/0 | n/a | n/a | n/a | 0.00 | host_rx:0.00Gbps(0.00%) | n/a% |
| same_queue_pfc | 973/1344 | 7785.36 | 7897.90 | 505.77 | 658.16 | host_rx@25:82.27Gbps(82.27%) | n/a% |
| mq_rdma | 994/1344 | 7697.55 | 7858.42 | 517.19 | 658.57 | host_rx@25:82.32Gbps(82.32%) | n/a% |
