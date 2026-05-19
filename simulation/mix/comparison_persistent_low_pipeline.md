Three-way comparison

| Setting | Prefix | Pipeline PG | All-to-all PG | PFC pauses | Ports paused | First pause vs all-to-all |
|---|---|---:|---:|---:|---:|---|
| no_pfc | 2tor_alltoall_no_pfc_persistent_low_pipeline | 3 | 3 | 0 | 0 | n/a |
| same_queue_pfc | 2tor_alltoall_pfc_persistent_low_pipeline | 3 | 3 | 108 | 7 | 178.36 us, after_alltoall_start |
| mq_rdma | 2tor_mq_rdma_persistent_low_pipeline | 4 | 3 | 300 | 8 | 98.34 us, after_alltoall_start |

overall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 385/1728 | 6100.13 | 7634.82 | 171.86 | 599.73 | host_tx@24:87.97Gbps(87.97%) | 0.00% |
| same_queue_pfc | 1106/1728 | 7148.14 | 7897.53 | 508.81 | 752.19 | host_rx@24:85.21Gbps(85.21%) | 17.18% |
| mq_rdma | 1068/1728 | 7213.63 | 7858.17 | 505.11 | 752.65 | host_rx@24:85.26Gbps(85.26%) | 18.25% |

pipeline metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 90/384 | 1956.17 | 4093.49 | 27.38 | 94.68 | host_rx@1:3.03Gbps(3.03%) | 0.00% |
| same_queue_pfc | 133/384 | 2486.39 | 4145.00 | 41.07 | 94.02 | host_rx@1:2.94Gbps(2.94%) | 27.11% |
| mq_rdma | 74/384 | 713.46 | 3081.44 | 23.64 | 94.08 | host_rx@1:2.94Gbps(2.94%) | -63.53% |

pipeline_non_hotspot metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 40/276 | 119.97 | 156.73 | 28.86 | 69.77 | host_rx@1:3.03Gbps(3.03%) | 0.00% |
| same_queue_pfc | 45/276 | 118.21 | 156.65 | 32.47 | 67.58 | host_rx@1:2.94Gbps(2.94%) | -1.46% |
| mq_rdma | 51/276 | 115.58 | 156.79 | 36.80 | 67.62 | host_rx@1:2.94Gbps(2.94%) | -3.66% |

pipeline_hotspot_touch metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 50/108 | 3425.13 | 4123.46 | 15.21 | 24.91 | host_rx@24:3.03Gbps(3.03%) | 0.00% |
| same_queue_pfc | 88/108 | 3697.39 | 4156.31 | 27.17 | 26.44 | host_rx@24:2.94Gbps(2.94%) | 7.95% |
| mq_rdma | 23/108 | 2039.20 | 3661.01 | 7.35 | 26.46 | host_rx@24:2.94Gbps(2.94%) | -40.46% |

alltoall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 295/1344 | 7364.39 | 7634.98 | 158.68 | 505.05 | host_tx@24:84.94Gbps(84.94%) | 0.00% |
| same_queue_pfc | 973/1344 | 7785.36 | 7897.90 | 505.77 | 658.16 | host_rx@25:82.27Gbps(82.27%) | 5.72% |
| mq_rdma | 994/1344 | 7697.55 | 7858.42 | 517.19 | 658.57 | host_rx@25:82.32Gbps(82.32%) | 4.52% |
