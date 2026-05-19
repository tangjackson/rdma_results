Three-way comparison

| Setting | Prefix | Pipeline PG | All-to-all PG | PFC pauses | Ports paused | First pause vs all-to-all |
|---|---|---:|---:|---:|---:|---|
| no_pfc | 2tor_alltoall_no_pfc_low_pipeline | 3 | 3 | 0 | 0 | n/a |
| same_queue_pfc | 2tor_alltoall_pfc_trigger_low_pipeline | 3 | 3 | 18 | 7 | 166.80 us, after_alltoall_start |
| mq_rdma | 2tor_mq_rdma_low_pipeline | 4 | 3 | 54 | 8 | 102.08 us, after_alltoall_start |

overall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 175/704 | 991.32 | 1284.86 | 191.59 | 748.51 | host_rx@24:68.05Gbps(68.05%) | 0.00% |
| same_queue_pfc | 179/704 | 1011.78 | 1297.58 | 196.86 | 748.05 | host_rx@24:68.00Gbps(68.00%) | 2.06% |
| mq_rdma | 183/704 | 997.50 | 1342.37 | 195.22 | 736.56 | host_rx@24:66.96Gbps(66.96%) | 0.62% |

pipeline metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 44/256 | 294.51 | 1140.80 | 48.17 | 272.19 | host_rx@1:8.51Gbps(8.51%) | 0.00% |
| same_queue_pfc | 41/256 | 225.90 | 1148.95 | 45.09 | 272.02 | host_rx@1:8.50Gbps(8.50%) | -23.30% |
| mq_rdma | 48/256 | 248.72 | 1081.94 | 52.79 | 267.84 | host_rx@1:8.37Gbps(8.37%) | -15.55% |

pipeline_non_hotspot metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 30/184 | 118.24 | 156.76 | 32.99 | 195.63 | host_rx@1:8.51Gbps(8.51%) | 0.00% |
| same_queue_pfc | 32/184 | 117.36 | 156.68 | 35.19 | 195.51 | host_rx@1:8.50Gbps(8.50%) | -0.75% |
| mq_rdma | 37/184 | 115.59 | 156.65 | 40.69 | 192.51 | host_rx@1:8.37Gbps(8.37%) | -2.24% |

pipeline_hotspot_touch metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 14/72 | 672.24 | 1166.62 | 15.33 | 76.55 | host_rx@24:8.51Gbps(8.51%) | 0.00% |
| same_queue_pfc | 9/72 | 611.82 | 1151.39 | 9.92 | 76.51 | host_rx@24:8.50Gbps(8.50%) | -8.99% |
| mq_rdma | 11/72 | 696.51 | 1097.87 | 12.48 | 75.33 | host_rx@24:8.37Gbps(8.37%) | 3.61% |

alltoall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 131/448 | 1225.36 | 1294.78 | 199.47 | 476.32 | host_rx@25:59.54Gbps(59.54%) | 0.00% |
| same_queue_pfc | 138/448 | 1245.26 | 1297.93 | 207.11 | 476.03 | host_rx@25:59.50Gbps(59.50%) | 1.62% |
| mq_rdma | 135/448 | 1263.73 | 1345.25 | 193.13 | 468.72 | host_rx@25:58.59Gbps(58.59%) | 3.13% |
