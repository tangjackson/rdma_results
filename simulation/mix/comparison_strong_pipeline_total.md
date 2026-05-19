Three-way comparison

| Setting | Prefix | Pipeline PG | All-to-all PG | PFC pauses | Ports paused | First pause vs all-to-all |
|---|---|---:|---:|---:|---:|---|
| no_pfc | 2tor_pipeline_persistent_low_baseline | 3 | 3 | 0 | 0 | n/a |
| same_queue_pfc | 2tor_alltoall_pfc_strong_low_pipeline | 3 | 3 | 844 | 17 | 131.00 us, after_alltoall_start |
| mq_rdma | 2tor_mq_rdma_strong_low_pipeline | 4 | 3 | 2034 | 16 | 92.99 us, after_alltoall_start |

overall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 7639/8064 | 43248.79 | 44165.13 | 1411.72 | 1464.63 | host_rx@17:90.97Gbps(90.97%) | 35440.04% |
| mq_rdma | 7237/8064 | 43178.04 | 44162.90 | 1356.14 | 1462.41 | host_rx@17:90.84Gbps(90.84%) | 35381.89% |

pipeline metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 191/384 | 10414.40 | 11406.95 | 28.63 | 18.08 | host_rx@1:0.57Gbps(0.57%) | 8458.11% |
| mq_rdma | 34/384 | 929.96 | 3747.55 | 6.09 | 18.05 | host_rx@1:0.56Gbps(0.56%) | 664.20% |

pipeline_non_hotspot metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 72/384 | 121.69 | 156.57 | 51.95 | 277.56 | host_rx@1:8.67Gbps(8.67%) | 0.00% |
| same_queue_pfc | 8/180 | 104.22 | 104.26 | 5.88 | 8.48 | host_rx@1:0.57Gbps(0.57%) | -14.35% |
| mq_rdma | 14/180 | 104.29 | 104.43 | 10.29 | 8.46 | host_rx@1:0.56Gbps(0.56%) | -14.30% |

pipeline_hotspot_touch metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 0/0 | n/a | n/a | n/a | 0.00 | host_rx:0.00Gbps(0.00%) | n/a% |
| same_queue_pfc | 183/204 | 10865.11 | 11407.69 | 27.43 | 9.61 | host_rx@17:0.57Gbps(0.57%) | n/a% |
| mq_rdma | 20/204 | 1507.93 | 5539.45 | 3.58 | 9.59 | host_rx@17:0.56Gbps(0.56%) | n/a% |

alltoall metrics
| Setting | Completed | Avg FCT (us) | p95 FCT (us) | Agg goodput (Gbps) | Cluster throughput (Gbps) | Busiest link | Avg FCT vs no-PFC |
|---|---:|---:|---:|---:|---:|---|---:|
| no_pfc | 0/0 | n/a | n/a | n/a | 0.00 | host_rx:0.00Gbps(0.00%) | n/a% |
| same_queue_pfc | 7448/7680 | 44090.81 | 44165.32 | 1410.64 | 1446.55 | host_rx@17:90.41Gbps(90.41%) | n/a% |
| mq_rdma | 7203/7680 | 43377.46 | 44163.03 | 1362.18 | 1444.36 | host_rx@17:90.27Gbps(90.27%) | n/a% |
