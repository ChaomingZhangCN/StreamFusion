# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket Iceberg input tables; four-bucket Iceberg output tables. Completion: configured checkpoint then draining native savepoint.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q7 | memory-off | exact | 5.764 [2.071–10.877] | 17.599 [1.700–23.490] | 0.33× |
| q18 | memory-off | exact | 7.123 [4.024–43.104] | 5.896 [4.222–32.976] | 1.21× |
| q20 | memory-off | exact | 9.454 [3.618–20.210] | 29.069 [19.143–31.788] | 0.33× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| memory-off | 3 | 0.50× | 211,544–346,984 | 68,801–339,220 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| memory-off | 3 | 0.50× | 1.46× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q7 | memory-off | exact | 10.877 | 17.599 | 0.62× |
| q18 | memory-off | exact | 43.104 | 4.222 | 10.21× |
| q20 | memory-off | exact | 9.454 | 19.143 | 0.49× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
