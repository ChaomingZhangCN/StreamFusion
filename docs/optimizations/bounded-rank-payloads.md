# Reject bounded rank batches before payload encoding

The fixed append-only Top-N ranker compares each incoming sort key with its partition's current
worst retained row before encoding full payloads. If every partition is already full and every
arrival orders at or behind that bound, the batch cannot change state. Bounds only improve as
append-only input arrives, so this read-only test is conservative even within a batch. Ties retain
arrival order. State bundle completion and memory accounting still run.

Rejected batches avoid Arrow row encoding of their payloads and mutable store probes that would
mark unchanged RocksDB partitions dirty. Partition/sort encoding and initial state hydration remain
inside execution. Mixed batches retain the existing whole-payload encoding path. TTL, changing or
first-observed bounds, ordinary non-time ROW_NUMBER, net-diff and specialized Top-1 retain their
respective existing paths. No retained-state or checkpoint encoding changes.

## Measurement

A release/mimalloc profile of wide-map low-churn Top-N found about 40% of 356 leaf samples in
Arrow row encoding functions. Sampling included input import and one full-oracle pass alongside
22 rank passes; the host could not unwind DWARF frames. It identifies a cost, not copied bytes.

The matched operation benchmark uses prepared BinaryRowData for released Flink 2.2.1 in Java and
prepared Arrow buffers for StreamFusion in Rust. Fresh operator setup, initial Arrow import, final
export and perimeter transposes are outside timing. Production keying, budgeted state, output
allocation and complete map key/value consumption remain inside. Every output payload, input ID,
rank and RowKind is independently checked against stock before measuring. Five warmups and nine
trials cover MAP/MULTISET, high/low churn, 16,384 rows/64 partitions/four entries and 65,536 rows/
4,096 partitions/64 entries. Native batch size is 1,024, rank bound three and budget 1 GiB.

Final combined-build low-churn medians improve 2.59–3.29x over the preceding native kernel;
stock-relative medians improve 2.27–6.90x. High churn shows effectively no optimization benefit,
including a 5.2% regression in the small MULTISET median. The wide high-churn stock/native medians
are close and their ranges overlap; no universal speedup is established. Final combined-build
trials and all variability are retained in the [raw evidence](../benchmarks/results/retained-map-payloads-2026-10-20.json).
Stock controls come from the preceding serial matrix on the same frozen inputs; the later
combined-build pass does not isolate engine drift. These measurements establish the small
functionality gate, not join/first-N throughput or
complete-job performance. The actual preceding planner rejects MAP-bearing retained rows;
its fallback is checked separately from the preceding native kernel's prepared-input comparison.
