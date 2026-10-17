# Iceberg-to-Iceberg Nexmark after boundary optimization

The complete streaming matrix is being rerun after the
[four-candidate investigation](../iceberg-boundary-round2-20261017/README.md).
It uses the unchanged harness and the same settings as the
[previous complete matrix](../iceberg-final-20261017/README.md): two million events,
parallelism four, four-bucket Iceberg inputs/outputs, identifier-field upserts,
Zstd 3, 4 GiB job heaps, one warmup and three alternating measured pairs per cell.
All 120 cells are included: memory/RocksDB state, mini-batching off/on, exact SQL
and the existing labelled expression variants. Snapshot compression remains off.

Each complete cell is retained with output parity, native witnesses, graphs,
plans, final table metadata and checkpoint sizes. Partial progress is recorded in
`progress.json`; final repeated results and comparison against the previous native
implementation will be published here after completion. Supported acceleration
remains on by default. No headline Kafka harness, query or corpus changes are made.
