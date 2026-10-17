# Iceberg streaming diagnostics

These profiles investigate the q3/q4 regressions observed during the full benchmark.
They are diagnostics, excluded from the final matrix and its geometric means.
Both engines use 2M deterministic events, parallelism four, four-bucket Iceberg
tables, a 4 GB job heap, one-second checkpoints and identical Parquet settings.
The production libraries are the same isolated release+mimalloc files used in the
matrix; hashes and environment are retained with each cohort.

`cpu/` contains one paired trial for q3/q4 in memory/off, memory/on and disk/off,
without a warmup. `wall/` contains one warmup and one measured pair for q4 in
memory/off and memory/on. Each archive retains the thread-labelled collapsed
profiles, plan, graph, final table metadata and checkpoint file sizes. All ten
pairs passed committed-result parity and native/stock file witnesses. The released
async-profiler 2.9 starts before each trial timer and stops before stock readback.
Profiled durations must not be substituted for unprofiled measurements.

The initial unprofiled q4 memory/off medians were 11.204 s stock and 15.590 s native;
individual samples ranged from 2.949–18.503 s and 2.355–16.506 s respectively.
The native median corpus/window phase was faster (1.504 s versus 2.409 s), while
checkpoint/drain waits varied substantially. The CPU diagnostic reproduced a
native memory/on drain wait (3.370 s), and the wall diagnostic reproduced long
waits in both engines. This is not evidence that Rust Parquet encoding is slower.

In the CPU-profiled q4 disk/off trial, the native updating join appears in 49.1%
of all JVM CPU samples and the native backend savepoint in 13.4%; the retained
Java sink flush appears in 2.8%. These are inclusive, overlapping sample shares,
not wall-time percentages. Stock RocksDB decompression appears in 61.0% of its
CPU samples. The next comparison holds native query execution constant and
switches only Iceberg IO, to distinguish connector costs from state/checkpoint work.
