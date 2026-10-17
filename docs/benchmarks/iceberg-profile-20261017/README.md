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

`io-baseline/` retains that six-cell comparison (one warmup and three measured
pairs per cell), with the original canonical-export protocol. Both legs run
native query operators; the baseline retains stock Iceberg IO and the row/Arrow
transposes. All 24 pairs pass. GC logging records 417 pauses, a maximum of
239.344 ms and 3.435 s total. Kernel snapshots at a nominal 200 ms interval
instead repeatedly find Java checkpoint workers in `jbd2_log_wait_commit` and
`folio_wait_bit_common`. Sample counts overlap across threads and are not summed
into elapsed time. The compressed raw GC/kernel logs are retained.

`normal-checkpoint/` is a separate storage/protocol probe, with one warmup and one
measured pair for each of the same six cells. All 12 pairs pass. It requested
JobManager checkpoint storage, but Flink's test MiniCluster supplies a filesystem
checkpoint directory; the inherited metadata location still persists the state.
It is excluded from final results. Flink's `MiniClusterResource` explicitly chooses
filesystem checkpoints for tests, including the existing Kafka headline harness.

The final protocol keeps that shared filesystem storage, waits for an ordinary
configured checkpoint, then drains through a backend-native savepoint. It keeps
checkpoint/commit time and the final disk export measured, while avoiding the
extra canonical migration exports. This changes benchmark completion, not the
production reader, writer, query or state backend. The first filesystem preflight
q4 memory/off pair is 6.741 s stock and 3.624 s native; this single diagnostic pair
is not a repeated performance claim. The final matrix reruns every cell under
the same revised protocol rather than mixing old and new observations.
`filesystem-checkpoint/` retains both validated preflight pairs, including q4
disk/on (46.452 s stock, 2.079 s native), checkpoint IDs/bytes/durations, native
task and file witnesses, and the final savepoint sizes. These remain preflight
observations, separate from the repeated final matrix.
