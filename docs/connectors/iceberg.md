# Apache Iceberg

**Status: partial, experimental streaming source and sink.**

SQL source/sink admission requires `-Dstreamfusion.experimental.iceberg.enabled=true`.
It stays off by default until release end-to-end comparisons demonstrate performance.
The admitted path is the normal legacy sink, unencrypted
format-v1/v2/v3 Parquet, distribution `none`/`hash`/`range`, and verified integer, long, string,
boolean, binary, date, float, double, decimal, time, and microsecond timestamp
columns (with and without a time zone). Java fanout handles partitioned append; a
four-bucket test verifies interleaved Arrow selections and independent rolling.
Java's original partition/key selector assigns destinations through borrowed Arrow
views. Rust splits the batches, then the existing columnar key-group exchange
transports them; no row payload crosses that shuffle. Unaligned checkpoints retain
actual key-group labels for channel-state rescaling. Format-v2/v3 tables with
identifier fields also admit INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE and the
normal upsert option. SQL primary keys must match the table identifier fields;
upsert materialization retains the stock sink. Java retains equality/position
delete encoding, partition validation, and key bookkeeping. For keyed tables,
`range` keeps Iceberg's normal hash fallback. Nested
structs/lists/maps, fixed-width binary/UUID, nanosecond timestamps, arbitrary dynamic
payload generators, normal Sink V2, and unknown writer-affecting options retain the
stock SQL sink. The experimental dynamic DataStream API is described below.

The optional `streamfusion-iceberg` module targets the published Iceberg 1.12.0
Flink 2.2 runtime and requires Java 17 or later. Build it with `-Piceberg`.
It is separate from connector-neutral core and uses the published `iceberg`
Rust crate 0.10.1 with Arrow/Parquet 58. No sibling Iceberg build is required.

The first implementation boundary is the normal legacy streaming sink. A
version-gated adapter replaces its task-writer factory before job serialization,
while retaining Iceberg's writer operator, checkpoint flushes, committer, table
loader, and downstream graph. With unaligned checkpoints enabled, the data exchange
still captures Arrow channel state; the legacy committable edge is aligned explicitly,
following Flink's Sink V2 rule that write results precede their checkpoint barrier. The host task writer traverses borrowed Arrow row
views; the data appender records only accepted positions. Those selections drain
into parquet-rs's single-file Arrow encoder before releasing each input batch,
at memory thresholds, at size checks, and before close. Java's per-row counting
and 1,000-row rolling checks remain in place. Different encoders can produce
different byte sizes and file boundaries under the same target-size policy.

Append range distribution retains the released statistics operator, coordinator,
union state, checkpoint events and range partitioner. Java observes borrowed Arrow
views and chooses each destination; the existing Rust splitter emits selected Arrow
batches. Statistics update the producer's original partitioner before the corresponding
checkpoint barrier, where the stock output gate normally applies them. The normal
custom exchange keeps unaligned channel capture disabled. No file-local sorting is
added. Map/sketch tests cover nullable sort keys in formats 1/2/3, traffic after learned
assignments, SQL entry points, and restoration of learned statistics at parallelism 2→3.
`nativeRangeStatisticsUpdates` exposes applied host assignments.

The experimental `NativeIcebergDynamicSink.append` API accepts Arrow input, one fixed
scalar payload schema and a routing-only callback. The callback returns destination,
branch, spec, distribution, parallelism and key settings; it cannot replace the payload.
The original dynamic processor/updater perform catalog operations, table/branch creation
and Java routing. Both forward and shuffled paths carry target metadata plus Arrow IPC
batches. The normal Sink V2 writers, pre-commit union/aggregator and committer remain.
A wrapper around the original LRU cache decorates each new task-writer factory before
initialization or file creation, changing only data-file encoding. Java writes all deletes.
`nativeDynamicFactories`/`stockDynamicFactories` report target-factory admission.

Resolved target layouts must match the input field names, order, types and nullability;
ID reassignment is allowed. Reordering, widening or filling payload columns requires a
future columnar adapter, so such direct-API contracts are rejected before data-file
writing. Arbitrary Java payload generators and the dynamic SQL connector stay stock.
Unsupported target encoding settings retain the original entire target factory, before
any file opens; there is no mid-file encoder switch. Deferred Java keyed updates retain
incoming equality-field IDs, so those IDs must already match the target table. This is
a released host constraint. Keyed dynamic Arrow writes additionally require
`dynamic-sink.immediate-table-update=true`: the deferred updater and direct output
are unioned, so metadata refresh can reorder updates across those branches. The direct
API rejects deferred keyed contracts rather than changing the requested setting.
Tests cover auto-created tables,
forward/shuffle interleaving, cache size one, immediate/deferred append updates,
partitioned immediate v2/v3 upserts and branches, deferred-keyed rejection, mixed
native/stock target policies, plus continuous
source/dynamic-sink 2→3 savepoint restore with aligned and unaligned checkpoint
configurations. Captured-channel fixtures cover 2→3 and 3→2 append/keyed recovery
for both sink APIs and 1↔2 for the normal sink. They keep the source live, inject a
failure after a completed checkpoint with actual retained Arrow channel state, then
verify changing values and exact committed readback after rescaling. An explicit
non-draining savepoint separates completed streaming writes from the terminal flush.
Published Java dynamic
sink/processor/writer/cache/serializer/aggregation/committer tests retain all 97 cases
in both configurations; these validate delegated host controls alongside native witnesses.

The Rust encoder writes through a bounded reusable JNI byte buffer into the
Java-selected Iceberg output stream. Java owns filesystem configuration and stream
close. Footer metrics use Java's metrics policy, supplemented by native NaN counts;
Java constructs the final data files and commits. Delete encoders remain Java-owned.
For format-v3 sinks, Java also assigns inherited row IDs, records snapshot lineage,
and writes Puffin deletion vectors; the native boundary still covers data files only.
Tests verify snapshot/file row IDs, vector blob references/offsets/lengths, and stock
readback after updates and restarts. Live format upgrades, variant/geometry/geography,
and nanosecond schemas retain their existing exclusions. Native v3 source admission
remains stock because crate 0.10.1 does not read deletion vectors.

Timestamp conversion reuses the common Arrow component representation and checked
unit conversion: negative epochs floor correctly and the full supported calendar
range is preserved. Parquet time units and decimals convert column-wise. Native
read output adapts to Flink's Arrow schema after Iceberg projection/deletes; no
payload rows are reconstructed in Java. Tests cover pre-epoch microseconds,
years 0001/9999, precision-38 decimals, NaN metrics, signed zero, infinity, and nulls.

Writer settings use Iceberg Java's effective precedence and defaults. The encoder
maps uncompressed, Snappy, Gzip, and Zstd (including Java's default Zstd level 3),
row-group/page/dictionary byte targets, page row limits, dictionary enablement,
and Parquet v1/v2 pages. Unknown controls, custom size-check heuristics,
per-column controls, bloom filters, content-defined chunking, and Hadoop Parquet
overrides fall back until verified. Metadata metrics modes remain Java-owned.

Validation includes stock Iceberg file readback across those codecs/page versions,
nullable primitive values, field IDs, split offsets and final sizes, sparse
partition selections, within-batch rolling, empty writer cleanup, native handle
reclamation, and SQL append/update native-file witnesses. Keyed tests verify
committed stock readback across multiple checkpoints and injected restarts, with
aligned and unaligned checkpoints enabled. Interrupted batch processing discards
pending selections and closes native resources before borrowed Arrow views expire.
The adapter preserves Flink's keyed context with a representative integer assigned
to each batch's exact key group. Removing that context reproduced stale committed
state in one of ten unaligned fault runs; retaining it passed the stress run and
combined suite. Captured-channel tests also verify changing keyed values after rescaling.
`bin/flink-suite.sh iceberg` requires
matching published upstream streaming cases on stock and StreamFusion. Native
Criterion's `iceberg_io` suite separately exercises 126 release fixtures across
encoding, routing, task reads, and scalar adaptation;
it does not establish an end-to-end speedup.

The internal source task-reader foundation now translates Java-planned Parquet
file tasks explicitly into the released iceberg-rust reader. Host-backed positional
I/O preserves Java FileIO and authenticates each data/delete read. JNI workers attach
to the JVM for callbacks; seek/read pairs are synchronized and streams close with the
task. Cancellation closes host streams without holding their seek/read locks, allowing
blocked reads to unwind before the fetcher releases its native handle. Field-ID projection, nullable strings, Parquet equality/position deletes
(including projected-away keys), zero-column row counts, sequential task ordinals,
and logical-row resume with changed batch sizes have direct readback tests. Queued
batch recycling and iterator cancellation reclaim prefetched Arrow roots; injected
host read errors propagate and native handles are reclaimed.

Task metadata now carries field-ID-bound source predicates, partition specs/tuples,
and legacy name mappings. Tests cover projected-away filter keys, identity constants
omitted from physical files, renamed no-ID columns, null-inclusive Java comparisons,
NaN/infinity and signed-zero set membership. Native page-index selection remains off:
the released reader rejects fixed-width decimal indexes. TIME and binary predicates
are rejected explicitly; residual-only task reads require the complete source filters
because Java uses residuals for pruning and full source filters for exact row filtering.
Partial-file tasks translate physical Parquet row-group midpoints to the released
reader's cumulative offset model; exact-boundary tests preserve Java ownership with
no omitted or duplicated rows.

The experimental SQL source now retains the released FLIP-27 streaming source,
including snapshot discovery, assigners, watermarks, split serializers and coordinator
state. It emits Arrow batches with logical row offsets and counts rows in source IO
metrics. SQL admission covers plaintext v1/v2 tables, verified top-level scalar types,
projection (including zero columns), case sensitivity, source filters, partition constants,
and name mappings. Host split watermarks retain the original emitter and ordered assigner.
Matching scan contracts can share one source through the normal planner reuse pass.

Each file task has an explicit dispatch contract: admitted plaintext Parquet data/delete
files decode in Rust; non-Parquet/encrypted tasks, TIME equality-delete keys and tasks
without complete exact source filters use the released Java task reader and transpose
its output to Arrow. Source counters `nativeParquetTasks` and `stockFallbackTasks` make
that distinction visible. Mixed Parquet/Avro/ORC tasks preserve file ordinals and restored
logical offsets with changed batch sizes. This fallback is not native read coverage.

Bounded reads, limits, metadata tables, nested schemas, non-null initial defaults,
TIME/binary predicates, unsupported source abilities and format-v3 tables keep the stock
SQL source. Published crate 0.10.1 cannot read deletion vectors. Java-owned streaming
snapshot policy remains unchanged; this does not add CDC semantics to append scans.

Continuous SQL tests verify initial discovery, later appends, filters/projection, final
draining savepoints and committed stock readback on both engines. A partially emitted
source task restores from a savepoint at parallelism 2→3 without duplicate committed
rows. Field IDs survive rename, integer promotion and nullable-column additions,
including historical delete files. Source and sink DataStream APIs accept normal explicit
UIDs (the sink overload takes a host UID prefix); rescaling must also preserve compatible
host chaining/max-parallelism boundaries.

The encoder charges parquet-rs's reported retained memory, an estimate for accumulated
row-group metadata, and its 1 MiB JNI output buffer to the normal task off-heap budget
per open file. Reservations update after each batch and release on close or failure;
a quota failure aborts pending appends. This is retention accounting, not a bound on
all transient codec allocations. The released iceberg-rust writer hides this memory
API, so encoding uses its underlying parquet-rs Arrow writer directly; byte-for-byte
fixtures compare file bytes, rolling-size estimates, NaN counts and split offsets with
the released wrapper across codecs/page versions. Iceberg schema conversion, FileIO
and source task decoding still use crate 0.10.1. Shared Arrow allocations use the same
budget; internal decoder retention remains opaque. Queued batches and native handles
have explicit cleanup. Acceleration remains opt-in
until the release end-to-end comparison establishes performance. The source follows
Comet's explicit task handoff and Paimon's host-FileIO/C Data bridge.
The remaining implementation and final four-partition Iceberg-to-Iceberg Nexmark
comparison are tracked in [issue #373](https://github.com/datafusion-contrib/StreamFusion/issues/373).
All work concerns streaming jobs; batch overwrite and maintenance are outside scope.

## Separate Iceberg Nexmark comparison

`JAVA_HOME`/`PATH` must select JDK 17. Run `bin/bench-iceberg.sh` to build and stage
isolated release+mimalloc libraries and execute the separate streaming comparison.
This leaves the headline Kafka/blackhole benchmark sources and SQL corpus unchanged.
`SF_ROWS`, `SF_PARALLELISM`, `SF_MATRIX_QUERIES`, `SF_WARMUP`, `SF_RUNS`,
`SF_ICEBERG_MODES`, `SF_ICEBERG_VARIANTS` and `SF_ICEBERG_ARTIFACTS` select the run.
`SF_ICEBERG_JAVA_HEAP` defaults to `4g` for both engines. `SF_ICEBERG_DISTRIBUTION`
defaults to `hash`; `range` adds the same append sort order to both output tables.
Defaults are 2M events, parallelism four, all 23 queries, one warmup and three measured
pairs, all four state/mini-batch modes and the existing labelled expression variants.
Small/subset runs validate the harness; they are not final throughput results.

The harness directly reuses the existing event generator, query definitions, UDF,
lookup fixture and updating-result keys through a test-only corpus artifact. It seeds
three normalized `person`/`auction`/`bid` tables outside timing, with four physical bucket
partitions each, using the original deterministic events and field order. Logical SQL
views retain the original TIMESTAMP(3) rowtimes over Iceberg's microsecond storage
types. Both engines use `SOURCE_WATERMARK()` from Iceberg's ordered file lower bounds,
with the headline's four-second delay applied once per split (plus the host's 1 ms
inclusive-boundary adjustment). The benchmark wraps the released split extractor,
leaving split ordering and emission in Java. Stripes contain 50,000 original events by default; each
stripe closes its partition files. Both readers use 128 MiB split/open-file costs and
Iceberg's timestamp-statistics ordered assigner. This avoids combining files into tasks
that traverse event-time stripes out of order. Recomputing a periodic watermark from
decoded rows can advance past unfinished files and produced intermittent late-window
loss in both engines; the file watermark preserves the source's conservative progress
contract. This is an Iceberg-source setting shared by both engines; the original
query corpus and headline harness stay unchanged.

Every trial writes a fresh four-bucket output table. Updating outputs declare the actual
result identifier fields and enable Iceberg upserts; Java writes all deletes. The bucket
source is an identifier field for keyed outputs. A low-cardinality query may occupy fewer
than four buckets; artifacts report the actual count. Both engines receive the same
Zstd level 3, 128 MiB row-group, 1 MiB page, 2 MiB dictionary, 512 MiB file target, hash
distribution and one-second checkpoints. State modes are Flink heap versus native memory,
and Flink RocksDB versus native RocksDB; mini-batching is off/on with two seconds and
50,000 rows. The benchmark records RocksDB engagement, with an explicit q4 assertion.
Arrow IPC shuffle remains enabled through the normal `bench` profile.

The timer includes planning/submission, Java discovery, file IO, decode, query and exchange,
JNI, data/delete encoding, checkpoints and final drain/commit. A benchmark-only delegating
source observer waits for the seeded snapshot to be enumerated, no pending assignments,
and acknowledgements that all assigned splits have been consumed. It then requests a
non-draining savepoint to settle the completed streaming writes, then a normal
draining savepoint and waits for job completion. Both checkpoints remain timed; the
explicit separation avoids merging a pending regular upsert flush with the terminal
flush in the released dynamic aggregator. Sources remain continuous and
use their normal checkpoint serializers. For q12, waiting for the last processing-time
window is included; validation conserves every bidder's count instead of comparing
inherently different wall-clock window timestamps.

Outside timing, stock Iceberg reads the committed output and computes a multiset
fingerprint of declared table columns. Delete-position metadata is excluded from payload
comparison. Runtime source counters, native writer-factory witnesses, Rust data-file
footers and Java delete-file footers distinguish native work from fallback. Stock trials
assert zero native source/writer factories and Java data-file footers. Empty outputs
are reported without inventing file witnesses. Artifacts retain all samples/warmups,
paired validation, plans, stream graphs, table locations, hardware/build metadata and
library hashes. Per-trial observations sample shared task reservations, Arrow allocations
and Linux process RSS at 100 ms intervals during progress polling; these are sampled
peaks, not exhaustive allocation maxima. RSS includes Java and opaque native retention;
baseline RSS and effective maximum Java heap are recorded as well.
`bin/report-iceberg.py <artifact-directory>` reports medians and ranges
and rejects incomplete/unpaired measured repetitions or missing declared matrix cells.

For interruption-safe execution, `bin/checkpoint-iceberg.py <evidence-directory>` runs
the same matrix and retains each complete cell (its warmup and all three paired
measurements). `--push upstream` commits and pushes these checkpoints on the current
branch. Resume with the same command: completed cells are skipped, while an interrupted
cell is rerun in full, including its warmup. Each cohort records its own input snapshot
IDs and environment; both engines within a pair share the same files. Plans, graphs,
table metadata and checkpoint sizes are retained without committing the Parquet corpus
or savepoint payloads. Final reporting requires every declared cell across the cohorts.
`SF_ICEBERG_IO_BASELINE=true` separately compares native query execution with stock
Iceberg IO against native query execution with native Iceberg IO. The stock-IO leg
keeps both row/Arrow transposes where a native query island exists; it is labelled
separately from stock Flink and excluded from stock-Flink aggregates. Observations
also split total time into corpus/window progress, the settling checkpoint, and final
drain/commit. `SF_ICEBERG_DYNAMIC_FORMAT_VERSION=3` selects v3 for the separate dynamic
writer diagnostic; keyed runs with repeated keys require actual Java Puffin deletion
vectors and stock readback. This does not admit native v3 reads.
Exact-query aggregates report geometric mean ratios per mode and nominal corpus-event
throughput; labelled expression variants remain separate.
It also reports the headline sampling convention (first memory sample, best of the first
two disk samples) alongside all repeated medians/ranges. A 10k-event smoke matrix passed
120 stock/native pairs across all 23 queries, four modes and labelled variants; q8 also
passed ten repeated disk/mini-batch pairs with file watermarks. These are correctness
checks, not throughput results. Final 2M-event results remain pending.

### Initial dynamic writer release diagnostic

On 2026-10-16, an Intel i7-12650H Linux host (16 available logical CPUs, 9.7 GiB
physical memory) ran 2M generated Arrow rows through the dynamic sink, with one
warmup and three measured pairs in alternating engine order. Both engines start
with the same 8,192-row Arrow batches: required integer ID and nullable Unicode
payload. The stock generator immediately visits Arrow row views and uses the normal
Java payload serializer/encoder; the native adapter retains batches. One producer,
parallelism four, one TaskManager/eight slots, 8 GiB maximum Java heap, 48 GiB logical
task-off-heap budget, Zstd 3, four buckets per target and one-second checkpoints
apply to both engines. The host metadata cache is 1,000; size-one eviction is covered
by correctness fixtures. Timing includes generation, host routing, transport, JNI,
Java deletes, data encoding and the final draining savepoint/commit. Stock Iceberg
validates every committed ID/payload outside timing; native file footers and released
handles/Arrow reservations are checked. Upserts repeat 100k keys over 2M input rows.

| Dynamic mode | Stock median [range], s | Native median [range], s | Median ratio |
|---|---:|---:|---:|
| Mixed forward/hash append | 2.791 [2.787–2.975] | 2.556 [2.352–3.425] | 1.09× |
| Partitioned upsert | 3.568 [3.557–3.573] | 2.973 [2.965–3.090] | 1.20× |

[All samples and warmups](../benchmarks/iceberg-dynamic-20261016.jsonl) retain the
append variability. This diagnostic uses JDK 17 and release Rust with `mimalloc`.
The core library SHA-256 is `7a218e3b3242b5ebaa18a22848be89b556eb29426b6037b625bbce3b650e3ed5`;
the Iceberg library is `73d14da4d49e056b89e92e0b31678b47c1254d38677a275a62fa6936a63168df`.
To repeat, use profile `iceberg,iceberg-nexmark,bench`, test `IcebergDynamicBenchmark`,
set `SF_ICEBERG_DYNAMIC_BENCHMARK=true` and `SF_ICEBERG_RELEASE_CONFIRMED=true`, and
stage the same release libraries as `bin/bench-iceberg.sh`. `SF_ROWS`, `SF_WARMUP`,
`SF_RUNS` and `SF_ICEBERG_ARTIFACTS` select the run. The final Nexmark matrix remains
pending; the connector stays an explicit opt-in.

### Resource-accounted dynamic comparison

A second 2M-row comparison uses a 4 GiB Java heap, immediate metadata updates, and a
non-draining savepoint before terminal drain for both engines. Other settings and the
input payload match the diagnostic above. One warmup and three alternating measured
pairs compare the prior released Iceberg wrapper (`3c537a19`) with the accounted
parquet-rs writer. Each implementation has its own stock Flink cohort.

| Implementation / mode | Stock median [range], s | Native median [range], s | Median ratio |
|---|---:|---:|---:|
| Previous / append | 2.256 [2.100–2.636] | 2.314 [2.298–2.384] | 0.97× |
| Accounted / append | 2.992 [2.943–3.927] | 3.169 [2.847–3.189] | 0.94× |
| Previous / keyed | 3.406 [3.336–4.535] | 3.179 [2.902–4.156] | 1.07× |
| Accounted / keyed | 3.150 [3.115–3.388] | 3.208 [2.880–3.391] | 0.98× |

[All observations and method](../benchmarks/iceberg-dynamic-accounted-20261016.jsonl)
retain the slower results and warmups. Other development workloads were observed on
the shared host, so these are noisy diagnostics, not an isolated measurement of the
accounting overhead. The current native path does not establish an append/keyed
end-to-end speedup here and remains opt-in. Sampled native task reservations now include
encoder retention (up to 71.9 MiB append / 25.2 MiB keyed); the previous encoder was
unaccounted. Sampled process RSS includes retained JVM heap and grows across trials,
so it cannot be attributed solely to a file encoder. The current Iceberg library
SHA-256 is `1b31d28ff3683b2ae3ec2934dc28ffc43d16af48b6098a5be89b1f68dae676cf`;
the core library is unchanged from the initial diagnostic.

A release async-profiler 2.9 `itimer` diagnostic (2M rows, one append and one keyed
stock/native pair; its timings are excluded from the tables) found 1,699 samples in
stream-task stacks containing a native dynamic control wrapper. Inclusive frames
contained routing/control in 52.9%, native Parquet writes in 9.2%, and host
`prepareCommit` in 22.0%. Table-identifier parsing alone appeared in 10.2%. These
percentages overlap and describe sampled CPU stacks, not wall time.
[Inclusive frame counts](../benchmarks/iceberg-dynamic-profile-20261016.csv) retain
these observations. This identifies
Java routing/metadata allocation and host completion as tuning targets; it does not
justify changing Java's delete or commit semantics.

A separate RANGE q0 release comparison uses 2M events, memory state, no mini-batching,
four buckets, one warmup and three measured pairs. Stock median is 1.028 s
[0.984–1.223], native 0.741 s [0.651–0.763], a 1.39× ratio. This short stateless
workload is dominated by setup/IO/drain costs; learned post-checkpoint range traffic
is established by the live correctness fixtures, not inferred from this timing.
[All RANGE observations](../benchmarks/iceberg-range-20261016.jsonl) include warmups,
exact committed-output parity and native/stock source/writer/footer witnesses.
