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
structs/lists/maps, fixed-width binary/UUID, nanosecond timestamps, dynamic
destinations, Sink V2, and unknown writer-affecting options retain the
stock sink.

The optional `streamfusion-iceberg` module targets the published Iceberg 1.12.0
Flink 2.2 runtime and requires Java 17 or later. Build it with `-Piceberg`.
It is separate from connector-neutral core and uses the published `iceberg`
Rust crate 0.10.1 with Arrow/Parquet 58. No sibling Iceberg build is required.

The first implementation boundary is the normal legacy streaming sink. A
version-gated adapter replaces its task-writer factory before job serialization,
while retaining Iceberg's writer operator, checkpoint flushes, committer, table
loader, and downstream graph. The host task writer traverses borrowed Arrow row
views; the data appender records only accepted positions. Those selections drain
into iceberg-rust's single-file Parquet encoder before releasing each input batch,
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
combined suite. Dedicated captured-channel-state rescaling coverage remains pending
for this sink. `bin/flink-suite.sh iceberg` requires
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
host chaining/max-parallelism boundaries. The legacy committer factory always chains,
so a 1→N topology change needs the same care as stock Iceberg.

Fine-grained accounting for internal decoder/encoder retention and wider recovery
coverage remain pending. Shared Arrow allocations use the normal task off-heap budget;
queued batches and native handles have explicit cleanup. Acceleration remains opt-in
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
Defaults are 2M events, parallelism four, all 23 queries, one warmup and three measured
pairs, all four state/mini-batch modes and the existing labelled expression variants.
Small/subset runs validate the harness; they are not final throughput results.

The harness directly reuses the existing event generator, query definitions, UDF,
lookup fixture and updating-result keys through a test-only corpus artifact. It seeds
three normalized `person`/`auction`/`bid` tables outside timing, with four physical bucket
partitions each, using the original deterministic events and field order. Logical SQL
views retain the original TIMESTAMP(3) rowtimes over Iceberg's microsecond storage
types. Both engines use `SOURCE_WATERMARK()` from Iceberg's ordered file lower bounds. Stripes contain 50,000 original events by default; each
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
normal draining savepoint and waits for job completion. Sources remain continuous and
use their normal checkpoint serializers. For q12, waiting for the last processing-time
window is included; validation conserves every bidder's count instead of comparing
inherently different wall-clock window timestamps.

Outside timing, stock Iceberg reads the committed output and computes a multiset
fingerprint of declared table columns. Delete-position metadata is excluded from payload
comparison. Runtime source counters, native writer-factory witnesses, Rust data-file
footers and Java delete-file footers distinguish native work from fallback. Empty outputs
are reported without inventing file witnesses. Artifacts retain all samples/warmups,
paired validation, plans, stream graphs, table locations, hardware/build metadata and
library hashes. `bin/report-iceberg.py <artifact-directory>` reports medians and ranges
and rejects incomplete/unpaired measured repetitions or missing declared matrix cells.
It also reports the headline sampling convention (first memory sample, best of the first
two disk samples) alongside all repeated medians/ranges. A 10k-event smoke matrix passed
120 stock/native pairs across all 23 queries, four modes and labelled variants; q8 also
passed ten repeated disk/mini-batch pairs with file watermarks. These are correctness
checks, not throughput results. Final 2M-event results remain pending.
