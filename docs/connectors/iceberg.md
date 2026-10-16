# Apache Iceberg

**Status: partial, experimental streaming sink; sources use stock Iceberg.**

SQL sink admission requires `-Dstreamfusion.experimental.iceberg.enabled=true`.
It stays off by default until release end-to-end comparisons demonstrate performance.
The admitted path is the normal legacy sink, unencrypted
format-v1/v2 Parquet, distribution `none`/`hash`, and verified integer, long, string,
boolean, binary, date, float, double, decimal, time, and microsecond timestamp
columns (with and without a time zone). Java fanout handles partitioned append; a
four-bucket test verifies interleaved Arrow selections and independent rolling.
Java's original partition/key selector assigns destinations through borrowed Arrow
views. Rust splits the batches, then the existing columnar key-group exchange
transports them; no row payload crosses that shuffle. Unaligned checkpoints retain
actual key-group labels for channel-state rescaling. Format-v2 tables with
identifier fields also admit INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE and the
normal upsert option. SQL primary keys must match the table identifier fields;
upsert materialization retains the stock sink. Java retains equality/position
delete encoding, partition validation, and key bookkeeping. For keyed tables,
`range` keeps Iceberg's normal hash fallback. Append range distribution, nested
structs/lists/maps, fixed-width binary/UUID, nanosecond timestamps, dynamic
destinations, Sink V2, format-v3, and unknown writer-affecting options retain the
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

The Rust encoder writes through a bounded reusable JNI byte buffer into the
Java-selected Iceberg output stream. Java owns filesystem configuration and stream
close. Footer metrics use Java's metrics policy, supplemented by native NaN counts;
Java constructs the final data files and commits. Delete encoders remain Java-owned.
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
Dedicated captured-channel-state rescaling coverage remains pending for this sink. `bin/flink-suite.sh iceberg` requires
matching published upstream streaming cases on stock and StreamFusion. Native
Criterion's `iceberg_io` suite separately exercises 18 release encoding fixtures
and 27 batch-routing fixtures across one, four, and 128 destinations;
it does not establish an end-to-end speedup.

The internal source task-reader foundation now translates Java-planned Parquet
file tasks explicitly into the released iceberg-rust reader. Host-backed positional
I/O preserves Java FileIO and authenticates each data/delete read. JNI workers attach
to the JVM for callbacks; seek/read pairs are synchronized and streams close with the
task. Field-ID projection, nullable strings, Parquet equality/position deletes
(including projected-away keys), zero-column row counts, sequential task ordinals,
and logical-row resume with changed batch sizes have direct readback tests. Queued
batch recycling and iterator cancellation reclaim prefetched Arrow roots; injected
host read errors propagate and native handles are reclaimed.

This foundation does **not yet admit SQL sources**. Residual predicates, partition
constants, name mapping, evolving task-feature admission/fallback, full streaming
recovery, watermark/configuration parity, reader memory accounting, and upstream
source SQL coverage remain required before source admission expands. Native deletion
vectors remain unavailable in published crate 0.10.1. This follows Comet's explicit
Iceberg task handoff and StreamFusion's Paimon host-FileIO/C Data bridge.

The remaining implementation and final four-partition Iceberg-to-Iceberg Nexmark
comparison are tracked in [issue #373](https://github.com/datafusion-contrib/StreamFusion/issues/373).
All work concerns streaming jobs; batch overwrite and maintenance are outside scope.
