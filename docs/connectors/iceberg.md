# Apache Iceberg

**Status: partial, experimental streaming sink; sources use stock Iceberg.**

SQL sink admission requires `-Dstreamfusion.experimental.iceberg.enabled=true`.
It stays off by default until release end-to-end comparisons demonstrate performance.
The admitted path is the normal legacy sink, INSERT-only input, unencrypted
format-v1/v2 Parquet, distribution `none`, and verified integer, long, string,
boolean, binary, and date columns. Java fanout handles partitioned append; a
four-bucket test verifies interleaved Arrow selections and independent rolling.
Other types, keyed/updating inputs, dynamic destinations, Sink V2, hash/range
exchange, format-v3, and unknown writer-affecting options retain the stock sink.

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

Writer settings use Iceberg Java's effective precedence and defaults. The encoder
maps uncompressed, Snappy, Gzip, and Zstd (including Java's default Zstd level 3),
row-group/page/dictionary byte targets, page row limits, dictionary enablement,
and Parquet v1/v2 pages. Unknown controls, custom size-check heuristics,
per-column controls, bloom filters, content-defined chunking, and Hadoop Parquet
overrides fall back until verified. Metadata metrics modes remain Java-owned.

Validation includes stock Iceberg file readback across those codecs/page versions,
nullable primitive values, field IDs, split offsets and final sizes, sparse
partition selections, within-batch rolling, empty writer cleanup, native handle
reclamation, and a SQL native-file witness. `bin/flink-suite.sh iceberg` requires
matching published upstream streaming cases on stock and StreamFusion. Native
Criterion's `iceberg_io` suite separately exercises 18 release encoding fixtures;
it does not establish an end-to-end speedup.

The remaining implementation and final four-partition Iceberg-to-Iceberg Nexmark
comparison are tracked in [issue #373](https://github.com/datafusion-contrib/StreamFusion/issues/373).
All work concerns streaming jobs; batch overwrite and maintenance are outside scope.
