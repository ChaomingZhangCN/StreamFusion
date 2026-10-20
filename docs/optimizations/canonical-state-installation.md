# Copy canonical state directly into managed chunks

Native memory operators export one byte array per key group. Its first four bytes identify
the group; the remaining bytes hold the logical snapshot. Flink retains that payload in managed
keyed state, split into 4 MiB chunks, before its normal checkpoint serializer writes it.

Previously installation first copied the complete payload out of the prefixed array, then
copied that temporary into the managed chunks. Installation now checksums the original payload
range and copies directly into independently owned chunks. This removes one payload-sized
allocation and memory copy per checkpoint. The canonical version, saved bytes, CRC32C, timers,
key-group assignment, backend ownership and checkpoint protocol remain unchanged. This is a
shared memory-state optimization; it does not change Iceberg's Parquet or delete writers.

A production-path Java benchmark uses the real Flink heap keyed-state backend. Fixture setup
and readback are outside timing; installation, CRC32C, managed-state lookups and replacement
are inside. Two independent previous/current JVM pairs each run two warmups and five measured
iterations, with 4 GiB heaps. For 128 groups of 2 MiB, median allocation falls from 512.10 to
256.10 MiB. Previous medians are 76.96 / 74.67 ms and current medians 48.88 / 50.97 ms
(1.57× / 1.46×). For 16 groups slightly larger than 4 MiB, allocation falls from 128.02
to 64.02 MiB and medians from 23.06 / 26.07 to 18.32 / 18.22 ms. The small 4 KiB case
has overlapping ranges. [All observations and method](../benchmarks/iceberg-perf-followups-20261018/README.md)
retain warmups and both JVM pairs.

Run the focused fixture with JDK 17:

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am \
  -Dtest=CanonicalStateBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx4g test
```

The fixture explicitly selects Flink's heap backend before configuring checkpoint storage.
Flink 1.18's legacy default backend also owns storage and ignores a separate storage
configuration; selecting the heap backend keeps large boundary snapshots equivalent across
both supported Flink lines.

The benchmark excludes native operator snapshot encoding, asynchronous checkpoint serialization,
filesystem I/O and table commits. Its speedup must not be substituted for whole-job timings.
Boundary and restore tests cover empty payloads, exact chunk boundaries, multiple chunks,
caller-buffer mutation, replacing a large partition with a small one, and restored timers.

This follows the existing Flink-managed ownership contract. Arroyo persists Arrow state through
asynchronous table writers; StreamFusion keeps Flink's checkpoint container and managed chunk
format for backend migration and rescaling. No native handoff or snapshot format changes.
