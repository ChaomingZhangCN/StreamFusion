# Rust-side feature admission, 2026-10-19

Small functionality now starts in **Rust on prepared Arrow** for StreamFusion and **Java on
prepared BinaryRowData** for stock Flink. Both consume the same logical rows and output fields.
The [earlier Java-wrapper audit](feature-admission-2026-10-19.md) includes JNI/C Data handoffs
and remains a separate diagnostic. Complete jobs still include their full boundaries.

## Changes and decisions

String MIN/MAX previously gathered every selected value into a temporary Arrow array for each
noncontiguous group, then asked DataFusion to scan it for an extreme. HOP repeated that work
across overlapping windows. The production fold now scans the original row indices and passes
only the winning value as a retained one-row slice to the existing accumulator. All-NULL groups
pass an empty typed slice. Contiguous groups keep their existing slice path. The DataFusion
accumulator, lexical UTF-8 ordering, state schema and checkpoint representation are unchanged.

String extrema now default native for all otherwise supported shapes. The string option remains
an explicit opt-out. Fixed processing-time and legacy windows reuse the measured fixed-window
kernel and retain their existing timer/property parity checks. The separate session kernel also
passes the Rust-side admission boundary in the follow-up measurements below.
Standalone DropUpdateBefore also passes the Rust gate and defaults native; its explicit false
override retains stock. Shared stateful and temporal UDFs default native under the maintainer's
accepted UDF performance exception, with no faster-than-Flink claim.

## Timing and correctness boundary

`FeatureAdmissionBenchmark` extracts Flink's generated production operator and exports its
prepared Arrow counterpart plus exact batch lengths. Key-group boundaries can shorten batches.
The Rust `feature_admission` example imports the Arrow file and prepares the native API's
columns before timing: timestamp components become exact millisecond buffers, value columns
retain their Arrow buffers, and COUNT(*) gets its non-NULL constant column. These are input
representation preparation, outside the operation clock.

Each instance receives the same first-batch prefix before timing; window state retains its
matching contribution. Rust's `Instant` surrounds production update/filter, window assignment,
state/accounting, final watermark flush, output checksums and output release. Planning, setup,
input preparation/import, initial/final JNI/C Data, parity-file writing and perimeter transposes
are excluded. Actual callbacks inside an operation would remain included. No JVM callback is
needed by the measured Rust kernels.

Both consumers hash row kinds, keys, numeric values, string byte lengths and both timestamp
components. First-warmup full value/RowKind multisets are checked against Rust's Arrow output;
later Java trials also check their checksum/count against that full-value warmup. Fixed-schema
Java checksum consumers remove repeated schema dispatch for the two measured shapes. Earlier
generic-consumer cohorts remain retained. Native output is completely materialized by the
production operator, including all strings. These measurements exclude checkpoint I/O and
full-pipeline routing. They are not Nexmark or complete-job speedups.

Environment: Intel Core i7-12650H, 16 logical CPUs, WSL2 Linux x86-64, JDK 17.0.20.1,
released Flink 2.2.1/1.18.1, release Rust/mimalloc with one codegen unit, Java heap 2 GiB,
native state budget 1 GiB. Timed jobs run serially without concurrent builds/tests. Rust and
Java use separate processes; five warmups precede seven or nine retained samples.

## Results

For 262,144 rows, 4,096 keys, 128-byte strings and maximum 1,024-row batches:

| Operation | Stock Flink 2.2 (ms) | Rust before (ms) | Rust after (ms) |
| --- | ---: | ---: | ---: |
| String HOP, five overlaps | 184.1–206.3 | 156.1 | 104.9 |
| DropUpdateBefore, four input row kinds | 7.49 | — | 2.97 |

The HOP change improves its own Rust baseline **1.49×**. Stock HOP medians are 192.6/184.1 ms
in the two generic-consumer cohorts and 206.3 ms in the final fixed-schema cohort; every trial
is retained, including JIT drift and outliers. The unchanged Rust kernel already beat those
stock controls once the correct boundary excluded its Java wrapper. DropUpdateBefore's Rust
implementation is unchanged: its new default follows the corrected timing boundary.

Additional medians:

| Profile | Stock (ms) | Rust (ms) |
| --- | ---: | ---: |
| HOP, Flink 1.18, high-cardinality profile above | 188.62 | 110.42 |
| HOP, 131,072 rows, 64 keys, width 64 | 39.37 | 11.66 |
| HOP, high-cardinality profile, maximum batch 4,096 | 183.51 | 74.82 |
| DropUpdateBefore, 131,072 rows, width 64 | 3.32 | 0.84 |
| DropUpdateBefore, Flink 1.18, high profile | 7.38 | 2.97 |

The low-cardinality and larger-batch HOP table uses its original generic Java consumer.
The later specialized low-cardinality cohort has substantial JIT drift (34.2–102.1 ms,
median 86.8 ms); it is retained rather than selected as a larger speedup. DropUpdateBefore
has no key-group partitioning, so both released lines use identical rows and batch lengths.
All Rust output files pass complete logical value and RowKind multiset comparison with Flink.

The separate Flink 2.2 RocksDB **Java-wrapper** HOP diagnostic measures 391.3 ms stock versus
381.4 ms native. Ranges overlap, so this is a modest, workload-specific result, not a Rust-only
or universal persistent-state claim. No standalone Rust RocksDB measurement is claimed here.

## Profiles and allocation evidence

Separate software CPU profiles at 499 Hz cover three warmups and 40 operations. They include
input loading/setup, so self-sample counts are not percentages of timed execution. Before
selection, `arrow_select::take::take_bytes` has 257 of 3,406 self samples; afterwards it has
zero of 2,268. Grouping, scalar state sizes, slicing and allocation remain. The local minimal
perf build lacks userspace DWARF unwinding; executable file offsets are resolved through ELF
load segments and symbol address/size ranges. Two before/one after executable samples remain
unresolved. Profiled durations are excluded from performance claims.

The release Criterion fixture exercises TUMBLE/HOP/CUMULATE, 256/4,096 rows, 1/64/batch-sized
keys, nullable 16/256-byte UTF-8, Unicode/NULs and all-NULL groups. For HOP/4,096 rows/64 keys/
width 256, time falls from 1.155 ms to 0.570 ms, approximately **2×**; its reported change
interval is −50.90% to −49.92%. Allocation requests fall from 10,076 to 6,247 (38%), and
requested bytes from 10,920,354 to 1,793,986 (84%). These are allocation requests, not copied
bytes or RSS. Output buffer accounting remains 171,608 new bytes and zero shared bytes.
All 36 fixtures retain before/after allocation reports and check full extrema independently.

[The artifact](results/rust-feature-admission-2026-10-19.json) retains all warmup/measured
trials, earlier consumer variants, Criterion estimates/samples, all 72 allocation probes,
profile summaries, binary/library/input hashes, configuration and limits.

## Validation and reproduction

The affected selections pass 312 checks on Flink 2.2.1 and 305 with seven expected capability
skips on 1.18.1, including the 79 UDF checks on each line. String ordering, NULLs, filters,
precisions/broad years, retained buffer aliases, explicit overrides, native execution and
checkpoint/recovery remain checked. The 26 native window tests and all 36 Criterion fixtures
pass. Production Rust changes only the string selection fold.

Build with JDK 17 and a release native library, then prepare/time the Java half:

```sh
mvn -pl streamfusion-runtime -am test-compile -Pbench -DskipTests
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test -Pbench \
  -Dtest=FeatureAdmissionBenchmark -Dsf.testForks=1 -Dsf.extraJvmArgs=-Xmx2g \
  -Dadmission.cases=string-hop -Dadmission.engines=stock \
  -Dadmission.rows=262144 -Dadmission.keys=4096 -Dadmission.width=128 \
  -Dadmission.warmup=5 -Dadmission.runs=9 -Dadmission.exportArrow=/tmp/hop.arrow
cargo run --manifest-path native/Cargo.toml --release --locked -p streamfusion \
  --features mimalloc --example feature_admission -- \
  /tmp/hop.arrow hop 5 9 /tmp/hop-output.arrow
```

Run the same Java command with `-Dadmission.rustOutput=/tmp/hop-output.arrow` to validate full
output parity. Use `drop` as the Java case/Rust operation for changelog filtering. Rust also
supports `tumble` and `cumulate`. Flink 1.18 uses `-Pbench,flink-1.18` on the Maven commands.
Only explicit Java `admission.engines=native`/`both` modes time native Java wrappers; those
are diagnostics. Criterion reproduction:

```sh
cargo bench --manifest-path native/Cargo.toml --locked -p streamfusion \
  --bench window_string_extrema --features mimalloc -- --test
```

## Final string-window toggle audit

The remaining session/processing-time/legacy restriction was a performance gate. Fixed windows
reuse the measured production fold regardless of SQL syntax or clock domain; existing tests
exercise controlled processing-time firings. Event-time SESSION uses a separate production
aggregator, now measured with the same Arrow/Rust versus RowData/Java boundary. No session
kernel optimization was needed. These measurements flush at the final watermark, do not include
checkpoint I/O and do not establish sustained periodic-watermark or processing-time throughput.

Legacy Flink group-window operators append unrequested rowtime/proctime properties. The session
fixture consumes the six requested SQL output columns on both engines. Initial generic-consumer
results and the rejected eight-column parity attempt are retained in the evidence; the corrected
six-column full-value/RowKind comparison passes. Stock still executes its normal production
operator, including its extra properties; only output consumption ignores unrequested fields.

| SESSION profile | Stock Flink 2.2 median (ms) | Rust median (ms) |
| --- | ---: | ---: |
| 131,072 rows, 64 keys, width 64 | 133.17 | 10.42 |
| 262,144 rows, 4,096 keys, width 256 | 364.59 / 433.73 | 45.60 |

The two retained high-cardinality stock cohorts show JIT/GC variation; both remain in the
[evidence](results/string-window-defaults-2026-10-19.json). The unchanged session implementation
passes the corrected gate by 8.0–9.5× at high cardinality and 12.8× at low cardinality.
These compare operation boundaries, not complete jobs. All otherwise-supported string shapes
therefore default on; explicit false retains stock. Numerical compatibility overrides and
independent implementation experiments are outside this feature-gate change.

The matching high-cardinality Flink 1.18 cohorts measure 371.47/364.11 ms stock and
45.16 ms Rust, also with complete value/RowKind parity. Default-admission regression
selections pass 105 cases on 2.2.1 and 101 on 1.18.1 with four expected compatibility skips,
including explicit-false fallback, legacy/session SQL, recovery and processing-time timers.
