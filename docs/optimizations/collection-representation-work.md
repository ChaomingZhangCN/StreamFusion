# Reduce collection representation work

The experimental collection evaluator must preserve Flink's Java string caches, nested row kinds
and generic/binary array copy category. Those details require extra physical children for sensitive
types; nullable primitive-only programs keep the ordinary layout and avoid serializer snapshots
that preserve no additional identity. This also leaves their final arrays as ordinary columnar
views instead of rebuilding generic arrays at the exit transpose.

Represented nested-row vectors retain one reader for the batch lifetime. Each returned row is
copied before that reader can move to the next index, preserving ownership without allocating a
new column-batch wrapper and reusable row wrapper for every nested element.

The initial release profiles use async-profiler CPU sampling at 1 ms and allocation sampling at
1 MiB. They include planning, warmups, stock and StreamFusion trials in one process; counts are
inclusive and overlap. In the append profile, array-writer serializer snapshots appeared in 777
of 30,161 CPU samples and 1,703 of 24,446 allocation samples. In map entries, reader construction
under the nested-row getter appeared in 1,343 of 57,320 CPU samples and 8,424 of 127,310 allocation
samples. These are sampled events, not counts of allocations or copied bytes.
[Profile counters](../benchmarks/extended-functions-profile-2026-10-17.csv) retain the measured scopes.

Unprofiled measurements retain both row/Arrow transposes, two warmups and five alternating trials,
JDK 17, Flink 2.2.1, release/mimalloc, parallelism 1 and a 2 GiB heap on Linux x86-64. The initial and
optimized implementations run in separate JVMs; paired Flink medians are shown to expose drift.
Rows are two million except width-64 sort at 500,000.

| Case | Initial Flink (s) | Initial StreamFusion (s) | Final Flink (s) | Final StreamFusion (s) |
| --- | ---: | ---: | ---: | ---: |
| Append, width 8 | 0.486709 | 0.859151 | 0.488629 | 0.771157 |
| Sort, width 64 | 1.372298 | 1.840761 | 1.342123 | 1.631030 |
| ARRAY constructor | 0.489814 | 0.656635 | 0.506142 | 0.611204 |
| Map entries, width 3 | 1.330885 | 3.411470 | 1.291837 | 3.225086 |
| ROW constructor | 0.507912 | 0.884793 | 0.499804 | 0.801584 |

The reduction is 5–11% of the initial StreamFusion time in these runs. None of these isolated
jobs beats Flink. Both collection and metadata extensions are enabled by default under the
maintainer's explicit acceptance of the measured regressions on 2026-10-17.
[Issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) tracks the remaining performance work. These results do
not establish a sustained-throughput gain. Full comparisons against previous StreamFusion,
longer workloads, reproducible commands and every trial belong in the
[scalar benchmark ledger](../benchmarks/scalar-functions.md#extended-function-coverage).

For profiling, add `-Dsf.extraJvmArgs="-Xmx2g -agentpath:$ASPROF_LIBRARY=start,event=cpu,alloc=1m,interval=1ms,file=collections-%p.jfr"`
to the reproduction command, pointing `ASPROF_LIBRARY` at the profiler installation. Keep the PID
suffix: Maven's separate ORC test execution can launch another JVM even when that execution has
no selected benchmark tests. Profiled timings are excluded from the elapsed-time tables.

## Arrays consumed immediately by UNNEST

The immediate UNNEST consumer can construct homogeneous scalar arrays with released DataFusion's
Arrow kernel: the container's generic/binary identity cannot escape this boundary. Flink's compact
timestamp writer rules still apply. The computed operand is materialized once per input batch,
and projections are folded into expansion before taking columns. Discarded source arrays and
hidden computed operands therefore avoid being copied once per element; repeated selected columns
share one result array. Empty projections retain cardinality and changelog tags remain independent.

TIMESTAMP_LTZ casts keep the JDK's zone rules. UTC needs no callback; other zones use the existing
batch bridge to obtain offsets, cached between transitions, while date/time arithmetic and array
construction stay in Arrow. The total offset lookup does not force the whole expression back into
row evaluation. Other JVM calls retain their failure-order safeguards. Signed Julian-date arithmetic
and the released version's TIME precision policy remain exact, including extreme timestamps.

The initial complete-job prototype lost to stock Flink: width-four append took 0.869 s versus
0.697 s stock, and the two-element integer constructor took 1.001 s versus 0.751 s. CPU sampling
found Arrow list copying in 530 of 622 inclusive UNNEST samples. Pruning removed that work;
constructor and time-zone specialization then removed unnecessary generated collection evaluation.
The intermediate losses remain in the raw artifact.

Flink 2.2.1/JDK 17 release/mimalloc comparisons use row sources, both transposes, production IPC,
row blackhole sinks, parallelism one, 2 GiB heap, 1,024-row batches, two warmups and three alternating
measured pairs. Each job uses its own metrics cluster; setup and the initial explain are outside
timing, while execution-time planning, deployment and complete processing remain inside. Prior
StreamFusion is actual revision `d85b241a`, with production code and native library frozen.
All twelve primary candidate medians beat both stock and prior StreamFusion. Selected results:

| Case | Rows / source width | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: | ---: |
| ARRAY_APPEND | 1M / 4 | 0.655305 | 0.606252 | 0.661601 |
| ARRAY_APPEND followed by COUNT | 2M / 4 | 2.785474 | 1.948070 | 2.805246 |
| Two-element BIGINT constructor | 2M / 2 | 0.764788 | 0.558604 | 0.743767 |
| Boolean constructor | 2M / 2 | 0.736582 | 0.574108 | 0.785867 |
| DECIMAL(20,3) constructor | 2M / 2 | 0.956496 | 0.750907 | 0.961498 |
| TIMESTAMP_LTZ constructor | 2M / 2 | 0.770737 | 0.729007 | 0.812969 |
| New York DATE casts | 2M / 2 | 0.958154 | 0.703143 | 0.959377 |
| New York TIME casts | 2M / 2 | 0.943658 | 0.720897 | 0.984899 |
| DATE casts across daylight-saving intervals | 2M / 2 | 0.957382 | 0.733676 | 0.954090 |
| TIME casts across daylight-saving intervals | 2M / 2 | 0.938308 | 0.800881 | 0.950435 |

Five additional two-million-row cases cover TINYINT, SMALLINT, INT, FLOAT and DOUBLE constructors.
Their native medians are 0.656460, 0.597462, 0.586633, 0.588739 and 0.607857 s, respectively;
each beats both its stock and prior-code control. Floating correctness checks separately retain
NaN payloads and signed-zero grouping identity.

Larger and updating comparisons retain the same resources and trial method:

| Case | Rows / source width / keys | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: | ---: |
| TIMESTAMP_LTZ constructor | 8M / 2 / 65,536 | 2.879445 | 2.672676 | 2.992164 |
| Wide ARRAY_APPEND | 1M / 16 / 64 | 2.089462 | 1.214559 | 2.158536 |
| Retracting LEFT ARRAY_APPEND followed by COUNT | 2M / 4 / 65,536 | 3.072911 | 2.306096 | 3.026504 |

The larger timestamp case has no overlap between candidate and paired stock ranges. A separate
updating LEFT expansion without aggregation retains fallback because Flink inserts the unsupported
`DropUpdateBefore` node. Its failed native-plan assertion and stock warmup are retained; it is not
included among the native speedups.

The normal temporal cases use millisecond IDs; interval cases multiply IDs by one day, spanning
1970–1981 at 4,096 keys. The source still constructs nullable arrays, including empty collections
and NULL elements, even when a particular expression does not read them. Every candidate job
asserts the native operators execute; prior controls assert zero substitutions.

These complete-job durations include startup effects. Timestamp ranges overlap paired stock
ranges, and separate-JVM controls expose drift; these medians are not sustained-throughput
guarantees. The [full trial artifact](../benchmarks/results/computed-unnest-2026-10-18.json) retains
warmups, all measured samples, implementation hashes, configurations and intermediate regressions.

The [CPU profile counters](../benchmarks/results/computed-unnest-profiles-2026-10-18.json) retain
inclusive scopes and profile hashes. In the matching three-million-row timestamp diagnostics,
the generated JVM evaluator's invocation frame falls from 1,029 samples to zero. These profiles
include startup, planning and warmups; their elapsed times do not contribute to the tables.

[Native allocation records](../benchmarks/results/computed-unnest-allocations-2026-10-18.csv)
retain requests and Arrow buffer sharing for all 156 selected fixtures. These describe native
kernel execution with fixture construction excluded; they do not measure bytes copied. The
time-zone fixtures supply offset columns, while complete jobs also include the JDK lookup.

Validation passes 157 selected checks on Flink 2.2.1 and 143 applicable checks on 1.18.1, with
14 version skips, plus eleven release Rust tests and 156 native benchmark fixtures. Coverage includes
both recovery backends and repeated restores, all RowKinds at the expansion boundary, projection
reordering/repetition/cardinality, numeric and temporal edge cases, time-zone transitions, and the
existing 67 expression error-order checks. Host serializer-failure probes use independent clusters
for each execution so a failed TaskManager cannot strand the following job.

Reproduce a complete-job case with the release native library built first:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=ComputedUnnestBenchmark -Dsf.testForks=1 \
  '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=America/New_York' \
  -Dunnest.operand=date_days -Dunnest.rows=2000000 -Dunnest.width=2 \
  -Dunnest.keys=4096 -Dunnest.warmup=2 -Dunnest.runs=3
```

Run profiles serially, outside other builds/tests/timings. Prior-code controls use identical
fixtures in the frozen revision and `-Dunnest.expectNative=false`. Native fixture validation uses
`python3 bin/bench-native.py --bench data_movement --bench collection_expressions --features mimalloc --filter 'unnest|array_constructor' --smoke`.
