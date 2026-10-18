# Floating GROUP BY release comparison

The existing grouped accumulators now admit FLOAT SUM/MIN/MAX and FLOAT/DOUBLE
FIRST_VALUE/LAST_VALUE/SINGLE_VALUE. This comparison includes the RowData source,
RowData-to-Arrow entry, native state/JNI, Arrow-to-RowData exit and rowwise blackhole sink.
It does not replace either edge with a columnar source or sink.

Measurements: 2026-10-14, Apple M4 Pro, 24 GiB RAM, macOS 26.5, Oracle JDK 17.0.12,
released Flink 2.2.1,
parallelism one, release Rust with mimalloc, 8 GiB JVM heap. The previous checkout is main
`8d23be3ed0b4f377715b68a63481f43aca1b07a9`; every newly supported profile there uses
Flink fallback. The same benchmark source is copied into the two isolated checkouts.
Each profile warms up both engines twice and measures five trials, alternating engine order.
Tables report median seconds and stock/new-native speedup. The fallback column is the
previous StreamFusion run, with native planning installed but aggregate admission rejected.

## Two million rows and 64 groups

SINGLE_VALUE instead uses 500,000 rows and one key per row so every group has exactly
one element. All rows use the same FLOAT/DOUBLE schema and one-eighth NULL inputs.

| Profile | Stock Flink | Previous fallback | New native | Stock/native | Stock range | Native range |
|---|---:|---:|---:|---:|---|---|
| `float-reductions-one` | 0.616 | 0.634 | 0.389 | 1.58x | 0.610-0.623 | 0.381-0.394 |
| `float-reductions-two` | 0.510 | 0.474 | 0.296 | 1.73x | 0.495-0.523 | 0.289-0.297 |
| `float-first-last` | 0.599 | 0.590 | 0.356 | 1.68x | 0.580-0.610 | 0.352-0.368 |
| `double-first-last` | 0.566 | 0.588 | 0.368 | 1.54x | 0.555-0.580 | 0.362-0.385 |
| `float-single` | 0.294 | 0.299 | 0.225 | 1.30x | 0.270-0.312 | 0.204-0.233 |
| `double-single` | 0.283 | 0.292 | 0.224 | 1.27x | 0.245-0.325 | 0.208-0.233 |
| `float-single-two` | 0.355 | 0.372 | 0.291 | 1.22x | 0.331-0.391 | 0.268-0.302 |
| `double-single-two` | 0.359 | 0.386 | 0.283 | 1.27x | 0.351-0.362 | 0.262-0.288 |
| `float-distinct-one` | 0.512 | 0.518 | 0.326 | 1.57x | 0.497-0.554 | 0.318-0.350 |
| `float-retract-reductions` | 0.996 | 0.996 | 0.419 | 2.38x | 0.975-1.041 | 0.412-0.426 |
| `float-retract-first-last` | 1.306 | 1.293 | 0.390 | 3.35x | 1.281-1.326 | 0.389-0.392 |
| `double-retract-first-last` | 1.301 | 1.285 | 0.400 | 3.25x | 1.295-1.305 | 0.391-0.407 |

## Eight million rows and 4096 groups

SINGLE_VALUE instead uses two million unique keys. This larger control compares stock and
new native paths; the previous checkout is measured with the smaller configuration only.

| Profile | Stock Flink | New native | Stock/native | Stock range | Native range |
|---|---:|---:|---:|---|---|
| `float-reductions-one` | 2.283 | 1.305 | 1.75x | 2.250-2.292 | 1.289-1.321 |
| `float-reductions-two` | 3.296 | 2.270 | 1.45x | 3.274-3.379 | 2.247-2.290 |
| `float-first-last` | 1.745 | 0.933 | 1.87x | 1.707-1.765 | 0.927-0.939 |
| `double-first-last` | 1.727 | 0.937 | 1.84x | 1.717-1.747 | 0.919-0.954 |
| `float-single` | 1.025 | 0.786 | 1.30x | 1.005-1.193 | 0.708-0.818 |
| `double-single` | 1.379 | 1.003 | 1.38x | 1.280-1.454 | 0.791-1.258 |
| `float-single-two` | 1.611 | 1.338 | 1.20x | 1.524-2.804 | 1.074-1.893 |
| `double-single-two` | 1.712 | 1.159 | 1.48x | 1.426-2.345 | 1.009-1.483 |

## Workload and limits

- Append input is `k=i%keys`, `v=(i%1024)*0.25`, with NULL whenever
  `(i/keys)%8=0`. The larger 4096-key FIRST/LAST fixture has a constant non-NULL
  value per key, so most updates preserve the result; the 64-key fixture changes it.
- Reduction profiles run SUM/MIN/MAX together; the DISTINCT profile replaces SUM
  with SUM DISTINCT. Ordered profiles run FIRST_VALUE/LAST_VALUE together.
- Retraction profiles first retain a fixed -1000 value and a mutable nullable value
  per key, then replace the mutable value through UPDATE_BEFORE/UPDATE_AFTER pairs.
  Groups stay live while SUM, extrema and ordered-value states retract and accumulate.
- Two-phase profiles use 1024-row bundles and 100 ms allowed latency. At 4096 keys,
  most local rows do not combine, exposing shuffle and partial-state costs.
- These are bounded jobs. Small SINGLE_VALUE trials include substantial startup and
  garbage-collection variation; two-million-key trials also have overlapping ranges.
  Faster observed medians do not imply a speedup for every workload or state backend.
- JVM measurements use finite values. SQL/state regressions and the native Criterion
  fixtures separately verify NaN, infinities, signed zero, NULLs, checkpoint restore,
  cardinality failures and FLOAT result widths.

The new native plan must contain the intended global aggregate, local aggregate when split,
and both transposes. SQL recovery tests additionally require consumed-row counters in each
native aggregate stage. Raw measured trials are retained in
[floating-group-by-results.txt](floating-group-by-results.txt).

## Reproduction

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FloatingGroupAggregateBenchmark

SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dsurefire.failIfNoSpecifiedTests=false -Dtest=FloatingGroupAggregateBenchmark \
  -Dfloating.group.rows=8000000 -Dfloating.group.singleRows=2000000 \
  -Dfloating.group.keys=4096 \
  -Dfloating.group.profiles=float-reductions-one,float-reductions-two,float-first-last,double-first-last,float-single,double-single,float-single-two,double-single-two

python3 bin/bench-native.py --package streamfusion --bench operators \
  --features mimalloc --filter floating_group --smoke
```

To reproduce the previous fallback control, copy only the benchmark test into an isolated
checkout of the base revision and set `-Dfloating.group.requireNative=false`. Queries,
input generation, sink and all resource settings remain identical. Smoke checks validate
112 native Criterion fixtures without producing timing estimates; production state updates,
retractions and local flushes are exercised at 256/4096 rows, one/64 keys and both NULL modes.
