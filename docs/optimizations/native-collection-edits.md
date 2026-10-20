# Evaluate collection edits on Arrow batches

The existing collection extension can execute verified ARRAY_APPEND signatures
without rebuilding per-row JVM containers. The kernel uses the list offsets to
copy a row's visible child range and its corresponding scalar element through
Arrow's mutable array builder. It dispatches storage types in Arrow rather than
converting values to DataFusion scalars. A NULL container stays NULL; an empty
container becomes a one-element array, and a NULL appended element is retained.
Sliced offsets and DECIMAL precision/scale remain intact.

Flink's getter, string representation, NOT NULL-element and failing-expression
restrictions remain enforced by the planner. This replaces evaluation inside the
existing extension; its configuration is unchanged. Standalone conversion costs
and the accepted regressions remain tracked by
[issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392).

The `array_edits` Criterion suite invokes the production compiled Calc with
independently constructed expected arrays, sliced inputs, empty batches, short
and long arrays and nullable parents/elements/needles. Allocation requests and
shared output buffers are recorded separately from elapsed time. Whole-job
measurements retain both row/Arrow transposes, JNI, startup and the rowwise sink.

## Whole-job measurements

Baseline revision `8798b8e6`; Apple M4 Pro (14 cores), macOS arm64,
JDK 17.0.12 and Flink 2.2.1; release Rust with mimalloc and no RocksDB.
Parallelism one, 2 GiB heap, rowwise source and blackhole sink. Every
cohort uses a fresh JVM, two warmups per engine and five alternating measured
trials; both transposes, normal copies, JNI, planning and startup are included.
No task builds or other tests overlap timing; desktop activity is uncontrolled
and CPUs are not pinned. The baseline ran before these functions were changed.
Times are medians in seconds; negative deltas mean less elapsed time. Baseline
stock controls remain visible to expose drift. These are complete-job results,
including startup, rather than isolated sustained kernel throughput.

| Function | Rows | Prior stock | Prior SF | New stock | New SF | Versus prior SF | Versus paired stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ARRAY_APPEND | 2,000,000 | 0.558645 | 0.674472 | 0.522853 | 0.448795 | -33.46% | -14.16% |


[ARRAY_APPEND trials and ranges](../benchmarks/collection-array-append-2026-10-20.csv),
[fixture and library checksums](../benchmarks/collection-array-append-2026-10-20.json),
and [90 allocation probes](../benchmarks/collection-array-append-allocations-2026-10-20.csv)
retain all observations. This short-array BIGINT case improves on both controls;
it does not establish a gain for every type or width.

Validation: six Flink 2.2.1 runtime SQL tests cover BOOLEAN, all integer widths
and exact precision-38 DECIMAL, including NULL appended elements, empty arrays,
multiple batches, filters and all-filtered batches. The Criterion suite verifies
90 independent expected results; the scalar registry covers opcode 182 in all
nine null/Unicode/batch profiles. Full stateful recovery tests are outside this
stateless scalar change's local validation.

## Prepending elements

ARRAY_PREPEND uses the same checked offset builder, placing the scalar before
the visible child range. It retains the same basic nullable-type admission and
NULL-container/element rules. The SQL and Criterion fixtures independently check
the resulting element order, including sliced and empty inputs.

## ARRAY_PREPEND complete-job evidence

Query: `ARRAY_PREPEND(arr,CAST(idx AS BIGINT))`, 1,000,000 rows, BIGINT, width 64. Method and resources match the complete-job method above.

| Cohort | Median seconds | Range seconds |
| --- | ---: | ---: |
| baseline stock | 0.579178 | 0.571242-0.585162 |
| baseline streamfusion | 1.030478 | 1.010875-1.038272 |
| candidate stock | 0.580153 | 0.567207-0.587232 |
| candidate streamfusion | 0.603231 | 0.594722-0.607353 |

Elapsed time changes by -41.46% versus the prior evaluator and +3.98% versus paired stock Flink. The remaining stock regression is retained under #392; native coverage is not a universal standalone speedup.

[All 20 trials](../benchmarks/collection-array-prepend-2026-10-20.csv), [fixture/checksums](../benchmarks/collection-array-prepend-2026-10-20.json) and [90 allocation probes](../benchmarks/collection-array-prepend-allocations-2026-10-20.csv) retain the evidence. Six per-function SQL tests pass across Boolean, integer widths and exact wide DECIMAL; all nine registry profiles and the independent compiled-Calc fixtures pass.
