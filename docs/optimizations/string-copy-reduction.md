# String copies and intermediate buffers

**Applies to:** the RowData→Arrow entry transpose, the synchronous lookup join, string expressions
in [Calc / Filter](../operators/calc-filter.md), and aligned window aggregation.

## Entry transpose: one copy instead of two

The entry transpose's VarChar writer called `StringData.toBytes()` — a fresh `byte[]` copy — only
for Arrow's `setSafe` to copy those same bytes again. A single-segment heap `BinaryStringData` —
what every string coming out of Flink's row formats already is — now feeds its segment straight
into the Arrow buffer, halving the copies and deleting the per-string garbage on the
string-dominated entry transposes (q9/q18/q19/q20/q21/q22).

A still-lazy `BinaryStringData` — what a rowwise source delivers through `RowRowConverter`, holding
only a `java.lang.String` — additionally skips Flink's char-at-a-time `StringUtf8Utils`
materialization: the JDK's intrinsified `String.getBytes(UTF_8)` produces the identical bytes
(Flink's encoder documents JDK-equivalent output and delegates its edge cases to it) at
near-memcpy speed for ASCII. A 2026-07-12 profile had this char loop at ~9% of q20's whole job.

**Measured:** with the batched BinaryRow keys above, the string-heavy stateless q22 gained +25%
native throughput (1.23x → 1.53x) on the 2M-event generator rung.

## Lookup join: collect straight into the Arrow builders

The sync lookup join stopped defensively copying every looked-up row (`RowDataSerializer.copy`,
~27% of q13's lookup path) plus buffering them in a list. The collector now writes each row's
fields into the Arrow builders at collect time, while the runner's reused row object is still
valid — removing both the copy and the intermediate list.

## Calc: construct only the final string buffers

`CONCAT` unions its inputs' validity bitmaps before copying strings. Batches without NULL results
return DataFusion's output directly, avoiding the full UTF-8 scan that rebuilding `ArrayData` with
a replacement mask would trigger. When there are NULL results, a narrow UTF-8 kernel appends only
valid rows; NULL rows retain the preceding offset and consume no payload bytes. Each child is
evaluated once, including nested expensive or nondeterministic calls. The unchecked Arrow
constructor is valid because the kernel copies complete UTF-8 strings, checks the i32 offset limit,
and preserves the mask length; native tests explicitly validate the resulting Arrow data.

MD5 and SHA-2 use DataFusion's released `md-5` and `sha2` dependencies directly. A batch reserves
one offsets vector and exactly enough value capacity for its non-NULL fixed-width hex strings.
Each digest stays on the stack and its lowercase hex bytes go straight into that values vector,
with no per-row heap allocation, intermediate `BinaryArray`, or MD5 `Utf8View` conversion. The
output is already the `Utf8` representation that the Java boundary requires. Scalar calls retain
scalar results; sliced and empty arrays retain their validity and offset contracts. Invalid arity
returns a DataFusion error instead of panicking at expression construction.

The end-to-end diagnostics and reproduction commands live on the [coverage page](../operators/calc-filter.md#string-concatenation-and-hashes).
They include both row/Arrow transposes and a rowwise sink; their ratios are not isolated kernel
speedups.

Measured on Apple M4 Pro / JDK 17 on 2026-09-08, with release + mimalloc, 2 million rows,
parallelism 1, one warmup and best of three jobs. Both versions include upstream `a9c6ebdc`;
the baseline uses the concatenation/hash kernels introduced by `53ed0c45`. The short-string
diagnostics run together, with the nullable diagnostic in a separate JVM, for both versions:

| Diagnostic | Native before | Native after | Native throughput change |
|---|---:|---:|---:|
| Short `CONCAT` / `CONCAT_WS` | 0.740 s | 0.733 s | No material change |
| MD5 / SHA-2 | 3.261 s | 2.211 s | +47% |
| `CONCAT`, 1 KiB prefix, 75% NULL results | 0.709 s | 0.613 s | +16% |

This table compares native implementations on the same upstream revision. Across patchsets,
Flink's short-concatenation baseline varied by about 18% (0.691 s to 0.566 s), while native
time stayed effectively unchanged (0.736 s to 0.733 s). The resulting 0.94x to 0.77x ratio
change is within the observed run-to-run baseline variance, not a native regression. The
native-to-native hashing comparison above is the evidence for the allocation optimization.

The concatenation cases still lose to Flink end to end. Removing redundant work improves the
nullable case within the native pipeline; it does not establish a standalone concatenation win.

## Window groups: borrow contiguous value ranges

The window update path groups input positions by key/window before folding each aggregate. A
high-cardinality batch often contributes only one row per group; Arrow `take` previously copied
each group's string payload anyway. Group positions arrive in increasing order without duplicates,
so their endpoints determine whether they form a contiguous range. Such groups now share Arrow
slices for primitive and UTF-8 values and apply the same selection to RowKinds. Noncontiguous
groups retain `take`. Nested values also retain it: a generic DISTINCT scalar can keep child
buffers in state, and a slice could retain an entire input batch rather than the group's values.
The partial/checkpoint layout, extrema ordering and NULL behavior remain unchanged.

An async-profiler 4.5 CPU recording of the high-cardinality wide-string prototype attributed
2,288 of 5,319 samples inside native grouped accumulation to Arrow `take` stacks (43%). The
recording includes all three engines and setup, so this is a share of that native subtree,
not whole-job CPU. Release Criterion isolates production update/flush with matching nullable
UTF-8 fixtures and the system counting allocator:

| Rows / keys / string bytes | Before mean (us) | After mean (us) | Allocation requests before / after |
| --- | ---: | ---: | ---: |
| 256 / 256 / 16 | 159.844 | 104.061 | 6,766 / 3,512 |
| 256 / 256 / 256 | 179.101 | 125.392 | 6,774 / 3,520 |
| 4096 / 4096 / 16 | 2644.862 | 1748.207 | 106,104 / 54,028 |
| 4096 / 4096 / 256 | 2920.753 | 2123.312 | 106,112 / 54,036 |

One-key fixtures also improve; noncontiguous 64-key controls are approximately flat, within
about +/-2% in this short run. Allocation requests are not copied-byte or peak-memory counts.
Aggregate output buffers are newly materialized, so zero shared output-buffer bytes does not
mean input selection copied. Criterion excludes JVM/JNI and uses the unaccounted-budget adapter;
the [complete-job comparison](../operators/window-aggregate.md#string-extrema-measurements)
retains normal production costs and both transposes. The
[result artifact](../benchmarks/results/window-string-extrema-2026-10-13.json) contains all native
samples, confidence intervals, allocation probes and end-to-end engine samples.

Rust regression coverage includes offset input slices, contiguous/noncontiguous groups, NULLs,
snapshot continuation and 4,096 one-value TIMESTAMP DISTINCT groups under a 32 MiB state budget.

After #336 merged, the same release Criterion fixtures were compared directly with main
`b2e6e629` rather than the earlier admission-only prototype. The production Java is identical
on both sides. Six matching 4096-row fixtures give:

| Keys / string bytes | Main mean (us) | Slice mean (us) | Main/slice | Allocation requests main / slice |
| --- | ---: | ---: | ---: | ---: |
| 1 / 16 | 106.885 | 79.946 | 1.337x | 120 / 107 |
| 1 / 256 | 120.919 | 86.594 | 1.396x | 120 / 107 |
| 64 / 16 | 179.426 | 180.329 | 0.995x | 2,103 / 2,103 |
| 64 / 256 | 202.997 | 209.685 | 0.968x | 2,111 / 2,111 |
| 4096 / 16 | 2650.422 | 1750.930 | 1.514x | 106,104 / 54,028 |
| 4096 / 256 | 2952.430 | 2064.243 | 1.430x | 106,112 / 54,036 |

The noncontiguous 64-key cases still use `take`: their allocation requests are unchanged,
and the wide control is about 3.3% slower in this follow-up. The improvement is specific to
contiguous selections. Full TUMBLE jobs retain both transposes and outperform matched stock
Flink by 1.15-1.39x in the four follow-up fixtures, while separate-JVM before/after comparisons
have differing stock baselines. The
[follow-up artifact](../benchmarks/results/window-string-extrema-main336-2026-10-13.json)
preserves all samples, intervals and allocation probes, including unfavorable controls.
