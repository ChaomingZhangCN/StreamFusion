# Unnest / Correlate

**Status:** native, with gaps.

`Correlate` is Flink's plan node for lateral cross-application — most commonly `UNNEST`, which
explodes a collection-typed column into rows. By default, only the verified UNNEST shapes below
run natively; other shapes retain fallback (see [Unsupported operators](unsupported.md)). A
default-off JVM table-function prototype is described separately below.

## What's native

Plain **INNER or LEFT** `UNNEST` of a single column of type:

- `ARRAY` (a scalar or `ROW` element, flattened),
- `MAP` (unnested to key + value columns), or
- `MULTISET` (unnested to the element, repeated by its count),

optionally with `WITH ORDINALITY`, and — for the INNER case — including a pushed element filter.

NULL MAP keys and MULTISET elements remain nullable when expanded, even though Arrow marks the
physical map key field as required. NULL values are preserved. Empty and NULL collections emit
no rows for INNER UNNEST and one padded row for LEFT UNNEST.

## What's not

- **Lateral table functions** retain fallback by default. The experimental boundary below is
  still under performance admission.
- **`UNNEST` with a pushed condition the expression engine can't encode**, or **any condition over
  a LEFT `UNNEST`**, falls back.

Either of these falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island). See
[Unsupported operators](unsupported.md) for the full accounting of these cases.

## Experimental JVM table functions

[#237](https://github.com/datafusion-contrib/StreamFusion/issues/237) has an unmerged,
default-off columnar prototype, enabled with the JVM property
`streamfusion.experimental.jvmTableFunctions=true`. It admits synchronous JVM `TableFunction`
definitions carried by Flink's bridging SQL function, INNER/LEFT application, and top-level
STRING, INT and BIGINT input/output columns. Other types, Python/async functions,
and legacy SQL wrappers retain fallback. The ordinary `streamfusion.operator.tableFunction.enabled`
switch can disable this operator.

The operator consumes and produces `ArrowBatch`. It borrows input row views only during the
current batch and invokes the released Flink correlate generator, retaining its conversions,
pushed predicates, NULL/empty behavior, LEFT padding and function lifecycle. Each collected row
is written immediately, before a function can mutate or reuse it. Output batches have the normal
transpose row limit and carry all four RowKinds. No function invocation crosses JNI; surrounding
native operators retain their existing boundaries. A pure column projection is fused into the
output writer, including repeated or empty projections, so discarded correlate columns never
enter an intermediate Arrow batch.

This prototype remains under verification. It does not complete #237's aggregate, OVER or nested
scalar work. Performance evidence includes release jobs with row sources, both transposes, row
sinks, stock Flink and the actual preceding revision; profiles include Java and symbolized Rust
CPU stacks plus sampled Java allocations. Profiled durations are excluded from speed claims.

Released Flink 2.2.1/JDK 17 release jobs measured 2M-row splitting at **1.055 s native versus
1.348 s stock and 1.393 s previous StreamFusion**, and 500k-row split-plus-JSON aggregation at
**1.012 s versus 4.409 s stock and 4.324 s previous**. Each has two warmups and five alternating
pairs, parallelism one, 2 GiB heap and 1,024-row batches. Each trial pair closes its metrics
cluster to release completed aggregate state; cluster boot is outside timing, while planning,
job submission, source/sink and state costs remain inside. Previous-code controls use actual
revision `b965b8bf`; separate JVMs show control drift. See the
[raw trials and profiles](../benchmarks/results/jvm-table-functions-2026-10-15.json).
The same released Flink 1.18.1 configuration measured split at 1.077 s native versus 1.352 s
stock and grouped output at 1.047 s versus 4.440 s. Each has five measured pairs; a separate
previous-revision control on that line is still pending. Wider payloads and additional numeric
shapes remain unverified; these results establish neither all-function acceleration nor completion
of #237.
