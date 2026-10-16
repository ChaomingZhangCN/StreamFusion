# Unnest / Correlate

**Status:** native, with gaps.

`Correlate` is Flink's plan node for lateral cross-application — most commonly `UNNEST`, which
explodes a collection-typed column into rows. Verified UNNEST shapes and basic synchronous JVM
table functions run inside columnar islands. Other shapes retain fallback (see
[Unsupported operators](unsupported.md)).

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

- **JVM table functions outside the verified STRING/INT/BIGINT boundary**, specialized functions,
  Python/async functions and legacy SQL wrappers retain fallback.
- **`UNNEST` with a pushed condition the expression engine can't encode**, or **any condition over
  a LEFT `UNNEST`**, falls back.

Either of these falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island). See
[Unsupported operators](unsupported.md) for the full accounting of these cases.

## JVM table functions

Synchronous JVM `TableFunction` definitions carried by Flink's bridging SQL function support
INNER/LEFT application with top-level STRING, INT and BIGINT input, output and argument types.
These verified forms are admitted by default. Other types, specialized functions, Python/async
functions and legacy SQL wrappers retain fallback. The ordinary `streamfusion.operator.tableFunction.enabled`
switch can disable this operator.

The operator consumes and produces `ArrowBatch`. It borrows input row views only during the
current batch and invokes the released Flink correlate generator, retaining its conversions,
pushed predicates, NULL/empty behavior, LEFT padding and function lifecycle. Each collected row
is written immediately, before a function can mutate or reuse it. Output batches have the normal
transpose row limit and carry all four RowKinds. No function invocation crosses JNI; surrounding
native operators retain their existing boundaries. A pure column projection is fused into the
output writer, including repeated or empty projections, so discarded correlate columns never
enter an intermediate Arrow batch.

This does not complete [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237): JVM
aggregate/OVER functions, nested scalar results and broader table-function types remain open. Performance evidence includes release jobs with row sources, both transposes, row
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
stock and 1.400 s previous, and grouped output at 1.047 s versus 4.440 s stock and 4.263 s previous.
The grouped gain includes existing native JSON aggregation. Every candidate range is below its
paired stock range; separate-JVM controls still limit attribution between implementations.

Further 2M-row release cases retain the same resources, warmups, five pairs and actual preceding
revision, including both transposes:

| Flink / case | Stock median (range), s | Native median (range), s | Previous median, s |
| --- | ---: | ---: | ---: |
| 2.2.1 / 128-character split tokens | 2.149 (2.147–2.219) | 1.905 (1.889–1.966) | 2.177 |
| 1.18.1 / 128-character split tokens | 2.116 (2.089–2.154) | 1.918 (1.876–1.943) | 2.122 |
| 2.2.1 / INT/BIGINT arguments and results | 0.767 (0.747–0.796) | 0.525 (0.512–0.600) | 0.796 |
| 1.18.1 / INT/BIGINT arguments and results | 0.794 (0.749–0.813) | 0.530 (0.520–0.642) | 0.749 |

The numeric source emits an INT key and nullable BIGINT value; each non-NULL invocation emits
two rows with BIGINT/INT results. The width parameter is unused there. These are representative
function boundaries, not a guarantee that every user function runs faster.

Validation passes 67 selected checks on Flink 2.2.1 and 64 on 1.18.1, with three existing UNNEST
version skips on 1.18.1. Coverage includes mutable output reuse, 8,193 results from one invocation,
all RowKinds, lifecycle errors, repeated/zero-column projection, numeric consumers and a
parallelism-three pipeline with keyed aggregates before and after the function. Four recovery
scenarios per line cover memory/native RocksDB and append/retracting input, each with two forced
restores and consumed-row/memory-release assertions. Unsupported nested output and an explicitly disabled table-function operator retain complete-query
fallback with zero native substitutions. Broader types and other JVM function kinds remain tracked in #237.
