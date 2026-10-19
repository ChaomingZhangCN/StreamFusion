# Unnest / Correlate

**Status:** native, with gaps.

`Correlate` is Flink's plan node for lateral cross-application — most commonly `UNNEST`, which
explodes a collection-typed column into rows. Verified UNNEST shapes and basic synchronous JVM
table functions run inside columnar islands. Other shapes retain fallback (see
[Unsupported operators](unsupported.md)).

## What's native

**INNER or LEFT** `UNNEST` of one collection operand of type:

- `ARRAY` (a scalar or `ROW` element, flattened),
- `MAP` (unnested to key + value columns), or
- `MULTISET` (unnested to the element, repeated by its count),

optionally with `WITH ORDINALITY`, including pushed element predicates for INNER and LEFT joins.

The operand may be a direct column, a nested array field, or an encodable computed ARRAY such as
`ARRAY[values_array[1], id]` or `ARRAY_APPEND(values_array, id)`. Sequential UNNESTs compose. LEFT
`ON TRUE` followed by a separate encodable WHERE filter also composes; the WHERE remains a filter
after expansion.

Computed MAPs, STRING/ROW/MAP-bearing ARRAYs and LEFT element predicates use the released Flink
correlate generator inside an Arrow-input/Arrow-output operator. This preserves the host's map
constructor ordering, duplicate-key policy, generic/binary container behavior and LEFT padding
after predicate evaluation. It covers verified builtin expressions; it does not expand admission
for user functions. The existing Arrow take kernel remains the first choice for ordinary direct
collections and native scalar ARRAY operands. The
`streamfusion.experimental.collectionExpressions` switch also controls computed collection
operands on the generated route; `streamfusion.operator.unnest.enabled` controls both routes.

The source transpose preserves collection representation and can prune unused input columns before
generated expansion. Generated collection Calc results and other generated expansions are verified
inputs; DropUpdateBefore, mini-batch assignment and watermark assignment preserve that contract.
Complex nested outputs require a verified preserving consumer or a final row boundary. Unverified
state, ordering, exchange and native-sink boundaries retain whole-query fallback. Scalar expanded
values can feed ordinary native aggregates, including checkpoint/recovery.

Scalar ARRAY constructors consumed here use the native constructor for resolved homogeneous
integer, floating, Boolean, decimal, DATE, TIME and timestamp elements. FLOAT/DOUBLE values retain
their primitive bits, including NaN payloads and signed zero. Compact timestamps (precision at
most three) discard hidden nanoseconds as Flink's binary-array writer does; higher precision keeps
both timestamp components. Timestamp elements remain a single column despite their physical
two-component struct. Ordinary collection projections retain their existing evaluation policy.

Within the computed operand, BIGINT `TO_TIMESTAMP_LTZ(value, 3)` preserves Flink's SQL epoch bounds
and NULL results outside them. TIMESTAMP_LTZ-to-DATE/TIME casts run in Arrow. Non-UTC casts obtain
offsets from JDK ZoneRules through the existing batch bridge, caching each transition interval;
UTC requires no callback. This retains the worker JDK's historical and daylight-saving rules.
TIME casts retain
the released version's precision behavior: 1.18 keeps milliseconds, while 2.2 truncates them to
the target precision. Other temporal forms retain generated Flink evaluation.

Pure projections, including repeated, reordered or empty projections, are folded into expansion.
Only selected columns are taken for each element; source collections and hidden computed operands
that the consumer discards are not copied into the expanded batch. Pushed INNER filters retain
their predicate inputs. The changelog tag is carried independently of the selected SQL columns.

NULL MAP keys and MULTISET elements remain nullable when expanded, even though Arrow marks the
physical map key field as required. NULL values are preserved. Empty and NULL collections emit
no rows for INNER UNNEST and one padded row for LEFT UNNEST.

Without ordinality, NULL ROW elements are skipped as in Flink. With ordinality, a nullable ROW with
one nullable field retains its NULL element and original position. Other nullable ROW shapes with
ordinality retain fallback because released Flink can fail with an output-arity or serializer-type
error for NULL ROW elements; they must not silently produce a different result natively.

## What's not

- **JVM table functions outside the verified STRING/INT/BIGINT boundary**, specialized functions,
  Python/async functions and legacy SQL wrappers retain fallback.
- **Types outside the Arrow bridge, user expressions outside their existing admission gates,
  or an unverified collection producer/consumer boundary** retain fallback. A builtin condition
  can use generated evaluation when it cannot use the Arrow expression engine.
- **Nullable ROW elements with ordinality** retain fallback unless the element has exactly one
  nullable field, as described above.

Flink 1.18 cannot plan `WITH ORDINALITY`. Both released lines reject some TIME(3) casts inside
UNNEST during host planning (TIME(3)/TIME(0) type assertion); native substitution does not bypass
that error. Multi-operand UNNEST also remains subject to the released host planner's restrictions.
Flink 1.18 cannot plan nontrivial LEFT element predicates or some nested MAP/ROW constructor
signatures. Its nonliteral TINYINT-key MAP constructor can fail in generated initialization.
Flink 2.2 rejects LEFT predicates referencing outer columns (for example `ON element > id`).
These host planning/initialization failures remain unchanged; they are not acceleration gaps.

An unsupported shape falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island). See
[Unsupported operators](unsupported.md) for the full accounting of these cases.
An updating source may insert [DropUpdateBefore](drop-update-before.md) before UNNEST when its
consumer omits update-before images. That surrounding filter runs columnarly inside the native
chain and no longer prevents an otherwise admitted UNNEST from accelerating. Its page records
exact changelog/recovery validation and complete-job comparisons against the preceding fallback.

## Generated builtin validation and performance

The generated expansion and its shared conversion paths pass 324 selected checks on Flink 2.2.1;
Flink 1.18.1 verifies 265 checks with 59 version skips. Coverage includes constructor ordering
and duplicate keys, NULL/empty containers, LEFT predicates, repeated/empty projections, exact
getter failures, ROW equality across chained expansions, watermark-carried caches and all four
RowKinds. COUNT/SUM consumers survive two restores on both memory and native RocksDB state.
Owned binary ROW and UTF-16 tests cover IPC, source mutation/closure, allocation failure and
late cache creation.

All 17 final complete-job profiles beat both stock Flink and actual preceding revision `8eb53c8b`
on median time, retaining runtime row sources, both transposes, production IPC and row sinks.
Flink 2.2.1/JDK 17 examples include numeric MAP expansion at **1.325 s versus 2.753 s stock**,
ROW-valued MAP at **3.258 s versus 3.870 s**, and LEFT element predicates at **0.677 s versus
1.124 s**, each with two million source rows and four entries. The wider updating STRING MAP
initially lost; checked bulk UTF-16 copies and lazy scalar cache sidecars reversed that loss.
Its four-million-row confirmation is **7.402 s versus 7.693 s stock and 7.655 s prior**, with
separated trial ranges. Smaller ranges can overlap, and separate-JVM controls expose drift.
See the [method, full comparisons and profiles](../optimizations/collection-representation-work.md#generated-builtin-expansion-and-owned-binary-row-output)
and [all 540 unprofiled jobs](../benchmarks/results/generated-unnest-2026-10-18.json), including
the rejected intermediate implementations. Generated evaluation remains JVM work inside the
Arrow island; these results do not imply every scalar expression runs in Rust.

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
