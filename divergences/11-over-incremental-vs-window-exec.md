# Event-time OVER: incremental accumulators, not a DataFusion window exec

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo (unbounded frame); Flink (bounded ROWS/RANGE frame — recompute
vs retraction, see below).
**Forced by parity:** partially — the *unbounded-preceding* frame makes the
incremental model strictly necessary for bounded memory; matching Flink's
streaming OVER operator is the prime directive either way.

## Their decision
Arroyo's `window_fn.rs` implements SQL `OVER` (window functions) by **buffering
rows per distinct event-time instant** and running a **DataFusion window
`ExecutionPlan`** over them — full delegation: the operator is a harness that
stages rows into the plan via channels and drains its output on the watermark.
This generalizes to any window function DataFusion supports (`ROW_NUMBER`, `LAG`,
`RANK`, …), because the plan does the work.

## What we do instead — which is what Flink itself does
For the unbounded-preceding RANGE and ROWS running aggregates, we
keep, per partition key, only the **incremental accumulators** (the same
DataFusion `Accumulator`s the window aggregates use — `sum`/`min`/`max`/`count`,
plus the int-semantics ones in [01](01-integer-truncating-avg.md)). Each row folds
into its key's accumulators in rowtime order and emits the running value; raw rows
are discarded once folded. This mirrors Flink's own `RowTimeUnboundedOver` operator,
which keeps an accumulator per key, not the rows. RANGE emits after folding the whole timestamp
peer group; ROWS emits after each row, so tied rows have successive running results. The same
accumulator layout and restore path serve both frames; the plan retains the emission mode.
COUNT(*) projects a non-null BIGINT constant into the existing numeric count kernel.

## Why not delegate to a DataFusion window exec here
The frame is **unbounded preceding** — every row's result depends on *all* prior
rows of its key, across every batch and watermark. A DataFusion window
`ExecutionPlan` computes over a materialized input, so delegating would require
**retaining every row ever seen, per key, forever** (or re-reading it from state
each emission). The incremental accumulator keeps a single running state per key
instead — bounded memory, no recomputation, and byte-identical to Flink, which is
built the same way. So here the Arroyo-style delegation is not just unnecessary
but strictly worse, and it would also diverge from Flink's actual operator.

We still use DataFusion's compute: the accumulators *are* DataFusion's. What we do
not adopt is Arroyo's *orchestration* (buffer rows, run a plan), because Flink's
orchestration (incremental per-key state) is both the parity target and the
bounded-memory choice. This is the same reasoning as
[03](03-incremental-window-merge.md), applied to OVER.

## General window functions — also incremental (investigated, not assumed)
`ROW_NUMBER`, `RANK`/`DENSE_RANK`/`FIRST_VALUE`/`LAST_VALUE` are the same `OverAggregate` rel with `UNBOUNDED PRECEDING` frames
(`ROW_NUMBER` uses `ROWS`). An earlier version of this note predicted they would
**delegate to a DataFusion window exec like Arroyo**. Investigating both sides
showed that is wrong here:

- **Arroyo's `window_fn` is not a model for this.** It buckets rows **per distinct
  event-time instant** and runs a DataFusion window exec per instant — so its
  window functions are scoped to one instant, *not* cumulative across the
  partition. Flink's `UNBOUNDED PRECEDING` `ROW_NUMBER` counts all prior rows of
  the partition; Arroyo's per-instant model does not produce that.
- **A DataFusion window exec would need unbounded retention.** With an unbounded
  preceding frame, delegating to a batch window plan means keeping every partition
  row forever (the partition never closes in a stream). Flink keeps a counter, not
  the rows.
- **DataFusion's incremental evaluator can't be checkpointed.** `PartitionEvaluator`
  *can* run incrementally (`ROW_NUMBER` is `n_rows += 1`), but unlike `Accumulator`
  it exposes **no serializable state**, so delegating the compute would break
  exactly-once. To checkpoint we must own the state anyway.

So general window functions take the **same incremental, own-the-state path** as the
running aggregates: a small per-key state (a counter for `ROW_NUMBER`; rank+last-value
for `RANK`/`DENSE_RANK`; the first / current value for `FIRST_/LAST_VALUE`),
serialized for checkpointing, matching Flink's `OverAggregate` exactly. For
`ROW_NUMBER` this is identical to DataFusion's own evaluator (`n+1`), so nothing is
re-derived; for the others the logic is small and Flink-defined.

## Bounded frame — recompute over the slice, not incremental retraction
A bounded frame — `ROWS BETWEEN n PRECEDING AND CURRENT ROW` (by row count) or
`RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW` (by rowtime interval) — slides:
rows fall off the trailing edge. The unbounded incremental accumulator above cannot
serve it, because `MIN`/`MAX` are not retractable — once the running extreme leaves
the frame there is no way to recover the new extreme without the underlying values.
Flink's `RowTimeRowsBoundedPrecedingFunction` / `RowTimeRangeBoundedPrecedingFunction`
solve this by keeping the frame's rows in `MapState` and a **retractable**
accumulator, retracting each row as it leaves.

We take the model **RisingWave defaults to** instead: keep a per-key sorted buffer of
the rows still reachable by some future frame and **recompute** each emitted row's
aggregate over its frame slice with a fresh accumulator. The result is identical to
Flink (both aggregate over the same frame extent), and recompute sidesteps the
non-retractable-`MIN`/`MAX` problem entirely — the bounded path reuses the same
`RunningAgg` fold the unbounded path uses, just folded fresh per row over the slice
rather than once cumulatively. The slice is `buffer[i-n ..= i]` for ROWS; for RANGE it
is `[first rt ≥ cur−offset, last rt ≤ cur]`, found by binary search, and rows of equal
rowtime share one frame (ending at the last tied row), matching Flink's RANGE
semantics. Cost is O(frame size) per row, fine for a bounded frame; the trailing edge
is evicted (ROWS: keep the last `max(n, 1)` rows per key; RANGE: drop rows older than
that key’s `lastRowtime − offset`) so memory stays bounded. This is an **algorithmic** divergence
from Flink's retraction-based operator with **identical output** — chosen because it is
simpler and correct where incremental retraction would need a MIN/MAX value multiset we
do not keep.

Flink seeds `lastTriggeringTs = 0` in these operators and drops a row whose rowtime is
`<= 0` as late. We preserve that rule and reject timestamps at or before the key's last firing,
independently of the global watermark. The last retained frame row is also the admission marker,
so the existing frame checkpoint preserves it without a second last-trigger state table. A
zero-width ROWS frame retains one marker that never contributes to the next aggregate. RANGE
keeps Flink's event-time cleanup deadline, including its hysteresis, alongside the frame; firing
that timer clears the frame and admission marker. Unbounded frames use the stricter global
watermark comparison. See the OVER coverage page for restore and expiry behavior.

## Scope

Decimal reductions add an exception to bounded recomputation. Its
sum/count fold is retained beside the final frame row in the existing frame
codec, and expired values retract before new values accumulate. SUM can replace a
NULL overflowed buffer on a later value, while SUM0 and AVG retain NULL. Rebuilding
each frame from its remaining rows loses that history. ROWS therefore keeps the
full prior frame for its next retraction; the existing non-arithmetic path keeps
its previous snapshot columns.

Released Flink's RANGE SUM also exposes state-backend iteration order after
decimal overflow. For a 1,000-ms frame with `(time,value)` arrivals
`(100, 10^38-1), (200, -1), (400, 2), (2000, 7)`, the final SUM is 8 on the
heap backend and 7 on RocksDB, while SUM0 and AVG remain NULL. The heap MapState
retracts timestamp groups in HashMap order; RocksDB iterates serialized keys.
The released-operator regression compares both outcomes against the native operator.
The operator retains heap timestamp-map order and distinguishes ordinary checkpoint
from canonical savepoint reconstruction, as described below. Expanded admission,
full SQL and end-to-end performance validation remain required in
[#322](https://github.com/datafusion-contrib/StreamFusion/issues/322).

Running aggregates (`SUM`/`MIN`/`MAX`/`COUNT`/`AVG`; `AVG` via Flink's
`$SUM0`+`COUNT` with the divide on the host), `FIRST_VALUE`/`LAST_VALUE`, and the
window functions `ROW_NUMBER`/`RANK`/`DENSE_RANK`, over the unbounded `RANGE`/`ROWS` frames, the
bounded `ROWS n PRECEDING` frame, or the bounded `RANGE INTERVAL n PRECEDING` frame.
Both event-time and proctime orders are supported; under proctime the operator folds in
arrival order and emits eagerly (no watermark), assigning a monotonic arrival sequence as
the ordering key so the same fold/frames apply — only a proctime bounded-`RANGE` frame
(a wall-clock interval) falls back as non-deterministic. Decimal-valued frame offsets fall back;
this refers to the PRECEDING bound, not decimal aggregate inputs. Ordinary COUNT over decimal,
string and temporal inputs uses Arrow validity and preserves that membership through bounded
frame snapshots and memory/RocksDB transitions. Other runtime non-numeric reductions remain
tracked in [#322](https://github.com/datafusion-contrib/StreamFusion/issues/322).

## Verification
Parity harness: running aggregates and `ROW_NUMBER`, partitioned and not, over
DataStream and Parquet sources, including rowtime ties within the unbounded frame.

### Typed value ownership

Primitive values remain copyable. Decimal values retain their unscaled i128 and declared
precision/scale; timestamps retain milliseconds and fractional nanoseconds. Strings own
immutable UTF-8 text and optional original UTF-16 cache units in a frame sidecar. Running
extrema compare before retaining a replacement, and bounded results share retained payloads.
Memory accounting includes payloads and ownership metadata. This preserves existing numeric
frame layouts rather than widening every primitive value into a heap-allocated scalar.

### RANGE heap membership and restore protocols

The RANGE overflow fixture also checkpoints after timestamp 400 and restores before
the arrival at 2000. On released Flink 1.18.1 and 2.2.1, recovered heap state returns
SUM 7 rather than uninterrupted heap's 8; RocksDB remains 7. Flink's map serializer
reconstructs a `HashMap` using `CollectionUtil.newHashMapWithExpectedSize(size)`.
The released helper uses the default 0.75 load factor and a capacity derived from
the retained count, rather than the old heap table's capacity. Restored iteration
order is therefore part of the contract, alongside live insertion/removal order
and pending timestamp keys. A single timestamp-sort rule cannot preserve all these cases. The operator retains
heap timestamp membership in Java-compatible bucket and tree order, including keys
whose rows have not yet fired. It ranks just the expired groups before removal;
there is no per-result scan of the entire pending map. Retained-row Arrow buffers
continue to own the values. A separate keyed IPC component at the end of the raw
partition carries timestamp membership order, leaving the primitive snapshot layouts
unchanged. The task memory counter includes table, node-arena and free-slot storage.

Canonical heap savepoint restore follows a different released path:
`HeapSavepointRestoreOperation` inserts each user entry into a fresh default `HashMap`.
The operator detects `SavepointKeyedStateHandle` through the restored task state and
rebuilds its native timestamp membership accordingly. The existing OVER restore JNI
signature is unchanged; a separate cold restore hook applies that map rule and
reconciles the memory reservation. A restore-hook failure closes the newly restored
handle. Canonical RocksDB exports retain the host's serialized timestamp order for
the target heap reconstruction.

`OverRangeOrderOracle` produces iteration traces from the running JDK and released
Flink map serializers. Eleven traces cover 1,064 mutation/recovery steps, including
collisions, tree splits/removals, signed hash comparisons, and ordinary/canonical
reconstruction. Both supported released SDK lines produce identical controls on
JDK 17. Actual released OVER operators independently test overflow retractions,
pending timestamp groups, ordinary recovery and all canonical backend pairs.
Full SQL comparisons cover expanded basic-value admission, actual native OVER consumption
and repeated recovery. Production comparisons include both row/Arrow transposes and stock plus
previous-StreamFusion fallback controls; results are recorded on the OVER operator page.

Heap `CopyOnWriteStateMap.get` can copy state while a snapshot is outstanding;
for a map value, `MapSerializer.copy` rebuilds capacity too. Released controls on
both supported lines hold the actual heap snapshot future open or complete it
before further input. Without recovery, the held snapshot yields SUM 7 and the
completed snapshot yields 8 for the same overflow fixture. A second pair records
the final input before capture and triggers the copy on watermark firing instead.

The operator registers small mutable markers in managed heap ValueState per
partition, separately for input membership and string value-map reads. Its namespace is the encoded native partition key and its explicit key
group follows the same BinaryRow hash used for native state redistribution. The
marker serializer's `copy` records the backend's real copy-on-write event; the
native operator observes it before an admitted input or completed group first
touches the partition and rebuilds the affected timestamp/value maps. This uses released
keyed-state APIs and follows snapshot completion through the backend's version
logic rather than approximating it with checkpoint notifications. No aggregate
row or value moves through this marker. Callbacks deduplicate partition keys per
Arrow batch, preserve the backend's key context and use scoped JNI local references.
The native map's actual capacity is re-accounted after the copy.

A cleared frame clears its marker, including after restore. The marker's copy flag
is transient and is never restored as a pending notification. The managed marker
is JVM heap metadata; native table/node/free-slot ownership remains under the task
native off-heap reservation. Release Criterion fixtures exercise the actual heap
backend and production JNI callbacks, including completed and outstanding snapshot futures.

## Cached string representations and bounded views

Released Flink compares two cached Java strings by UTF-16, and otherwise compares their
UTF-8 bytes. Ordinary object copy retains a cache; serialized recovery removes it. Those
comparisons can form a cycle: cached U+1F600 precedes cached U+E000, which precedes binary
U+E100, which precedes U+1F600. Recomputing a fresh bounded-frame minimum would therefore
change a selected value that has not expired. Arroyo/Comet start from Arrow/binary string
values, so this host representation requires a Flink-specific state sidecar.

Metadata-marked Arrow List<UInt16> companions retain original Java units when the boundary
requests cached representations. A Boolean flag cannot preserve two distinct unpaired
surrogates with identical UTF-8 replacement bytes. Reads reconstruct both original bytes
and cache units. Shuffle/checkpoint serialization normalizes caches; RocksDB proctime views
normalize between each output, including multiple rows within one Arrow batch. Released
controls cover cached and binary input, source/computed SQL strings, chained native OVER,
window-to-OVER and both supported Flink lines.

Bounded FIRST/LAST retain selected value/order, value-to-order lists and order-to-value lists
independently of physical frame rows. Raw/persistent checkpoints encode these views as Arrow
IPC in appended binary fields; restore applies serialized key collapse without reconstructing
selection history from the remaining rows. An alias collision can leave a selected value
outside the physical frame. Normal/canonical backend transitions and peer controls preserve
this behavior. Immutable text/cache payloads can be shared with output columns.

Bounded MIN/MAX keep their selected scalar, positive-membership count and counted value map
independently. Retraction rescans only when a winner retires; negative counts balance later
additions without becoming positive membership. Heap scans reproduce JDK bucket and tree-bin
order using the existing timestamp membership implementation. A synthetic Long key stores an
entry identifier in its high word and identifier XOR string hash in its low word, giving the
same raw Java hash. Distinct hashes therefore follow the same signed hash/tree ordering.
Persistent scans use serialized string-key order. Ordinary restore reconstructs capacity from
serialized entry count; canonical heap restore rebuilds a default map in original entry order.
The managed snapshot marker applies actual heap copy-on-write reconstruction to string value
maps as well as timestamp membership, including proctime bounded ROWS. Cleanup drops the marker
and native ownership together. Separating the two markers also preserves snapshot completion
between rowtime input buffering and watermark firing. Original numeric snapshots keep their
existing state layouts.

Large malformed cached-key collisions also expose inherently nondeterministic released behavior.
The retractable FIRST_VALUE helper assigns implicit System.currentTimeMillis order groups:
controlled identical arrivals with tied versus distinct groups select different values after
alias retraction. Identity-ordered full-hash tree entries can serialize different duplicate
counts last, changing restored MIN. Untouched independent stock jobs demonstrate both effects;
deterministic SDK contract tests separately pin the two mechanisms. Native FIRST/LAST use relative
arrival order, and exact-hash ties use stable native identifiers. These cases remain admitted;
there is no unique stock execution output to mirror. Deterministic valid-Unicode tree histories
retain exact parity across uninterrupted execution, normal/canonical restore, and outstanding/
completed heap snapshots. The OVER page links all retained control and performance samples.


## Non-time ROW_NUMBER uses a retained sorted buffer

Released Flink 2.2.1 also plans an ordinary numeric `ORDER BY` as `StreamPhysicalOverAggregate`.
Its exec node chooses `NonTimeRowsUnboundedPrecedingFunction` for ROWS unbounded preceding,
verified against the released planner JAR before broadening StreamFusion admission. A nonmonotonic
arrival changes already-emitted ranks: the inserted row emits +I, then each shifted existing row
emits -U at its previous rank and +U at its new rank. Equal sort keys retain arrival order.
This differs from Top-N's rank-slot replacements and from arrival-order proctime counters.

Arroyo's planner `plan/window_fn.rs` rejects non-windowed input ("Window functions require
already windowed input"); its worker buffers by timestamp and drains a window exec after a
watermark. A non-time Flink rank can never finalize an unbounded partition that way. We instead
reuse StreamFusion's Arrow-row sorted rank state and key-group/checkpoint representation, adding
an explicit row-identity emission mode. No generic OVER time guard is removed. Native operators
still consume and emit Arrow batches, and existing Top-N slot updates remain unchanged.

Admission includes the verified ordinary scalar keys and exact TIMESTAMP/TIMESTAMP_LTZ
precision, one ascending key, and one ROW_NUMBER over ROWS UNBOUNDED PRECEDING
through CURRENT ROW, insert-only input and disabled TTL. NULL placement is explicit; ties remain
stable across physical batches and recovery. Every row remains in state; there is no implicit
Top-N truncation. Accounted off-heap limits fail normally rather than discarding retained ranks.
Updating input, TTL, other order specifications and other non-time analytic functions retain
fallback. Flink 1.18 rejects this query itself, before native admission.

The released heap operator failed an injected restore with a NULL prior accumulator in its
update emission. Native checkpoint validation therefore compares its twice-restored output
with an uninterrupted stock run, independently verifying restored offsets and source cleanup;
it does not claim parity between two successfully restored host/native jobs.

DATE, TIME and BOOLEAN OVER value state stores the selected physical 32-bit value plus
its logical Arrow type, following the existing typed decimal/timestamp selection pattern.
Frame rows and snapshots retain logical DATE/TIME/BOOLEAN scalar fields rather than exposing
an integer carrier. Readers and restored frames fold into the same native state; no new JNI
or checkpoint framing is introduced. Arroyo's watermark-finalized window exec cannot replace
Flink's immediate running and bounded-frame selection, as described above.
