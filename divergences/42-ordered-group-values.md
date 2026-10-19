# Ordered grouped values

Arroyo's aggregation operators use DataFusion's partial/final plans inside window
operators. They do not supply a matching non-windowed, retractable grouped first/last
operator. StreamFusion keeps the existing Arrow group-aggregate operator and extends
its per-key accumulator state.

RisingWave's `first_last_value.rs` and aggregate `minput.rs` are related references:
append-only aggregation retains a value, while retractable aggregation materializes
ordered inputs and reads an extreme from an ordered cache. Its first/last aggregates
retain NULLs and use explicit ordering columns, which differ from released Flink's
one-argument functions.

We retain one scalar for append-only FIRST_VALUE/LAST_VALUE and an ordered sequence
of non-NULL occurrences for retractable input. Flink's one-argument retractable functions
assign processing-time order and preserve insertion order within equal milliseconds;
the sequence models arrival order, including removal of the oldest matching duplicate.
It does not impose event-time or SQL sort order. Wall-clock regressions are inherently
timing-dependent; portable parity fixtures control arrival order.

DISTINCT first/last use the existing typed multiplicity maps alongside an arrival-order
queue of unique live values. Only a value's final removal erases its queue entry;
reinsertion appends it. Floating membership follows Java boxed equality, canonicalizing
NaNs and distinguishing signed zero, while the queue retains the original selected
payload. Snapshot side frames keep their existing ordered value/count fields. Timestamp
membership uses the same inline nanosecond carrier as the queue, and restore accepts the
existing Arrow timestamp columns. DATE/TIME maps and column readers use their physical
32-bit values while scalar wire entries retain their logical types. Capacity accounting includes both containers and any
duplicated dynamic payload, following Comet's task-shared ownership accounting.

Arroyo's incremental aggregator collects changed keys and compares output tuples at flush.
Flink immediate aggregation requires an ordered transition after each row. StreamFusion
therefore retains the existing per-key result tuple and updates only possibly changed cells
for zero-TTL DISTINCT first/last groups mixed with COUNT DISTINCT or COUNT(*). Multiplicity-
only changes reuse selected payloads. NaN still forces the host's non-reflexive emission;
signed-zero payload replacements still update the cached postimage. Group liveness determines
insertion/deletion. Typed Arrow builders consume borrowed cached cells without retaining a
second scalar tuple for each emitted row; finished arrays follow Comet's owned C Data export
pattern. This changes transient output construction, not checkpoint fields or ownership ABI.

Flink's retractable first/last value and order maps have independent entry TTL. A live
group can retain an accumulator value whose supporting map entry has expired. Until both
lifetimes and their lazy reads are represented, positive retention gates these retractable
forms; append-only scalar state and SINGLE_VALUE retain ordinary group TTL.

SINGLE_VALUE uses Flink's separate nullable value and element count. NULL consumes an
element, so eager NULL skipping would incorrectly admit a second value. Cardinality
errors cross JNI as Flink's own exception class after imported Arrow buffers have been
released, retaining the ownership pattern checked against Comet's JNI Arrow import path.

Ordered occurrences checkpoint through the existing Arrow side-batch framing, preserving
per-key occurrence order. These new accumulator kinds use the raw keyed snapshot fallback
on RocksDB rather than treating a value multiset as an ordered table. A direct persistent
representation needs explicit order keys before it can replace that fallback. Local/global
admission verifies each aggregate's declared partial schema and merge semantics.

Pure, unfiltered local SINGLE_VALUE partials retain owned Arrow key/value columns and
compact per-key counts and row positions. NULL is still an element, and a second
element fails during accumulation. Flush gathers values from the retained batches;
an unchanged single batch shares its key/value buffers. Mixed aggregates and FILTER
continue through the general local accumulator. Retained input buffers and container
capacity participate in the existing shared reservation, following Comet's owned C Data
import and task-shared pool lifetime. Imported columns may share a producer release
callback with unused siblings, so the reservation includes the complete input batch's
buffer estimate until flush, rather than only the selected columns.

The global split SINGLE_VALUE path likewise retains fresh-key partials as owned Arrow
batches when every INT count is one. State still uses the existing keyed accumulator
and checkpoint codec. Flush forwards one batch's columns or concatenates physical
batches in arrival order, emitting INSERTs. An existing key or any other partial shape
first promotes the pending fresh rows into the ordinary first-preimage tracker, then
uses the existing merge. This preserves duplicate failures, zero/negative counts,
changelog handling and TTL expiry without building per-key change maps and temporary
result tuples for the successful fresh-key case.

Ordered timestamp cells retain exact nanoseconds in an inline Decimal128 carrier.
This is an internal scalar representation: Arrow output and checkpoint columns retain
the declared timestamp struct. Column readers bind once per batch, and restore accepts
the existing struct encoding. This avoids constructing a one-row Arrow struct for each
value and NULL accumulator.

LISTAGG follows the same per-key arrival-order decision. Arroyo's incremental aggregate
uses DataFusion accumulators; its batch fallback recomputes an unordered input multiset,
which cannot preserve Flink's ordered concatenation or first-duplicate retraction.
Append-only LISTAGG owns one prefix; retractable LISTAGG owns the ordered live strings
and pending unmatched retractions. Both use a versioned binary accumulator snapshot through
the existing keyed store, with O(1) capacity accounting. Secondary separator columns reuse
the aggregate argument metadata without changing the JNI signature. Numeric plans retain
their historical empty secondary-column encoding and TTL emission mode.

JSON_ARRAYAGG likewise retains an ordered sequence of already serialized JSON values,
removing the first equal occurrence rather than recomputing from an unordered multiset.
JSON_OBJECTAGG owns a per-key field map and sorts Java UTF-16 keys at materialization.
Both store versioned accumulator snapshots. Flink 1.18 NULL-key serialization failures
are deferred until materialization; structured errors cross JNI only after the imported
Arrow batch has been dropped. The cold legacy failure delegates to the released Flink
aggregate, preserving its wrapped exception without reconstructing JDK-specific messages.
