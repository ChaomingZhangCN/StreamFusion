# OVER

**Status:** Native across all four frame shapes, with the gaps enumerated below.

`OVER` runs over one ascending order (rowtime or, where noted, proctime) and one window group. Each
aggregate reads its own — possibly different — value column of type
bigint/int/smallint/tinyint/double/float, exact DECIMAL, CHAR/VARCHAR, TIMESTAMP and TIMESTAMP_LTZ.
Narrow ints and 4-byte float retain the host result type. Decimal SUM/AVG use exact widened
state; temporal extrema and selections retain milliseconds and fractional nanoseconds. `FIRST_VALUE`/`LAST_VALUE` and the window functions
`ROW_NUMBER`/`RANK`/`DENSE_RANK` (no value column, unbounded frame) are admitted alongside the
aggregates below.

Constant NULL arguments are also supported with their declared type, including
`MIN(CAST(NULL AS VARCHAR))`, `MAX(CAST(NULL AS VARCHAR))`, and `COUNT(CAST(NULL AS VARCHAR))`.
They can share a frame with numeric aggregates and `COUNT(*)`; typed NULL Arrow columns are
handled without attempting a numeric conversion.
Running MIN/MAX checkpoints preserve this typed NULL state alongside other aggregates.

Event-time parity fixtures keep the watermark behind the entire input timestamp range until
end of input, so file enumeration order cannot introduce accidental late rows. Late-row tests
control the input order separately.

## Frame shapes

Running checkpoints retain raw widened sums and non-null counts for AVG and decimal SUM.
The evaluated average is never restored as a sum. Bounded decimal SUM/SUM0/AVG retract expired
values before adding arrivals and preserve overflow history, including precision 38. Typed
extrema/selections retain the input decimal scale or both timestamp components. Precision 0/3
values normalize to the released serializer's millisecond representation before aggregation;
precision 6/9 values retain their fractional nanoseconds.

Bounded string state keeps its selected value, duplicate membership and retractable views
independently of the physical frame. Serialized duplicate keys can collapse without discarding
the independent selection/order history. MIN/MAX update incrementally and rescan only when the
winner retires. FIRST/LAST retain their selected value and value-to-order/order-to-value views.
Heap rescans follow JDK hash buckets and tree bins; RocksDB follows serialized key order.
Numeric frames keep their existing state layout. Managed memory accounts for owned payloads,
view storage and map membership, and cleanup releases both frame and selection ownership.

Cached Flink strings compare by UTF-16 when both operands have original Java caches, and by
UTF-8 otherwise. Metadata-marked Arrow List<UInt16> companions can retain the original units,
including unpaired surrogates with identical replacement bytes. Reads reconstruct the original
representation. Shuffle and serialized recovery clear caches; RocksDB normalizes proctime views
between individual rows, including within one Arrow batch. Explicit transport controls and full
SQL comparisons cover source/computed strings, unpartitioned input, chained OVER and window-to-OVER.

Decimal RANGE folds and cached string views also retain the backend's expired-timestamp-group
order. Heap membership includes pending groups and follows JDK insertion, tree splitting and
removal. Ordinary checkpoints reconstruct capacity from the serialized membership count;
canonical heap savepoints insert into a default-capacity map. A managed marker observes actual
heap copy-on-write while a snapshot is outstanding, so live native maps rebuild when the host
backend copies state. Aggregate values remain in Arrow-owned native state; the marker carries
only the lifecycle observation. Input and value maps have separate markers: snapshot completion
between buffering and firing must not copy a value map prematurely. Completed-snapshot controls
preserve the uninterrupted order.
Timestamp and value maps apply their respective normal/canonical reconstruction rules.

Released-stock controls establish that large malformed cached-key collisions do not define a
unique byte-for-byte oracle. FIRST_VALUE can change with its helper's implicit wall-clock groups;
restored MIN can change when identity-ordered tree entries serialize to the same binary key.
Native FIRST/LAST use relative arrival order for these inherently nondeterministic cases.
Deterministic ordering and supported string representations retain exact stock parity. The
[raw controls](../benchmarks/results/over-basic-values-2026-10-13.json) and
[architecture note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/11-over-incremental-vs-window-exec.md)
record the released behavior and state model.

### Unbounded `RANGE … CURRENT ROW` and `ROWS … CURRENT ROW` (running folds)

Both retain incremental aggregates per partition across batches. Event-time RANGE folds all
rows tied at a timestamp before emitting their shared result. ROWS emits after each row,
including separate running results for tied rows in their arrival order. Watermarks release
buffered event-time rows in timestamp order; proctime folds eagerly in arrival order.

`COUNT(*)` counts every row, while `COUNT(value)` skips NULL values. The planner materializes
one non-null BIGINT constant for row counting and reuses the existing aggregate kernel, frame
state, TTL and checkpoint paths. This works for all four admitted frame shapes, including
mixed aggregates, nullable partition keys and unpartitioned input.

Non-distinct `COUNT(value)` also admits runtime DECIMAL, CHAR/VARCHAR, TIMESTAMP and
TIMESTAMP_LTZ values. It reads the Arrow validity bitmap, without converting these values to
numbers or inspecting decimal precision and temporal fractions. Bounded frames retain NULL
membership with the declared column type through snapshots and memory/RocksDB transitions;
running COUNT state remains a BIGINT. DISTINCT retains the numeric value whitelist.

### Bounded `ROWS BETWEEN n PRECEDING AND CURRENT ROW`

Retains the preceding row slice plus the current row. Decimal SUM/SUM0/AVG and string
reductions retract expired membership; other reductions fold the retained slice.

### Bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW`

Retains every row within `n` of the current row's rowtime. Incremental reductions retract
expired timestamp groups in the backend's membership order, including peers.

## Late event-time rows

Unbounded frames admit only timestamps strictly greater than the current watermark.
Bounded ROWS and RANGE frames instead compare against that partition's last-fired timestamp,
initially zero. Equality is late; an untouched partition can accept a positive timestamp behind
the global watermark. Rows tied before their timer fires remain admissible together.

The newest retained frame row preserves the last-fired timestamp through memory snapshots,
RocksDB checkpoints, and backend transitions. A zero-width ROWS frame keeps one marker row,
which is excluded from the next row's aggregate. RANGE eviction uses each partition's own
last timestamp. Its event-time cleanup timer clears the whole partition and resets admission:
Flink's deadline hysteresis registers `timestamp + floor(1.5 * interval) + 1` when the existing
deadline precedes `timestamp + interval + 1`. Those deadlines are checkpointed alongside frames;
pending-only partitions reconstruct them from their buffered arrivals on canonical restore.

## Proctime order

The running and bounded-ROWS frames are native on proctime as well: arrival order, eager emit, no
wall-clock timer needed.

A **bounded-RANGE frame over proctime** falls back — with processing time materialized as a fixed
per-batch timestamp, a wall-clock-interval frame has no meaningful definition.

## Gaps

The matcher declines:

- Direct `AVG` outside DECIMAL. Flink can lower some other AVG forms to SUM/COUNT aggregates.
- A non-numeric value column outside DECIMAL, CHAR/VARCHAR, TIMESTAMP and TIMESTAMP_LTZ.
  These basic types admit ordinary COUNT and typed MIN/MAX/FIRST_VALUE/LAST_VALUE;
  DECIMAL also admits SUM/SUM0/AVG. DISTINCT retains its numeric whitelist.
- Temporal FIRST_VALUE/LAST_VALUE on released Flink 1.18, whose aggregate factory rejects them.
  Native planning retains the same failure; Flink 2.2 admits these functions.
- A `PARTITION BY` key outside bigint/int/string/boolean/date/timestamp/decimal.
- A frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that isn't a
  constant preceding offset or UNBOUNDED PRECEDING).
- A bounded-RANGE frame over a proctime order.

## Parity, not gaps

Flink itself rejects or single-groups these in streaming, so not running them natively matches Flink
rather than falling short of it: more than one window group, decimal-valued frame offsets, `FOLLOWING`
frames, non-time or descending order, and `LAG`/`LEAD`.

## Idle-state TTL

`OVER` runs `table.exec.state.ttl` natively across all four frame shapes, but the mechanics differ
by shape:

- **Rowtime frames and the proctime bounded-ROWS frame** share a per-key cleanup deadline (the same
  scheme as the temporal join): registered on every element, with hysteresis and a
  `minRetentionTime > 1` enablement threshold, checked lazily and swept, clearing the key's
  accumulator and frame buffer silently. One wrinkle: at the deadline, the rowtime shapes *defer*
  while the key still has buffered rows the watermark hasn't folded (the timer re-registers and
  waits), whereas the proctime bounded-ROWS frame clears its retract frame unconditionally — so that
  frame can observably restart short.
- **The proctime unbounded fold** instead puts a per-value TTL (`> 0` enables, refreshed on last
  write) directly on its accumulator. An expired key visibly restarts its running fold — and its
  `ROW_NUMBER`/`RANK` numbering — from zero, exactly as Flink's `NeverReturnExpired` state does.
- **The bounded-RANGE rowtime frame** takes no retention at all: Flink's own function accepts none,
  since event-time frame eviction and cleanup bound state, so `table.exec.state.ttl` changes nothing there.

With that, nothing declines a nonzero retention setting. See [Configuration](../configuration.md) for
the TTL flag surface, and [window aggregate](window-aggregate.md) for the (unaffected — no idle-state
TTL applies) window operators.

## Running ROWS validation

The released-SDK operator matrix covers all supported event-time/proctime frames, heap/RocksDB,
ordinary checkpoints and canonical backend transitions. Fixtures include precision-38 decimal
arithmetic/overflow, wide temporal values and widening casts, all-NULL frames, peers, duplicate
extrema, Unicode cache representations, malformed serialized aliases, pending rows and state TTL.
Large valid-Unicode hash collisions additionally cover winner retirement, tree bins, normal and
canonical restore, and outstanding/completed heap snapshots. Controlled SQL recovery fails a job
repeatedly after completed checkpoints and compares mixed decimal/string/temporal output against
stock. Native execution counters establish actual OVER work for SQL comparisons.

The focused Flink 2.2.1 suite covers COUNT(*) versus nullable COUNT, mixed aggregates, constant
arguments, DISTINCT, empty/all-NULL input, multiple partitions, timestamp peers, late admission,
and 5,003-row multi-batch input. Operator tests cover TTL expiry and running-state continuation
across RocksDB checkpoints and memory/RocksDB backend transitions.

`RunningRowsBenchmark` measures proctime COUNT(*), COUNT(value), SUM, MIN and MAX over one
million rows and 64 keys, parallelism one, with both row/Arrow transposes and a row blackhole sink.
On an Apple M1 Max/JDK 17, the September 16, 2026 release run used two warmups and five interleaved
measurements: median Flink **0.605496 s**, native **0.591282 s** (**1.024x**). The difference is small;
this establishes comparable end-to-end performance for the new coverage, not a substantial
speedup. Keeping OVER columnar also lets it compose with adjacent native operators.

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench -Dtest=RunningRowsBenchmark
```

## Basic-value complete-job comparisons

The October 13, 2026 Linux/JDK 17 comparison uses released Flink 2.2.1, UTC, parallelism one,
release+mimalloc, IPC exchange, a runtime row source and row blackhole sink. Both transposes,
JNI, buffering, state and output conversion remain inside each job. Two warmups precede three
measured trials per engine; stock, native and forced-OVER-fallback jobs rotate execution order.
Current-job metrics must establish actual native OVER consumption. SQL parity is checked
separately through collecting queries, including decimal overflow, all-NULL partitions, peers,
broad years, Unicode, unpartitioned/computed strings and two native-island pipeline controls.

At 500,000 rows and 64 keys, all sixteen type/frame profiles beat both stock and previous
fallback; stock/native medians span 1.853–4.806×. Decimal profiles cover SUM, AVG, extrema,
FIRST/LAST and COUNT together; string and temporal profiles cover extrema, FIRST/LAST and COUNT.
The input is globally ordered by event time. Watermarks run every 50 ms with no out-of-order
delay, retaining all peers at the current maximum timestamp until it closes.

At one million rows and 4096 keys, all eight running/bounded ROWS profiles also beat stock
and previous fallback, spanning 2.335–4.761× stock. All samples, including the
11.008-second LTZ stock observation, are retained.

The [raw artifact](../benchmarks/results/over-basic-values-2026-10-13.json) retains every
warmup and observation. An earlier diagnostic withheld watermarks for a year and therefore
buffered the entire bounded input until EOF: its running decimal median was 2.429× stock,
while its first bounded stock/fallback warmups took 48.991/48.740 seconds versus native
0.952 seconds. That run was stopped during warmup to correct the streaming fixture; no
completed bounded estimate is claimed. The observations remain visible.

After the final string map/copy/restore corrections, all six affected string comparisons were
rerun with the same fixture and controls. They beat stock by 2.176–3.913× and also beat the
previous fallback. These supersede the earlier string profiles; unchanged decimal and precision-9
temporal paths retain their original comparisons. All 90 final string trials, including warmups
and the 11.805-second stock bounded-ROWS observation, remain in the artifact.

| String profile | Stock median (s) | Native median (s) | Previous fallback (s) | Stock/native |
|---|---:|---:|---:|---:|
| 500k rows, 64 keys, running RANGE | 1.370 | 0.599 | 1.390 | 2.286× |
| 500k rows, 64 keys, bounded ROWS | 3.559 | 0.910 | 3.537 | 3.913× |
| 500k rows, 64 keys, bounded RANGE | 3.533 | 1.034 | 3.510 | 3.416× |
| 500k rows, 64 keys, running ROWS | 1.236 | 0.568 | 1.267 | 2.176× |
| 1M rows, 4096 keys, bounded ROWS | 10.008 | 3.089 | 9.764 | 3.240× |
| 1M rows, 4096 keys, running ROWS | 3.170 | 1.095 | 3.118 | 2.894× |

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=OverBasicValuesBenchmark -Dsf.testForks=1 -Dsurefire.failIfNoSpecifiedTests=false \
  -Dover.values.rows=500000 -Dover.values.keys=64
```

Properties select `over.values.types`, `over.values.frames` (0 running RANGE, 1 bounded ROWS,
2 bounded RANGE, 3 running ROWS), rows, keys, warmups, runs and watermark delay in milliseconds.
