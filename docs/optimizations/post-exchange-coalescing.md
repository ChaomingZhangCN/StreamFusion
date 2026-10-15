# Post-exchange batch coalescing

**Applies to:** GROUP BY aggregate, both Top-N rankers, the updating join, keep-last dedup, the
changelog normalizer

## The problem

The columnar exchange splits every source batch into at most one sub-batch per destination channel.
A changelog operator feeding another keyed operator can still compound fragmentation: in the
2M/p=4 q4 pipeline the second
aggregate processed 26-row batches at ~71× the p=1 batch count. The per-batch fixed cost (JNI
crossings, per-call setup, per-batch emission) dominated wall time, and every task thread sat
starved (see [Benchmarks](../benchmarks.md), "Why the off-mode changelog shapes stop scaling").

## The fix

Every keyed changelog operator now re-assembles processing-sized batches in front of the native
push: sub-batches buffer until a row target ([`streamfusion.exchange.coalesceRows`](../configuration.md),
default 4096) and merge in one native `concat_batches` call; watermarks, checkpoint barriers, and end
of input always flush first, and a processing-time backstop
([`streamfusion.exchange.coalesceLatencyMs`](../configuration.md), default 50 ms) bounds the wait on
trickle streams — the role Flink's own mini-batch `allow-latency` plays. The two-input join drains
the opposite side before buffering a side, so the cross-side arrival order the join changelog
depends on survives coalescing exactly. Only physical chunking changes: every operator still emits
its per-record cascade, so the off-mode byte-parity contract is untouched (the parallelism-2 shuffle
parity test asserts a merge with enough input to force multiple Arrow batches even when one source
subtask reads every file; file boundaries do not flush the row-to-Arrow buffer).

UNION inputs can have different physical schemas even when their logical SQL types agree.
The merge validates logical arity and types, then aligns optional RowKind and Java string-cache
columns before concatenating. An append-only input contributes INSERT tags when another input
carries a changelog; absent string caches contribute null cache entries. Logical field names
may differ between producers. Existing arrays remain borrowed during normalization; only
concatenation materializes their values, and equal physical schemas use direct concat.
A failed merge releases exported inputs and clears the pending roots before cleanup can run again.

## Measured

(2M events, parallelism 4, Kafka source → blackhole ladder, interleaved same-binary A/B, two legs
per side): q4 — the two-aggregate + join chain where the collapse compounds — runs **1.59× faster
with coalescing** on json (1.95 → 1.23 s; 1.45× avro, 2.06× protobuf), moving q4 vs Flink from
~1.06× to **1.78×** on json; the row-fed q4 variants gain ~2.5× (5.5 → 2.2 s). q3/q19 sit within
cross-leg noise.

The remaining off-mode lever is the source-side batch floor: smaller source polls create sparse
key-group fragments before the first keyed operator.

### Mixed UNION correctness fix (2026-10-15)

The former first-batch-schema concat lost RowKinds or indexed missing columns when UNION mixed
append-only and updating branches ([#365](https://github.com/datafusion-contrib/StreamFusion/issues/365)).
On canonical baseline `35b54e57`, all 12 enabled-coalescing SQL variants fail exact final-value
checks, while all four disabled-coalescing controls pass. The corrected path passes 36 selected
tests on each of Flink 2.2.1 and 1.18.1/JDK 17, including both UNION arm orders, SELECT and typed
sink collection, multi-branch ROLLUP, different batch sizes, parallelism 1/3, local zero-copy/IPC,
and two checkpoint restores with controlled watermarks on memory and RocksDB. Native consumed-row
metrics, restored offsets and cleanup are asserted for the recovery cases. Four native regressions
and all 24 release Criterion fixtures also pass.

Complete-job measurements use Flink 2.2.1/JDK 17, release native code with mimalloc, parallelism 1,
memory state, 991 STRING keys plus NULL and nullable DECIMAL(7,2) values, mini-batching disabled,
and production IPC shuffle. Both transposes and the rowwise blackhole sink remain in the path.
Each variant has two warmups followed by five retained stock/native pairs, alternating engine order.
Times are medians with observed ranges:

| Source rows | Stock Flink | Fixed default | Previous code, coalescing disabled |
| --- | --- | --- | --- |
| 200,000 | 0.455 s (0.420–0.542) | 0.317 s (0.211–0.400) | 0.346 s (0.322–0.413) |
| 2,000,000 | 2.703 s (2.636–2.727) | 1.616 s (1.519–1.620) | 1.663 s (1.604–1.706) |

The fixed default is 1.67× faster than stock Flink at 2M rows. The smaller jobs show substantial
startup/JIT variability. The previous default panics in native concat at **both** sizes, so its
partial timings do not establish a successful speed comparison. The disabled-coalescing control
is a correct-result workaround with different coalescing settings; its difference from the fix
does not isolate an optimization speedup. Its separate stock controls have medians 0.450/2.632 s.

The isolated native merge of two 4096-row batches with 256-byte strings takes about 80–81 µs in
the two mixed physical orders (20 Criterion samples, one-second warmup and measurement windows).
It requests 49 allocations totalling 1,978,492 bytes, and its output has 1,971,204 bytes of new
Arrow buffers with no shared output buffers. These are allocation and sharing measurements,
not copied-byte counts, and exclude JNI and Flink costs. The
[raw results](../benchmarks/results/mixed-union-coalescing-2026-10-15.json) retain every end-to-end
trial, failed-baseline partial timings, native confidence intervals, configuration and artifact hashes.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=MixedUnionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dbasic.rows=2000000 -Dbasic.keys=991 -Dbasic.miniBatch=false \
  -Dbasic.warmup=2 -Dbasic.runs=5
python3 bin/bench-native.py --package streamfusion --bench coalescing --features mimalloc --smoke
```
