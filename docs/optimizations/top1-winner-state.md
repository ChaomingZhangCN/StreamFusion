# Top-1 winner state

For constant AppendFast ranks 1..1, a partition contains one retained winner.
Comparing the incoming sort key against that winner avoids sorted-list insertion,
eviction, and owned-row allocation for rejected arrivals. A single map probe applies
an immutable admission predicate before exposing mutable state or marking a
hydrated RocksDB entry dirty: only changed winners or writes
required by existing TTL semantics persist. Immediate mode retains per-record
changes; mini-batch mode captures a preimage only when the winner can change,
unless TTL requires first-touch tracking.
Unchanged partitions still retain first-touch order markers. If a later arrival
changes their winner in the same bundle, the preimage is captured before that
change and the flush retains the original partition order.

The algorithm uses the same raw state codec, key-group routing, Arrow comparator,
payload encoding and snapshot formats as the general append ranker. The direct RocksDB constant Top-1 path retains a clean decoded-winner cache
across bundles. This cache has a FIFO byte bound of 64 MiB and a separate
reservation against the operator's existing task off-heap pool. A rejected
reservation clears and disables it, retaining the ordinary read-through path.
Only keys read from existing committed state enter it: new unique keys bypass
admission. Dirty state always writes through before retained values enter the
cache; eviction never causes a write. Checkpoint and canonical scans clear it,
and restore starts cold. TTL and winner comparisons still execute when a cached
winner is touched. No RocksDB pins or Arrow input buffers outlive a bundle.
Whole-batch payload encoding remains in the measured production path. Other rank ranges and variable bounds
retain the general implementation.

## Verification and measurement

Native tests compare the specialized and general algorithms across ascending and
descending order, NULL placement, NaN, signed zeros, ties, projected/hidden rank,
TTL and logical mini-batch boundaries. Raw checkpoints continue after recovery,
and key-group snapshots scale 1→2→1. A counting codec over the real RocksDB store
verifies that replaying rejected inputs encodes no state writes. Managed state
accounting agrees after bundle flushes.

The `operators` Criterion suite's `append_top1` cases compare the production
algorithms on memory and RocksDB, at 256/4096 rows, repeated-key and one-key-per-row
cardinalities, nullable sorts/payloads and widths 8/256. They separate fresh state,
improving arrivals and rejected arrivals, in immediate and mini-batch modes.
Fixtures, store creation and initial state are outside timing. Timed execution
includes Arrow key/payload encoding, state hydration, updates, writes, output
construction and mini-batch flush. Retained stores and outputs are dropped after
timing.

The complementary `AppendTop1Benchmark` keeps a bounded row source, keyed exchange,
both transposes and a blackhole sink. It uses `basic.backend=memory` or `rocksdb`;
the RocksDB stock control uses Flink's released backend and the native control uses
the StreamFusion backend. `basic.keys` selects repeated-key or high-cardinality
profiles. These complete-job measurements supplement the fixed-window sustained
throughput check required by [#317](https://github.com/datafusion-contrib/StreamFusion/issues/317).

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=AppendTop1Benchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dbasic.backend=rocksdb -Dbasic.rows=2000000 -Dbasic.keys=64
```

## Sustained end-to-end measurements

Linux x86-64, Intel i7-12650H, released Flink 2.2.1/JDK 17 and release/mimalloc libraries.
Each cell reports the arithmetic mean over three separate ten-second jobs after five-second warmups.
The stock column comes from the same invocation as the candidate. The previous native control
uses the same harness and resources in a separate JVM; its independently measured stock trials
are also retained in [the raw results](../benchmarks/results/append-top1-final-2026-10-13.json).
Both transposes, source generation, JNI, state access and Arrow IPC shuffle remain in the job.

| Backend | Keys | Payload chars | Arrivals | Stock rows/s | Previous native rows/s | Candidate rows/s | Candidate/stock | Candidate/previous |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| rocksdb | 65,536 | 256 | Rejected | 201,963 | 105,263 | 1,037,999 | 5.14× | 9.86× |
| rocksdb | 65,536 | 8 | Rejected | 498,756 | 139,839 | 2,367,054 | 4.75× | 16.93× |
| rocksdb | 65,536 | 8 | Improving | 127,838 | 120,418 | 533,797 | 4.18× | 4.43× |
| rocksdb | 64 | 8 | Rejected | 2,704,261 | 3,723,295 | 5,312,817 | 1.96× | 1.43× |
| rocksdb | 64 | 8 | Improving | 1,624,584 | 2,357,330 | 2,924,446 | 1.80× | 1.24× |
| memory | 65,536 | 8 | Rejected | 2,223,466 | 2,419,857 | 3,691,356 | 1.66× | 1.53× |
| memory | 64 | 8 | Rejected | 2,706,022 | 5,079,498 | 5,126,722 | 1.89× | 1.01× |
| memory | 65,536 | 8 | Improving | 719,292 | 830,853 | 873,784 | 1.21× | 1.05× |
| memory | 64 | 8 | Improving | 1,757,046 | 3,911,181 | 4,061,505 | 2.31× | 1.04× |

The low-cardinality rejected memory case changes only slightly against the previous native
implementation (about 0.9%); it does not demonstrate a substantial improvement there. The larger
RocksDB gains depend on repeated existing keys fitting the clean decoded-winner cache. Fresh keys
bypass admission; cache denial and checkpoint/canonical scans leave ordinary RocksDB execution.
Startup and unique-key insertion are outside these sustained results and require the complementary
bounded complete-job measurement.

## Bounded unique-key comparison before compact winner allocation

Two million input rows at two million keys retain every fresh winner. The bounded
row-source/blackhole jobs include startup, both transposes and IPC, with two warmups
and three alternating measured trials. These are median elapsed seconds, rather
than the sustained mean rates above. All samples are retained in the raw results.

| Backend | Stock with previous | Previous native | Stock with candidate | Candidate native |
| --- | ---: | ---: | ---: | ---: |
| RocksDB | 407.861 | 75.745 | 481.514 | 20.484 |
| Memory | 4.767 | 5.344 | 3.968 | 5.001 |

The memory candidate improves the previous native median but remains slower than
its stock control. The stock trials vary materially (3.556–6.412 seconds); these
measurements do not establish an accelerated unique-key memory path. Further
optimization now reserves one retained row per Top-1 partition and borrows the
final winner during bundle flush instead of allocating another singleton vector.
The fresh release comparison below measures that change separately from this earlier candidate.

## Compact winner allocation and unique-key memory limits

Constant Top-1 reserves exactly one retained row per partition instead of the
ordinary growing-vector allocation. Bundle flush borrows that winner directly;
projected ranks use the existing positional diff over a borrowed singleton slice,
and hidden ranks compare the old/new payload directly instead of building a
multiset. Native correctness remains 679 passing tests with two existing ignores.

Three independent JVM pairs, each with two warmups followed by one measured
2-million-row/2-million-key job per engine, gave these median elapsed seconds:
previous native **5.858074**, candidate **5.326029**, and candidate's stock control
**4.950620**. The candidate is 9.08% faster than previous native, but remains 7.58%
slower than stock for this insertion-only memory workload. Every duration, including
candidate's 13.639240-second trial and the previous JVM's 19.071254-second stock
trial, remains in the raw results. Those outliers make variability material.

A separate CPU diagnostic attributed 2,441 samples to native rank updates and 879
to native flushes, versus 10,696 to stock FastTop1; row/Arrow conversion contributed
1,883 samples and G1 activity 10,463 across the combined JVM. These sampled costs
are diagnostic, not comparable job rates, and do not establish a stock win. The
9.1% previous-native improvement and the sustained hot-key wins do not remove this
complete-job limitation. The maintainer explicitly approved shipping this documented
tradeoff on 2026-10-13. [Follow-up #339](https://github.com/datafusion-contrib/StreamFusion/issues/339)
tracks the remaining unique-key memory gap. No additional query shapes are admitted
by this optimization.

## Profiling and unfavorable development results

The [initial winner-loop comparison](../benchmarks/results/append-top1-e2e-2026-10-13.json)
retains every trial, including the rejected 65,536-key RocksDB case that reached about
298,000 rows/s versus 494,000 for stock Flink. A rank-task sample attributed 83.66%
of native rank CPU samples to RocksDB lookup, including decompression; changing the
winner-update loop alone did not remove persistent hydration between bundles.

The default 64 MiB write buffer selected an 8 MiB arena quantum even under a 10 MiB
slot budget. Bounding the quantum to one eighth of the effective shared write-buffer
budget reduced it to 438,272 bytes in that configuration. This improved the lookup
case but remained slower than stock (426,005 versus 498,935 rows/s). A bounded clean
winner cache then removed recurring decoded-state lookups. All intermediate results
remain in [the cache development measurements](../benchmarks/results/append-top1-cache-2026-10-13.json).
The first wide-payload estimate counted value and sort headers twice; only 62,229 of
65,536 winners fit, cyclic eviction left zero cache hits, and throughput was about
171,000 rows/s. Correcting the duplicate header accounting retained every winner at
64,910,866 accounted bytes, within the same 64 MiB limit. The final candidate reserves
memory before retaining values and returns unused reservation after eviction.

## Native measurements before the decoded cache

On Linux x86-64 / Intel i7-12650H with the system allocator, release mode and one
codegen unit, 4,096-row mini-batches at 4,096 partition keys and eight-byte payloads
gave the following mean update/flush times. Twenty Criterion samples follow a
0.5-second warmup and a two-second measurement target; NULL sorts/payloads remain
in the input. All confidence intervals, variability, source hashes and unfavorable
samples are retained in [the native results](../benchmarks/results/append-top1-native-2026-10-12.json).

| Native boundary | Generic mean | Specialized mean | Generic/specialized |
| --- | ---: | ---: | ---: |
| Memory, rejected arrivals | 1,629.85 µs | 816.70 µs | 2.00× |
| RocksDB, rejected arrivals | 7,422.53 µs | 4,985.00 µs | 1.49× |
| Memory, fresh keys | 2,049.69 µs | 1,946.00 µs | 1.05× |
| RocksDB, fresh keys | 6,130.98 µs | 6,051.12 µs | 1.01× |

The broad run's 16-key RocksDB improving-input case was slower: 1,096.74 µs
specialized versus 543.90 µs generic. Three subsequent isolated repeats with
one-second warmup and three-second measurement targets instead favored the
specialization by 2–5%. This variability is retained rather than discarded, and
does not establish an end-to-end non-regression. The earlier two-probe prototype
also regressed repeated improving memory input; the current conditional-mutation
probe removes that second lookup and preserves clean persistent reads.

The native timings above precede the decoded-winner cache and its allocation-quantum correction.
They isolate the winner-update loop and do not describe the final end-to-end implementation.
