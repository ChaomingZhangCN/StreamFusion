# Reduce collection representation work

The experimental collection evaluator must preserve Flink's Java string caches, nested row kinds
and generic/binary array copy category. Those details require extra physical children for sensitive
types; nullable primitive-only programs keep the ordinary layout and avoid serializer snapshots
that preserve no additional identity. This also leaves their final arrays as ordinary columnar
views instead of rebuilding generic arrays at the exit transpose.

Represented nested-row vectors retain one reader for the batch lifetime. Each returned row is
copied before that reader can move to the next index, preserving ownership without allocating a
new column-batch wrapper and reusable row wrapper for every nested element.

The initial release profiles use async-profiler CPU sampling at 1 ms and allocation sampling at
1 MiB. They include planning, warmups, stock and StreamFusion trials in one process; counts are
inclusive and overlap. In the append profile, array-writer serializer snapshots appeared in 777
of 30,161 CPU samples and 1,703 of 24,446 allocation samples. In map entries, reader construction
under the nested-row getter appeared in 1,343 of 57,320 CPU samples and 8,424 of 127,310 allocation
samples. These are sampled events, not counts of allocations or copied bytes.
[Profile counters](../benchmarks/extended-functions-profile-2026-10-17.csv) retain the measured scopes.

Unprofiled measurements retain both row/Arrow transposes, two warmups and five alternating trials,
JDK 17, Flink 2.2.1, release/mimalloc, parallelism 1 and a 2 GiB heap on Linux x86-64. The initial and
optimized implementations run in separate JVMs; paired Flink medians are shown to expose drift.
Rows are two million except width-64 sort at 500,000.

| Case | Initial Flink (s) | Initial StreamFusion (s) | Final Flink (s) | Final StreamFusion (s) |
| --- | ---: | ---: | ---: | ---: |
| Append, width 8 | 0.486709 | 0.859151 | 0.488629 | 0.771157 |
| Sort, width 64 | 1.372298 | 1.840761 | 1.342123 | 1.631030 |
| ARRAY constructor | 0.489814 | 0.656635 | 0.506142 | 0.611204 |
| Map entries, width 3 | 1.330885 | 3.411470 | 1.291837 | 3.225086 |
| ROW constructor | 0.507912 | 0.884793 | 0.499804 | 0.801584 |

The reduction is 5–11% of the initial StreamFusion time in these runs. None of these isolated
jobs beats Flink; both collection and metadata extensions remain off by default.
[Issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) tracks the remaining admission work. These results do
not establish a sustained-throughput gain. Full comparisons against previous StreamFusion,
longer workloads, reproducible commands and every trial belong in the
[scalar benchmark ledger](../benchmarks/scalar-functions.md#extended-function-coverage).

For profiling, add `-Dsf.extraJvmArgs="-Xmx2g -agentpath:$ASPROF_LIBRARY=start,event=cpu,alloc=1m,interval=1ms,file=collections-%p.jfr"`
to the reproduction command, pointing `ASPROF_LIBRARY` at the profiler installation. Keep the PID
suffix: Maven's separate ORC test execution can launch another JVM even when that execution has
no selected benchmark tests. Profiled timings are excluded from the elapsed-time tables.
