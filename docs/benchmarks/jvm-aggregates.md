# JVM aggregate release diagnostics

This is a default-off investigation for [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237),
not admitted acceleration. [GROUP BY](../operators/group-by.md#jvm-aggregate-investigation)
describes coverage and remaining contracts. The [results artifact](results/jvm-aggregates-investigation-2026-10-15.json)
retains all local comparisons and profiles, including regressions and runs with external activity.

## Isolated measurement

Shared-workspace builds repeatedly overlapped local trials. The JVM aggregate diagnostics
workflow gives each released Flink line its own GitHub-hosted Ubuntu runner and JDK 17.
Release compilation finishes first, followed by the focused correctness/recovery suite at
65,536-entry and three-entry capacities, including checkpoint rescaling. Only then does `bin/bench-jvm-aggregates.py` run
complete jobs; it never overlaps its build or correctness checks with measurements.

The workflow runs on relevant pull-request changes, and can be dispatched once available on
the default branch. It changes no Nexmark fixtures. Its successful completion proves fixture
execution and retained evidence; it is not a throughput threshold or admission decision.
Running diagnostic jobs are preserved across later pushes; results identify their exact source
commit. Newer runs queue rather than cancel an expensive build or measurement halfway through.
Test/documentation-only changes can reuse evidence when the measured code and fixtures match.

Each query is `SELECT k, test_aggregate(v) FROM src GROUP BY k`, using the existing portable
SUM UDAF. Typed runtime inputs cover INT, BIGINT and STRING grouping keys, at 16, 65,536 and
2M distinct keys over 2M input rows. STRING keys have a 16-character prefix containing `é`,
followed by decimal key digits; their final lengths vary. Every eighth value is NULL.
Type-specific row generators are chosen before execution, keeping the original INT generator
unchanged and retaining generation inside the timed job.

Each case runs in a fresh JVM with a 2 GiB heap, parallelism one, production IPC and both
1,024-row transposes. Every job owns and closes its metrics cluster. Cluster boot/config is
outside the clock; `executeSql(insert).await()` retains planning/submission, row source,
blackhole sink, perimeter conversions, functions and state. Two warmup pairs precede five
alternating measured pairs. Stock execution must have no native operators; candidate plans
must contain the JVM aggregate and both transposes, with positive consumed-row metrics.
Cache counters distinguish a retained hot working set from a cold-fill bypass.

The runner retains all trials, ranges, complete logs, GC/safepoint logs, commands, source
commit, native-library checksum, JDK and host metadata. INT/BIGINT caches are measured with
the diagnostic flag; STRING currently uses the uncached wrapper. A slower case remains
visible and does not become accelerated merely because the workflow passed.

## Profiling

The workflow installs released async-profiler 4.0 from its canonical release with a pinned
SHA-256 checksum. After the unprofiled runs, separate INT and STRING medium/distinct-key jobs
record mixed Java/symbolized Rust CPU stacks with `cpu-clock`, 2 ms sampling and DWARF native
unwinding, plus Java allocation samples at 1 MB intervals. These use 1M rows, two warmup pairs
and three measured pairs, and include a builtin-SUM Rust control alongside the JVM UDAF.
All profile timings are excluded from speed claims. Java allocation samples are estimates,
not peak memory, copied bytes or Rust heap totals.

The artifact preserves raw JFR files, GC logs and benchmark records. Profiler startup writes
a native stdout message that Surefire may report as a channel diagnostic; inspect the log,
JFR and final test result rather than treating that message as a query failure.

## Local reproduction

Finish the release build before starting measurements:

```sh
mvn -B -ntp -Pbench -pl streamfusion-runtime -am -DskipTests clean install
python3 bin/bench-jvm-aggregates.py --flink-line 2.2 --output native/target/jvm-aggregate-diagnostics
```

For Flink 1.18, build with `-Pbench,flink-1.18` and pass `--flink-line 1.18` to the runner.
The runner pins the release resource/library directory and rejects missing typed results,
which prevents stale test classes from silently measuring a different fixture. Supply
`--profiler-agent "$SF_ASYNC_PROFILER_DIR/lib/libasyncProfiler.so"` to collect profiles.
Use an otherwise idle machine and retain observations when concurrent work occurs.
