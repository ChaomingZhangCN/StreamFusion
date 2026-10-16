# JVM aggregate release diagnostics

This is a default-off investigation for [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237),
not admitted acceleration. [GROUP BY](../operators/group-by.md#jvm-aggregate-investigation)
describes coverage and remaining contracts. The [results artifact](results/jvm-aggregates-investigation-2026-10-15.json)
retains all local comparisons and profiles, including regressions and runs with external activity.

## Isolated measurement

Shared-workspace builds repeatedly overlapped local trials. The JVM aggregate diagnostics
workflow gives each released Flink line its own GitHub-hosted Ubuntu runner and JDK 17.
Release compilation finishes first, followed by the focused correctness/recovery suite at
65,536-entry and three-entry capacities, including checkpoint rescaling, actual canonical-savepoint restores and borrowed-string
probe slice/ownership checks. Only then does `bin/bench-jvm-aggregates.py` run
complete jobs; it never overlaps its build or correctness checks with measurements.

The workflow runs on relevant pull-request changes, and can be dispatched once available on
the default branch. It changes no Nexmark fixtures. Its successful completion proves fixture
execution and retained evidence; it is not a throughput threshold or admission decision.
Running diagnostic jobs are preserved across later pushes; results identify their exact source
commit. Newer runs queue rather than cancel an expensive build or measurement halfway through.
Test/documentation-only changes can reuse evidence when the measured code and fixtures match.
GitHub concurrency groups retain at most one pending run; later pushes can replace that pending
run. When distinct runtime revisions need comparisons, dispatch the earlier immutable revision
separately and verify its recorded SHA before pushing the next one. A branch dispatch and PR
run use different ref groups and execute on separate diagnostic hosts. Cancel only the redundant
pending copy after the separate run has been scheduled with the verified source.
The pinned released Rust cache action saves dependencies and workspace crates for canonical
branches. A separate release-library cache is keyed by native sources, Cargo files/configuration,
Rust toolchain, root build configuration, workflow, OS/architecture and Flink line. The input
fingerprint is computed before restoring outputs. A verified library-cache hit skips Cargo;
Java compilation still runs. Native-source/configuration changes miss that cache and rebuild.
The payload is saved immediately after a successful release build, before correctness and
measurements, so later Java-only runs can reuse it promptly. The report retains the native
checksum, source fingerprint and cache-hit state.
The initial run had no Rust cache hit and saved none because PR saves were disabled. Cache
restoration and compilation both finish before correctness and timed execution.

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
commit, native-library checksum, JDK and host metadata. INT/BIGINT/STRING caches are measured
with the diagnostic flag. The initial isolated baseline used uncached STRING keys; the subsequent
STRING-cache experiment requires both hot-hit and cold-bypass counters. A slower case remains
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


## First isolated baseline

[The completed 2026-10-16 matrix](results/jvm-aggregates-isolated-2026-10-16.json) retains all
18 unprofiled cases and eight separate mixed CPU/allocation profiles from both released Flink
lines. It predates the STRING cache experiment. Medium numeric keys improve, STRING low/medium
keys regress, and distinct-key ranges remain broad. Profiles motivate STRING key reuse;
they do not establish candidate speed or default admission. Compressed per-case logs and full
CPU/allocation stacks are checked in before the workflow artifacts expire.


[The local copying-probe follow-up](results/jvm-aggregate-copying-string-probe-2026-10-16.json)
retains all three STRING cardinalities and a separate Java/Rust profile. It still loses at
medium and distinct keys. These measurements precede the borrowed UTF-8 probe experiment;
its new results must be reported separately rather than replacing unfavorable trials.


[The borrowed-probe follow-up](results/jvm-aggregate-borrowed-string-probe-2026-10-16.json)
retains a matched local CPU/allocation profile and the contaminated unprofiled repeat. Cache
allocation decreases, but map lookup remains hot and whole-job admission is still unproven.
The monitor stopped the matrix after observing external Java/Rust work; low/distinct cases
were not rerun on that shared host.


The subsequent primitive STRING-hash index has [54-case validation](results/jvm-aggregate-string-hash-index-validation-2026-10-16.json),
including full equality for hash collisions and entry-based capacity accounting. It targets
the remaining sampled cache lookup cost. The borrowed-probe revision is retained in a separate
isolated dispatch; the index needs its own Java/Rust profile and complete-job measurements.


## Symmetric preflight correction

Earlier aggregate and table-function fixtures explained only the native plan before starting
the clock. Native and stock preflight were asymmetric. Both now explain the identical insert
outside timing, then time complete `executeSql().await()` execution. Stock plans and metrics
must have no native operators. Table-function metrics clusters now close after every job,
matching aggregate ownership. SQL, data generators, resources, both transposes and row sinks
are unchanged. The runner marks this method `symmetric-preflight-v2`.

[Corrected local STRING trials](results/jvm-aggregate-symmetric-preflight-2026-10-16.json)
retain all ranges: 16 keys stock/native .721/.637 s; 65,536 keys 1.361/1.261 s; 2M distinct
2.048/2.213 s. The low/medium ranges overlap, while distinct keys still regress. Both released
Flink lines pass all six 2,000-row aggregate/table-function fixture shapes; those are smokes,
not speed evidence. The isolated workflow repeats all nine aggregate cases and profiles plus
split/numeric, wide split and grouped table functions with corrected preflight. Prior v1
results remain historical diagnostics and do not establish current admission.


[The completed copying-cache matrix](results/jvm-aggregates-copying-isolated-2026-10-16.json)
retains all 18 v1 cases, eight profiles and full compressed logs/stacks. Medium STRING medians
are stock/native 1.792/1.911 s on 2.2 and 1.874/1.725 s on 1.18; ranges overlap and the preflight
limitation applies. These results are retained as diagnostics and do not establish admission.
