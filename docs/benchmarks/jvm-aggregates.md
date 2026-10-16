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
Rust compiler identity, root/module build configuration, native build flags, OS/architecture
and Flink line. The input fingerprint is computed before restoring outputs; Java source and
unrelated workflow edits do not invalidate it. A verified library-cache hit skips Cargo;
Java compilation still runs. Native-source/configuration changes miss that cache and rebuild.
The payload is saved immediately after a successful release build, before correctness and
measurements, so later Java-only runs can reuse it promptly. The report retains the native
checksum, source fingerprint and cache-hit state.
The initial run had no Rust cache hit and saved none because PR saves were disabled. The
corrected-preflight matrix saved a release payload for each Flink line; a subsequent hit with
the updated compiler/configuration fingerprint still needs verification. Cache restoration
and compilation both finish before correctness and timed execution.

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

## Completed symmetric-preflight matrix

[The full corrected matrix](results/jvm-aggregates-symmetric-isolated-2026-10-16.json)
retains all 18 aggregate cases, eight table-function cases, eight mixed Java/Rust profiles and
compressed logs/stacks from run 38861235198. Both released Flink lines completed successfully.
This measures the primitive STRING hash index with the ordinary copying STRING exit.
Medium INT/BIGINT keys improve on both lines; STRING results remain mixed and distinct-key
regressions remain visible. A successful workflow is not admission of the aggregate prototype.

The corrected table-function comparisons improve for split, numeric and grouped output on
both lines. Wide STRING output improves on 2.2 but regresses on 1.18: stock/native medians
4.441/4.571 s. All five paired native trials are slower there. There is no corrected-preflight
previous-fallback revision comparison in this matrix. See the [operator results](../operators/unnest-correlate.md#corrected-complete-job-results)
for all case medians and the evidence JSON for ranges and every trial.

The next [borrowed STRING exit experiment](../optimizations/zero-copy-exit-transpose.md#experimental-borrowed-string-exit)
targets the profiled temporary string copy while retaining mandatory output ownership.
It defaults off; pass `--borrowed-string-exit` to the runner after building the candidate.
The workflow measures that flag explicitly and records it in configuration. Its matched local
profiles show lower sampled allocations, while local distinct-key throughput still regresses.
The new candidate requires its own isolated matrix and remains draft.

The following [borrowed-key projection](../optimizations/borrowed-key-probes.md#borrow-before-projecting-an-owned-jvm-key)
targets temporary strings on cache misses and after cold bypass. It retains the released
selector's owned key copy and ordinary callback inputs. Matched distinct-key Java/Rust profiles
show lower sampled allocations but observed external work; their timings are excluded. The
[full evidence](results/jvm-aggregate-borrowed-key-projection-2026-10-16.json) includes both
recordings, the earlier diagnostic, source hashes and both released-line validation suites.
No new clean whole-job speed comparison or default admission is claimed.

## Actual previous-runtime control

After candidate measurements and profiles finish, the workflow builds and measures immutable
revision `b965b8bf0c97450f6971f20db0fe6abc6bbac8ae`, before JVM table/aggregate implementation.
`bin/bench-jvm-previous.py` creates a separate worktree and copies only the current aggregate
fixture, table-function fixture and metrics helper. Production source stays at the previous
revision. Both engines use symmetric preflight and one cluster per job; the old native-planner
attempt must fall back completely, with zero native substitutions and runtime operators.
It is an actual historical runtime comparison, rather than a current-code feature toggle.

The driver requires JDK17, finishes compilation before measurements, then repeats all nine
typed-key aggregate cases and four table-function shapes. Native build inputs (the full native
tree, root/module build files, Cargo configuration and toolchain files) are compared against
the candidate. Identical inputs reuse its release payload with a recorded checksum; otherwise
the previous revision builds its own release library first. No source/sink or boundary is
removed from either implementation; whole-query fallback naturally has no native transposes.
The previous-runtime jobs never overlap their build with measurements or run concurrently
with candidate measurements on the same runner.

Commands, fixture hashes, actual runtime/fixture revisions, native checksum, every trial and
GC log are retained under the workflow artifact's `previous-runtime/` directory. JSON uses
`previous_seconds`; inherited raw fixture logs label that engine `native` even though the
assertions require fallback. Separate JVMs and control drift still limit causal attribution.
The new full-size previous-runtime comparison remains pending until that workflow completes.

After the candidate release build, reproduce with a new worktree path:

```sh
python3 bin/bench-jvm-previous.py --flink-line 2.2 \
  --worktree native/target/jvm-previous-runtime \
  --output native/target/jvm-aggregate-diagnostics/previous-runtime
```

Use `--flink-line 1.18` after the corresponding profile build. `--smoke` selects 2,000 rows,
one warmup/pair and one cardinality per key type plus all four table-function shapes; those
durations are fixture checks, not speed evidence. Existing worktrees are never replaced.

[Both released-line fixture checks](results/jvm-previous-runtime-fixture-validation-2026-10-16.json)
pass all 14 typed aggregate/table-function smoke shapes on the actual prior runtime. The
artifact records fixture hashes, stock/fallback trials, production-source cleanliness and
complete build/query logs. These small jobs establish execution and fallback, not throughput.
