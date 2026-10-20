# CI setup and failure handling

Ordinary CI runs Rust tests, release Criterion fixture checks, Java tests on Flink 2.2 and
1.18, optional lake and format/connector suites, and optimized image integration tests.
The upstream Flink workflow runs separately; both aggregate gates must pass before merging.
Confirm the actual post-merge workflows as well as the PR checks when verifying a repair.

CI, upstream Flink, and Paimon compatibility workflows cancel older test runs when a newer
push or PR update targets the same ref. The latest main commit includes the earlier changes
and receives the complete checks, without waiting behind tests for superseded main commits.
Release publishing retains its non-canceling concurrency policy.

## Shared debug native libraries

Ordinary CI builds all workspace debug native libraries once in a dedicated Ubuntu 24.04
job. That job verifies the package dependencies and JNI export boundaries, then uploads only
the shared libraries as a commit-specific artifact. The four Java and eleven lake-module
test jobs download those libraries into `native/target/debug` and pass
`-Dnative.build.skip=true` to Maven. Both JDKs and Flink lines exercise the same native build;
each consumer still compiles its selected Java reactor and runs its existing tests. A failed
producer blocks the consumers and fails the aggregate CI gate. Consumers have no fallback to
an older cache or a local native rebuild.

The producer is the only writer of the `debug-jvm` dependency cache. Consumers transfer final
libraries rather than restoring Cargo intermediates or installing a Rust toolchain. The Rust
test job separately compiles its test harnesses and checks every tagged native package in
isolation once, preserving the checks for dependencies accidentally supplied by the workspace.
The native build retains Cargo's `--timings` report as a separate diagnostic artifact.

Optimized image builds, release Criterion executables, and the separately prepared upstream
Flink suite keep their own build configurations. The debug artifact is scoped to its current
workflow run and commit; it is not a release payload or a cache of test outcomes. This replaces
the independent debug library builds with one without changing test selection or test concurrency.
Actual elapsed-time savings must be measured in GitHub Actions after this change runs.

All Rust workflows use the local `setup-rust` action to install Rust 1.94.0 and remove the
runner image's unused `stable` toolchain before restoring caches. The Rust cache action hashes
every installed compiler, not just the selected compiler. Previous logs showed the unused
hosted toolchain changing from 1.99.0 to 1.99.1 while the project stayed on 1.94.0, splitting
the same debug dependency cache into two approximately 2.43 GiB entries. Removing the unused
toolchain prevents those image updates from invalidating the pinned build's cache; dependency,
compiler-option, target and actual project-toolchain changes still affect the cache keys.

## Connector and format test coverage

The full Java suite includes every connector and format tag on both Flink lines and both
JDKs. Its default Surefire execution runs untagged tests and every tag except ORC; a second
execution runs ORC with its isolated protobuf classpath. These same tests do not need six
additional tagged-module jobs. Standalone native package checks remain in the Rust job.
`bin/test_ci_module_groups.py` exercises the actual runtime Surefire configuration with one
fixture for every declared module tag and one untagged fixture, requiring each to run once.

## Consolidated feature validation

Keep related coverage and default-enable changes on one PR while repairing their combined CI.
A newer PR head cancels obsolete runs, including JVM aggregate release diagnostics; correctness
checks and diagnostic fixtures still run on the current head. Superseded PRs should be closed
and their active runs canceled rather than leaving multiple copies queued.

Default-enabled paths need positive native-route assertions. Fixed-size-binary compatibility
tests construct that legacy physical schema explicitly rather than changing global feature
flags. Component-timestamp extrema exercise checkpoint/retraction values on RocksDB, including
cases that previously asserted fallback. Hidden-offset recovery expectations distinguish heap
row mutation from serialized state copies; the released-Flink SQL controls remain authoritative.
Nested connector schemas must exclude internal ROW transport fields while retaining user fields,
nulls, logical ordering and independently owned Arrow buffer references.

## Maven dependencies

Java CI jobs resolve their selected reactor's dependencies before building or testing:

```sh
python3 bin/ci-maven-dependencies.py --projects streamfusion-runtime
python3 bin/ci-maven-dependencies.py --profile flink-1.18 --projects streamfusion-runtime
```

The helper invokes the released Maven dependency plugin's `go-offline` goal, which resolves
project, plugin and report dependencies without invoking a build or test lifecycle. It keeps
the job's profiles, project selection and reactor dependencies. The settings file uses the
project's public repositories without adding mirrors or changing dependency versions.

Resolution has three attempts, with waits of 5 and 15 seconds and a ten-minute limit per
attempt. Each attempt uses Maven's `-U` option to recheck missing releases, including a lookup
that failed during a previous attempt. Persistent download failures, invalid dependencies and
timeouts remain blocking. Once resolution succeeds, the existing build and test commands run
once with their original assertions and failure handling. Tests are never retried by this helper;
their commands still use normal Maven resolution for any artifacts requested dynamically by plugins.

[PR #311's post-merge row-format job](https://github.com/datafusion-contrib/StreamFusion/actions/runs/37938400080/job/113846210991)
failed before compilation because one runner reported published Flink 2.2.1 and Kafka dependencies
as missing. Other jobs resolved those releases successfully. Separating downloads from tests
allows transient resolution failures to recover without rerunning test code; forcing missing-release
checks prevents Maven's failed-lookup cache from defeating the next setup attempt.

The upstream suite transfers its prepared pinned Maven distribution to its consumer runners
and verifies that it starts with downloads blocked. See the
[shared-build guide](upstream-flink-suite.md#shared-builds-and-runtime-shards).
