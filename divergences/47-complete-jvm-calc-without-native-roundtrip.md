# Complete generated JVM Calcs can stay on Java Arrow batches

Comet's UDF bridge exports Arrow arguments from its native executor and invokes a generated JVM
batch kernel. StreamFusion follows that pattern for expressions inside Rust Calc execution.

A complete Flink-generated Calc has a narrower opportunity: its owning Flink operator already
runs on the JVM and owns an Arrow batch, while every expression is scheduled by Java. The
experimental temporal path borrows timestamp input vectors directly and returns Arrow columns,
avoiding a native round trip that performs no useful expression work. Other shapes and
changelog-tagged input retain the native bridge. The operator remains columnar on both sides;
row conversion stays at the island edges. Function binding, lifecycle, predicates, exceptions
and call order use the same released Flink evaluator.

The reference reviewed before this change is CometUdfBridge.java and
CometBatchKernelCodegen.scala in datafusion-comet. CPU profiles with symbolized Rust frames show
Arrow FFI/ownership and allocator work in the redundant native stage. Exact timestamp output
comparison can retain Arrow buffers after every call executes; changed columns materialize their
prior prefix. This follows Arrow reference ownership and does not infer function purity.

The temporal path remains explicitly experimental and default off: two standalone workloads beat paired
stock controls, but identity still trails by about 1% and the prior-correct-fallback repeat is
outstanding. See docs/operators/calc-filter.md and docs/optimizations/udf-columnar-upcall.md.
The semantic support shipped in [PR #368](https://github.com/datafusion-contrib/StreamFusion/pull/368),
closing #364; [issue #389](https://github.com/datafusion-contrib/StreamFusion/issues/389) tracks
remaining temporal default-performance admission.

Already-admitted complete programs with primitive numeric/boolean inputs and ordinary
nonempty numeric/boolean/string outputs also use this direct batch evaluation for insert-only input.
Their results are independently owned flat Arrow vectors; no timestamp aliases or function-purity
assumptions are involved. String arguments, represented layouts, binary/decimal/temporal outputs,
and RowKind-tagged batches retain the existing bridge. The same function binding, row order and
exception causes remain intact.

This improves the existing shared-stateful opt-in INT/BIGINT/STRING jobs by about 28%/32%/10%, but
they remain slower than their paired stock controls. Expression admission and the shared-stateful
option's default do not change. [Issue #398](https://github.com/datafusion-contrib/StreamFusion/issues/398)
remains open for default performance admission; the current comparison and all samples are in
[the optimization page](../docs/optimizations/udf-columnar-upcall.md#complete-generated-calcs-with-primitive-inputs).
