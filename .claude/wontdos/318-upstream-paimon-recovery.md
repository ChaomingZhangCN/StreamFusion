# Preserve released Paimon recovery behavior

[StreamFusion #318](https://github.com/datafusion-contrib/StreamFusion/issues/318) is closed
as not planned. Do not implement a StreamFusion-specific split-commit recovery workaround.

Released Paimon alone reproduces recovery losing a pending COMPACT after APPEND is durable,
including missing final changelog. The stock Flink connector shares the core commit protocol,
but has lookup recovery safeguards; the SDK reproducer does not establish failure in every
stock Flink configuration. Captured durable-state replay agrees between stock and native readers.

Keep canonical released dependencies and the existing recovery diagnostics and upstream tests.
The defect remains an upstream limitation, with a proposed fix in
[apache/paimon#10568](https://github.com/apache/paimon/pull/10568); closing the StreamFusion
issue does not claim that the upstream defect is fixed. Reassess when a verified canonical
release is available, or track a separately demonstrated StreamFusion-only defect.

Evidence and recovery boundaries remain in [the Paimon connector documentation](../../docs/connectors/paimon.md).
