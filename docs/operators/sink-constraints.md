# Sink constraints

**Status: partial.** `NativeConstraintEnforcer` is a connector-neutral Arrow operator. It currently
implements only top-level NOT NULL with Flink's `table.exec.sink.not-null-enforcer=ERROR` policy;
its name does not imply support for every constraint implemented by Flink's rowwise enforcer.

## Admitted boundaries

Both [Paimon](../connectors/paimon.md) adapters use the shared operator for their already-admitted
NOT NULL ERROR targets. It runs before local merging and bucket routing, so a later update cannot
hide an earlier invalid row. The extraction does not change connector admission or add constraint
support to other sinks. Paimon's existing `native-paimon-not-null-enforcer` transformation name and
position are unchanged.

Connector planning still derives required fields from the physical target schema and verifies
formats, types, changelog modes and options independently. No row-to-Arrow conversion is inserted
just to reach this operator.

## Runtime semantics and ownership

The normal path checks Arrow null counts on constrained columns and forwards the original batch
and stream record. Inspection does not take an Arrow consumer share, create a replacement batch,
or materialize RowData. RowKinds, timestamps, routing and source metadata stay on the same batch.

On a violation, the operator searches in row order and then target-column order, matching stock
Flink's first offending row, column and exception message. Any valid prefix is forwarded before
the error; the exception type follows the target Flink line as well. The failing row and later rows
are not forwarded. Prefix slices retain their RowKinds, row-aligned ordering tags, routing and
source metadata. Slicing is an error-path operation, not a
claim that every Arrow layout slices without allocating.

The rejected consumer releases its owned root synchronously. Other consumers of a shared batch
and the emitted prefix retain independent references to their buffers. A downstream failure while
accepting an unclaimed prefix also closes that prefix. Arrow's `slice` uses transfer pairs to retain
the sliced buffers; closing the original root does not release a prefix's references. Tests read
fixed-width, variable-width and nested prefix values after closing all original consumers and check
that the retained buffers are freed only after the prefix closes.

The operator is stateless and forwards watermarks through Flink's ordinary operator implementation.
It retains no Arrow batches in checkpoint state. Connector commit, recovery and delivery guarantees
are unchanged.

## Remaining gaps and validation boundary

- NOT NULL `DROP` and type-length enforcement remain on the existing Flink fallback paths.
- Nested NOT NULL is not implemented by the shared operator; connector-specific restrictions remain.
- Other sinks do not gain admission merely because a shared enforcer is available.

Operator tests compare exceptions and valid prefixes with the released Flink enforcer and cover
identity, shared ownership, nullable children, empty batches, RowKinds, metadata, failure cleanup,
watermarks and stateless restore. This extraction does not establish an end-to-end throughput gain
or independently revalidate Paimon's service recovery guarantees.
