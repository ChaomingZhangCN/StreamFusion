# Flink collection value representation

Comet's batch-kernel input adapters create fresh nested views over Arrow array, struct and map
slices. StreamFusion follows their lifetime rule: each view belongs to its input batch, and a
synchronous generated evaluator writes its result before that batch closes. No input buffer or
mutable lookup view is retained across evaluations.

Flink adds two deterministic contracts beyond the SQL fields. BinaryStringData can retain a Java
UTF-16 cache, which changes comparison order and preserves isolated surrogates in string consumers.
Nested RowData also has a RowKind. Ordinary UTF-8 bytes and SQL struct fields cannot reconstruct
either detail. The experimental collection entry schema therefore carries nested strings as a
marked struct of UTF-8 plus an optional list of UTF-16 units, and appends a marked kind child to
nested rows. Markers distinguish these children from user fields; row-kind names avoid collisions.
Arrow reversal and range copying retain the complete children and their validity.

Flink's ArrayDataSerializer preserves Java caches when copying GenericArrayData and
ColumnarArrayData. It serializes unfamiliar ArrayData implementations to binary instead. Since
those Flink classes are final, a custom cache-aware view cannot inherit their copy behavior.
Represented arrays undergo the released serializer's snapshot before writing. Their element
carriers record the resulting generic/binary copy category, including when the first slot is NULL.
Readers restore that category: generic arrays share cached strings between consumers, while binary
arrays return fresh values. Row headers also retain their generic/binary category. Ordinary arrays
keep the existing columnar views. SQL checks include generic, binary and independent proxy arrays,
repeated string consumers, NULLs in declared NOT NULL elements and every nested row kind.

Programs containing only nullable primitive collection types keep ordinary layouts and avoid
serializer snapshots that preserve no additional identity. Represented nested-row vectors reuse
their reader for the lifetime of the batch; each returned row is still copied before that reader
can move to another index.

The planner captures the entry and generated-result Arrow schemas in serialized operator/function
specifications. Task JVM properties cannot change those layouts after planning. Connector schemas
retain their ordinary layouts. Representation-sensitive inputs from other columnar producers and
string/ROW/MAP-bearing or NOT NULL-element array results crossing unverified operator boundaries
keep the host route. These schemas are scoped to generated collection programs; merely enabling
the experimental switch does not change other operators' schemas.

Arrow Java 18.3 requires a non-nullable map-key field declaration. The existing declaration stays,
while Flink's logical key nullability and actual vector validity preserve NULL keys in the verified
generated collection readers/writers. This does not admit Arrow map comparison or state operations
over those extended results. Tests cover nullable keys at fourteen released bridge types/nullability combinations, nested
values, empty/NULL maps and runtime constructors.

The switch defaults on under the maintainer's explicit acceptance on 2026-10-17, despite slower
full row-source jobs. Setting it false restores host execution for the extension.
[Issue #392](https://github.com/datafusion-contrib/StreamFusion/issues/392) tracks performance improvements. Coverage, restrictions and
performance evidence belong in [Calc/filter](../docs/operators/calc-filter.md#collection-expressions)
and the [scalar benchmark page](../docs/benchmarks/scalar-functions.md#extended-function-coverage).
