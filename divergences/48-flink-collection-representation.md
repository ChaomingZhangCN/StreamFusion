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

Represented nested-row vectors reuse
their reader for the lifetime of the batch; each returned row is still copied before that reader
can move to another index.

Entry transposes capture an explicit serialized schema before task execution; JVM result
registration also freezes its schema for the task lifetime. Ordinary schema conversion remains
unchanged. The extended layout is selected through an explicit API, not task JVM configuration.

Arrow Java 18.3 requires a non-nullable map-key field declaration. The declaration remains,
while the verified readers and writers retain actual NULL-key validity. This layout does not
admit Arrow map comparison or state operations over those values.

This foundation adds no SQL admission. The [Calc/filter page](../docs/operators/calc-filter.md)
remains the source of truth for available functions and boundaries.
