# Nested Flink ROW kinds

Comet's Arrow struct vectors expose logical children directly. Flink's Java `Row` and `RowData`
also carry a `RowKind`, including when a ROW is a SQL field or an ARRAY element. A Java scalar
function can inspect that kind. Carrying only the SQL fields changes observable callback results:
the nested-kind fixture computed different BIGINT values from stock Flink for UPDATE_BEFORE,
UPDATE_AFTER and DELETE inputs.

The default-off `streamfusion.experimental.nestedScalarUdfs` investigation appends a nullable Int8 child
tagged `streamfusion.nested-row-kind=1` to each SQL ROW's Arrow struct. Readers/writers keep it
outside logical field ordinals. The field name is made unique when user fields use the internal
prefix. NULL parents remain NULL, and the marker follows nested structs and array elements through
ordinary Arrow ownership and serialization. No borrowed Arrow value is retained by a callback;
released Flink converters materialize the external Java ROW, and results are written immediately.

The generated Calc's outer struct is a projection container rather than a SQL ROW value, so it
has no extra kind child. ROW-valued fields inside it do. Computed ROW-valued programs and filters, including SQL ROW constructors and comparisons,
use Flink whole-Calc generation. Pure input-reference projections keep the native path. Missing or NULL markers read as INSERT. Arrow type compatibility compares logical
children separately from the marked child.

Native key encoding excludes metadata from logical type descriptors and writes its value into
the released Flink BinaryRow header. ARRAY-of-ROW UNNEST excludes the tagged child from output
columns. Sort-key preparation compares a constant kind child while preserving the physical schema
and the original payload: Flink SQL ROW ordering ignores RowKind. This transport does not replace
Flink's released array converters; generic Java-backed array elements changed Unicode ordering
in an ARRAY_SORT regression and were rejected.

This differs from Comet because Spark structs have no corresponding per-value changelog kind.
The existing outer record RowKind column is unchanged. The experiment must be configured
consistently in the planner and task JVMs because the flag also selects this physical struct
schema. Default schemas remain unchanged. Distributed configuration and performance admission
remain part of the investigation before enabling the feature by default.
