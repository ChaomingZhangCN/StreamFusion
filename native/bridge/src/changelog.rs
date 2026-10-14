use crate::*;

/// The hidden column carrying a changelog row's `RowKind` as a byte across the row/Arrow boundary
/// (must match the JVM converter's column name). See divergences/13.
pub const ROW_KIND_COLUMN: &str = "$row_kind$";

/// The `$row_kind$` byte column if the batch carries one. A columnar batch from an insert-only
/// producer (a native source/exchange) has none — every row is then an INSERT.
pub fn row_kind_column(batch: &RecordBatch) -> Option<&Int8Array> {
    batch.column_by_name(ROW_KIND_COLUMN).map(|column| {
        column
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("row kind must be int8")
    })
}

/// Logical data columns, excluding RowKind and Java string-cache companions.
pub fn data_arity(batch: &RecordBatch) -> usize {
    batch
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != ROW_KIND_COLUMN && !crate::is_string_cache_field(field))
        .count()
}
