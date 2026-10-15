use crate::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchOptions;

fn invalid(message: &str) -> ArrowError {
    ArrowError::SchemaError(format!("cannot coalesce Arrow batches: {message}"))
}

struct Layout {
    data: Vec<usize>,
    kinds: Option<usize>,
    caches: BTreeMap<usize, usize>,
}

impl Layout {
    fn new(batch: &RecordBatch) -> Result<Self, ArrowError> {
        let mut layout = Self {
            data: Vec::new(),
            kinds: None,
            caches: BTreeMap::new(),
        };
        for (index, field) in batch.schema().fields().iter().enumerate() {
            if field.name() == ROW_KIND_COLUMN {
                if layout.kinds.replace(index).is_some() || field.data_type() != &DataType::Int8 {
                    return Err(invalid("RowKind must be one INT8 column"));
                }
                let kinds = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                if kinds.null_count() != 0
                    || kinds.values().iter().any(|kind| !(0..=3).contains(kind))
                {
                    return Err(invalid(
                        "RowKind must contain non-null Flink tags 0 through 3",
                    ));
                }
            } else if is_string_cache_field(field) {
                let logical = string_cache_index(field.name())
                    .ok_or_else(|| invalid("invalid string-cache name"))?;
                if layout.caches.insert(logical, index).is_some() {
                    return Err(invalid("duplicate string cache"));
                }
                let DataType::List(child) = field.data_type() else {
                    return Err(invalid("string cache must be LIST<UINT16>"));
                };
                if child.data_type() != &DataType::UInt16 || child.is_nullable() {
                    return Err(invalid("string cache must contain non-null UINT16 units"));
                }
            } else {
                layout.data.push(index);
            }
        }
        for &logical in layout.caches.keys() {
            let Some(&index) = layout.data.get(logical) else {
                return Err(invalid("string-cache index exceeds logical arity"));
            };
            if batch.column(index).data_type() != &DataType::Utf8 {
                return Err(invalid("string cache must accompany a UTF8 column"));
            }
        }
        Ok(layout)
    }
}

/// UNION inputs share logical types, but can use different names and optional physical sidecars.
/// Normalize by logical position and sidecar identity before Arrow's positional concat. Existing
/// arrays are only borrowed here; concat owns the single materialization of the merged columns.
pub(crate) fn coalesce_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ArrowError> {
    let first = batches.first().ok_or_else(|| invalid("no input batches"))?;
    let layouts = batches
        .iter()
        .map(Layout::new)
        .collect::<Result<Vec<_>, _>>()?;
    if batches
        .iter()
        .skip(1)
        .all(|batch| batch.schema() == first.schema())
    {
        return concat_batches(&first.schema(), batches);
    }
    let mut fields: Vec<Field> = layouts[0]
        .data
        .iter()
        .map(|&index| first.schema().field(index).clone())
        .collect();
    let mut caches = BTreeMap::<usize, Field>::new();
    let with_kinds = layouts.iter().any(|layout| layout.kinds.is_some());
    for (batch, layout) in batches.iter().zip(&layouts) {
        if layout.data.len() != fields.len() {
            return Err(invalid("logical column counts differ"));
        }
        for (field, &index) in fields.iter_mut().zip(&layout.data) {
            let schema = batch.schema();
            let other = schema.field(index);
            if field.data_type() != other.data_type() {
                return Err(invalid("logical column types differ"));
            }
            if other.is_nullable() {
                *field = field.clone().with_nullable(true);
            }
        }
        for (&logical, &index) in &layout.caches {
            let schema = batch.schema();
            let field = schema.field(index);
            if let Some(previous) = caches.get(&logical) {
                if previous.data_type() != field.data_type() {
                    return Err(invalid("string-cache types differ"));
                }
            } else {
                caches.insert(logical, field.clone().with_nullable(true));
            }
        }
    }
    if with_kinds {
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
    }
    fields.extend(caches.values().cloned());
    let schema = Arc::new(Schema::new(fields));
    let normalized = batches
        .iter()
        .zip(&layouts)
        .map(|(batch, layout)| {
            let mut columns: Vec<ArrayRef> = layout
                .data
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect();
            if with_kinds {
                columns.push(match layout.kinds {
                    Some(index) => batch.column(index).clone(),
                    None => Arc::new(Int8Array::from(vec![0; batch.num_rows()])),
                });
            }
            for (&logical, field) in &caches {
                columns.push(match layout.caches.get(&logical) {
                    Some(&index) => batch.column(index).clone(),
                    None => new_null_array(field.data_type(), batch.num_rows()),
                });
            }
            RecordBatch::try_new_with_options(
                schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    concat_batches(&schema, &normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(values: Vec<Option<i64>>, kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = vec![Field::new("v", DataType::Int64, true)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values))];
        if let Some(kinds) = kinds {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn mixed_layouts_preserve_values_nulls_and_all_four_kinds() {
        let append = batch(vec![Some(10), None], None);
        let changes = batch(
            vec![Some(20), None, Some(22), Some(23)],
            Some(vec![0, 1, 2, 3]),
        );
        for reverse in [false, true] {
            let inputs = if reverse {
                vec![changes.clone(), append.clone()]
            } else {
                vec![append.clone(), changes.clone()]
            };
            let merged = coalesce_batches(&inputs).unwrap();
            let expected = if reverse {
                vec![0, 1, 2, 3, 0, 0]
            } else {
                vec![0, 0, 0, 1, 2, 3]
            };
            assert_eq!(
                row_kind_column(&merged).unwrap().values().as_ref(),
                expected
            );
            assert_eq!(merged.num_rows(), 6);
            assert_eq!(merged.column(0).null_count(), 2);
            let projected: Vec<_> = inputs
                .iter()
                .map(|input| input.project(&[0]).unwrap())
                .collect();
            let expected_values = concat_batches(&projected[0].schema(), &projected).unwrap();
            assert_eq!(merged.column(0), expected_values.column(0));
        }
    }

    #[test]
    fn empty_zero_column_and_zero_row_batches_retain_counts() {
        assert!(coalesce_batches(&[]).is_err());
        let zero_columns = |rows| {
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()
        };
        assert_eq!(
            coalesce_batches(&[zero_columns(2), zero_columns(3)])
                .unwrap()
                .num_rows(),
            5
        );
        for inputs in [
            vec![batch(vec![], None), batch(vec![], Some(vec![]))],
            vec![batch(vec![], Some(vec![])), batch(vec![None], None)],
        ] {
            let merged = coalesce_batches(&inputs).unwrap();
            assert_eq!(
                merged.num_rows(),
                inputs.iter().map(RecordBatch::num_rows).sum::<usize>()
            );
            assert!(row_kind_column(&merged).is_some());
        }
    }

    #[test]
    fn cache_sidecars_align_by_logical_index_and_missing_caches_are_null() {
        let make = |cache: bool, kinds: bool| {
            let mut fields = vec![Field::new("s", DataType::Utf8, true)];
            let mut columns: Vec<ArrayRef> =
                vec![Arc::new(StringArray::from(vec![Some("?"), None]))];
            if kinds {
                fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
                columns.push(Arc::new(Int8Array::from(vec![1, 2])));
            }
            if cache {
                let values = string_cache_array([Some(&[0xd800][..]), None]);
                fields.push(string_cache_field(0, values.data_type().clone()));
                columns.push(values);
            }
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        for reverse in [false, true] {
            let inputs = if reverse {
                vec![make(false, true), make(true, false)]
            } else {
                vec![make(true, false), make(false, true)]
            };
            let output = coalesce_batches(&inputs).unwrap();
            let cache = JavaStringCacheColumn::new(string_cache_column(&output, 0).unwrap());
            assert_eq!(
                cache.value(if reverse { 2 } else { 0 }),
                Some(&[0xd800][..])
            );
            assert_eq!(cache.value(if reverse { 0 } else { 2 }), None);
            assert_eq!(
                row_kind_column(&output).unwrap().values().as_ref(),
                if reverse {
                    vec![1, 2, 0, 0]
                } else {
                    vec![0, 0, 1, 2]
                }
            );
        }
    }

    #[test]
    fn caches_for_different_logical_columns_do_not_exchange_string_values() {
        let make = |logical| {
            let values = if logical == 0 {
                ["?", "left"]
            } else {
                ["right", "?"]
            };
            let mut fields = vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ];
            let mut columns: Vec<ArrayRef> = values
                .iter()
                .map(|value| Arc::new(StringArray::from(vec![*value])) as ArrayRef)
                .collect();
            if logical == 1 {
                fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
                columns.push(Arc::new(Int8Array::from(vec![2])));
            }
            let units: &[u16] = if logical == 0 { &[0xd800] } else { &[0xdc00] };
            let cache = string_cache_array([Some(units)]);
            fields.push(string_cache_field(logical, cache.data_type().clone()));
            columns.push(cache);
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        for reverse in [false, true] {
            let mut inputs = vec![make(0), make(1)];
            if reverse {
                inputs.reverse();
            }
            let output = coalesce_batches(&inputs).unwrap();
            assert_eq!(output.num_columns(), 5);
            for (row, input) in inputs.iter().enumerate() {
                for logical in 0..2 {
                    let actual = output
                        .column(logical)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let expected = input
                        .column(logical)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(actual.value(row), expected.value(0));
                    let cache =
                        JavaStringCacheColumn::new(string_cache_column(&output, logical).unwrap());
                    let expected = string_cache_column(input, logical)
                        .and_then(|cache| JavaStringCacheColumn::new(cache).value(0));
                    assert_eq!(cache.value(row), expected);
                }
            }
            assert_eq!(
                row_kind_column(&output).unwrap().values().as_ref(),
                if reverse { &[2, 0] } else { &[0, 2] }
            );
        }
    }

    #[test]
    fn incompatible_shapes_fail_before_arrow_concat() {
        let plain = batch(vec![Some(1)], None);
        let different_type = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
        )])
        .unwrap();
        let extra = RecordBatch::try_from_iter(vec![
            ("v", plain.column(0).clone()),
            ("extra", plain.column(0).clone()),
        ])
        .unwrap();
        for other in [different_type, extra, batch(vec![Some(1)], Some(vec![4]))] {
            for inputs in [
                vec![plain.clone(), other.clone()],
                vec![other.clone(), plain.clone()],
            ] {
                assert!(coalesce_batches(&inputs).is_err());
            }
        }
    }
}
