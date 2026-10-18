use crate::*;
use arrow::array::BooleanArray;

/// Flink's stateless DropUpdateBeforeFunction, retaining row order and every sidecar column.
pub(crate) fn drop_update_before(batch: &RecordBatch) -> RecordBatch {
    let Some(kinds) = row_kind_column(batch) else {
        return batch.clone();
    };
    let retained = BooleanArray::from_iter(kinds.values().iter().map(|kind| Some(*kind != 1)));
    let count = retained.true_count();
    if count == batch.num_rows() {
        batch.clone()
    } else if count == 0 {
        batch.slice(0, 0)
    } else {
        arrow::compute::filter_record_batch(batch, &retained)
            .expect("failed to filter UPDATE_BEFORE rows")
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_dropUpdateBefore<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        export_record_batch(
            drop_update_before(&batch),
            out_array_address,
            out_schema_address,
        );
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, Int64Builder, Int8Array, ListBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    #[test]
    fn retains_other_kinds_and_filters_nested_values_and_sidecars_in_order() {
        let mut lists = ListBuilder::new(Int64Builder::new());
        for i in 0..8 {
            lists.values().append_value(i);
            lists.append(i != 6);
        }
        let nested = lists.finish();
        let fields = vec![
            Field::new("nested", nested.data_type().clone(), true),
            Field::new("cache", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ];
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("sf".into(), "kept".into())]),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(nested),
                Arc::new(Int64Array::from(vec![
                    None,
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    None,
                    Some(7),
                ])),
                Arc::new(Int8Array::from(vec![1, 0, 1, 2, 3, 1, 0, 1])),
            ],
        )
        .unwrap()
        .slice(1, 6);
        let output = drop_update_before(&batch);
        assert_eq!(output.schema(), schema);
        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            row_kind_column(&output).unwrap().values().as_ref(),
            &[0, 2, 3, 0]
        );
        let expected = arrow::compute::take_record_batch(
            &batch,
            &arrow::array::UInt32Array::from(vec![0, 2, 3, 5]),
        )
        .unwrap();
        assert_eq!(output, expected);
    }

    #[test]
    fn absent_kinds_and_all_retained_share_buffers() {
        let batch = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let out = drop_update_before(&batch);
        assert!(Arc::ptr_eq(out.column(0), batch.column(0)));
        let batch = RecordBatch::try_from_iter(vec![
            ("v", batch.column(0).clone()),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(vec![0, 2, 3])) as ArrayRef,
            ),
        ])
        .unwrap();
        let out = drop_update_before(&batch);
        assert!(Arc::ptr_eq(out.column(0), batch.column(0)));
    }

    #[test]
    fn empty_all_removed_and_zero_data_columns_preserve_schema_and_counts() {
        let batch = RecordBatch::try_from_iter(vec![(
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![1, 1])) as ArrayRef,
        )])
        .unwrap();
        let empty = drop_update_before(&batch);
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), batch.schema());
        assert_eq!(drop_update_before(&empty), empty);
        let zero = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(drop_update_before(&zero).num_rows(), 3);
    }
}
