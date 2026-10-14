use super::*;

fn scalar(datatype: &DataType, value: Option<f64>) -> ScalarValue {
    match datatype {
        DataType::Float32 => ScalarValue::Float32(value.map(|v| v as f32)),
        DataType::Float64 => ScalarValue::Float64(value),
        _ => unreachable!(),
    }
}

fn number(datatype: &DataType, value: f64) -> Num {
    match datatype {
        DataType::Float32 => Num::F32(value as f32),
        DataType::Float64 => Num::F64(value),
        _ => unreachable!(),
    }
}

#[test]
fn boxed_extrema_keys_canonicalize_nan_and_keep_both_zero_signs() {
    for datatype in [DataType::Float32, DataType::Float64] {
        let negative_nan = MinMaxKey::from_scalar(&scalar(&datatype, Some(-f64::NAN)));
        let positive_nan = MinMaxKey::from_scalar(&scalar(&datatype, Some(f64::NAN)));
        assert!(negative_nan == positive_nan);
        assert_eq!(negative_nan.cmp(&positive_nan), std::cmp::Ordering::Equal);
        let mut keys = ahash::HashSet::default();
        keys.insert(negative_nan);
        assert!(!keys.insert(positive_nan));
        let positive_zero = MinMaxKey::from_scalar(&scalar(&datatype, Some(0.0)));
        let negative_zero = MinMaxKey::from_scalar(&scalar(&datatype, Some(-0.0)));
        assert!(negative_zero < positive_zero);
        assert!(keys.insert(positive_zero));
        assert!(keys.insert(negative_zero));
        assert_eq!(keys.len(), 3);
    }
}

#[test]
fn retractable_extrema_count_nan_duplicates_and_reveal_signed_zeros() {
    for datatype in [DataType::Float32, DataType::Float64] {
        for kind in [1, 2] {
            let mut state = GroupAggState::new(kind, &datatype);
            for value in [-f64::NAN, 0.0, -0.0, f64::NAN] {
                state.accumulate(number(&datatype, value));
            }
            assert!(crate::flink_float::boxed_equal(
                &state.emit(&datatype),
                &scalar(&datatype, Some(if kind == 1 { -0.0 } else { f64::NAN }))
            ));
            state.retract(number(&datatype, -f64::NAN));
            state.retract(number(&datatype, f64::NAN));
            assert_eq!(
                state.emit(&datatype),
                scalar(&datatype, Some(if kind == 1 { -0.0 } else { 0.0 }))
            );
            state.retract(number(&datatype, -0.0));
            assert_eq!(state.emit(&datatype), scalar(&datatype, Some(0.0)));
            state.retract(number(&datatype, 0.0));
            assert!(state.emit(&datatype).is_null());
        }
    }
}

#[test]
fn append_sum_and_extrema_keep_the_first_negative_zero() {
    for datatype in [DataType::Float32, DataType::Float64] {
        for kind in [0, 10, 11] {
            let mut state = GroupAggState::new(kind, &datatype);
            state.accumulate(number(&datatype, -0.0));
            assert_eq!(state.emit(&datatype), scalar(&datatype, Some(-0.0)));
            state.accumulate(number(&datatype, 0.0));
            assert_eq!(
                state.emit(&datatype),
                scalar(&datatype, Some(if kind == 0 { 0.0 } else { -0.0 }))
            );
        }
    }
}

#[test]
fn ordered_values_retract_nan_by_boxed_equality_and_preserve_zeros() {
    for datatype in [DataType::Float32, DataType::Float64] {
        for kind in [15, 16] {
            let mut state = OrderedValueState::new(kind, &datatype);
            for value in [-f64::NAN, -0.0, 0.0, 4.0] {
                state.update(scalar(&datatype, Some(value)), false).unwrap();
            }
            state
                .update(scalar(&datatype, Some(f64::NAN)), true)
                .unwrap();
            state.update(scalar(&datatype, Some(4.0)), true).unwrap();
            assert_eq!(
                state.emit(),
                scalar(&datatype, Some(if kind == 15 { -0.0 } else { 0.0 }))
            );
            state.update(scalar(&datatype, Some(0.0)), true).unwrap();
            assert_eq!(state.emit(), scalar(&datatype, Some(-0.0)));
        }
    }
}

#[test]
fn grouped_float_multisets_and_order_survive_arrow_snapshot_restore() {
    for (datatype, code) in [(DataType::Float32, 6), (DataType::Float64, 1)] {
        let batch = |values: Vec<Option<f64>>, kinds: Vec<i8>| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("v", datatype.clone(), true),
                    Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
                ])),
                vec![
                    scalars_to_array(
                        values.into_iter().map(|v| scalar(&datatype, v)).collect(),
                        &datatype,
                    ),
                    Arc::new(Int8Array::from(kinds)),
                ],
            )
            .unwrap()
        };
        let kinds = vec![1, 2, 15, 16];
        let mut aggregate =
            GroupAggregator::new(kinds.clone(), vec![code; 4], vec![0; 4], vec![], true);
        aggregate
            .update(
                &batch(
                    vec![Some(-f64::NAN), Some(-0.0), Some(0.0), Some(3.0), None],
                    vec![0; 5],
                ),
                0,
            )
            .unwrap();
        let mut restored = GroupAggregator::restore(
            kinds,
            vec![code; 4],
            vec![0; 4],
            vec![],
            true,
            &aggregate.snapshot(),
            0,
        );
        let output = restored
            .update(&batch(vec![Some(f64::NAN), Some(3.0)], vec![3; 2]), 0)
            .unwrap();
        let row = output.num_rows() - 1;
        for (column, value) in [(0, -0.0), (1, 0.0), (2, -0.0), (3, 0.0)] {
            assert_eq!(
                ScalarValue::try_from_array(output.column(column), row).unwrap(),
                scalar(&datatype, Some(value))
            );
        }
    }
}
