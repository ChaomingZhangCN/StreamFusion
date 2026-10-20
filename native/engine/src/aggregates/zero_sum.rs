use super::*;

/// Flink's one-field SUM0 buffer, shared with GROUP BY's typed running state. NULL inputs are
/// ignored; NULL decimal partials instead carry sticky overflow through merge and restore.
#[derive(Debug)]
pub(super) struct ZeroSumAccumulator {
    sum: RunningAgg,
}

impl ZeroSumAccumulator {
    pub(super) fn new(value_type: &DataType) -> Self {
        Self {
            sum: RunningAgg::new(22, value_type),
        }
    }

    fn fold<T: arrow::datatypes::ArrowPrimitiveType>(
        &mut self,
        values: &ArrayRef,
        changes: Option<&Int8Array>,
        merge: bool,
        number: impl Fn(T::Native) -> Num,
    ) {
        let array = values
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<T>>()
            .expect("SUM0 input type");
        for (row, value) in array.iter().enumerate() {
            if let Some(value) = value {
                let value = number(value);
                if changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3)) {
                    self.sum.retract(value);
                } else {
                    self.sum.fold(value);
                }
            } else if merge {
                if let RunningAgg::AvgPartialSumDecimal { overflow, .. } = &mut self.sum {
                    *overflow = true;
                }
            }
        }
    }

    fn update(&mut self, values: &[ArrayRef], merge: bool) {
        use arrow::datatypes::*;
        let changes = values.get(1).map(|array| {
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("row kinds")
        });
        match values[0].data_type() {
            DataType::Float32 => self.fold::<Float32Type>(&values[0], changes, merge, Num::F32),
            DataType::Float64 => self.fold::<Float64Type>(&values[0], changes, merge, Num::F64),
            DataType::Int8 => self.fold::<Int8Type>(&values[0], changes, merge, Num::I8),
            DataType::Int16 => self.fold::<Int16Type>(&values[0], changes, merge, Num::I16),
            DataType::Int32 => self.fold::<Int32Type>(&values[0], changes, merge, Num::I32),
            DataType::Int64 => self.fold::<Int64Type>(&values[0], changes, merge, Num::I64),
            DataType::Decimal128(_, _) => {
                self.fold::<Decimal128Type>(&values[0], changes, merge, Num::I128)
            }
            other => panic!("unsupported SUM0 input type: {other:?}"),
        }
    }
}

impl Accumulator for ZeroSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update(values, false);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update(states, true);
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.emit()])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(self.sum.emit())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array};

    #[test]
    fn float_zero_sum_keeps_zero_initializer_and_rounds_each_addition_at_input_width() {
        let mut sum = ZeroSumAccumulator::new(&DataType::Float32);
        sum.update_batch(&[Arc::new(Float32Array::from(vec![None, Some(-0.0)]))])
            .unwrap();
        match sum.evaluate().unwrap() {
            ScalarValue::Float32(Some(value)) => assert_eq!(value.to_bits(), 0),
            other => panic!("expected positive float zero, got {other:?}"),
        }
        sum.update_batch(&[Arc::new(Float32Array::from(vec![
            16_777_216.0,
            1.0,
            -16_777_216.0,
        ]))])
        .unwrap();
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Float32(Some(0.0)));
        assert_eq!(sum.state().unwrap(), vec![ScalarValue::Float32(Some(0.0))]);
    }

    #[test]
    fn double_zero_sum_keeps_zero_initializer_and_rounds_at_double_width() {
        let mut sum = ZeroSumAccumulator::new(&DataType::Float64);
        sum.update_batch(&[Arc::new(Float64Array::from(vec![None, Some(-0.0)]))])
            .unwrap();
        match sum.evaluate().unwrap() {
            ScalarValue::Float64(Some(value)) => assert_eq!(value.to_bits(), 0),
            other => panic!("expected positive double zero, got {other:?}"),
        }
        sum.update_batch(&[Arc::new(Float64Array::from(vec![
            9_007_199_254_740_992.0,
            1.0,
            -9_007_199_254_740_992.0,
        ]))])
        .unwrap();
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn typed_zero_sum_state_restores_before_retraction_and_merge() {
        for data_type in [DataType::Float32, DataType::Float64] {
            let (one, two, saved) = if data_type == DataType::Float32 {
                (
                    Num::F32(1.0),
                    Num::F32(2.0),
                    ScalarValue::Float32(Some(3.0)),
                )
            } else {
                (
                    Num::F64(1.0),
                    Num::F64(2.0),
                    ScalarValue::Float64(Some(3.0)),
                )
            };
            let mut running = RunningAgg::new(22, &data_type);
            running.restore_value(&saved);
            running.retract(two);
            running.fold(one);
            let expected = if data_type == DataType::Float32 {
                ScalarValue::Float32(Some(2.0))
            } else {
                ScalarValue::Float64(Some(2.0))
            };
            assert_eq!(running.emit(), expected);
            assert_eq!(running.state_type(), data_type);
            assert_eq!(
                crate::group_agg::emit_running(&running, 0, &data_type),
                expected
            );
            let mut merged = ZeroSumAccumulator::new(&data_type);
            merged
                .merge_batch(&[running.emit().to_array().unwrap()])
                .unwrap();
            assert_eq!(merged.evaluate().unwrap(), expected);
        }
    }

    #[test]
    fn floating_zero_sum_retraction_preserves_ieee_infinity_and_nan() {
        let mut sum = ZeroSumAccumulator::new(&DataType::Float32);
        sum.update_batch(&[Arc::new(Float32Array::from(vec![f32::MAX, f32::MAX]))])
            .unwrap();
        assert_eq!(
            sum.evaluate().unwrap(),
            ScalarValue::Float32(Some(f32::INFINITY))
        );
        sum.update_batch(&[
            Arc::new(Float32Array::from(vec![f32::INFINITY])),
            Arc::new(Int8Array::from(vec![3])),
        ])
        .unwrap();
        match sum.evaluate().unwrap() {
            ScalarValue::Float32(Some(value)) => assert!(value.is_nan()),
            other => panic!("expected NaN, got {other:?}"),
        }
    }
}
