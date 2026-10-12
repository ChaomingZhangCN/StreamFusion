use super::*;

/// Flink's one-field SUM0 buffer, shared with GROUP BY's exact running state. NULL inputs are
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
