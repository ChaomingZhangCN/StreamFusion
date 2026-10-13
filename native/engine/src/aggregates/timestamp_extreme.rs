use super::*;
use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};

/// Own the two timestamp digits, so extrema never retain a source batch's child buffers.
#[derive(Debug)]
pub(crate) struct TimestampExtremeAccumulator {
    value: Option<TimestampValue>,
    minimum: bool,
}

impl TimestampExtremeAccumulator {
    pub(crate) fn value(&self) -> Option<TimestampValue> {
        self.value
    }

    pub(crate) fn merge_value(&mut self, value: Option<TimestampValue>) {
        if let Some(value) = value {
            if self.value.is_none_or(|current| {
                if self.minimum {
                    value < current
                } else {
                    value > current
                }
            }) {
                self.value = Some(value);
            }
        }
    }

    pub(super) fn new(minimum: bool) -> Self {
        Self {
            value: None,
            minimum,
        }
    }

    pub(crate) fn update_rows(
        &mut self,
        column: &TimestampColumn<'_>,
        rows: &[u32],
    ) -> datafusion::common::Result<()> {
        for &row in rows {
            self.update_row(column, row as usize)?;
        }
        Ok(())
    }

    pub(crate) fn update_row(
        &mut self,
        column: &TimestampColumn<'_>,
        row: usize,
    ) -> datafusion::common::Result<()> {
        if !column.is_null(row) {
            self.merge_value(Some(column.value(row)?));
        }
        Ok(())
    }
}

impl Accumulator for TimestampExtremeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let column = TimestampColumn::try_new(values[0].as_ref())?;
        for row in 0..column.len() {
            self.update_row(&column, row)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Struct(Arc::new(timestamp_array([self.value]))))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampNanosecondArray;

    fn output(accumulator: &mut dyn Accumulator) -> Option<TimestampValue> {
        let array = accumulator.evaluate().unwrap().to_array().unwrap();
        let column = TimestampColumn::try_new(array.as_ref()).unwrap();
        (!column.is_null(0)).then(|| column.value(0).unwrap())
    }

    #[test]
    fn extrema_and_merge_keep_the_full_millisecond_range_and_fractional_digits() {
        let values = [
            TimestampValue::new(i64::MIN, 1).unwrap(),
            TimestampValue::new(-1, 999_999).unwrap(),
            TimestampValue::new(0, 1).unwrap(),
            TimestampValue::new(0, 2).unwrap(),
            TimestampValue::new(i64::MAX, 999_999).unwrap(),
        ];
        let array: ArrayRef = Arc::new(timestamp_array(
            [None].into_iter().chain(values.map(Some)).chain([None]),
        ));
        for minimum in [true, false] {
            let mut accumulator = TimestampExtremeAccumulator::new(minimum);
            accumulator.update_batch(&[array.slice(1, 2)]).unwrap();
            let size = accumulator.size();
            let partial = accumulator.state().unwrap()[0].to_array().unwrap();
            let mut restored = TimestampExtremeAccumulator::new(minimum);
            restored.merge_batch(&[partial]).unwrap();
            restored.update_batch(&[array.slice(3, 4)]).unwrap();
            assert_eq!(
                output(&mut restored),
                Some(if minimum { values[0] } else { values[4] })
            );
            assert_eq!(restored.size(), size);
        }
    }

    #[test]
    fn same_millisecond_ties_and_null_partials_are_compared_without_truncation() {
        for minimum in [true, false] {
            let mut accumulator = TimestampExtremeAccumulator::new(minimum);
            assert_eq!(output(&mut accumulator), None);
            accumulator
                .merge_batch(&[Arc::new(timestamp_array([None, None]))])
                .unwrap();
            assert_eq!(output(&mut accumulator), None);
            let early = TimestampValue::new(-1, 999_998).unwrap();
            let late = TimestampValue::new(-1, 999_999).unwrap();
            accumulator
                .update_batch(&[Arc::new(timestamp_array([Some(late), None]))])
                .unwrap();
            accumulator
                .merge_batch(&[Arc::new(timestamp_array([None, Some(early), Some(late)]))])
                .unwrap();
            assert_eq!(
                output(&mut accumulator),
                Some(if minimum { early } else { late })
            );
        }
    }

    #[test]
    fn primitive_timestamp_input_merges_with_component_state() {
        for minimum in [true, false] {
            let mut accumulator = TimestampExtremeAccumulator::new(minimum);
            accumulator
                .update_batch(&[Arc::new(TimestampNanosecondArray::from(vec![
                    Some(-1),
                    None,
                    Some(2),
                ]))])
                .unwrap();
            let state = accumulator.state().unwrap()[0].to_array().unwrap();
            let mut restored = TimestampExtremeAccumulator::new(minimum);
            restored.merge_batch(&[state]).unwrap();
            assert_eq!(
                output(&mut restored),
                Some(TimestampValue::from_nanos(if minimum { -1 } else { 2 }).unwrap())
            );
        }
    }
}
