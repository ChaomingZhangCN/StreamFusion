use super::*;
use arrow::array::{Date32Array, Time32MillisecondArray};

pub(crate) enum ScalarExtremeColumn<'a> {
    Date(&'a Date32Array),
    Time(&'a Time32MillisecondArray),
    Boolean(&'a BooleanArray),
}

impl<'a> ScalarExtremeColumn<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Date32 => Self::Date(array.as_any().downcast_ref().expect("DATE values")),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                Self::Time(array.as_any().downcast_ref().expect("TIME values"))
            }
            DataType::Boolean => {
                Self::Boolean(array.as_any().downcast_ref().expect("BOOLEAN values"))
            }
            _ => unreachable!("scalar extrema type"),
        }
    }

    pub(crate) fn value(&self, row: usize) -> Option<i32> {
        match self {
            Self::Date(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Time(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Boolean(array) => (!array.is_null(row)).then(|| i32::from(array.value(row))),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ScalarExtremeAccumulator {
    value: Option<i32>,
    minimum: bool,
    value_type: DataType,
}

impl ScalarExtremeAccumulator {
    pub(super) fn new(minimum: bool, value_type: &DataType) -> Self {
        Self {
            value: None,
            minimum,
            value_type: value_type.clone(),
        }
    }

    pub(crate) fn value(&self) -> Option<i32> {
        self.value
    }

    pub(crate) fn merge_value(&mut self, value: Option<i32>) {
        if let Some(value) = value {
            if self.value.is_none_or(|old| {
                if self.minimum {
                    value < old
                } else {
                    value > old
                }
            }) {
                self.value = Some(value);
            }
        }
    }

    pub(crate) fn update_rows(&mut self, column: &ScalarExtremeColumn<'_>, rows: &[u32]) {
        for &row in rows {
            self.merge_value(column.value(row as usize));
        }
    }
}

impl Accumulator for ScalarExtremeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let column = ScalarExtremeColumn::new(&values[0]);
        for row in 0..values[0].len() {
            self.merge_value(column.value(row));
        }
        Ok(())
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(values)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(match self.value_type {
            DataType::Date32 => ScalarValue::Date32(self.value),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                ScalarValue::Time32Millisecond(self.value)
            }
            DataType::Boolean => ScalarValue::Boolean(self.value.map(|value| value != 0)),
            _ => unreachable!("scalar extrema type"),
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_selection_and_both_legacy_state_directions_preserve_extrema() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![
                None,
                Some(-719162),
                Some(2932896),
                Some(0),
                None,
            ])),
            Arc::new(Time32MillisecondArray::from(vec![
                None,
                Some(86399999),
                Some(0),
                Some(123),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                None,
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
        ];
        for input in columns {
            for minimum in [true, false] {
                let kind = if minimum { 1 } else { 2 };
                let mut selected = ScalarExtremeAccumulator::new(minimum, input.data_type());
                selected.update_rows(&ScalarExtremeColumn::new(&input), &[0, 3, 1, 2, 4]);
                let mut legacy = build_builtin(kind, input.data_type())
                    .create_accumulator()
                    .unwrap();
                legacy.update_batch(std::slice::from_ref(&input)).unwrap();
                assert_eq!(selected.evaluate().unwrap(), legacy.evaluate().unwrap());
                let legacy_state: Vec<_> = legacy
                    .state()
                    .unwrap()
                    .iter()
                    .map(|v| v.to_array().unwrap())
                    .collect();
                let mut restored = ScalarExtremeAccumulator::new(minimum, input.data_type());
                assert!(restored.evaluate().unwrap().is_null());
                restored.merge_batch(&legacy_state).unwrap();
                assert_eq!(restored.evaluate().unwrap(), selected.evaluate().unwrap());
                let current_state: Vec<_> = selected
                    .state()
                    .unwrap()
                    .iter()
                    .map(|v| v.to_array().unwrap())
                    .collect();
                let mut legacy_reader = build_builtin(kind, input.data_type())
                    .create_accumulator()
                    .unwrap();
                legacy_reader.merge_batch(&current_state).unwrap();
                assert_eq!(
                    legacy_reader.evaluate().unwrap(),
                    selected.evaluate().unwrap()
                );
                let mut nulls = ScalarExtremeAccumulator::new(minimum, input.data_type());
                nulls.update_rows(&ScalarExtremeColumn::new(&input), &[0, 4]);
                restored
                    .merge_batch(&[nulls.evaluate().unwrap().to_array().unwrap()])
                    .unwrap();
                assert_eq!(restored.evaluate().unwrap(), selected.evaluate().unwrap());
            }
        }
    }
}
