use crate::*;
use std::collections::VecDeque;

pub(super) const SINGLE_VALUE_ERROR: &str =
    "SingleValueAggFunction received more than one element.";

pub(super) fn is_ordered_value(kind: i64) -> bool {
    matches!(kind, 12..=16 | 19..=21)
}

pub(super) struct OrderedValueColumn<'a> {
    column: &'a ArrayRef,
    timestamp: Option<streamfusion_bridge::timestamp::TimestampColumn<'a>>,
}

impl<'a> OrderedValueColumn<'a> {
    pub(super) fn new(column: &'a ArrayRef) -> Self {
        let timestamp = streamfusion_bridge::timestamp::is_component_timestamp(column.data_type())
            .then(|| {
                streamfusion_bridge::timestamp::TimestampColumn::try_new(column.as_ref())
                    .expect("ordered timestamp column")
            });
        Self { column, timestamp }
    }

    pub(super) fn value(&self, row: usize) -> Result<ScalarValue, DataFusionError> {
        if let Some(values) = &self.timestamp {
            return Ok(ScalarValue::Decimal128(
                (!self.column.is_null(row))
                    .then(|| values.value(row).expect("timestamp value").nanos()),
                38,
                0,
            ));
        }
        ScalarValue::try_from_array(self.column, row)
    }
}

pub(super) fn accumulator_type(kind: i64, value_type: &DataType) -> DataType {
    if is_ordered_value(kind) && streamfusion_bridge::timestamp::is_component_timestamp(value_type)
    {
        DataType::Decimal128(38, 0)
    } else {
        value_type.clone()
    }
}

fn compact_value(value: ScalarValue) -> ScalarValue {
    if let ScalarValue::Struct(array) = &value {
        if streamfusion_bridge::timestamp::is_component_timestamp(array.data_type()) {
            let values = streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                .expect("ordered timestamp scalar");
            return ScalarValue::Decimal128(
                (!array.is_null(0)).then(|| values.value(0).expect("timestamp value").nanos()),
                38,
                0,
            );
        }
    }
    value
}

/// Append-only first/last retain one scalar; retractable first/last retain arrival order.
/// SINGLE_VALUE counts every element, including NULL, and can fail during accumulation.
pub(crate) struct OrderedValueState {
    kind: i64,
    value: ScalarValue,
    count: i32,
    values: VecDeque<ScalarValue>,
    value_bytes: usize,
    distinct: Option<Box<ahash::HashMap<ScalarValue, i64>>>,
}

impl OrderedValueState {
    pub(super) fn new(kind: i64, value_type: &DataType) -> Self {
        Self {
            kind,
            value: if streamfusion_bridge::timestamp::is_component_timestamp(value_type) {
                ScalarValue::Decimal128(None, 38, 0)
            } else {
                null_scalar(value_type)
            },
            count: 0,
            values: VecDeque::new(),
            value_bytes: 0,
            distinct: matches!(kind, 19 | 20).then(Box::default),
        }
    }

    pub(super) fn update(
        &mut self,
        value: ScalarValue,
        retract: bool,
    ) -> Result<(), DataFusionError> {
        let value = compact_value(value);
        if let Some(counts) = &mut self.distinct {
            if value.is_null() {
                return Ok(());
            }
            if retract {
                let Some(count) = counts.get_mut(&value) else {
                    return Ok(());
                };
                *count -= 1;
                if *count != 0 {
                    return Ok(());
                }
                counts.remove(&value);
                let index = self
                    .values
                    .iter()
                    .position(|existing| *existing == value)
                    .expect("distinct membership retains arrival order");
                self.value_bytes -= self.values.remove(index).unwrap().size();
            } else {
                let count = counts.entry(value.clone()).or_insert(0);
                *count += 1;
                if *count == 1 {
                    self.value_bytes += value.size();
                    self.values.push_back(value);
                }
            }
            return Ok(());
        }
        if matches!(self.kind, 14 | 21) {
            if (retract && !matches!(self.count, 0 | 1)) || (!retract && self.count > 0) {
                return Err(DataFusionError::Execution(SINGLE_VALUE_ERROR.into()));
            }
            if retract {
                if self.kind != 21 {
                    self.value = null_scalar(&self.value.data_type());
                }
                self.count = self.count.wrapping_sub(1);
            } else {
                if self.kind != 21 {
                    self.value = value;
                }
                self.count = self.count.wrapping_add(1);
            }
        } else if !value.is_null() {
            match self.kind {
                12 | 13 => {
                    assert!(!retract, "append-only first/last cannot retract");
                    if self.kind == 13 || self.value.is_null() {
                        self.value = value;
                    }
                }
                15 | 16 => {
                    if retract {
                        if let Some(index) = self
                            .values
                            .iter()
                            .position(|existing| crate::flink_float::boxed_equal(existing, &value))
                        {
                            self.value_bytes -= self.values.remove(index).unwrap().size();
                        }
                    } else {
                        self.append_restored(value);
                    }
                }
                _ => unreachable!("ordered aggregate kind"),
            }
        }
        Ok(())
    }

    pub(super) fn merge_single(
        &mut self,
        value: ScalarValue,
        count: i32,
    ) -> Result<(), DataFusionError> {
        let value = compact_value(value);
        let total = self.count.wrapping_add(count);
        if total > 1 {
            return Err(DataFusionError::Execution(SINGLE_VALUE_ERROR.into()));
        }
        self.value = if total == 0 && self.count != 0 && count <= 0 {
            null_scalar(&self.value.data_type())
        } else {
            value
        };
        self.count = total;
        Ok(())
    }

    pub(super) fn emit(&self) -> ScalarValue {
        match self.kind {
            21 => ScalarValue::Int32(Some(self.count)),
            15 | 19 => self.values.front().unwrap_or(&self.value).clone(),
            16 | 20 => self.values.back().unwrap_or(&self.value).clone(),
            _ => self.value.clone(),
        }
    }

    pub(super) fn snapshot_value(&self) -> ScalarValue {
        self.value.clone()
    }

    pub(super) fn count(&self) -> i64 {
        self.count as i64
    }

    pub(super) fn restore_value(&mut self, value: ScalarValue, count: i64) {
        self.value = compact_value(value);
        self.count = count as i32;
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&ScalarValue, i64)> {
        self.values.iter().map(|value| {
            (
                value,
                self.distinct.as_ref().map_or(1, |counts| counts[value]),
            )
        })
    }

    pub(super) fn restore_entry(&mut self, value: ScalarValue, count: i64) {
        let value = compact_value(value);
        if let Some(counts) = &mut self.distinct {
            counts.insert(value.clone(), count);
        }
        self.append_restored(value);
    }

    pub(super) fn append_restored(&mut self, value: ScalarValue) {
        let value = compact_value(value);
        self.value_bytes += value.size();
        self.values.push_back(value);
    }

    pub(super) fn bytes(&self) -> usize {
        let distinct_bytes = self.distinct.as_ref().map_or(0, |counts| {
            std::mem::size_of_val(counts.as_ref())
                + self.value_bytes
                + counts.len() * std::mem::size_of::<i64>()
                + (counts.capacity() - counts.len()) * std::mem::size_of::<(ScalarValue, i64)>()
                + counts.capacity()
        });
        distinct_bytes
            + std::mem::size_of::<Self>()
            + self.value.size()
            + self.value_bytes
            + (self.values.capacity() - self.values.len()) * std::mem::size_of::<ScalarValue>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_values_remain_inline_until_arrow_output_and_restore() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
        let values: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::new(-1, 999999).unwrap()),
            None,
        ]));
        let mut state = OrderedValueState::new(14, values.data_type());
        assert!(matches!(state.value, ScalarValue::Decimal128(None, 38, 0)));
        state
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), false)
            .unwrap();
        assert!(matches!(
            state.value,
            ScalarValue::Decimal128(Some(-1), 38, 0)
        ));
        let output = scalars_to_array(vec![state.emit()], values.data_type());
        assert_eq!(
            TimestampColumn::try_new(output.as_ref())
                .unwrap()
                .value(0)
                .unwrap(),
            TimestampValue::new(-1, 999999).unwrap()
        );
        let mut restored = OrderedValueState::new(14, values.data_type());
        restored.restore_value(ScalarValue::try_from_array(&output, 0).unwrap(), 1);
        assert!(matches!(
            restored.value,
            ScalarValue::Decimal128(Some(-1), 38, 0)
        ));
        restored
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), true)
            .unwrap();
        restored
            .update(OrderedValueColumn::new(&values).value(1).unwrap(), false)
            .unwrap();
        assert!(restored.emit().is_null());
        assert!(restored
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), false)
            .is_err());

        let mut count = OrderedValueState::new(21, values.data_type());
        count.update(ScalarValue::Null, false).unwrap();
        assert_eq!(count.emit(), ScalarValue::Int32(Some(1)));
        assert!(matches!(count.value, ScalarValue::Decimal128(None, 38, 0)));
    }

    #[test]
    fn duplicate_retractions_preserve_arrival_order() {
        for kind in [15, 16] {
            let mut state = OrderedValueState::new(kind, &DataType::Utf8);
            let scalar = |s: &str| ScalarValue::Utf8(Some(s.to_owned()));
            for value in ["a", "b", "a", "c"] {
                state.update(scalar(value), false).unwrap();
            }
            state.update(ScalarValue::Utf8(None), false).unwrap();
            state.update(scalar("a"), true).unwrap();
            assert_eq!(state.emit(), scalar(if kind == 15 { "b" } else { "c" }));
            state.update(scalar("c"), true).unwrap();
            assert_eq!(state.emit(), scalar(if kind == 15 { "b" } else { "a" }));
            state.update(scalar("a"), true).unwrap();
            state.update(scalar("absent"), true).unwrap();
            assert_eq!(state.emit(), scalar("b"));
            state.update(scalar("b"), true).unwrap();
            assert!(state.emit().is_null());
            assert_eq!(state.value_bytes, 0);
        }
    }

    #[test]
    fn single_value_merge_preserves_zero_and_negative_count_rules() {
        let value = |s: &str| ScalarValue::Utf8(Some(s.into()));
        let mut state = OrderedValueState::new(14, &DataType::Utf8);
        state.merge_single(value("zero"), 0).unwrap();
        assert_eq!(state.emit(), value("zero"));
        state.merge_single(value("one"), 1).unwrap();
        state.merge_single(ScalarValue::Utf8(None), 0).unwrap();
        assert!(state.emit().is_null());
        state.merge_single(value("removed"), -1).unwrap();
        assert!(state.emit().is_null());
        state.merge_single(value("negative"), -1).unwrap();
        state.merge_single(value("replacement"), 1).unwrap();
        assert_eq!(state.count(), 0);
        assert_eq!(state.emit(), value("replacement"));
    }

    #[test]
    fn single_value_counts_null_and_preserves_retraction_underflow() {
        let mut state = OrderedValueState::new(14, &DataType::Int64);
        state.update(ScalarValue::Int64(None), false).unwrap();
        assert_eq!(state.count(), 1);
        assert!(state.update(ScalarValue::Int64(None), false).is_err());
        state.update(ScalarValue::Int64(None), true).unwrap();
        state.update(ScalarValue::Int64(None), true).unwrap();
        assert_eq!(state.count(), -1);
        assert!(state.update(ScalarValue::Int64(None), true).is_err());
        state.update(ScalarValue::Int64(Some(7)), false).unwrap();
        assert_eq!(state.count(), 0);
        assert_eq!(state.emit(), ScalarValue::Int64(Some(7)));
        state.update(ScalarValue::Int64(Some(8)), false).unwrap();
        assert_eq!(state.count(), 1);
        assert_eq!(state.emit(), ScalarValue::Int64(Some(8)));
    }
}
