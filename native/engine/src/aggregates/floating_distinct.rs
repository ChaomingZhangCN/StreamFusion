use super::*;
use arrow::array::{Float32Array, Float64Array};
use datafusion::common::cast::{as_float32_array, as_float64_array, as_list_array};
use datafusion::common::utils::SingleRowListArrayBuilder;

#[derive(Debug)]
enum Membership {
    Float(ahash::HashSet<u32>),
    Double(ahash::HashSet<u64>),
}

#[derive(Debug)]
pub(super) struct FloatingDistinctCountAccumulator {
    membership: Membership,
}

impl FloatingDistinctCountAccumulator {
    pub(super) fn new(value_type: &DataType) -> Self {
        Self {
            membership: match value_type {
                DataType::Float32 => Membership::Float(Default::default()),
                DataType::Float64 => Membership::Double(Default::default()),
                other => panic!("floating DISTINCT count type: {other:?}"),
            },
        }
    }
}

impl Accumulator for FloatingDistinctCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let Some(values) = values.first() else {
            return Ok(());
        };
        match &mut self.membership {
            Membership::Float(set) => set.extend(
                as_float32_array(values)?
                    .iter()
                    .flatten()
                    .map(|value| crate::flink_float::canonical_f32(value).to_bits()),
            ),
            Membership::Double(set) => set.extend(
                as_float64_array(values)?
                    .iter()
                    .flatten()
                    .map(|value| crate::flink_float::canonical_f64(value).to_bits()),
            ),
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let Some(states) = states.first() else {
            return Ok(());
        };
        for values in as_list_array(states)?.iter().flatten() {
            self.update_batch(&[values])?;
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let values: ArrayRef = match &self.membership {
            Membership::Float(set) => Arc::new(Float32Array::from_iter_values(
                set.iter().map(|&bits| f32::from_bits(bits)),
            )),
            Membership::Double(set) => Arc::new(Float64Array::from_iter_values(
                set.iter().map(|&bits| f64::from_bits(bits)),
            )),
        };
        Ok(vec![
            SingleRowListArrayBuilder::new(values).build_list_scalar()
        ])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        let count = match &self.membership {
            Membership::Float(set) => set.len(),
            Membership::Double(set) => set.len(),
        };
        Ok(ScalarValue::Int64(Some(count as i64)))
    }

    fn size(&self) -> usize {
        let buckets = match &self.membership {
            Membership::Float(set) => set.capacity() * 2 * (std::mem::size_of::<u32>() + 1),
            Membership::Double(set) => set.capacity() * 2 * (std::mem::size_of::<u64>() + 1),
        };
        std::mem::size_of::<Self>() + buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_distinct_counts_canonical_nan_and_both_zero_signs_through_partial_restore() {
        for ty in [DataType::Float32, DataType::Float64] {
            let input: ArrayRef = if ty == DataType::Float32 {
                Arc::new(Float32Array::from(vec![
                    None,
                    Some(0.0),
                    Some(-0.0),
                    Some(f32::from_bits(0x7fc00001)),
                    Some(f32::from_bits(0xffc00002)),
                    Some(f32::INFINITY),
                    Some(f32::NEG_INFINITY),
                    Some(f32::MIN_POSITIVE),
                ]))
            } else {
                Arc::new(Float64Array::from(vec![
                    None,
                    Some(0.0),
                    Some(-0.0),
                    Some(f64::from_bits(0x7ff8000000000001)),
                    Some(f64::from_bits(0xfff8000000000002)),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::MIN_POSITIVE),
                ]))
            };
            let mut first = FloatingDistinctCountAccumulator::new(&ty);
            first.update_batch(&[input.slice(0, 4)]).unwrap();
            let state = first.state().unwrap()[0].to_array().unwrap();
            let mut restored = FloatingDistinctCountAccumulator::new(&ty);
            restored.merge_batch(&[state.clone()]).unwrap();
            let before = restored.size();
            restored.update_batch(&[input.slice(4, 4)]).unwrap();
            assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
            assert!(restored.size() >= before);
            restored.update_batch(&[input]).unwrap();
            restored.merge_batch(&[state]).unwrap();
            assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
            let scalar = restored.state().unwrap().remove(0);
            let ScalarValue::List(list) = scalar else {
                panic!("typed distinct list")
            };
            assert_eq!(list.value(0).data_type(), &ty);
        }
    }
}
