use super::*;

#[derive(Default)]
pub(super) struct LocalSingleValue {
    indices: HashMap<ByteKey, usize>,
    entries: Vec<Entry>,
    batches: Vec<RecordBatch>,
    converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    key_bytes: usize,
    batch_bytes: usize,
}

struct Entry {
    key: (usize, usize),
    value: Option<(usize, usize)>,
    count: i32,
}

impl LocalSingleValue {
    pub(super) fn for_aggregates(
        kinds: &[i64],
        columns: &[i64],
        filters: &[i64],
        distinct_views: &[i64],
    ) -> Option<Self> {
        (!kinds.is_empty()
            && kinds.iter().all(|kind| matches!(kind, 14 | 21))
            && columns[0] >= 0
            && columns.iter().all(|column| *column == columns[0])
            && filters.iter().all(|filter| *filter < 0)
            && distinct_views.is_empty())
        .then(Self::default)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.key_bytes
            + self.batch_bytes
            + self.indices.capacity() * (std::mem::size_of::<(ByteKey, usize)>() + 1)
            + self.entries.capacity() * std::mem::size_of::<Entry>()
            + self.batches.capacity() * std::mem::size_of::<RecordBatch>()
    }

    pub(super) fn update(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        value: usize,
        memory: &mut OperatorMemory,
    ) -> Result<(), DataFusionError> {
        let key_arrays: Vec<_> = keys.iter().map(|&column| batch.column(column)).collect();
        self.key_types = key_types(&key_arrays);
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let encoded = encode_keys(&mut self.converter, &key_arrays, batch.num_rows());
        let batch_index = self.batches.len();
        let row_kinds = row_kind_column(batch);
        for row in 0..batch.num_rows() {
            let encoded_row = encoded.row(row);
            let key = encoded_row.as_ref();
            let index = if let Some(&index) = self.indices.get(key) {
                index
            } else {
                let index = self.entries.len();
                self.key_bytes += key.len();
                self.indices.insert(ByteKey::from(key), index);
                self.entries.push(Entry {
                    key: (batch_index, row),
                    value: None,
                    count: 0,
                });
                index
            };
            let entry = &mut self.entries[index];
            let retract = row_kinds.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3));
            if (retract && !matches!(entry.count, 0 | 1)) || (!retract && entry.count > 0) {
                return Err(DataFusionError::Execution(
                    ordered_value::SINGLE_VALUE_ERROR.into(),
                ));
            }
            if retract {
                entry.value = None;
                entry.count = entry.count.wrapping_sub(1);
            } else {
                entry.value = Some((batch_index, row));
                entry.count = entry.count.wrapping_add(1);
            }
        }
        // Retain only keys and the selected value, with Arrow ownership until the logical flush.
        let columns: Vec<_> = keys.iter().copied().chain(std::iter::once(value)).collect();
        let retained = batch.project(&columns)?;
        // Imported columns can share one producer release callback with unused siblings.
        self.batch_bytes += batch.get_array_memory_size();
        self.batches.push(retained);
        memory.set(self.bytes());
        memory.account()
    }

    fn gather(&self, column: usize, values: bool, data_type: &DataType) -> ArrayRef {
        if self.entries.is_empty() {
            return new_empty_array(data_type);
        }
        let position = |entry: &Entry| if values { entry.value } else { Some(entry.key) };
        if self.batches.len() == 1
            && self.entries.len() == self.batches[0].num_rows()
            && self
                .entries
                .iter()
                .enumerate()
                .all(|(row, entry)| position(entry) == Some((0, row)))
        {
            return self.batches[0].column(column).clone();
        }
        let data: Vec<_> = self
            .batches
            .iter()
            .map(|batch| batch.column(column).to_data())
            .collect();
        let refs = data.iter().collect();
        let mut gathered = MutableArrayData::new(refs, values, self.entries.len());
        for entry in &self.entries {
            if let Some((batch, row)) = position(entry) {
                gathered.extend(batch, row, row + 1);
            } else {
                gathered.extend_nulls(1);
            }
        }
        make_array(gathered.freeze())
    }

    pub(super) fn flush(
        &mut self,
        kinds: &[i64],
        result_types: &[DataType],
        memory: &mut OperatorMemory,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut temporary = memory.temporary_reservation();
        if let Some(reservation) = &mut temporary {
            // Gathering can allocate at most one key/value row per retained input row.
            reservation.try_grow(self.batch_bytes + self.entries.len() * 4)?;
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns: Vec<_> = self
            .key_types
            .iter()
            .enumerate()
            .map(|(column, ty)| self.gather(column, false, ty))
            .collect();
        let mut value: Option<ArrayRef> = None;
        let mut count: Option<ArrayRef> = None;
        for (i, (&kind, ty)) in kinds.iter().zip(result_types).enumerate() {
            fields.push(Field::new(format!("partial{i}"), ty.clone(), true));
            columns.push(if kind == 21 {
                count
                    .get_or_insert_with(|| {
                        Arc::new(Int32Array::from(
                            self.entries
                                .iter()
                                .map(|entry| entry.count)
                                .collect::<Vec<_>>(),
                        )) as ArrayRef
                    })
                    .clone()
            } else {
                value
                    .get_or_insert_with(|| self.gather(self.key_types.len(), true, ty))
                    .clone()
            });
        }
        let output = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        self.indices.clear();
        self.entries.clear();
        self.batches.clear();
        self.key_bytes = 0;
        self.batch_bytes = 0;
        memory.set(self.bytes());
        memory.account_shrink();
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(code: i64, compact: bool) -> LocalGroupAggregator {
        let mut aggregate = LocalGroupAggregator::new(
            vec![14, 21],
            vec![code; 2],
            vec![1; 2],
            vec![],
            vec![0],
            vec![],
        );
        if !compact {
            aggregate.single = None;
        }
        aggregate
    }

    fn batch(
        keys: Vec<Option<i32>>,
        values: Vec<ScalarValue>,
        kinds: Option<Vec<i8>>,
    ) -> RecordBatch {
        let ty = values[0].data_type();
        let mut fields = vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", ty.clone(), true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(keys)),
            scalars_to_array(values, &ty),
        ];
        if let Some(kinds) = kinds {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn typed_partials_match_scalar_state_across_physical_splits_and_bundles() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
        let timestamp: ArrayRef = Arc::new(timestamp_array([Some(
            TimestampValue::new(-1, 999999).unwrap(),
        )]));
        for (code, value) in [
            (0, ScalarValue::Int64(Some(i64::MIN))),
            (2, ScalarValue::Int32(Some(i32::MAX))),
            (1, ScalarValue::Float64(Some(-0.0))),
            (3, ScalarValue::Utf8(Some("\0𝄞".repeat(1024)))),
            (
                4003,
                ScalarValue::Decimal128(Some(-1234567890123456789), 20, 3),
            ),
            (8, ScalarValue::Date32(Some(-719528))),
            (9, ScalarValue::Time32Millisecond(Some(123))),
            (10, ScalarValue::Boolean(Some(true))),
            (7, ScalarValue::try_from_array(&timestamp, 0).unwrap()),
            (5, ScalarValue::Int8(Some(i8::MIN))),
            (4, ScalarValue::Int16(Some(i16::MAX))),
            (6, ScalarValue::Float32(Some(-0.0))),
        ] {
            let null = null_scalar(&value.data_type());
            for width in [1, 3, 17] {
                let mut compact = create(code, true);
                let mut generic = create(code, false);
                for _ in 0..2 {
                    let input = batch(
                        (0..17).map(|key| (key != 8).then_some(key)).collect(),
                        (0..17)
                            .map(|row| {
                                if row % 3 == 0 {
                                    null.clone()
                                } else {
                                    value.clone()
                                }
                            })
                            .collect(),
                        None,
                    );
                    for start in (0..17).step_by(width) {
                        let physical = input.slice(start, width.min(17 - start));
                        compact.update(&physical).unwrap();
                        generic.update(&physical).unwrap();
                    }
                    drop(input);
                    assert_eq!(
                        compact.flush(),
                        generic.flush(),
                        "code={code}, width={width}"
                    );
                    assert_eq!(compact.flush(), generic.flush());
                }
            }
        }
    }

    #[test]
    fn direct_partials_share_owned_buffers_and_gathered_partials_survive_input_drop() {
        let values = vec![ScalarValue::Int64(Some(7)), ScalarValue::Int64(None)];
        let input = batch(vec![Some(1), None], values, None);
        let mut compact = create(0, true);
        compact.update(&input).unwrap();
        let output = compact.flush();
        assert!(Arc::ptr_eq(output.column(0), input.column(0)));
        assert!(Arc::ptr_eq(output.column(1), input.column(1)));
        drop((compact, input));
        assert_eq!(
            ScalarValue::try_from_array(output.column(1), 0).unwrap(),
            ScalarValue::Int64(Some(7))
        );
        assert!(output.column(1).is_null(1));
    }

    #[test]
    fn retractions_and_negative_counts_match_released_declarative_accumulator() {
        for kinds in [vec![0, 1, 2], vec![3, 2], vec![0, 3, 3, 2]] {
            let mut compact = create(0, true);
            let mut generic = create(0, false);
            for (row, kind) in kinds.into_iter().enumerate() {
                let input = batch(
                    vec![Some(1)],
                    vec![ScalarValue::Int64(Some(row as i64))],
                    Some(vec![kind]),
                );
                compact.update(&input).unwrap();
                generic.update(&input).unwrap();
            }
            assert_eq!(compact.flush(), generic.flush());
        }
    }

    #[test]
    fn null_and_nonnull_duplicates_fail_within_and_across_physical_batches() {
        for value in [ScalarValue::Int64(None), ScalarValue::Int64(Some(1))] {
            for split in [false, true] {
                let input = batch(vec![None; 2], vec![value.clone(); 2], None);
                let mut compact = create(0, true);
                let error = if split {
                    compact.update(&input.slice(0, 1)).unwrap();
                    compact.update(&input.slice(1, 1))
                } else {
                    compact.update(&input)
                }
                .unwrap_err();
                assert_eq!(
                    error.to_string(),
                    format!("Execution error: {}", ordered_value::SINGLE_VALUE_ERROR)
                );
            }
        }
    }

    #[test]
    fn shared_budget_accounts_retained_arrow_and_releases_on_flush_and_drop() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
        let input = batch(
            (0..32).map(Some).collect(),
            vec![ScalarValue::Int64(Some(7)); 32],
            None,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut compact = create(0, true);
        compact.memory.attach_pool("single-test", &pool, 0).unwrap();
        for _ in 0..16 {
            compact.update(&input.slice(0, 0)).unwrap();
        }
        assert_eq!(pool.reserved(), 0);
        compact.update(&input).unwrap();
        assert!(pool.reserved() >= input.get_array_memory_size());
        let peak = pool.reserved();
        assert_eq!(compact.flush().num_rows(), 32);
        assert!(pool.reserved() < peak);
        compact.update(&input).unwrap();
        drop(compact);
        assert_eq!(pool.reserved(), 0);
        let mut denied = create(0, true).with_memory_budget(64).unwrap();
        assert!(denied
            .update(&input)
            .unwrap_err()
            .to_string()
            .contains("task off-heap"));
    }
}
