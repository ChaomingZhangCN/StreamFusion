use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    ListBuilder, MapBuilder, RecordBatch, StringBuilder, StructArray, StructBuilder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::Over;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn payload(kind: &str, valid: &[bool], width: usize) -> ArrayRef {
    match kind {
        "binary" => {
            let mut builder = BinaryBuilder::new();
            let bytes = vec![0xff; width];
            for (row, &present) in valid.iter().enumerate() {
                if !present {
                    builder.append_null();
                } else if row % 3 == 0 {
                    builder.append_value([]);
                } else {
                    builder.append_value(&bytes);
                }
            }
            Arc::new(builder.finish())
        }
        "day_time" => {
            Arc::new(Int64Array::from_iter(valid.iter().enumerate().map(
                |(row, &present)| present.then_some(row as i64 * 1234 - 1_000_000),
            )))
        }
        "year_month" => Arc::new(Int32Array::from_iter(
            valid
                .iter()
                .enumerate()
                .map(|(row, &present)| present.then_some(row as i32 - 100)),
        )),
        "array" => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    for entry in 0..width {
                        if entry % 3 == 0 {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(row as i64 + entry as i64);
                        }
                    }
                }
                builder.append(present);
            }
            Arc::new(builder.finish())
        }
        "map" => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    for entry in 0..width {
                        builder.keys().append_value(format!("key{entry}"));
                        if entry % 3 == 0 {
                            builder.values().append_null();
                        } else {
                            builder.values().append_value(row as i64 + entry as i64);
                        }
                    }
                }
                builder.append(present).unwrap();
            }
            Arc::new(builder.finish())
        }
        "map_list" => {
            let fields = vec![
                Arc::new(Field::new("key", DataType::Utf8, true)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ];
            let entries = StructBuilder::new(
                fields,
                vec![
                    Box::new(StringBuilder::new()),
                    Box::new(Int64Builder::new()),
                ],
            );
            let mut builder = ListBuilder::new(entries);
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    for entry in 0..width {
                        let keys = builder.values().field_builder::<StringBuilder>(0).unwrap();
                        if entry % 3 == 0 {
                            keys.append_null();
                        } else {
                            keys.append_value(format!("key{entry}"));
                        }
                        let values = builder.values().field_builder::<Int64Builder>(1).unwrap();
                        if entry % 3 == 0 {
                            values.append_null();
                        } else {
                            values.append_value(row as i64 + entry as i64);
                        }
                        builder.values().append(true);
                    }
                }
                builder.append(present);
            }
            Arc::new(builder.finish())
        }
        "multiset" => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    for entry in 0..width {
                        builder.keys().append_value(format!("element{entry}"));
                        builder.values().append_value(1 + (entry % 5) as i32);
                    }
                }
                builder.append(present).unwrap();
            }
            Arc::new(builder.finish())
        }
        "multiset_list" => {
            let fields = vec![
                Arc::new(Field::new("key", DataType::Utf8, true)),
                Arc::new(Field::new("value", DataType::Int32, false)),
            ];
            let entries = StructBuilder::new(
                fields,
                vec![
                    Box::new(StringBuilder::new()),
                    Box::new(Int32Builder::new()),
                ],
            );
            let mut builder = ListBuilder::new(entries);
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    for entry in 0..width {
                        let keys = builder.values().field_builder::<StringBuilder>(0).unwrap();
                        if entry == 0 {
                            keys.append_null();
                        } else {
                            keys.append_value(format!("element{entry}"));
                        }
                        builder
                            .values()
                            .field_builder::<Int32Builder>(1)
                            .unwrap()
                            .append_value(1 + (entry % 5) as i32);
                        builder.values().append(true);
                    }
                }
                builder.append(present);
            }
            Arc::new(builder.finish())
        }
        "row" => Arc::new(StructArray::new(
            vec![Arc::new(Field::new("child", DataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from_iter(
                (0..valid.len()).map(|row| (row % 3 == 0).then_some(row as i64)),
            ))],
            Some(NullBuffer::from(valid.to_vec())),
        )),
        _ => unreachable!(),
    }
}

fn counts(batch: &RecordBatch) -> BTreeMap<i64, i64> {
    if batch.num_rows() == 0 {
        return BTreeMap::new();
    }
    let ids = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (ids.value(row), counts.value(row)))
        .collect()
}

fn expected(valid: &[bool], keys: usize, frame: i64, proctime: bool) -> BTreeMap<i64, i64> {
    let rows = valid.len();
    (0..rows)
        .map(|row| {
            let time = row / 4;
            let upper = if matches!(frame, 0 | 2) && !proctime {
                (time * 4 + 3).min(rows - 1)
            } else {
                row
            };
            let lower = if frame == 1 {
                row.saturating_sub(3 * keys)
            } else {
                0
            };
            let count = (lower..=upper)
                .filter(|&candidate| {
                    candidate % keys == row % keys
                        && (frame != 2 || candidate / 4 >= time.saturating_sub(10))
                        && valid[candidate]
                })
                .count();
            (row as i64, count as i64)
        })
        .collect()
}

fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("over_complex_count");
    for rows in [64usize, 4096] {
        for keys in [1usize, 64] {
            let valid: Vec<_> = (0..rows)
                .map(|row| row % 5 != 0 && (keys == 1 || row % keys != 0))
                .collect();
            for (kind, width) in [
                ("array", 4usize),
                ("array", 64),
                ("map", 4),
                ("map", 64),
                ("map_list", 4),
                ("map_list", 64),
                ("row", 1),
                ("multiset", 4),
                ("multiset", 64),
                ("multiset_list", 4),
                ("multiset_list", 64),
                ("binary", 8),
                ("binary", 256),
                ("day_time", 8),
                ("year_month", 4),
            ] {
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "k",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| (row % keys) as i64),
                        )) as ArrayRef,
                    ),
                    (
                        "rt",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| 1000 + row as i64 / 4),
                        )) as ArrayRef,
                    ),
                    ("v", payload(kind, &valid, width)),
                    (
                        "id",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| row as i64),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap();
                for proctime in [false, true] {
                    if proctime && kind.starts_with("multiset") {
                        continue;
                    }
                    for frame in 0..4 {
                        if proctime && frame == 2 {
                            continue;
                        }
                        let offset = if frame == 1 { 3 } else { 10 };
                        let create = || Over::framed(0, 3, frame, offset, proctime);
                        let execute = |state: &mut Over| {
                            if proctime {
                                state.push_proctime(input.clone())
                            } else {
                                state.push(input.clone());
                                state.flush(i64::MAX)
                            }
                        };
                        let expected = expected(&valid, keys, frame, proctime);
                        assert_eq!(counts(&execute(&mut create())), expected);
                        let mut state = create();
                        let prefix = input.slice(0, rows / 2);
                        let mut recovered = if proctime {
                            counts(&state.push_proctime(prefix))
                        } else {
                            state.push(prefix);
                            counts(&state.flush(1000 + (rows / 2 - 1) as i64 / 4 - 1))
                        };
                        state =
                            Over::restore_framed(0, 3, frame, offset, proctime, &state.snapshot());
                        let suffix = input.slice(rows / 2, rows - rows / 2);
                        let output = if proctime {
                            state.push_proctime(suffix)
                        } else {
                            state.push(suffix);
                            state.flush(i64::MAX)
                        };
                        for (id, count) in counts(&output) {
                            assert!(recovered.insert(id, count).is_none());
                        }
                        assert_eq!(recovered, expected);
                        let mut probe = create();
                        let (output, allocations) = measure(|| execute(&mut probe));
                        let label = format!("over_complex_count/{kind}/width={width}/rows={rows}/keys={keys}/frame={frame}/proctime={proctime}");
                        report(&label, input.columns(), output.columns(), allocations);
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(label, |b| {
                            b.iter_batched_ref(
                                create,
                                |state| black_box(execute(state)),
                                BatchSize::LargeInput,
                            )
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
