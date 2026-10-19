use arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, ListBuilder, MapBuilder, RecordBatch, StringBuilder,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::Tumbling;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn payload(kind: &str, valid: &[bool]) -> ArrayRef {
    match kind {
        "array" => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for (row, &present) in valid.iter().enumerate() {
                if present && row % 3 != 0 {
                    if row % 3 == 1 {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(row as i64);
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
                    builder.keys().append_value("key");
                    if row % 3 == 1 {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(row as i64);
                    }
                }
                builder.append(present).unwrap();
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
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column_by_name("result0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (keys.value(row), values.value(row)))
        .collect()
}

fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("window_complex_count");
    for rows in [128usize, 4096] {
        for keys in [1usize, 64, 1024] {
            let valid: Vec<_> = (0..rows)
                .map(|row| row % 7 != 0 && (keys == 1 || row % keys != 0))
                .collect();
            let expected: BTreeMap<_, _> = (0..rows.min(keys))
                .map(|key| {
                    (
                        key as i64,
                        (key..rows).step_by(keys).filter(|&row| valid[row]).count() as i64,
                    )
                })
                .collect();
            for kind in ["array", "map", "row"] {
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_value(500, rows)) as ArrayRef,
                    ),
                    ("value0", payload(kind, &valid)),
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| (row % keys) as i64),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap();
                for split in [false, true] {
                    let create = || {
                        (
                            Tumbling::new(1000, 0, vec![3]),
                            Tumbling::new(1000, 0, vec![3]),
                        )
                    };
                    let execute = |state: &mut (Tumbling, Tumbling), chunked: bool| {
                        let chunk = if chunked { 17 } else { rows };
                        for offset in (0..rows).step_by(chunk) {
                            let part = input.slice(offset, (rows - offset).min(chunk));
                            if split {
                                state.0.update_local(&part);
                                state.1.update_partial(&state.0.drain_partial());
                            } else {
                                state.1.update(&part);
                            }
                        }
                        state.1.flush(i64::MAX)
                    };
                    assert_eq!(counts(&execute(&mut create(), false)), expected);
                    assert_eq!(counts(&execute(&mut create(), true)), expected);
                    let mut probe = create();
                    let (output, allocations) = measure(|| execute(&mut probe, false));
                    let label = format!(
                        "window_complex_count/{kind}/rows={rows}/keys={keys}/split={split}"
                    );
                    report(&label, input.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            create,
                            |state| black_box(execute(state, false)),
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
