use super::*;
use arrow::array::{Date32Array, Time32MillisecondArray};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_count_validity");
    for rows in [128, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64, 1024] {
            for (name, code, width) in [
                ("date", 8, 4),
                ("time", 9, 4),
                ("boolean", 10, 1),
                ("string8", 3, 8),
                ("string256", 3, 256),
                ("timestamp9", 1009, 12),
            ] {
                let valid = |row: usize| row % 7 != 0 && (keys == 1 || row % keys != 0);
                let values: ArrayRef =
                    match code {
                        8 => Arc::new(Date32Array::from_iter(
                            (0..rows).map(|row| valid(row).then_some(row as i32)),
                        )),
                        9 => Arc::new(Time32MillisecondArray::from_iter(
                            (0..rows).map(|row| valid(row).then_some(row as i32)),
                        )),
                        10 => Arc::new(BooleanArray::from_iter(
                            (0..rows).map(|row| valid(row).then_some(row % 2 == 0)),
                        )),
                        3 => Arc::new(StringArray::from_iter(
                            (0..rows).map(|row| valid(row).then(|| "x".repeat(width))),
                        )),
                        1009 => Arc::new(timestamp_array((0..rows).map(|row| {
                            valid(row).then(|| TimestampValue::new(-1, 123456).unwrap())
                        }))),
                        _ => unreachable!(),
                    };
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_value(500, rows)) as ArrayRef,
                    ),
                    ("value0", values),
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| (row % keys) as i64),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap();
                let create = || Tumbling::new(1000, code, vec![3]);
                let execute = |aggregate: &mut Tumbling| {
                    aggregate.update(&batch);
                    aggregate.flush(i64::MAX)
                };
                let checked = execute(&mut create());
                let groups = checked
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = checked
                    .column_by_name("result0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(checked.num_rows(), rows.min(keys));
                for row in 0..checked.num_rows() {
                    let key = groups.value(row) as usize;
                    let expected = (key..rows).step_by(keys).filter(|&row| valid(row)).count();
                    assert_eq!(counts.value(row), expected as i64);
                }
                group.bench_function(
                    BenchmarkId::new(name, format!("rows{rows}_keys{keys}")),
                    |b| {
                        b.iter_batched_ref(
                            create,
                            |aggregate| black_box(execute(aggregate)),
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
        }
    }
    group.finish();
}
