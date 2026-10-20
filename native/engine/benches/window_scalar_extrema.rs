use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::{Session, Tumbling};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
use arrow::array::{Date32Array, Time32MillisecondArray};
use datafusion::common::ScalarValue;

enum ScalarWindow {
    Fixed(Tumbling),
    Session(Session),
}

impl ScalarWindow {
    fn update(&mut self, batch: &RecordBatch) {
        match self {
            Self::Fixed(window) => window.update(batch),
            Self::Session(window) => window.update(batch),
        }
    }
    fn flush(&mut self) -> RecordBatch {
        match self {
            Self::Fixed(window) => window.flush(i64::MAX),
            Self::Session(window) => window.flush(i64::MAX),
        }
    }
}

fn bench(c: &mut Criterion) {
    header();
    for shape in ["TUMBLE", "HOP", "CUMULATE", "SESSION"] {
        fixtures(c, shape);
    }
}

fn fixtures(c: &mut Criterion, shape: &str) {
    let mut group = c.benchmark_group(format!("window_scalar_extrema/{shape}"));
    for rows in [128, 4096] {
        for keys in [1, 64, 1024] {
            for (name, code) in [("date", 8), ("time", 9), ("boolean", 10)] {
                let values: Vec<_> = (0..rows)
                    .map(|row| {
                        (row % 7 != 0 && (keys == 1 || row % keys != 0))
                            .then_some((rows - row) as i32)
                    })
                    .collect();
                let value: ArrayRef = match code {
                    8 => Arc::new(Date32Array::from(values)),
                    9 => Arc::new(Time32MillisecondArray::from(values)),
                    10 => Arc::new(BooleanArray::from_iter(
                        values.into_iter().map(|v| v.map(|v| v % 2 == 0)),
                    )),
                    _ => unreachable!(),
                };
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_value(100, rows)) as ArrayRef,
                    ),
                    ("value0", value.clone()),
                    ("value1", value.clone()),
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|row| (row % keys) as i64),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap();
                let next_value: ArrayRef = match code {
                    8 => Arc::new(Date32Array::from_iter(
                        value
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap()
                            .iter()
                            .map(|v| v.map(|v| -v - 1)),
                    )),
                    9 => Arc::new(Time32MillisecondArray::from_iter(
                        value
                            .as_any()
                            .downcast_ref::<Time32MillisecondArray>()
                            .unwrap()
                            .iter()
                            .map(|v| v.map(|v| v + rows as i32 + 1)),
                    )),
                    10 => Arc::new(BooleanArray::from_iter(
                        value
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .iter()
                            .map(|v| v.map(|v| !v)),
                    )),
                    _ => unreachable!(),
                };
                let mut next_columns = batch.columns().to_vec();
                next_columns[0] = Arc::new(Int64Array::from_value(700, rows));
                next_columns[1] = next_value.clone();
                next_columns[2] = next_value.clone();
                let following = RecordBatch::try_new(batch.schema(), next_columns).unwrap();
                let create = || {
                    if shape == "SESSION" {
                        ScalarWindow::Session(Session::with_budget(
                            1000,
                            code,
                            vec![1, 2],
                            64 << 20,
                        ))
                    } else {
                        ScalarWindow::Fixed(Tumbling::with_window_budget(
                            1000,
                            if shape == "TUMBLE" { 1000 } else { 500 },
                            shape == "CUMULATE",
                            code,
                            vec![1, 2],
                            64 << 20,
                        ))
                    }
                };
                let execute = |agg: &mut ScalarWindow| {
                    agg.update(&batch);
                    if shape == "SESSION" {
                        agg.update(&following);
                    }
                    agg.flush()
                };
                let mut probe = create();
                let (output, allocations) = measure(|| execute(&mut probe));
                report(
                    &format!("window_scalar_extrema/{shape}/{name}/rows{rows}/keys{keys}"),
                    batch.columns(),
                    output.columns(),
                    allocations,
                );
                let output_keys = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(
                    output.num_rows(),
                    rows.min(keys)
                        * if matches!(shape, "HOP" | "CUMULATE") {
                            2
                        } else {
                            1
                        }
                );
                for row in 0..output.num_rows() {
                    let key = output_keys.value(row) as usize;
                    let mut expected: Vec<_> = (key..rows)
                        .step_by(keys)
                        .map(|index| ScalarValue::try_from_array(&value, index).unwrap())
                        .filter(|value| !value.is_null())
                        .collect();
                    if shape == "SESSION" {
                        expected.extend(
                            (key..rows)
                                .step_by(keys)
                                .map(|index| {
                                    ScalarValue::try_from_array(&next_value, index).unwrap()
                                })
                                .filter(|value| !value.is_null()),
                        );
                    }
                    expected.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    for (result, expected) in
                        [("result0", expected.first()), ("result1", expected.last())]
                    {
                        let actual = ScalarValue::try_from_array(
                            output.column_by_name(result).unwrap(),
                            row,
                        )
                        .unwrap();
                        if let Some(expected) = expected {
                            assert_eq!(&actual, expected);
                        } else {
                            assert!(actual.is_null());
                        }
                    }
                }
                group.throughput(Throughput::Elements(
                    (rows * if shape == "SESSION" { 2 } else { 1 }) as u64,
                ));
                group.bench_function(format!("{name}/rows{rows}/keys{keys}"), |b| {
                    b.iter_batched_ref(
                        create,
                        |agg| black_box(execute(agg)),
                        BatchSize::SmallInput,
                    );
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
