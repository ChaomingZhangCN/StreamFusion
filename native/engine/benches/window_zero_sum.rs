use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array, RecordBatch,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::Tumbling;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn bench_window_zero_sum(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("window_zero_sum");
    for rows in [256usize, 4096] {
        for cardinality in [1usize, 64, rows] {
            for name in ["bigint", "decimal38", "float", "double"] {
                let decimal = name == "decimal38";
                let value = |i: usize| {
                    ((cardinality == 1 || i % cardinality != 0) && i % 7 != 0).then_some(i as i128)
                };
                let (values, code): (ArrayRef, i64) = if decimal {
                    (
                        Arc::new(
                            Decimal128Array::from_iter((0..rows).map(value))
                                .with_precision_and_scale(38, 2)
                                .unwrap(),
                        ),
                        5802,
                    )
                } else if name == "float" {
                    (
                        Arc::new(Float32Array::from_iter(
                            (0..rows).map(|i| value(i).map(|v| v as f32)),
                        )),
                        6,
                    )
                } else if name == "double" {
                    (
                        Arc::new(Float64Array::from_iter(
                            (0..rows).map(|i| value(i).map(|v| v as f64)),
                        )),
                        1,
                    )
                } else {
                    (
                        Arc::new(Int64Array::from_iter(
                            (0..rows).map(|i| value(i).map(|v| v as i64)),
                        )),
                        0,
                    )
                };
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_value(100, rows)) as ArrayRef,
                    ),
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|i| (i % cardinality) as i64),
                        )) as ArrayRef,
                    ),
                    ("value0", values.clone()),
                    ("value1", values),
                ])
                .unwrap();
                let mut probe = Tumbling::new(1000, code, vec![3, 22]);
                probe.update(&batch);
                let output = probe.flush(i64::MAX);
                assert_eq!(output.num_rows(), cardinality);
                let keys = output
                    .column_by_name("key0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = output
                    .column_by_name("result0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..output.num_rows() {
                    let key = keys.value(row) as usize;
                    let selected: Vec<i128> =
                        (key..rows).step_by(cardinality).filter_map(value).collect();
                    assert_eq!(counts.value(row), selected.len() as i64);
                    let sum: i128 = selected.iter().sum();
                    let actual = output.column_by_name("result1").unwrap();
                    assert!(!actual.is_null(row));
                    if decimal {
                        assert_eq!(
                            actual
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            sum
                        );
                    } else if name == "float" {
                        assert_eq!(
                            actual
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .unwrap()
                                .value(row),
                            sum as f32
                        );
                    } else if name == "double" {
                        assert_eq!(
                            actual
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row),
                            sum as f64
                        );
                    } else {
                        assert_eq!(
                            actual
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row),
                            sum as i64
                        );
                    }
                }
                group.throughput(Throughput::Elements(rows as u64));
                let mut allocation_probe = Tumbling::new(1000, code, vec![3, 22]);
                let (probe_output, allocations) = measure(|| {
                    allocation_probe.update(&batch);
                    allocation_probe.flush(i64::MAX)
                });
                report(
                    &format!("window_zero_sum/{name}/{rows}/{cardinality}"),
                    batch.columns(),
                    probe_output.columns(),
                    allocations,
                );
                group.bench_function(format!("{name}/{rows}/{cardinality}"), |b| {
                    b.iter_batched(
                        || Tumbling::new(1000, code, vec![3, 22]),
                        |mut agg| {
                            agg.update(black_box(&batch));
                            black_box(agg.flush(i64::MAX));
                        },
                        BatchSize::SmallInput,
                    );
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_window_zero_sum);
criterion_main!(benches);
