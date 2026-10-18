use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::{Session, Tumbling};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

enum TemporalWindow {
    Tumbling(Tumbling),
    Session(Session),
}

impl TemporalWindow {
    fn update(&mut self, batch: &RecordBatch) {
        match self {
            Self::Tumbling(window) => window.update(batch),
            Self::Session(window) => window.update(batch),
        }
    }

    fn flush(&mut self) -> RecordBatch {
        match self {
            Self::Tumbling(window) => window.flush(i64::MAX),
            Self::Session(window) => window.flush(i64::MAX),
        }
    }
}

fn bench_window_temporal_extrema(c: &mut Criterion) {
    header();
    for shape in ["TUMBLE", "HOP", "CUMULATE", "SESSION"] {
        fixtures(c, shape);
    }
}

fn fixtures(c: &mut Criterion, shape: &str) {
    let session = shape == "SESSION";
    let label = match shape {
        "SESSION" => "session_temporal_extrema",
        "HOP" => "hopping_temporal_extrema",
        "CUMULATE" => "cumulative_temporal_extrema",
        _ => "window_temporal_extrema",
    };
    let mut group = c.benchmark_group(label);
    for rows in [256usize, 4096] {
        for cardinality in [1usize, 64, rows] {
            for precision in [3, 9] {
                let source: Vec<_> = (0..rows)
                    .map(|i| {
                        (!(i % 7 == 0 || (cardinality > 1 && i % cardinality == 0))).then(|| {
                            TimestampValue::new(
                                -62_135_596_800_000 + (i % 1024) as i64,
                                if precision == 9 { (i % 1024) as u32 } else { 0 },
                            )
                            .unwrap()
                        })
                    })
                    .collect();
                let values: ArrayRef = Arc::new(timestamp_array(source.iter().copied()));
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
                    ("value1", values.clone()),
                    ("value2", values),
                ])
                .unwrap();
                let mut following_columns = batch.columns().to_vec();
                following_columns[0] = Arc::new(Int64Array::from_value(1100, rows));
                let following_values: ArrayRef =
                    Arc::new(timestamp_array(source.iter().map(|value| {
                        value.map(|value| {
                            TimestampValue::from_nanos(value.nanos() + 42_000_000).unwrap()
                        })
                    })));
                following_columns[2] = following_values.clone();
                following_columns[3] = following_values;
                let following = RecordBatch::try_new(batch.schema(), following_columns).unwrap();
                let create = || {
                    if session {
                        TemporalWindow::Session(Session::with_budget(
                            1000,
                            1000 + precision,
                            vec![1, 2, 3],
                            64 * 1024 * 1024,
                        ))
                    } else {
                        TemporalWindow::Tumbling(Tumbling::with_window_budget(
                            1000,
                            if shape == "TUMBLE" { 1000 } else { 500 },
                            shape == "CUMULATE",
                            1000 + precision,
                            vec![1, 2, 3],
                            64 * 1024 * 1024,
                        ))
                    }
                };
                let mut probe = create();
                probe.update(&batch);
                if session {
                    probe.update(&following);
                }
                let output = probe.flush();
                assert_eq!(
                    output.num_rows(),
                    cardinality
                        * if shape == "HOP" || shape == "CUMULATE" {
                            2
                        } else {
                            1
                        }
                );
                let keys = output
                    .column_by_name("key0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = output
                    .column_by_name("result2")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..output.num_rows() {
                    let count = (keys.value(row) as usize..rows)
                        .step_by(cardinality)
                        .filter(|i| source[*i].is_some())
                        .count() as i64;
                    assert_eq!(counts.value(row), count * if session { 2 } else { 1 });
                }
                for (name, minimum) in [("result0", true), ("result1", false)] {
                    let actual =
                        TimestampColumn::try_new(output.column_by_name(name).unwrap().as_ref())
                            .unwrap();
                    for row in 0..output.num_rows() {
                        let candidates = (keys.value(row) as usize..rows)
                            .step_by(cardinality)
                            .filter_map(|i| source[i]);
                        let expected = if minimum {
                            candidates.min()
                        } else {
                            candidates.max().map(|value| {
                                if session {
                                    TimestampValue::from_nanos(value.nanos() + 42_000_000).unwrap()
                                } else {
                                    value
                                }
                            })
                        };
                        assert_eq!(
                            (!actual.is_null(row)).then(|| actual.value(row).unwrap()),
                            expected
                        );
                    }
                }
                let name = format!("{rows}/{cardinality}/{precision}");
                let mut probe = create();
                let (output, allocations) = measure(|| {
                    probe.update(&batch);
                    if session {
                        probe.update(&following);
                    }
                    probe.flush()
                });
                report(
                    &format!("{label}/{name}"),
                    batch.columns(),
                    output.columns(),
                    allocations,
                );
                group.throughput(Throughput::Elements(
                    (rows * if session { 2 } else { 1 }) as u64,
                ));
                group.bench_function(name, |b| {
                    b.iter_batched(
                        create,
                        |mut aggregate| {
                            aggregate.update(black_box(&batch));
                            if session {
                                aggregate.update(black_box(&following));
                            }
                            black_box(aggregate.flush());
                        },
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_window_temporal_extrema);
criterion_main!(benches);
