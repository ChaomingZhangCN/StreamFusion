use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, Time32MillisecondArray};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use streamfusion::bench::Tumbling;

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_basic_distinct");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        let cardinalities = if rows == 64 {
            vec![1, 64]
        } else {
            vec![1, 64, rows]
        };
        for keys in cardinalities {
            for (name, code) in [("boolean", 10), ("time", 9)] {
                let values: ArrayRef = if name == "boolean" {
                    Arc::new(BooleanArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(i % 3 == 0)),
                    ))
                } else {
                    Arc::new(Time32MillisecondArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some((i % 256) as i32)),
                    ))
                };
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_value(100, rows)) as ArrayRef,
                    ),
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|i| (i % keys) as i64),
                        )) as ArrayRef,
                    ),
                    ("value0", values),
                ])
                .unwrap();
                for partial in [false, true] {
                    group.bench_function(
                        format!("{name}/rows{rows}/keys{keys}/partial{partial}"),
                        |b| {
                            b.iter_batched(
                                || {
                                    (
                                        Tumbling::new(1000, code, vec![7]),
                                        partial.then(|| Tumbling::new(1000, code, vec![7])),
                                    )
                                },
                                |(mut local, global)| {
                                    if let Some(mut global) = global {
                                        local.update_local(black_box(&batch));
                                        global.update_partial(&local.drain_partial());
                                        black_box(global.flush(1000))
                                    } else {
                                        local.update(black_box(&batch));
                                        black_box(local.flush(1000))
                                    }
                                },
                                BatchSize::SmallInput,
                            );
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
