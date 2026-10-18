use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::AppendTopN;

fn bench_non_time_row_number(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_time_row_number");
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for descending_input in [false, true] {
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "k",
                        Arc::new(Int64Array::from_iter(
                            (0..rows).map(|i| (i % 13 != 0).then_some((i % keys) as i64)),
                        )) as ArrayRef,
                    ),
                    (
                        "v",
                        Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                            (i % 17 != 0).then_some(if descending_input {
                                (rows - i) as i64
                            } else {
                                i as i64
                            })
                        }))) as ArrayRef,
                    ),
                ])
                .unwrap();
                let partition = input
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let order = input
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let value =
                    |array: &Int64Array, row| (!array.is_null(row)).then(|| array.value(row));
                let mut expected = rows;
                for row in 0..rows {
                    for previous in 0..row {
                        let earlier = match (value(order, row), value(order, previous)) {
                            (Some(a), Some(b)) => a < b,
                            (Some(_), None) => true,
                            _ => false,
                        };
                        if value(partition, row) == value(partition, previous) && earlier {
                            expected += 2;
                        }
                    }
                }
                let mut check = AppendTopN::non_time_row_number(vec![0], 1);
                assert_eq!(check.push(&input).num_rows(), expected);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(
                    format!("rows={rows}/keys={keys}/reverse={descending_input}"),
                    |b| {
                        b.iter_batched(
                            || AppendTopN::non_time_row_number(vec![0], 1),
                            |mut ranker| black_box(ranker.push(black_box(&input))),
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_non_time_row_number);
criterion_main!(benches);
