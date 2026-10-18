//! Production transformations and stateful paths missing from the historical operator suite.
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use streamfusion::bench::{
    decode_ipc, encode_ipc, expand_grouping_sets, unnest, unnest_projected, ArrivalFirstN,
    Projection, TemporalJoin, TemporalSort,
};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn batch(rows: usize, width: usize, keys: usize) -> RecordBatch {
    let text = "x".repeat(width);
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "rowtime",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).rev().map(|i| i as i64),
            )) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn transformations(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/data_movement");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            let input = batch(rows, width, 64);
            group.throughput(Throughput::Elements(rows as u64));
            let mut projection = Projection::columns(&[0, 2]);
            projection.run(input.clone());
            let label = format!("projection/{rows}/bytes={width}");
            let (output, allocations) = measure(|| projection.run(input.clone()));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(projection.run(std::hint::black_box(input.clone()))))
            });
            let label = format!("expand/{rows}/bytes={width}");
            let (output, allocations) = measure(|| expand_grouping_sets(&input));
            assert_eq!(output.num_rows(), rows * 2);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(expand_grouping_sets(std::hint::black_box(&input))))
            });
            let encoded = encode_ipc(&input);
            assert_eq!(decode_ipc(&encoded), vec![input.clone()]);
            let label = format!("ipc_encode/{rows}/bytes={width}");
            let (output, allocations) = measure(|| encode_ipc(&input));
            report(&label, input.columns(), &[], allocations);
            std::hint::black_box(output);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(encode_ipc(std::hint::black_box(&input))))
            });
            let label = format!("ipc_decode/{rows}/bytes={width}");
            let (output, allocations) = measure(|| decode_ipc(&encoded));
            report(&label, &[], output[0].columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(decode_ipc(std::hint::black_box(&encoded))))
            });
        }
        for width in [0, 4, 64] {
            let mut arrays = ListBuilder::new(Int64Builder::new());
            for row in 0..rows {
                for value in 0..width {
                    arrays
                        .values()
                        .append_option((value % 7 != 0).then_some(value as i64));
                }
                arrays.append(row % 7 != 0);
            }
            let input = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                ("items", Arc::new(arrays.finish()) as ArrayRef),
            ])
            .unwrap();
            for left in [false, true] {
                for (shape, projection) in [
                    ("key_element", vec![0, 2]),
                    ("repeat_ordinal", vec![3, 2, 2]),
                ] {
                    let label =
                        format!("unnest_projected/{rows}/width={width}/left={left}/{shape}");
                    let (output, allocations) =
                        measure(|| unnest_projected(&input, left, true, &projection));
                    assert_eq!(output.num_rows(), unnest(&input, left, true).num_rows());
                    report(&label, input.columns(), output.columns(), allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(unnest_projected(
                                std::hint::black_box(&input),
                                left,
                                true,
                                &projection,
                            ))
                        })
                    });
                }
                let label = format!("unnest/{rows}/width={width}/left={left}");
                let (output, allocations) = measure(|| unnest(&input, left, true));
                report(&label, input.columns(), output.columns(), allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(unnest(std::hint::black_box(&input), left, true))
                    })
                });
            }
        }
    }
    for rows in [16, 1024, 16384] {
        for width in [2, 16] {
            let count = rows * width;
            for shape in ["timestamp", "one_field_row"] {
                let values: ArrayRef = if shape == "timestamp" {
                    Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                        (0..count).map(|i| {
                            (i % 7 != 0).then(|| {
                                streamfusion_bridge::timestamp::TimestampValue::from_nanos(
                                    i as i128 * 1234567 - 999999,
                                )
                                .unwrap()
                            })
                        }),
                    ))
                } else {
                    Arc::new(arrow::array::StructArray::new(
                        vec![Arc::new(arrow::datatypes::Field::new(
                            "a",
                            arrow::datatypes::DataType::Int64,
                            true,
                        ))]
                        .into(),
                        vec![Arc::new(Int64Array::from_iter_values(0..count as i64))],
                        Some(arrow::buffer::NullBuffer::from(
                            (0..count).map(|i| i % 7 != 0).collect::<Vec<_>>(),
                        )),
                    ))
                };
                let list = arrow::array::ListArray::new(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        values.data_type().clone(),
                        true,
                    )),
                    arrow::buffer::OffsetBuffer::new(
                        (0..=rows)
                            .map(|i| (i * width) as i32)
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                    values,
                    Some(arrow::buffer::NullBuffer::from(
                        (0..rows).map(|i| i % 11 != 0).collect::<Vec<_>>(),
                    )),
                );
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "key",
                        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                    ),
                    ("items", Arc::new(list) as ArrayRef),
                ])
                .unwrap();
                group.throughput(Throughput::Elements(rows as u64));
                for left in [false, true] {
                    let label = format!("unnest_typed/{shape}/{rows}/width={width}/left={left}");
                    let (output, allocations) =
                        measure(|| unnest_projected(&input, left, true, &[0, 2, 3]));
                    assert_eq!(output.num_columns(), 3);
                    report(&label, input.columns(), output.columns(), allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(unnest_projected(
                                std::hint::black_box(&input),
                                left,
                                true,
                                &[0, 2, 3],
                            ))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}

fn stateful(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/stateful");
    for rows in [16, 1024, 16384] {
        for keys in [1, 64, rows] {
            let input = batch(rows, 264, keys);
            group.throughput(Throughput::Elements(rows as u64));
            let label = format!("first_n/{rows}/keys={keys}");
            let mut operator = ArrivalFirstN::new(vec![0], 4);
            let (output, allocations) = measure(|| operator.push(&input));
            assert!(output.num_rows() <= rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || ArrivalFirstN::new(vec![0], 4),
                    |mut operator| {
                        std::hint::black_box(operator.push(std::hint::black_box(&input)))
                    },
                    BatchSize::LargeInput,
                )
            });
            let label = format!("temporal_sort/{rows}/keys={keys}");
            let run = |mut operator: TemporalSort| {
                operator.push(input.clone());
                operator.flush(i64::MAX)
            };
            let operator = TemporalSort::new(1);
            let (output, allocations) = measure(|| run(operator));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || TemporalSort::new(1),
                    |operator| std::hint::black_box(run(operator)),
                    BatchSize::LargeInput,
                )
            });
            let label = format!("temporal_join/{rows}/keys={keys}");
            let run = |mut operator: TemporalJoin| {
                operator.push(&input, false);
                operator.push(&input, true);
                operator.flush(i64::MAX)
            };
            let operator = TemporalJoin::new(input.schema());
            let (output, allocations) = measure(|| run(operator));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || TemporalJoin::new(input.schema()),
                    |operator| std::hint::black_box(run(operator)),
                    BatchSize::LargeInput,
                )
            });
            let mut operator = TemporalJoin::new(input.schema());
            operator.push(&input, false);
            operator.push(&input, true);
            let label = format!("temporal_join_snapshot/{rows}/keys={keys}");
            let (output, allocations) = measure(|| operator.snapshot());
            report(&label, input.columns(), &[], allocations);
            assert!(!output.is_empty());
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(operator.snapshot()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, transformations, stateful);
criterion_main!(benches);
