use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::{GroupBy, LocalGroupBy};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixture(rows: usize, ty: &str) -> (RecordBatch, i64) {
    let present = |row: usize| row % 7 != 0;
    let (values, code): (ArrayRef, i64) = match ty {
        "boolean" => (
            Arc::new(BooleanArray::from_iter(
                (0..rows).map(|row| present(row).then_some(row % 2 == 0)),
            )),
            10,
        ),
        "decimal20" => (
            Arc::new(
                Decimal128Array::from_iter(
                    (0..rows).map(|row| present(row).then_some(-(row as i128))),
                )
                .with_precision_and_scale(20, 3)
                .unwrap(),
            ),
            4003,
        ),
        "timestamp9" => (
            Arc::new(timestamp_array((0..rows).map(|row| {
                present(row).then(|| TimestampValue::new(-1, row as u32).unwrap())
            }))),
            1009,
        ),
        "string8" | "string256" => {
            let width = if ty == "string8" { 8 } else { 256 };
            (
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|row| present(row).then(|| format!("{row:0width$}"))),
                )),
                3,
            )
        }
        _ => (
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|row| present(row).then_some(-(row as i64))),
            )),
            0,
        ),
    };
    (
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int32Array::from_iter(
                    (0..rows).map(|row| (row != rows / 2).then_some(row as i32)),
                )) as ArrayRef,
            ),
            ("v", values),
        ])
        .unwrap(),
        code,
    )
}

fn bench_local_single_values(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("local_single_values");
    for rows in [16, 1024, 16384] {
        for ty in [
            "bigint",
            "boolean",
            "decimal20",
            "timestamp9",
            "string8",
            "string256",
        ] {
            let (input, code) = fixture(rows, ty);
            for physical_rows in [1, 13, rows] {
                let physical: Vec<_> = (0..rows)
                    .step_by(physical_rows)
                    .map(|start| input.slice(start, physical_rows.min(rows - start)))
                    .collect();
                let create = || LocalGroupBy::new(vec![14, 21], vec![code; 2], vec![1; 2], vec![0]);
                let execute = |aggregate: &mut LocalGroupBy| {
                    for batch in &physical {
                        aggregate.update(batch);
                    }
                    aggregate.flush()
                };
                let mut probe = create();
                let (output, allocations) = measure(|| execute(&mut probe));
                assert_eq!(output.column(0), input.column(0));
                assert_eq!(output.column(1), input.column(1));
                let counts = output
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert!(counts.iter().all(|count| count == Some(1)));
                if physical_rows == rows {
                    assert!(Arc::ptr_eq(output.column(0), physical[0].column(0)));
                    assert!(Arc::ptr_eq(output.column(1), physical[0].column(1)));
                }
                let name = format!("{ty}/{rows}/physical{physical_rows}");
                report(
                    &format!("local_single_values/{name}"),
                    input.columns(),
                    output.columns(),
                    allocations,
                );
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(name, |b| {
                    b.iter_batched_ref(
                        create,
                        |aggregate| black_box(execute(aggregate)),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}

fn bench_global_single_values(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("global_single_partials");
    for rows in [16, 1024, 16384] {
        for ty in [
            "bigint",
            "boolean",
            "decimal20",
            "timestamp9",
            "string8",
            "string256",
        ] {
            let (source, code) = fixture(rows, ty);
            let input = RecordBatch::try_from_iter(vec![
                ("k", source.column(0).clone()),
                ("v", source.column(1).clone()),
                (
                    "count",
                    Arc::new(Int32Array::from_value(1, rows)) as ArrayRef,
                ),
            ])
            .unwrap();
            for physical_rows in [1, 13, rows] {
                let physical: Vec<_> = (0..rows)
                    .step_by(physical_rows)
                    .map(|start| input.slice(start, physical_rows.min(rows - start)))
                    .collect();
                let create = || GroupBy::single_value_partials(code);
                let execute = |aggregate: &mut GroupBy| {
                    for batch in &physical {
                        assert_eq!(aggregate.update(batch).num_rows(), 0);
                    }
                    aggregate.flush()
                };
                let mut probe = create();
                let (output, allocations) = measure(|| execute(&mut probe));
                assert_eq!(output.column(0), input.column(0));
                assert_eq!(output.column(1), input.column(1));
                let kinds = output
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Int8Array>()
                    .unwrap();
                assert!(kinds.iter().all(|kind| kind == Some(0)));
                if physical_rows == rows {
                    assert!(Arc::ptr_eq(output.column(0), physical[0].column(0)));
                    assert!(Arc::ptr_eq(output.column(1), physical[0].column(1)));
                }
                let name = format!("{ty}/{rows}/physical{physical_rows}");
                report(
                    &format!("global_single_partials/{name}"),
                    input.columns(),
                    output.columns(),
                    allocations,
                );
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(name, |b| {
                    b.iter_batched_ref(
                        create,
                        |aggregate| black_box(execute(aggregate)),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_local_single_values,
    bench_global_single_values
);
criterion_main!(benches);
