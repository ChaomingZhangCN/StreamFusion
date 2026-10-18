use arrow::array::{ArrayRef, Decimal128Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::coalesce;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixture(rows: usize, width: usize, changelog: bool) -> RecordBatch {
    let text = "x".repeat(width);
    let mut fields = vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Decimal128(38, 2), true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
        )),
        Arc::new(
            Decimal128Array::from_iter((0..rows).map(|i| (i % 11 != 0).then_some(i as i128)))
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
    ];
    if changelog {
        fields.push(Field::new("$row_kind$", DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from_iter_values(
            (0..rows).map(|i| (i % 4) as i8),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn benchmarks(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/coalescing");
    for rows in [16, 1024, 4096] {
        for width in [8, 256] {
            for shape in [
                "append",
                "updating",
                "mixed-append-first",
                "mixed-update-first",
            ] {
                let append = fixture(rows, width, false);
                let updates = fixture(rows, width, true);
                let inputs = match shape {
                    "append" => vec![append.clone(), append],
                    "updating" => vec![updates.clone(), updates],
                    "mixed-append-first" => vec![append, updates],
                    _ => vec![updates, append],
                };
                let name = format!("{shape}/rows={rows}/width={width}");
                let (output, allocations) = measure(|| coalesce(&inputs));
                assert_eq!(output.num_rows(), rows * 2);
                assert_eq!(output.num_columns(), if shape == "append" { 2 } else { 3 });
                report(&name, inputs[0].columns(), output.columns(), allocations);
                group.throughput(Throughput::Elements((rows * 2) as u64));
                group.bench_function(BenchmarkId::from_parameter(name), |b| {
                    b.iter(|| std::hint::black_box(coalesce(std::hint::black_box(&inputs))))
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, benchmarks);
criterion_main!(benches);
