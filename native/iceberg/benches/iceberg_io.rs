use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_iceberg::bench::encode;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn iceberg(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("iceberg/data-file");
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(17, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(38, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            let text = "x".repeat(width);
            let input = RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                    )) as ArrayRef,
                ),
            ])
            .unwrap();
            for selected in [false, true] {
                let indices: Vec<u32> = (0..rows as u32)
                    .step_by(if selected { 2 } else { 1 })
                    .collect();
                let label = format!("encode/{rows}/bytes={width}/selected={selected}");
                let (bytes, allocations) = measure(|| encode(&schema, &input, &indices));
                report(&label, input.columns(), &[], allocations);
                let decoded = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .unwrap()
                    .build()
                    .unwrap()
                    .map(|batch| batch.unwrap().num_rows())
                    .sum::<usize>();
                assert_eq!(decoded, indices.len());
                group.throughput(Throughput::Elements(indices.len() as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(encode(&schema, &input, &indices)))
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, iceberg);
criterion_main!(benches);
