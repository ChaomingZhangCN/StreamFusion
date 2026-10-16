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

fn routing(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("iceberg/routing");
    for rows in [16usize, 1024, 16384] {
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
            for channels in [1, 4, 128] {
                let destinations: Vec<i32> = (0..rows).map(|row| (row % channels) as i32).collect();
                let label = format!("split/{rows}/bytes={width}/destinations={channels}");
                let (batches, allocations) =
                    measure(|| streamfusion_iceberg::bench::route(&input, &destinations));
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    rows
                );
                let arrays = batches
                    .iter()
                    .flat_map(|batch| batch.columns().iter().cloned())
                    .collect::<Vec<_>>();
                report(&label, input.columns(), &arrays, allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(streamfusion_iceberg::bench::route(
                            &input,
                            &destinations,
                        ))
                    })
                });
            }
        }
    }
    group.finish();
}
fn reading(c: &mut Criterion) {
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(17, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(38, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let mut group = c.benchmark_group("iceberg/task-read");
    for rows in [16usize, 1024, 16384] {
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
            let bytes = encode(&schema, &input, &(0..rows as u32).collect::<Vec<_>>());
            let fixture = streamfusion_iceberg::bench::ReadFixture::new(&schema, bytes);
            for project in [false, true] {
                let ids: &[i32] = if project { &[38] } else { &[17, 38] };
                for batch_rows in [16, 1024] {
                    let label =
                        format!("read/{rows}/bytes={width}/project={project}/batch={batch_rows}");
                    let (batches, allocations) = measure(|| fixture.read(ids, batch_rows, 0));
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows
                    );
                    let arrays = batches
                        .iter()
                        .flat_map(|b| b.columns().iter().cloned())
                        .collect::<Vec<_>>();
                    report(&label, &[], &arrays, allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| std::hint::black_box(fixture.read(ids, batch_rows, 0)))
                    });
                }
            }
        }
    }
    group.finish();
}
fn scalar_types(c: &mut Criterion) {
    use arrow::array::{
        Array, Decimal128Array, Float32Array, Float64Array, Time32MillisecondArray,
    };
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    let fields = [
        (17, "id", PrimitiveType::Long),
        (38, "f", PrimitiveType::Float),
        (39, "d", PrimitiveType::Double),
        (
            40,
            "amount",
            PrimitiveType::Decimal {
                precision: 38,
                scale: 6,
            },
        ),
        (41, "clock", PrimitiveType::Time),
        (42, "local", PrimitiveType::Timestamp),
        (43, "instant", PrimitiveType::Timestamptz),
    ];
    let ids = fields.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(|(id, name, kind)| {
            Arc::new(NestedField::optional(id, name, Type::Primitive(kind)))
        }))
        .build()
        .unwrap();
    let mut group = c.benchmark_group("iceberg/scalar-types");
    for rows in [16usize, 1024, 16384] {
        let valid = |row: usize| row % 7 != 0;
        let timestamps = Arc::new(timestamp_array((0..rows).map(|row| {
            valid(row).then(|| {
                TimestampValue::new(-62_135_596_800_000 + row as i64 * 1000, 999_000).unwrap()
            })
        }))) as ArrayRef;
        let input = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ),
            (
                "f",
                Arc::new(Float32Array::from_iter((0..rows).map(|row| {
                    valid(row).then_some(if row % 17 == 0 {
                        f32::NAN
                    } else {
                        -(row as f32)
                    })
                }))) as ArrayRef,
            ),
            (
                "d",
                Arc::new(Float64Array::from_iter((0..rows).map(|row| {
                    valid(row).then_some(if row % 19 == 0 {
                        f64::NEG_INFINITY
                    } else {
                        row as f64
                    })
                }))) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from_iter((0..rows).map(|row| {
                        valid(row).then_some((row as i128 - rows as i128 / 2) * 100_001)
                    }))
                    .with_precision_and_scale(38, 6)
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                "clock",
                Arc::new(Time32MillisecondArray::from_iter(
                    (0..rows).map(|row| valid(row).then_some((row % 86_400_000) as i32)),
                )) as ArrayRef,
            ),
            ("local", timestamps.clone()),
            ("instant", timestamps),
        ])
        .unwrap();
        let selected = (0..rows as u32).collect::<Vec<_>>();
        let bytes = encode(&schema, &input, &selected);
        let fixture = streamfusion_iceberg::bench::ReadFixture::new(&schema, bytes);
        let target = input.schema();
        let decoded = fixture.read_as(&ids, 1024, 0, Some(target.clone()));
        assert_eq!(
            decoded.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
        assert!(decoded.iter().all(|batch| batch.schema() == target));
        let first = &decoded[0];
        for column in 0..first.num_columns() {
            assert_eq!(
                first.column(column).is_null(0),
                input.column(column).is_null(0)
            );
        }
        for read in [false, true] {
            let label = format!("{}/{rows}", if read { "read-adapt" } else { "encode" });
            group.throughput(Throughput::Elements(rows as u64));
            if read {
                let (batches, allocations) =
                    measure(|| fixture.read_as(&ids, 1024, 0, Some(target.clone())));
                let arrays = batches
                    .iter()
                    .flat_map(|b| b.columns().iter().cloned())
                    .collect::<Vec<_>>();
                report(&label, &[], &arrays, allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(fixture.read_as(&ids, 1024, 0, Some(target.clone())))
                    })
                });
            } else {
                let (_, allocations) = measure(|| encode(&schema, &input, &selected));
                report(&label, input.columns(), &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(encode(&schema, &input, &selected)))
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, iceberg, routing, reading, scalar_types);
criterion_main!(benches);
