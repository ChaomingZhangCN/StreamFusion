use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::Over;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
use streamfusion_bridge::string_cache::{
    string_cache_array, string_cache_column, string_cache_field, JavaStringCacheColumn,
};

fn bench(c: &mut Criterion) {
    header();
    let cache = string_cache_array([Some(&[97u16][..])]);
    let seed = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rt", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
            string_cache_field(2, cache.data_type().clone()),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![1000])),
            Arc::new(StringArray::from(vec!["a"])),
            cache,
        ],
    )
    .unwrap();
    let mut group = c.benchmark_group("over_sparse_string_cache");
    for rows in [64, 4096] {
        let tail = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef),
            (
                "rt",
                Arc::new(Int64Array::from_iter_values(1001..=1000 + rows as i64)) as ArrayRef,
            ),
            (
                "v",
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|row| (row % 7 != 0).then_some("z")),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        group.throughput(Throughput::Elements(rows as u64));
        for frame in 0..4 {
            for kind in [1, 2, 5, 6] {
                let create = || {
                    let mut aggregate = Over::framed(3, kind, frame, rows as i64 + 2, false);
                    aggregate.push(seed.clone());
                    aggregate.flush(1000);
                    aggregate
                };
                let execute = |aggregate: &mut Over| {
                    aggregate.push(tail.clone());
                    aggregate.flush(1000 + rows as i64)
                };
                let mut probe = create();
                let (checked, allocations) = measure(|| execute(&mut probe));
                let cache = JavaStringCacheColumn::new(string_cache_column(&checked, 3).unwrap());
                let values = checked
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..rows {
                    let cached = matches!(kind, 1 | 5) || row == 0;
                    assert_eq!(cache.value(row), cached.then_some(&[97u16][..]));
                    assert_eq!(values.value(row), if cached { "a" } else { "z" });
                }
                let name = format!("rows{rows}/frame{frame}/kind{kind}");
                report(
                    &format!("over_sparse_string_cache/{name}"),
                    tail.columns(),
                    checked.columns(),
                    allocations,
                );
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
criterion_group!(benches, bench);
criterion_main!(benches);
