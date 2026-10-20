use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::Tumbling;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn bench_window_string_extrema(c: &mut Criterion) {
    header();
    for (shape, size, slide, cumulative) in [
        ("tumble", 1000, 1000, false),
        ("hop", 10000, 2000, false),
        ("cumulate", 10000, 2000, true),
    ] {
        let mut group = c.benchmark_group(format!("window_string_extrema/{shape}"));
        let setup = || {
            Tumbling::with_window_budget(size, slide, cumulative, 3, vec![1, 2], 1024 * 1024 * 1024)
        };
        for rows in [256usize, 4096] {
            for cardinality in [1usize, 64, rows] {
                for width in [16usize, 256] {
                    let source: Vec<Option<String>> = (0..rows)
                        .map(|i| {
                            (!(i % 7 == 0 || (cardinality > 1 && i % cardinality == 0))).then(
                                || format!("{:08}/\u{4e2d}\0{}", i % 1024, "x".repeat(width - 13)),
                            )
                        })
                        .collect();
                    let values: ArrayRef =
                        Arc::new(StringArray::from_iter(source.iter().map(Option::as_deref)));
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
                    let mut probe = setup();
                    probe.update(&batch);
                    let output = probe.flush(i64::MAX);
                    assert_eq!(
                        output.num_rows(),
                        cardinality * if shape == "tumble" { 1 } else { 5 }
                    );
                    let keys = output
                        .column_by_name("key0")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for (name, minimum) in [("result0", true), ("result1", false)] {
                        let actual = output
                            .column_by_name(name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        for row in 0..output.num_rows() {
                            let key = keys.value(row) as usize;
                            let candidates = (key..rows)
                                .step_by(cardinality)
                                .filter_map(|i| source[i].as_deref());
                            let expected = if minimum {
                                candidates.min()
                            } else {
                                candidates.max()
                            };
                            assert_eq!((!actual.is_null(row)).then(|| actual.value(row)), expected);
                        }
                    }
                    let name = format!("{rows}/{cardinality}/{width}");
                    let mut allocation_probe = setup();
                    let (output, allocations) = measure(|| {
                        allocation_probe.update(&batch);
                        allocation_probe.flush(i64::MAX)
                    });
                    report(
                        &format!("window_string_extrema/{shape}/{name}"),
                        batch.columns(),
                        output.columns(),
                        allocations,
                    );
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(name, |b| {
                        b.iter_batched(
                            || setup(),
                            |mut aggregate| {
                                aggregate.update(black_box(&batch));
                                black_box(aggregate.flush(i64::MAX));
                            },
                            BatchSize::SmallInput,
                        );
                    });
                }
            }
        }
        group.finish();
    }
}

criterion_group!(benches, bench_window_string_extrema);
criterion_main!(benches);
