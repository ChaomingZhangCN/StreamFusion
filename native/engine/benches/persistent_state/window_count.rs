use super::OPTIONS;
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::Tumbling;
use streamfusion_benchmark_support::{measure, report};

/// Repeated batches, non-closing watermarks and final firing include all deferred state writes.
pub(super) fn count(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/window_count");
    for rows in [16, 1024, 8192] {
        for domain in [8, rows] {
            for width in [8, 256] {
                for nullable in [false, true] {
                    if !streamfusion_benchmark_support::smoke_fixture() {
                        continue;
                    }
                    let input = RecordBatch::try_from_iter(vec![
                        (
                            "key0",
                            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                                (!(nullable && i % 11 == 0))
                                    .then(|| format!("{}/{}", i % domain, "x".repeat(width)))
                            }))) as ArrayRef,
                        ),
                        (
                            "value0",
                            Arc::new(Int64Array::from_iter(
                                (0..rows).map(|i| (!(nullable && i % 7 == 0)).then_some(i as i64)),
                            )) as ArrayRef,
                        ),
                        (
                            "ts",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| (i % 3) as i64 * 1000 + 100),
                            )) as ArrayRef,
                        ),
                    ])
                    .unwrap();
                    for (shape, size, cumulative) in [
                        ("tumble", 1000, false),
                        ("hop", 5000, false),
                        ("cumulate", 5000, true),
                    ] {
                        let setup = || {
                            let dir = tempfile::tempdir().unwrap();
                            let operator = Tumbling::persistent_fixed_calendar(
                                dir.path().join("db").to_str().unwrap(),
                                OPTIONS,
                                size,
                                1000,
                                cumulative,
                            );
                            (operator, dir)
                        };
                        let run = |state: &mut (Tumbling, tempfile::TempDir)| {
                            for watermark in [0, 100, 200, 300] {
                                state.0.update(&input);
                                assert_eq!(state.0.flush(watermark).num_rows(), 0);
                            }
                            state.0.flush(i64::MAX)
                        };
                        let mut reference = Tumbling::fixed_calendar(size, 1000, cumulative, 0);
                        for _ in 0..4 {
                            reference.update(&input);
                        }
                        let expected = reference.flush(i64::MAX);
                        let mut state = setup();
                        let (output, allocations) = measure(|| run(&mut state));
                        assert_eq!(output, expected);
                        assert_eq!(state.0.flush(i64::MAX).num_rows(), 0);
                        let label = format!(
                            "count/{shape}/{rows}/domain={domain}/bytes={width}/nulls={nullable}"
                        );
                        report(&label, input.columns(), output.columns(), allocations);
                        group.throughput(Throughput::Elements(4 * rows as u64));
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |state| std::hint::black_box(run(state)),
                                BatchSize::PerIteration,
                            )
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
