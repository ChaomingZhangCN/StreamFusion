use super::*;
use streamfusion::bench::HiddenOffsetRank;

fn input(rows: usize, keys: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i / 5 % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| [10, 20, 30, 5, 20][i % 5]),
            )) as ArrayRef,
        ),
        (
            "p",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| [10, 20, 30, 5, 20][i % 5]),
            )) as ArrayRef,
        ),
        (
            "$row_kind$",
            Arc::new(Int8Array::from_iter_values((0..rows).map(|i| {
                if i % 5 == 4 {
                    3
                } else {
                    0
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap()
}
pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialized_offset_kinds");
    for rocks in [false, true] {
        if rocks && !cfg!(feature = "rocksdb-state") {
            continue;
        }
        for rows in [64, 4096] {
            for keys in [1, 64] {
                let batch = input(rows, keys);
                let schema = Arc::new(arrow::datatypes::Schema::new(
                    batch.schema().fields()[..3].to_vec(),
                ));
                let setup = || {
                    let directory = tempfile::tempdir().unwrap();
                    let operator = HiddenOffsetRank::new(
                        schema.clone(),
                        rocks.then(|| directory.path().to_str().unwrap()),
                    );
                    (operator, directory)
                };
                let (mut oracle, _directory) = setup();
                let prefix = input(5, 1);
                let output = oracle.push(&prefix);
                let kinds = output
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                assert_eq!(
                    kinds.values().as_ref(),
                    if rocks {
                        &[0, 0, 1, 2, 1, 2, 0, 1, 2, 3][..]
                    } else {
                        &[0, 0, 1, 2, 1, 2, 0][..]
                    }
                );
                for chunked in [false, true] {
                    let chunks = (0..rows)
                        .step_by(if chunked { 7 } else { rows })
                        .map(|start| {
                            batch.slice(start, (if chunked { 7 } else { rows }).min(rows - start))
                        })
                        .collect::<Vec<_>>();
                    let label = format!("rocks{rocks}/rows{rows}_keys{keys}_chunks{chunked}");
                    let (mut measured, _dir) = setup();
                    let (_, alloc) = measure(|| {
                        for chunk in &chunks {
                            std::hint::black_box(measured.push(chunk));
                        }
                    });
                    report(
                        &format!("serialized_offset_kinds/{label}"),
                        batch.columns(),
                        &[],
                        alloc,
                    );
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            setup,
                            |(operator, _)| {
                                for chunk in &chunks {
                                    std::hint::black_box(operator.push(chunk));
                                }
                            },
                            BatchSize::SmallInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}
