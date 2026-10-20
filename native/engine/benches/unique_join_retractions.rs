use arrow::array::{ArrayRef, Int64Array, Int8Array, RecordBatch};
use arrow::datatypes::Schema;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::UniqueUpdatingJoin;

#[cfg(feature = "rocksdb-state")]
const OPTIONS: &str = include_str!("fixtures/rocks-options.json");

enum Join {
    Heap(UniqueUpdatingJoin),
    #[cfg(feature = "rocksdb-state")]
    Rocks(streamfusion::bench::PersistentUpdatingJoin),
}
impl Join {
    fn push(&mut self, input: &RecordBatch, left: bool) -> RecordBatch {
        match self {
            Self::Heap(op) => op.push(input, left),
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(op) => op.push(input, left),
        }
    }
}
fn batch(rows: usize, keys: usize, nullable: bool, retract: bool) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                if retract {
                    Some(-9999999 - i as i64)
                } else {
                    (!(nullable && i % 7 == 0)).then_some(i as i64)
                }
            }))) as ArrayRef,
        ),
        (
            "$row_kind$",
            Arc::new(Int8Array::from(vec![if retract { 3 } else { 0 }; rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique_join_retractions");
    let backends: &[&str] = if cfg!(feature = "rocksdb-state") {
        &["heap", "rocks"]
    } else {
        &["heap"]
    };
    for rows in [16, 1024] {
        for keys in [1, 16] {
            for nullable in [false, true] {
                let seed = batch(rows, keys, nullable, false);
                let retract = batch(rows, keys, nullable, true);
                let schema = Arc::new(Schema::new(seed.schema().fields()[..2].to_vec()));
                for left in [false, true] {
                    for backend in backends {
                        let setup = || {
                            let directory =
                                (*backend == "rocks").then(|| tempfile::tempdir().unwrap());
                            let mut op = match directory.as_ref() {
                                #[cfg(feature = "rocksdb-state")]
                                Some(dir) => Join::Rocks(
                                    streamfusion::bench::PersistentUpdatingJoin::new(
                                        dir.path().to_str().unwrap(),
                                        schema.clone(),
                                        OPTIONS,
                                        false,
                                    )
                                    .with_unique_join_keys(true, true),
                                ),
                                _ => Join::Heap(
                                    UniqueUpdatingJoin::new(schema.clone(), false)
                                        .with_unique_join_keys(true, true),
                                ),
                            };
                            assert_eq!(op.push(&seed, left).num_rows(), 0);
                            (op, directory)
                        };
                        let (mut check, _directory) = setup();
                        assert_eq!(check.push(&retract, left).num_rows(), 0);
                        assert_eq!(check.push(&seed, !left).num_rows(), 0);
                        drop((check, _directory));
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(
                            format!("{backend}/rows{rows}_keys{keys}_null{nullable}_left{left}"),
                            |b| {
                                b.iter_batched_ref(
                                    setup,
                                    |(op, _directory)| {
                                        black_box(op.push(black_box(&retract), left))
                                    },
                                    BatchSize::SmallInput,
                                )
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
