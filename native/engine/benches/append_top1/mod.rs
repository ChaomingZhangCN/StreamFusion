use super::*;
#[cfg(feature = "rocksdb-state")]
use streamfusion::bench::PersistentTop1;

enum Top1 {
    Memory(AppendTopN),
    #[cfg(feature = "rocksdb-state")]
    Rocks(PersistentTop1),
}

#[cfg(feature = "rocksdb-state")]
fn bench_shared_pool(c: &mut Criterion) {
    let mut options: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/rocks-options.json")).unwrap();
    options["writeBufferSize"] = (64 * 1024 * 1024).into();
    let options = options.to_string();
    let mut group = c.benchmark_group("append_top1_shared_pool");
    for rows in [256, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [16, rows] {
            for width in [8, 256] {
                let fixture = |seed: bool| {
                    RecordBatch::try_from_iter(vec![
                        (
                            "key",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|row| (row % keys) as i64),
                            )) as ArrayRef,
                        ),
                        (
                            "sort",
                            Arc::new(Int64Array::from_iter((0..rows).map(|row| {
                                if seed {
                                    Some(-1)
                                } else {
                                    (row % 7 != 0).then_some(row as i64)
                                }
                            }))) as ArrayRef,
                        ),
                        (
                            "payload",
                            Arc::new(StringArray::from_iter(
                                (0..rows).map(|row| (row % 11 != 0).then(|| "x".repeat(width))),
                            )) as ArrayRef,
                        ),
                    ])
                    .unwrap()
                };
                let input = fixture(false);
                let seed = fixture(true);
                for fast in [false, true] {
                    let setup = || {
                        let directory = tempfile::tempdir().unwrap();
                        let mut operator = PersistentTop1::shared(
                            directory.path().join("db").to_str().unwrap(),
                            &options,
                            input.schema(),
                            true,
                            fast,
                            10 * 1024 * 1024,
                        );
                        operator.push(&seed);
                        operator.flush();
                        operator.push(&seed);
                        operator.flush();
                        (operator, directory)
                    };
                    let (mut operator, _directory) = setup();
                    assert_eq!(operator.push(&input).num_rows(), 0);
                    assert_eq!(operator.flush().num_rows(), 0);
                    group.bench_function(
                        format!("rows{rows}/keys{keys}/width{width}/fast{fast}"),
                        |b| {
                            b.iter_batched(
                                setup,
                                |(mut operator, directory)| {
                                    operator.push(black_box(&input));
                                    let output = operator.flush();
                                    black_box((operator, directory, output))
                                },
                                BatchSize::SmallInput,
                            )
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

impl Top1 {
    fn run(&mut self, batch: &RecordBatch, bundle: bool) -> RecordBatch {
        match self {
            Self::Memory(operator) => {
                let output = operator.push(batch);
                if bundle {
                    operator.flush()
                } else {
                    output
                }
            }
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(operator) => {
                let output = operator.push(batch);
                if bundle {
                    operator.flush()
                } else {
                    output
                }
            }
        }
    }
}

pub fn bench_append_top1(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_top1");
    for rows in [256, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [16, rows] {
            for width in [8, 256] {
                let fixture = |seed: bool, rejects: bool| {
                    RecordBatch::try_from_iter(vec![
                        (
                            "key",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|row| (row % keys) as i64),
                            )) as ArrayRef,
                        ),
                        (
                            "sort",
                            Arc::new(Int64Array::from_iter((0..rows).map(|row| {
                                if seed {
                                    Some(if rejects { -1 } else { rows as i64 + 1 })
                                } else {
                                    (row % 7 != 0).then_some((rows - row) as i64)
                                }
                            }))) as ArrayRef,
                        ),
                        (
                            "payload",
                            Arc::new(StringArray::from_iter(
                                (0..rows).map(|row| (row % 11 != 0).then(|| "x".repeat(width))),
                            )) as ArrayRef,
                        ),
                    ])
                    .unwrap()
                };
                let input = fixture(false, false);
                for mode in ["reject", "improve", "fresh"] {
                    let seed = fixture(true, mode == "reject");
                    for bundle in [false, true] {
                        for persistent in [false, true] {
                            if persistent && !cfg!(feature = "rocksdb-state") {
                                continue;
                            }
                            for fast in [false, true] {
                                let setup = || {
                                    let directory =
                                        persistent.then(|| tempfile::tempdir().unwrap());
                                    let mut operator = if persistent {
                                        #[cfg(feature = "rocksdb-state")]
                                        {
                                            Top1::Rocks(PersistentTop1::new(
                                                directory
                                                    .as_ref()
                                                    .unwrap()
                                                    .path()
                                                    .join("db")
                                                    .to_str()
                                                    .unwrap(),
                                                include_str!("../fixtures/rocks-options.json"),
                                                input.schema(),
                                                bundle,
                                                fast,
                                            ))
                                        }
                                        #[cfg(not(feature = "rocksdb-state"))]
                                        {
                                            unreachable!()
                                        }
                                    } else {
                                        Top1::Memory(
                                            AppendTopN::new(
                                                vec![0],
                                                vec![(1, true)],
                                                1,
                                                false,
                                                bundle,
                                            )
                                            .with_top1_fast(fast),
                                        )
                                    };
                                    if mode != "fresh" {
                                        operator.run(&seed, bundle);
                                    }
                                    (operator, directory)
                                };
                                let backend = if persistent { "rocksdb" } else { "memory" };
                                let phase = if bundle { "bundle" } else { "immediate" };
                                let strategy = if fast { "specialized" } else { "generic" };
                                let id = format!("{backend}/{mode}/rows{rows}/keys{keys}/width{width}/{phase}/{strategy}");
                                let (mut operator, _directory) = setup();
                                let output = operator.run(&input, bundle);
                                if mode == "reject" {
                                    assert_eq!(output.num_rows(), 0, "{id}");
                                }
                                group.bench_function(id, |b| {
                                    b.iter_batched(
                                        setup,
                                        |(mut operator, directory)| {
                                            let output = operator.run(black_box(&input), bundle);
                                            black_box((operator, directory, output))
                                        },
                                        BatchSize::SmallInput,
                                    )
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
    #[cfg(feature = "rocksdb-state")]
    bench_shared_pool(c);
}
