use super::*;
use arrow::compute::concat_batches;

fn fixture(rows: usize, keys: usize, code: i64) -> RecordBatch {
    let precision = ((code - 2000) / 100) as u8;
    let scale = ((code - 2000) % 100) as i8;
    let values = (0..rows)
        .map(|row| {
            if row % 5 == 0 || (keys > 1 && row % keys == 0) {
                None
            } else if precision == 38 && row % 257 == 1 {
                Some(10i128.pow(38) - 1)
            } else {
                Some((row % 31) as i128 - 15)
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 1000 + row as i64),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn execute(operator: &mut Over, batch: RecordBatch, proctime: bool) -> RecordBatch {
    if proctime {
        operator.push_proctime(batch)
    } else {
        let timestamp = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let watermark = timestamp.value(timestamp.len() - 1);
        operator.push(batch);
        operator.flush(watermark)
    }
}

fn validate(batch: &RecordBatch, code: i64, kind: i64, frame: i64, offset: i64, proctime: bool) {
    let mut operator = Over::framed(code, kind, frame, offset, proctime);
    let whole = execute(&mut operator, batch.clone(), proctime);
    let mut operator = Over::framed(code, kind, frame, offset, proctime);
    let mut fragments = Vec::new();
    for start in (0..batch.num_rows()).step_by(17) {
        fragments.push(execute(
            &mut operator,
            batch.slice(start, 17.min(batch.num_rows() - start)),
            proctime,
        ));
        operator = Over::restore_framed(code, kind, frame, offset, proctime, &operator.snapshot());
    }
    let restored = concat_batches(&whole.schema(), &fragments).unwrap();
    assert_eq!(
        whole, restored,
        "decimal OVER fragmented checkpoint continuation"
    );
    assert_eq!(whole.num_rows(), batch.num_rows());
    let scale = ((code - 2000) % 100) as i8;
    let result_scale = if kind == 4 { scale.max(6) } else { scale };
    assert_eq!(
        whole.column(3).data_type(),
        &DataType::Decimal128(38, result_scale)
    );
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_decimal_reductions");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for (name, code) in [
                ("decimal12_2", 3202),
                ("decimal38_0", 5800),
                ("decimal38_18", 5818),
            ] {
                let batch = fixture(rows, keys, code);
                for kind in [0, 22, 4] {
                    for proctime in [false, true] {
                        for frame in 0..4 {
                            if proctime && frame == 2 {
                                continue;
                            }
                            let offset = if frame == 1 { 3 } else { 10 };
                            validate(&batch, code, kind, frame, offset, proctime);
                            group.bench_function(BenchmarkId::new(name,
                                format!("rows{rows}_keys{keys}_kind{kind}_frame{frame}_proctime{proctime}")), |b| {
                                b.iter_batched_ref(|| Over::framed(code, kind, frame, offset, proctime),
                                    |operator| black_box(execute(operator, batch.clone(), proctime)), BatchSize::SmallInput)
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();

    let mut snapshots = c.benchmark_group("over_decimal_checkpoint");
    for keys in [1, 64] {
        let batch = fixture(4096, keys, 5800);
        for kind in [0, 22, 4] {
            for frame in 0..4 {
                let offset = if frame == 1 { 3 } else { 10 };
                let setup = || {
                    let mut operator = Over::framed(5800, kind, frame, offset, false);
                    let _ = execute(&mut operator, batch.clone(), false);
                    operator
                };
                let bytes = setup().snapshot();
                let id = format!("keys{keys}_kind{kind}_frame{frame}");
                snapshots.bench_function(BenchmarkId::new("export", &id), |b| {
                    b.iter_batched_ref(
                        setup,
                        |operator| black_box(operator.snapshot()),
                        BatchSize::SmallInput,
                    )
                });
                snapshots.bench_function(BenchmarkId::new("restore", &id), |b| {
                    b.iter_batched(
                        || (),
                        |_| {
                            black_box(Over::restore_framed(
                                5800, kind, frame, offset, false, &bytes,
                            ))
                        },
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    snapshots.finish();
}
