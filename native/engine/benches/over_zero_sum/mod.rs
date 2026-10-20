use super::*;
use arrow::array::*;
use arrow::compute::concat_batches;
use datafusion::scalar::ScalarValue;

fn fixture(rows: usize, keys: usize, code: i64) -> RecordBatch {
    let values = (0..rows).map(|row| {
        let value = (row % 7) as i64 - 3;
        let valid = row % 5 != 0 && (keys == 1 || row % keys != 0);
        match code {
            0 => ScalarValue::Int64(valid.then_some(value)),
            2 => ScalarValue::Int32(valid.then_some(value as i32)),
            4 => ScalarValue::Int16(valid.then_some(value as i16)),
            5 => ScalarValue::Int8(valid.then_some(value as i8)),
            6 => ScalarValue::Float32(valid.then_some(value as f32 * 0.25)),
            1 => ScalarValue::Float64(valid.then_some(value as f64 * 0.25)),
            _ => unreachable!(),
        }
    });
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
        ("v", ScalarValue::iter_to_array(values).unwrap()),
    ])
    .unwrap()
}

fn execute(operator: &mut Over, batch: RecordBatch, proctime: bool) -> RecordBatch {
    if proctime {
        operator.push_proctime(batch)
    } else {
        let watermark = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(batch.num_rows() - 1);
        operator.push(batch);
        operator.flush(watermark)
    }
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_primitive_zero_sum");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for code in [0, 2, 4, 5, 6, 1] {
                let batch = fixture(rows, keys, code);
                for kind in [22, 122] {
                    for proctime in [false, true] {
                        for frame in 0..4 {
                            if proctime && frame == 2 {
                                continue;
                            }
                            let offset = if frame == 1 { 3 } else { 10 };
                            let mut operator = Over::framed(code, kind, frame, offset, proctime);
                            let whole = execute(&mut operator, batch.clone(), proctime);
                            assert_eq!(whole.num_rows(), rows);
                            assert_eq!(whole.column(3).null_count(), 0);
                            let mut operator = Over::framed(code, kind, frame, offset, proctime);
                            let mut parts = Vec::new();
                            for start in (0..rows).step_by(17) {
                                parts.push(execute(
                                    &mut operator,
                                    batch.slice(start, 17.min(rows - start)),
                                    proctime,
                                ));
                                operator = Over::restore_framed(
                                    code,
                                    kind,
                                    frame,
                                    offset,
                                    proctime,
                                    &operator.snapshot(),
                                );
                            }
                            assert_eq!(
                                whole,
                                concat_batches(&whole.schema(), &parts).unwrap(),
                                "SUM0 checkpoint continuation"
                            );
                            group.bench_function(
                                BenchmarkId::new(
                                    format!("type{code}_kind{kind}"),
                                    format!(
                                        "rows{rows}_keys{keys}_frame{frame}_proctime{proctime}"
                                    ),
                                ),
                                |b| {
                                    b.iter_batched_ref(
                                        || Over::framed(code, kind, frame, offset, proctime),
                                        |operator| {
                                            black_box(execute(operator, batch.clone(), proctime))
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
    }
    group.finish();
}
