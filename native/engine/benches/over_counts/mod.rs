use super::*;
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

fn fixture(rows: usize, keys: usize, type_code: i64, width: usize) -> RecordBatch {
    let valid = |row: usize| row % 5 != 0 && (keys == 1 || row % keys != 0);
    let values: ArrayRef = match type_code {
        0 => Arc::new(Int64Array::from_iter(
            (0..rows).map(|row| valid(row).then_some(row as i64)),
        )),
        3 => Arc::new(StringArray::from_iter(
            (0..rows).map(|row| valid(row).then(|| "x".repeat(width))),
        )),
        3202 | 5838 => {
            let (precision, scale) = if type_code == 3202 { (12, 2) } else { (38, 38) };
            Arc::new(
                Decimal128Array::from_iter((0..rows).map(|row| valid(row).then_some(row as i128)))
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            )
        }
        1009 => Arc::new(timestamp_array((0..rows).map(|row| {
            valid(row).then(|| TimestampValue::new(253_402_300_799_999, 999_999).unwrap())
        }))),
        _ => unreachable!(),
    };
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
                (0..rows).map(|row| 1000 + row as i64 / 4),
            )) as ArrayRef,
        ),
        ("v", values),
    ])
    .unwrap()
}

fn expected(rows: usize, keys: usize, frame: i64, proctime: bool) -> Vec<i64> {
    (0..rows)
        .map(|row| {
            let key = row % keys;
            let time = row / 4;
            let upper = if matches!(frame, 0 | 2) && !proctime {
                (time * 4 + 3).min(rows - 1)
            } else {
                row
            };
            let lower = if frame == 1 {
                row.saturating_sub(3 * keys)
            } else {
                0
            };
            (lower..=upper)
                .filter(|&candidate| {
                    candidate % keys == key
                        && (frame != 2 || candidate / 4 >= time.saturating_sub(10))
                        && candidate % 5 != 0
                        && (keys == 1 || key != 0)
                })
                .count() as i64
        })
        .collect()
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_count_validity");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for (name, code, width) in [
                ("bigint", 0, 8),
                ("string8", 3, 8),
                ("string256", 3, 256),
                ("decimal12", 3202, 16),
                ("decimal38", 5838, 16),
                ("timestamp9", 1009, 12),
            ] {
                let batch = fixture(rows, keys, code, width);
                for proctime in [false, true] {
                    for frame in 0..4 {
                        if proctime && frame == 2 {
                            continue;
                        }
                        let offset = if frame == 1 { 3 } else { 10 };
                        let create = || Over::framed(code, 3, frame, offset, proctime);
                        let execute = |operator: &mut Over| {
                            if proctime {
                                operator.push_proctime(batch.clone())
                            } else {
                                operator.push(batch.clone());
                                operator.flush(i64::MAX)
                            }
                        };
                        let out = execute(&mut create());
                        let counts = out.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                        assert_eq!(
                            counts.values().as_ref(),
                            expected(rows, keys, frame, proctime)
                        );
                        group.bench_function(
                            BenchmarkId::new(
                                name,
                                format!("rows{rows}_keys{keys}_frame{frame}_proctime{proctime}"),
                            ),
                            |b| {
                                b.iter_batched_ref(
                                    create,
                                    |operator| black_box(execute(operator)),
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
