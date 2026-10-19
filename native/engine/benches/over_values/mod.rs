use super::*;
mod cached_strings;
mod cached_transport;
mod decimal_distinct;
mod primitives;
use streamfusion_benchmark_support::{measure, report};
mod string_tree;
mod string_view_checkpoints;
use arrow::compute::concat_batches;
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

fn fixture(rows: usize, keys: usize, code: i64) -> RecordBatch {
    let present = |row: usize| row % 5 != 0 && (keys == 1 || row % keys != 0);
    let values: ArrayRef = if code == 3 {
        let wide = "😀".repeat(1024);
        Arc::new(StringArray::from_iter((0..rows).map(|row| {
            present(row).then_some(match row % 5 {
                0 => "",
                1 => "a",
                2 => "\u{e000}",
                3 => "😀",
                _ => &wide,
            })
        })))
    } else if code == 1009 {
        Arc::new(timestamp_array((0..rows).map(|row| {
            present(row).then(|| {
                TimestampValue::new(
                    [-62_135_596_800_000, 0, 253_402_300_799_999][row % 3],
                    (row % 1_000_000) as u32,
                )
                .unwrap()
            })
        })))
    } else {
        let maximum = 10_i128.pow(((code - 2000) / 100) as u32) - 1;
        Arc::new(
            Decimal128Array::from_iter((0..rows).map(|row| {
                present(row).then_some(match row / (2 * keys) % 5 {
                    0 => maximum,
                    1 => -maximum,
                    2 => 0,
                    3 => 1,
                    _ => -1,
                })
            }))
            .with_precision_and_scale(((code - 2000) / 100) as u8, ((code - 2000) % 100) as i8)
            .unwrap(),
        )
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
                (0..rows).map(|row| 1000 + row as i64),
            )) as ArrayRef,
        ),
        ("v", values),
    ])
    .unwrap()
}

fn execute(operator: &mut Over, batch: RecordBatch, proctime: bool) -> RecordBatch {
    if proctime {
        operator.push_proctime(batch)
    } else {
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let watermark = timestamps.value(timestamps.len() - 1);
        operator.push(batch);
        operator.flush(watermark)
    }
}

fn validate(batch: &RecordBatch, code: i64, kind: i64, frame: i64, offset: i64, proctime: bool) {
    let whole = execute(
        &mut Over::framed(code, kind, frame, offset, proctime),
        batch.clone(),
        proctime,
    );
    if kind == 103 && matches!(batch.column(2).data_type(), DataType::Decimal128(_, _)) {
        decimal_distinct::check(batch, &whole, frame, offset);
    }
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
    assert_eq!(
        whole,
        concat_batches(&whole.schema(), &fragments).unwrap(),
        "typed value checkpoint continuation"
    );
    assert_eq!(whole.num_rows(), batch.num_rows());
    assert_eq!(
        whole.column(3).data_type(),
        if kind == 103 {
            &DataType::Int64
        } else {
            batch.column(2).data_type()
        }
    );
}

pub(super) fn bench(c: &mut Criterion) {
    primitives::bench(c);
    string_tree::bench(c);
    cached_strings::bench(c);
    cached_transport::bench(c);
    string_view_checkpoints::bench(c);
    let mut group = c.benchmark_group("over_typed_values");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for (name, code) in [
                ("decimal12_2", 3202),
                ("decimal38_0", 5800),
                ("decimal38_38", 5838),
                ("timestamp9", 1009),
                ("string_variable", 3),
            ] {
                let batch = fixture(rows, keys, code);
                for kind in [1, 2, 5, 6, 103] {
                    if kind == 103 && code < 2000 {
                        continue;
                    }
                    for proctime in [false, true] {
                        for frame in 0..4 {
                            if proctime && frame == 2 {
                                continue;
                            }
                            let offset = if frame == 1 { 3 } else { 10 };
                            validate(&batch, code, kind, frame, offset, proctime);
                            if kind == 103 {
                                let mut probe = Over::framed(code, kind, frame, offset, proctime);
                                let (output, allocations) =
                                    measure(|| execute(&mut probe, batch.clone(), proctime));
                                let label = format!("over_typed_values/{name}/rows{rows}_keys{keys}_kind{kind}_frame{frame}_proctime{proctime}");
                                report(&label, batch.columns(), output.columns(), allocations);
                            }
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
}
