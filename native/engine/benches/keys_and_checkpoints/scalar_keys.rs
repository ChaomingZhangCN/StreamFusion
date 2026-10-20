use super::*;
use arrow::array::{Float32Array, Float64Array, Int16Array, Int32Array, Time32MillisecondArray};
use streamfusion::bench::{Over, Session, Tumbling};

fn key(code: i64, rows: usize, keys: usize) -> ArrayRef {
    let values = (0..rows)
        .map(|row| (row % 11 != 0).then_some((row % keys) as i64 - 32))
        .collect::<Vec<_>>();
    match code {
        1 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|v| {
                    v.map(|v| {
                        if v % 17 == 0 {
                            f64::from_bits(0x7ff8000000000000 | v.unsigned_abs())
                        } else {
                            v as f64 * 0.25
                        }
                    })
                })
                .collect::<Vec<_>>(),
        )),
        6 => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| {
                    v.map(|v| {
                        if v % 17 == 0 {
                            f32::from_bits(0x7fc00000 | v.unsigned_abs() as u32)
                        } else {
                            v as f32 * 0.25
                        }
                    })
                })
                .collect::<Vec<_>>(),
        )),
        5 => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i8))
                .collect::<Vec<_>>(),
        )),
        4 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i16))
                .collect::<Vec<_>>(),
        )),
        9 => Arc::new(Time32MillisecondArray::from(
            values
                .iter()
                .map(|v| v.map(|v| ((v + 32) * 1001) as i32))
                .collect::<Vec<_>>(),
        )),
        11 => Arc::new(BinaryArray::from_iter(
            values.iter().map(|v| v.map(|v| v.to_le_bytes().repeat(8))),
        )),
        12 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Vec<_>>(),
        )),
        13 => Arc::new(Int64Array::from(values)),
        _ => unreachable!(),
    }
}
pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_partition_keys");
    for rows in [64, 4096] {
        for keys in [1, 64] {
            for code in [5, 4, 9, 11, 12, 13, 6, 1] {
                let array = key(code, rows, keys);
                let shard: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|r| (r % 3) as i64),
                ));
                let arrays = vec![array.clone(), shard.clone()];
                let codec = KeyCodec::new(&arrays);
                let encoded = codec.encode(&arrays, rows);
                assert_eq!(codec.decode(&encoded), arrays);
                let batch = RecordBatch::try_from_iter(vec![
                    ("k", array.clone()),
                    ("shard", shard.clone()),
                ])
                .unwrap();
                assert_eq!(flink_key_hashes(&batch, &[0, 1], &[-1, -1]).len(), rows);
                let id = format!("type{code}_rows{rows}_keys{keys}");
                group.bench_function(format!("encode/{id}"), |b| {
                    b.iter(|| std::hint::black_box(codec.encode(&arrays, rows)))
                });
                group.bench_function(format!("decode/{id}"), |b| {
                    b.iter(|| std::hint::black_box(codec.decode(&encoded)))
                });
                group.bench_function(format!("flink_hash/{id}"), |b| {
                    b.iter(|| std::hint::black_box(flink_key_hashes(&batch, &[0, 1], &[-1, -1])))
                });
                let window = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|r| 1000 + (r % 80) as i64),
                        )) as ArrayRef,
                    ),
                    (
                        "value0",
                        Arc::new(Int64Array::from(vec![Some(1); rows])) as ArrayRef,
                    ),
                    ("key0", array.clone()),
                    ("key1", shard.clone()),
                ])
                .unwrap();
                let mut fixed = Tumbling::with_budget(100, 0, vec![3], 1 << 30);
                fixed.update(&window);
                let output = fixed.flush(2000);
                let total = output
                    .column_by_name("result0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>();
                assert_eq!(total, rows as i64);
                group.bench_function(format!("tumble/{id}"), |b| {
                    b.iter_batched_ref(
                        || Tumbling::with_budget(100, 0, vec![3], 1 << 30),
                        |op| {
                            op.update(&window);
                            std::hint::black_box(op.flush(2000))
                        },
                        BatchSize::SmallInput,
                    )
                });
                let mut session = Session::with_budget(20, 0, vec![3], 1 << 30);
                session.update(&window);
                let state = session.snapshot();
                session = Session::restore(20, 0, vec![3], &state);
                let output = session.flush(2000);
                let total = output
                    .column_by_name("result0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>();
                assert_eq!(total, rows as i64);
                group.bench_function(format!("session/{id}"), |b| {
                    b.iter_batched_ref(
                        || Session::with_budget(20, 0, vec![3], 1 << 30),
                        |op| {
                            op.update(&window);
                            std::hint::black_box(op.flush(2000))
                        },
                        BatchSize::SmallInput,
                    )
                });
                let over = RecordBatch::try_from_iter(vec![
                    ("k", array.clone()),
                    (
                        "rt",
                        Arc::new(Int64Array::from_iter_values(
                            (0..rows).map(|r| 1000 + r as i64),
                        )) as ArrayRef,
                    ),
                    (
                        "v",
                        Arc::new(Int64Array::from(vec![Some(1); rows])) as ArrayRef,
                    ),
                ])
                .unwrap();
                let mut op = Over::framed(0, 3, 1, 2, true).with_budget(1 << 30);
                let output = op.push_proctime(over.clone());
                assert_eq!(output.num_rows(), rows);
                group.bench_function(format!("over/{id}"), |b| {
                    b.iter_batched_ref(
                        || Over::framed(0, 3, 1, 2, true).with_budget(1 << 30),
                        |op| std::hint::black_box(op.push_proctime(over.clone())),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}
