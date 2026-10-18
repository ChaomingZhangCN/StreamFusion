use super::*;
use arrow::array::{Decimal128Array, Int16Array, Int32Array};
use streamfusion::bench::GroupBy;

pub fn bench_typed_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("typed_distinct");
    group.throughput(Throughput::Elements(ROWS as u64));
    for cardinality in [4, 256] {
        for (name, code) in [
            ("bigint", 0),
            ("int", 2),
            ("smallint", 4),
            ("tinyint", 5),
            ("decimal", 4002),
            ("date", 8),
            ("time", 9),
            ("timestamp", 7),
            ("string8", 3),
            ("string256", 3),
        ] {
            let values: Vec<Option<i64>> = (0..ROWS)
                .map(|i| (i % 7 != 0).then_some(((i / 16) % cardinality) as i64 - 128))
                .collect();
            let value: ArrayRef = match name {
                "bigint" => Arc::new(Int64Array::from(values)),
                "int" => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                "smallint" => Arc::new(Int16Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i16))
                        .collect::<Vec<_>>(),
                )),
                "tinyint" => Arc::new(Int8Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i8))
                        .collect::<Vec<_>>(),
                )),
                "decimal" => Arc::new(
                    Decimal128Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| i128::from(v) * 100_000_000_000))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
                ),
                "date" => Arc::new(arrow::array::Date32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                "time" => Arc::new(arrow::array::Time32MillisecondArray::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| (v + 128) as i32))
                        .collect::<Vec<_>>(),
                )),
                "timestamp" => Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                    values.iter().map(|v| {
                        v.map(|v| {
                            streamfusion_bridge::timestamp::TimestampValue::new(
                                -1,
                                (v + 128) as u32,
                            )
                            .unwrap()
                        })
                    }),
                )),
                _ => {
                    let width = if name == "string8" { 8 } else { 256 };
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| format!("{v:0width$}")))
                            .collect::<Vec<_>>(),
                    ))
                }
            };
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("v", value.data_type().clone(), true),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS).map(|i| (i % 16) as i64),
                    )),
                    value,
                ],
            )
            .unwrap();
            let mut fixture =
                LocalGroupBy::filtered(vec![7], vec![code], vec![1], vec![-1], vec![0], vec![0]);
            fixture.update(&batch);
            let output = fixture.flush();
            assert_eq!(output.num_rows(), 16);
            let DataType::List(entries) = output.column(2).data_type() else {
                panic!("expected a typed distinct view");
            };
            let DataType::Struct(fields) = entries.data_type() else {
                panic!("expected value/count entries");
            };
            assert_eq!(fields[0].data_type(), batch.column(1).data_type());
            for local in [false, true] {
                let id = format!(
                    "{}/{name}/cardinality{cardinality}",
                    if local { "local" } else { "single" }
                );
                group.bench_function(id, |b| {
                    if local {
                        b.iter_batched(
                            || {
                                LocalGroupBy::filtered(
                                    vec![7],
                                    vec![code],
                                    vec![1],
                                    vec![-1],
                                    vec![0],
                                    vec![0],
                                )
                            },
                            |mut agg| {
                                agg.update(black_box(&batch));
                                black_box(agg.flush());
                            },
                            BatchSize::SmallInput,
                        );
                    } else {
                        b.iter_batched(
                            || GroupBy::new(vec![7], vec![code], vec![1], vec![0]),
                            |mut agg| black_box(agg.update(black_box(&batch))),
                            BatchSize::SmallInput,
                        );
                    }
                });
            }
        }
    }
    group.finish();
}
