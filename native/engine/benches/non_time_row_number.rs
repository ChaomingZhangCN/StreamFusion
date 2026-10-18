use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, Time32MillisecondArray,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::AppendTopN;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum SortValue {
    Number(i64),
    Text(String),
}

fn sort_value(ty: &str, value: i64) -> SortValue {
    match ty {
        "tinyint" => SortValue::Number(value as i8 as i64),
        "smallint" => SortValue::Number(value as i16 as i64),
        "boolean" => SortValue::Number(i64::from(value % 2 == 0)),
        "string8" | "string256" => {
            let width = if ty == "string8" { 8 } else { 256 };
            SortValue::Text(format!("{value:0width$}"))
        }
        _ => SortValue::Number(value),
    }
}

fn column(ty: &str, values: &[Option<i64>]) -> ArrayRef {
    match ty {
        "tinyint" => Arc::new(Int8Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i8)),
        )),
        "smallint" => Arc::new(Int16Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i16)),
        )),
        "int" => Arc::new(Int32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "date" => Arc::new(Date32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "time" => Arc::new(Time32MillisecondArray::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "boolean" => Arc::new(BooleanArray::from_iter(
            values.iter().map(|v| v.map(|v| v % 2 == 0)),
        )),
        "decimal" => Arc::new(
            Decimal128Array::from_iter(
                values
                    .iter()
                    .map(|v| v.map(|v| i128::from(v) * 100_000_000_000)),
            )
            .with_precision_and_scale(20, 2)
            .unwrap(),
        ),
        "string8" | "string256" => Arc::new(StringArray::from_iter(values.iter().map(|v| {
            v.map(|v| {
                let SortValue::Text(text) = sort_value(ty, v) else {
                    unreachable!()
                };
                text
            })
        }))),
        _ => Arc::new(Int64Array::from(values.to_vec())),
    }
}

fn bench_non_time_row_number(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_time_row_number");
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for descending_input in [false, true] {
                for ty in [
                    "bigint",
                    "int",
                    "tinyint",
                    "smallint",
                    "decimal",
                    "date",
                    "time",
                    "boolean",
                    "string8",
                    "string256",
                ] {
                    let values: Vec<_> = (0..rows)
                        .map(|i| {
                            (i % 17 != 0).then_some(if descending_input {
                                (rows - i) as i64
                            } else {
                                i as i64
                            })
                        })
                        .collect();
                    let oracle: Vec<_> = values
                        .iter()
                        .map(|v| v.map(|v| sort_value(ty, v)))
                        .collect();
                    let input = RecordBatch::try_from_iter(vec![
                        (
                            "k",
                            Arc::new(Int64Array::from_iter(
                                (0..rows).map(|i| (i % 13 != 0).then_some((i % keys) as i64)),
                            )) as ArrayRef,
                        ),
                        ("v", column(ty, &values)),
                    ])
                    .unwrap();
                    let partition = input
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let key = |row| (!partition.is_null(row)).then(|| partition.value(row));
                    let mut expected = rows;
                    for row in 0..rows {
                        for previous in 0..row {
                            let earlier = match (&oracle[row], &oracle[previous]) {
                                (Some(a), Some(b)) => a < b,
                                (Some(_), None) => true,
                                _ => false,
                            };
                            if key(row) == key(previous) && earlier {
                                expected += 2;
                            }
                        }
                    }
                    let mut check = AppendTopN::non_time_row_number(vec![0], 1);
                    assert_eq!(check.push(&input).num_rows(), expected);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(
                        format!("{ty}/rows={rows}/keys={keys}/reverse={descending_input}"),
                        |b| {
                            b.iter_batched(
                                || AppendTopN::non_time_row_number(vec![0], 1),
                                |mut ranker| black_box(ranker.push(black_box(&input))),
                                BatchSize::SmallInput,
                            );
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_non_time_row_number);
criterion_main!(benches);
