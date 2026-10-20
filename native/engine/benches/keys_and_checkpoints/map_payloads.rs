use super::*;
use arrow::array::Int32Array;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use streamfusion::bench::parameterized_scalar;

fn map(rows: usize, width: usize, bag: bool) -> ArrayRef {
    let mut offsets = vec![0i32];
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for row in 0..rows {
        if row % 11 != 0 && row % 7 != 0 {
            for i in 0..width {
                keys.push((i != 0).then_some(i as i64));
                values.push(if bag {
                    Some(1 + (i % 5) as i32)
                } else {
                    (i % 13 != 0).then_some((row + i) as i32)
                });
            }
        }
        offsets.push(keys.len() as i32);
    }
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Int64, true)),
            Arc::new(Int64Array::from(keys)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Int32, true)),
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ),
    ]);
    Arc::new(ListArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        Some(NullBuffer::from_iter((0..rows).map(|i| i % 11 != 0))),
    ))
}
fn payload(shape: &str, rows: usize, width: usize) -> ArrayRef {
    match shape {
        "map" | "multiset" => map(rows, width, shape == "multiset"),
        "array_map" => {
            let values = map(rows * 2, width, false);
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::new(
                    (0..=rows)
                        .map(|i| (i * 2) as i32)
                        .collect::<Vec<_>>()
                        .into(),
                ),
                values,
                Some(NullBuffer::from_iter((0..rows).map(|i| i % 17 != 0))),
            ))
        }
        "row_map" => {
            let maps = map(rows, width, false);
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("m", maps.data_type().clone(), true)),
                    maps,
                ),
                (
                    Arc::new(Field::new("n", DataType::Int64, true)),
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
            ]))
        }
        "map_map" => {
            let values = map(rows, width, false);
            let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
            let entries = StructArray::from(vec![
                (Arc::new(Field::new("key", DataType::Int64, true)), keys),
                (
                    Arc::new(Field::new("value", values.data_type().clone(), true)),
                    values,
                ),
            ]);
            Arc::new(ListArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new((0..=rows).map(|i| i as i32).collect::<Vec<_>>().into()),
                Arc::new(entries),
                None,
            ))
        }
        _ => unreachable!(),
    }
}
fn rank() -> AppendTopN {
    AppendTopN::new(vec![0], vec![(1, false)], 3, true, false).with_budget(1 << 30)
}
pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("retained_map_payloads");
    for rows in [64, 4096] {
        for keys in [1, 64] {
            for width in [4, 64] {
                for shape in ["map", "multiset", "array_map", "row_map", "map_map"] {
                    let p = payload(shape, rows, width);
                    let batch = RecordBatch::try_from_iter(vec![
                        (
                            "k",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| (i % keys) as i64),
                            )) as ArrayRef,
                        ),
                        (
                            "id",
                            Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                        ),
                        ("p", p.clone()),
                    ])
                    .unwrap();
                    let label = format!("{shape}/rows{rows}_keys{keys}_width{width}");
                    let mut op = rank();
                    let output = op.push(&batch);
                    assert!(output.num_rows() >= rows);
                    let ids = output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for row in 0..output.num_rows() {
                        assert_eq!(
                            output.column(2).slice(row, 1).as_ref(),
                            p.slice(ids.value(row) as usize, 1).as_ref()
                        );
                    }
                    let mut measured = rank();
                    let (output, alloc) = measure(|| measured.push(&batch));
                    report(
                        &format!("retained_map_payloads/churn/{label}"),
                        batch.columns(),
                        output.columns(),
                        alloc,
                    );
                    group.bench_function(format!("churn/{label}"), |b| {
                        b.iter_batched_ref(
                            rank,
                            |op| std::hint::black_box(op.push(&batch)),
                            BatchSize::SmallInput,
                        )
                    });
                    let seeded = || {
                        let mut op = rank();
                        for _ in 0..3 {
                            op.push(&batch);
                        }
                        op
                    };
                    assert_eq!(seeded().push(&batch).num_rows(), 0);
                    let mut check = seeded();
                    let (output, alloc) = measure(|| check.push(&batch));
                    report(
                        &format!("retained_map_payloads/reject/{label}"),
                        batch.columns(),
                        output.columns(),
                        alloc,
                    );
                    group.bench_function(format!("reject/{label}"), |b| {
                        b.iter_batched_ref(
                            seeded,
                            |op| std::hint::black_box(op.push(&batch)),
                            BatchSize::SmallInput,
                        )
                    });
                    let state = seeded();
                    assert!(!state.snapshot(128).is_empty());
                    group.bench_function(format!("snapshot/{label}"), |b| {
                        b.iter(|| std::hint::black_box(state.snapshot(128)))
                    });
                    if shape == "map" {
                        for dynamic in [false, true] {
                            let name = if dynamic {
                                "map_lookup_dynamic"
                            } else {
                                "map_lookup_literal"
                            };
                            let udf = parameterized_scalar(name, p.data_type().clone());
                            let lookup: ArrayRef = Arc::new(Int64Array::from_iter(
                                (0..rows).map(|i| (i % 17 != 0).then_some(1)),
                            ));
                            let mut args = vec![ColumnarValue::Array(p.clone())];
                            if dynamic {
                                args.push(ColumnarValue::Array(lookup));
                            }
                            let fields = args
                                .iter()
                                .map(|a| Arc::new(Field::new("arg", a.data_type(), true)))
                                .collect::<Vec<_>>();
                            let field = Arc::new(Field::new("result", DataType::Int32, true));
                            let options = Arc::new(ConfigOptions::default());
                            let run = || {
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: fields.clone(),
                                    number_rows: rows,
                                    return_field: field.clone(),
                                    config_options: options.clone(),
                                })
                                .unwrap()
                                .into_array(rows)
                                .unwrap()
                            };
                            let actual = run();
                            let expected = Int32Array::from_iter((0..rows).map(|i| {
                                (i % 11 != 0 && i % 7 != 0 && (!dynamic || i % 17 != 0))
                                    .then_some((i + 1) as i32)
                            }));
                            assert_eq!(actual.as_ref(), &expected);
                            let (actual, alloc) = measure(run);
                            report(
                                &format!("retained_map_payloads/{name}/{label}"),
                                std::slice::from_ref(&p),
                                &[actual],
                                alloc,
                            );
                            group.bench_function(format!("{name}/{label}"), |b| {
                                b.iter(|| std::hint::black_box(run()))
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
