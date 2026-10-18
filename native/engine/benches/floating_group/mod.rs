use super::*;
use arrow::array::{Float32Array, Float64Array};
use streamfusion::bench::GroupBy;

fn fixture(rows: usize, keys: usize, code: i64, nullable: bool, unique: bool) -> RecordBatch {
    let values: Vec<_> = (0..rows)
        .map(|i| {
            if nullable && i % 7 == 0 {
                None
            } else {
                Some(match i % 31 {
                    0 => -f64::NAN,
                    1 => -0.0,
                    2 => 0.0,
                    _ => (i % 1024) as f64 * 0.25,
                })
            }
        })
        .collect();
    let value: ArrayRef = if code == 6 {
        Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Float64Array::from(values))
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", value.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| if unique { i } else { i % keys } as i64),
            )),
            value,
        ],
    )
    .unwrap()
}

pub fn bench_floating_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("floating_group");
    for rows in [256, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for (name, code) in [("float", 6), ("double", 1)] {
                for nullable in [false, true] {
                    for (shape, kinds) in [
                        ("append_reductions", vec![0, 10, 11]),
                        ("distinct_sum", vec![9]),
                        ("distinct_avg", vec![17]),
                        ("append_ordered", vec![12, 13]),
                        ("single_value", vec![14]),
                        ("retract", vec![0, 1, 2, 15, 16]),
                    ] {
                        let batch = fixture(rows, keys, code, nullable, shape == "single_value");
                        let mut fields = batch.schema().fields().to_vec();
                        fields.push(Arc::new(Field::new("$row_kind$", DataType::Int8, false)));
                        let mut columns = batch.columns().to_vec();
                        columns.push(Arc::new(Int8Array::from(vec![3; rows])));
                        let deletion =
                            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
                        let create = || {
                            let mut agg = GroupBy::new(
                                kinds.clone(),
                                vec![code; kinds.len()],
                                vec![1; kinds.len()],
                                vec![0],
                            );
                            if shape == "retract" {
                                black_box(agg.update(&batch));
                            }
                            agg
                        };
                        let input = if shape == "retract" {
                            &deletion
                        } else {
                            &batch
                        };
                        let output = create().update(input);
                        assert!(output.num_rows() > 0);
                        for column in 1..=kinds.len() {
                            assert_eq!(
                                output.column(column).data_type(),
                                batch.column(1).data_type()
                            );
                        }
                        let id = format!("{shape}/{name}/keys{keys}/nulls{nullable}/rows{rows}");
                        group.bench_function(id, |b| {
                            b.iter_batched_ref(
                                create,
                                |agg| black_box(agg.update(black_box(input))),
                                BatchSize::SmallInput,
                            );
                        });
                    }
                    let batch = fixture(rows, keys, code, nullable, false);
                    let id =
                        format!("local_reductions/{name}/keys{keys}/nulls{nullable}/rows{rows}");
                    group.bench_function(id, |b| {
                        b.iter_batched_ref(
                            || LocalGroupBy::new(vec![0, 1, 2], vec![code; 3], vec![1; 3], vec![0]),
                            |agg| {
                                agg.update(black_box(&batch));
                                black_box(agg.flush());
                            },
                            BatchSize::SmallInput,
                        );
                    });
                    let batch = fixture(rows, keys, code, nullable, true);
                    let id = format!("local_single/{name}/keys{keys}/nulls{nullable}/rows{rows}");
                    let create =
                        || LocalGroupBy::new(vec![14, 21], vec![code; 2], vec![1; 2], vec![0]);
                    let mut checked = create();
                    checked.update(&batch);
                    let partial = checked.flush();
                    assert_eq!(partial.num_rows(), rows);
                    assert_eq!(partial.column(1).data_type(), batch.column(1).data_type());
                    assert_eq!(partial.column(2).data_type(), &DataType::Int32);
                    group.bench_function(id, |b| {
                        b.iter_batched_ref(
                            create,
                            |agg| {
                                agg.update(black_box(&batch));
                                black_box(agg.flush());
                            },
                            BatchSize::SmallInput,
                        );
                    });
                }
            }
        }
    }
    group.finish();
}
