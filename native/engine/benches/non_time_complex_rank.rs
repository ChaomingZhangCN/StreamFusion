use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, Int8Array, ListBuilder, RecordBatch,
    StringArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::AppendTopN;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    Array(Vec<Option<i64>>),
    Row(Option<i64>, Option<String>),
    Nested(Vec<Option<Vec<Option<i64>>>>),
    RowArray(Option<Vec<Option<i64>>>, Option<i64>),
}

fn values(kind: &str, rows: usize) -> Vec<Option<Value>> {
    (0..rows)
        .map(|i| {
            if i % 7 == 0 {
                return None;
            }
            let n = (i % 13) as i64 - 6;
            let array = if i % 11 == 0 {
                vec![]
            } else {
                vec![(i % 5 != 0).then_some(n), None]
            };
            Some(match kind {
                "array" => Value::Array(array),
                "row" => Value::Row(
                    (i % 5 != 0).then_some(n),
                    (i % 3 != 0).then(|| format!("{n}é🚀")),
                ),
                "nested" => Value::Nested(vec![(i % 5 != 0).then_some(array), None]),
                "row_array" => {
                    Value::RowArray((i % 5 != 0).then_some(array), (i % 3 != 0).then_some(n))
                }
                _ => unreachable!(),
            })
        })
        .collect()
}

fn arrays(values: &[Option<Vec<Option<i64>>>]) -> ArrayRef {
    let mut b = ListBuilder::new(Int64Builder::new()).with_field(Arc::new(Field::new(
        "$data$",
        DataType::Int64,
        true,
    )));
    for value in values {
        if let Some(value) = value {
            for v in value {
                b.values().append_option(*v);
            }
        }
        b.append(value.is_some());
    }
    Arc::new(b.finish())
}

fn column(kind: &str, values: &[Option<Value>]) -> ArrayRef {
    let valid = NullBuffer::from(values.iter().map(Option::is_some).collect::<Vec<_>>());
    match kind {
        "array" => arrays(
            &values
                .iter()
                .map(|v| match v {
                    Some(Value::Array(a)) => Some(a.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ),
        "row" => {
            let a = Int64Array::from_iter(values.iter().map(|v| match v {
                Some(Value::Row(a, _)) => *a,
                _ => None,
            }));
            let b = StringArray::from_iter(values.iter().map(|v| match v {
                Some(Value::Row(_, b)) => b.as_deref(),
                _ => None,
            }));
            Arc::new(StructArray::new(
                vec![
                    Arc::new(Field::new("a", DataType::Int64, true)),
                    Arc::new(Field::new("b", DataType::Utf8, true)),
                ]
                .into(),
                vec![Arc::new(a), Arc::new(b)],
                Some(valid),
            ))
        }
        "nested" => {
            let child = Arc::new(Field::new("$data$", DataType::Int64, true));
            let mut b =
                ListBuilder::new(ListBuilder::new(Int64Builder::new()).with_field(child.clone()))
                    .with_field(Arc::new(Field::new("$data$", DataType::List(child), true)));
            for v in values {
                if let Some(Value::Nested(a)) = v {
                    for child in a {
                        if let Some(child) = child {
                            for value in child {
                                b.values().values().append_option(*value);
                            }
                        }
                        b.values().append(child.is_some());
                    }
                }
                b.append(v.is_some());
            }
            Arc::new(b.finish())
        }
        "row_array" => {
            let a = arrays(
                &values
                    .iter()
                    .map(|v| match v {
                        Some(Value::RowArray(a, _)) => a.clone(),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            );
            let b = Int64Array::from_iter(values.iter().map(|v| match v {
                Some(Value::RowArray(_, b)) => *b,
                _ => None,
            }));
            Arc::new(StructArray::new(
                vec![
                    Arc::new(Field::new("a", a.data_type().clone(), true)),
                    Arc::new(Field::new("b", DataType::Int64, true)),
                ]
                .into(),
                vec![a, Arc::new(b)],
                Some(valid),
            ))
        }
        _ => unreachable!(),
    }
}

fn changes(batch: &RecordBatch, rank: usize) -> Vec<(i64, i8, i64)> {
    let id = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ranks = batch
        .column(rank)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = batch
        .column_by_name("$row_kind$")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (id.value(row), kinds.value(row), ranks.value(row)))
        .collect()
}

fn oracle(values: &[Option<Value>], keys: usize, nulls_last: bool) -> Vec<(i64, i8, i64)> {
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut result = vec![];
    for (id, value) in values.iter().enumerate() {
        let group = groups.entry(id % keys).or_default();
        let compare = |other: &Option<Value>| match (value, other) {
            (None, Some(_)) if nulls_last => std::cmp::Ordering::Greater,
            (Some(_), None) if nulls_last => std::cmp::Ordering::Less,
            _ => value.cmp(other),
        };
        let at = group
            .iter()
            .position(|&other| compare(&values[other]).is_lt())
            .unwrap_or(group.len());
        result.push((id as i64, 0, at as i64 + 1));
        for (position, &other) in group.iter().enumerate().skip(at) {
            result.push((other as i64, 1, position as i64 + 1));
            result.push((other as i64, 2, position as i64 + 2));
        }
        group.insert(at, id);
    }
    result
}

fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("non_time_complex_rank");
    for rows in [16usize, 256] {
        for keys in [1usize, 64] {
            for kind in ["array", "row", "nested", "row_array"] {
                let values = values(kind, rows);
                for nulls_last in [false, true] {
                    let mut columns = vec![
                        (
                            "k",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| (i % keys) as i64),
                            )) as ArrayRef,
                        ),
                        ("v", column(kind, &values)),
                        (
                            "id",
                            Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64)))
                                as ArrayRef,
                        ),
                    ];
                    if nulls_last {
                        columns.push((
                            "parent_null",
                            Arc::new(BooleanArray::from(
                                values.iter().map(Option::is_none).collect::<Vec<_>>(),
                            )),
                        ));
                    }
                    let input = RecordBatch::try_from_iter(columns).unwrap();
                    let orders = if nulls_last {
                        vec![(3, true), (1, true)]
                    } else {
                        vec![(1, true)]
                    };
                    let create = || AppendTopN::non_time_row_number_orders(vec![0], &orders);
                    let expected = oracle(&values, keys, nulls_last);
                    assert_eq!(
                        changes(&create().push(&input), input.num_columns()),
                        expected
                    );
                    let mut state = create();
                    let mut actual = vec![];
                    for offset in (0..rows).step_by(7) {
                        if offset == 7 {
                            state = AppendTopN::restore_non_time_row_number_orders(
                                vec![0],
                                &orders,
                                &state.snapshot(128),
                            );
                        }
                        actual.extend(changes(
                            &state.push(&input.slice(offset, (rows - offset).min(7))),
                            input.num_columns(),
                        ));
                    }
                    assert_eq!(actual, expected);
                    let mut probe = create();
                    let (output, allocations) = measure(|| probe.push(&input));
                    let label = format!("non_time_complex_rank/{kind}/rows={rows}/keys={keys}/nulls_last={nulls_last}");
                    report(&label, input.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            create,
                            |state| black_box(state.push(&input)),
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
