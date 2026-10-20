use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int64Array, Int8Array, RecordBatch,
};
use criterion::{black_box, BatchSize, Criterion, Throughput};
use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};
use streamfusion::bench::AppendTopN;
use streamfusion_benchmark_support::{measure, report};

fn changes(batch: &RecordBatch) -> Vec<(i8, i64, i64)> {
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ranks = batch
        .column(3)
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
        .map(|i| (kinds.value(i), ids.value(i), ranks.value(i)))
        .collect()
}

fn oracle(values: &[Option<f64>], keys: &[Option<i64>], nulls_first: bool) -> Vec<(i8, i64, i64)> {
    let compare = |a: Option<f64>, b: Option<f64>| match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    };
    let mut partitions: BTreeMap<Option<i64>, Vec<(Option<f64>, Vec<i64>)>> = BTreeMap::new();
    let mut out = vec![];
    for (id, &value) in values.iter().enumerate() {
        let groups = partitions.entry(keys[id]).or_default();
        let group = groups
            .iter()
            .position(|(key, _)| compare(*key, value) != Ordering::Less)
            .unwrap_or(groups.len());
        let equal = group < groups.len() && compare(groups[group].0, value) == Ordering::Equal;
        let pos = groups
            .iter()
            .take(group)
            .map(|(_, ids)| ids.len())
            .sum::<usize>()
            + if equal { groups[group].1.len() } else { 0 };
        let before: Vec<_> = groups
            .iter()
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect();
        out.push((0, id as i64, pos as i64 + 1));
        for (rank, &row) in before.iter().enumerate().skip(pos) {
            out.push((1, row, rank as i64 + 1));
            out.push((2, row, rank as i64 + 2));
        }
        if equal {
            groups[group].0 = value;
            groups[group].1.push(id as i64);
        } else {
            groups.insert(group, (value, vec![id as i64]));
        }
    }
    out
}

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_time_floating_rank");
    for rows in [16usize, 256] {
        for key_count in [1usize, 64] {
            for single in [false, true] {
                for nulls_first in [false, true] {
                    for nan_first in [false, true] {
                        let corpus = [
                            3.0,
                            -0.0,
                            0.0,
                            f64::NEG_INFINITY,
                            f64::NAN,
                            -2.0,
                            f64::from_bits(0xfff8000000001234),
                            f64::INFINITY,
                            1.0,
                            f64::from(f32::from_bits(1)),
                        ];
                        let values: Vec<_> = (0..rows)
                            .map(|i| {
                                if nan_first && i == 0 {
                                    Some(f64::NAN)
                                } else if i % 17 == 0 {
                                    None
                                } else {
                                    Some(corpus[i % corpus.len()])
                                }
                            })
                            .collect();
                        let keys: Vec<_> = (0..rows)
                            .map(|i| (i % 13 != 0).then_some((i % key_count) as i64))
                            .collect();
                        let value: ArrayRef = if single {
                            Arc::new(Float32Array::from_iter(
                                values.iter().map(|v| v.map(|v| v as f32)),
                            ))
                        } else {
                            Arc::new(Float64Array::from(values.clone()))
                        };
                        let input = RecordBatch::try_from_iter(vec![
                            ("k", Arc::new(Int64Array::from(keys.clone())) as ArrayRef),
                            ("v", value),
                            (
                                "id",
                                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64)))
                                    as ArrayRef,
                            ),
                        ])
                        .unwrap();
                        let expected = oracle(&values, &keys, nulls_first);
                        let orders = [(1usize, nulls_first)];
                        let create = || AppendTopN::non_time_row_number_orders(vec![0], &orders);
                        assert_eq!(changes(&create().push(&input)), expected);
                        let mut state = create();
                        let mut recovered = vec![];
                        for offset in (0..rows).step_by(7) {
                            if offset == 7 || offset == 14 {
                                state = AppendTopN::restore_non_time_row_number_orders(
                                    vec![0],
                                    &orders,
                                    &state.snapshot(128),
                                );
                            }
                            recovered.extend(changes(
                                &state.push(&input.slice(offset, (rows - offset).min(7))),
                            ));
                        }
                        assert_eq!(recovered, expected);
                        let mut probe = create();
                        let (output, allocations) = measure(|| probe.push(&input));
                        let label = format!("non_time_floating_rank/single={single}/rows={rows}/keys={key_count}/nulls_first={nulls_first}/nan_first={nan_first}");
                        report(&label, input.columns(), output.columns(), allocations);
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(label, |b| {
                            b.iter_batched(
                                create,
                                |mut ranker| black_box(ranker.push(&input)),
                                BatchSize::SmallInput,
                            )
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
