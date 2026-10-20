use super::*;
use arrow::array::{BooleanArray, Date32Array, Time32MillisecondArray};
use streamfusion_benchmark_support::{header, measure, report};

fn value(row: usize, keys: usize, code: i64) -> Option<i32> {
    if row % 7 == 0 || keys > 1 && row % keys == 0 {
        return None;
    }
    let position = row / keys;
    Some(match code {
        8 => (position / 2 % 17) as i32 - 8,
        9 => (position / 2 % 17) as i32 * 12345,
        10 => i32::from(position % 3 == 0),
        _ => unreachable!(),
    })
}

fn input(rows: usize, keys: usize, code: i64) -> RecordBatch {
    let values = (0..rows).map(|row| value(row, keys, code));
    let values: ArrayRef = match code {
        8 => Arc::new(Date32Array::from_iter(values)),
        9 => Arc::new(Time32MillisecondArray::from_iter(values)),
        10 => Arc::new(BooleanArray::from_iter(values.map(|v| v.map(|v| v != 0)))),
        _ => unreachable!(),
    };
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|r| (r % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|r| 1000 + r as i64),
            )) as ArrayRef,
        ),
        ("v", values),
    ])
    .unwrap()
}

fn check(
    batch: &RecordBatch,
    rows: usize,
    keys: usize,
    code: i64,
    kind: i64,
    frame: i64,
    proctime: bool,
) {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    let mut history: BTreeMap<usize, VecDeque<(usize, Option<i32>)>> = BTreeMap::new();
    let mut running: BTreeMap<usize, Option<i32>> = BTreeMap::new();
    let mut expected = Vec::new();
    let mut membership: BTreeMap<usize, BTreeSet<i32>> = BTreeMap::new();
    for row in 0..rows {
        let key = row % keys;
        let value = value(row, keys, code);
        let fold = |selected: Option<i32>, candidate: i32| {
            Some(match (kind, selected) {
                (1, Some(old)) => old.min(candidate),
                (2, Some(old)) => old.max(candidate),
                (5, Some(old)) => old,
                _ => candidate,
            })
        };
        let selected = if kind == 103 && (frame == 0 || frame == 3) {
            let values = membership.entry(key).or_default();
            if let Some(value) = value {
                values.insert(value);
            }
            Some(values.len() as i32)
        } else if frame == 0 || frame == 3 {
            let selected = running.entry(key).or_default();
            if let Some(value) = value {
                *selected = fold(*selected, value);
            }
            *selected
        } else {
            let records = history.entry(key).or_default();
            records.push_back((row, value));
            while if frame == 1 {
                records.len() > 4
            } else {
                records
                    .front()
                    .is_some_and(|(previous, _)| row - previous > 10)
            } {
                records.pop_front();
            }
            if kind == 103 {
                Some(
                    records
                        .iter()
                        .filter_map(|(_, value)| *value)
                        .collect::<BTreeSet<_>>()
                        .len() as i32,
                )
            } else {
                records
                    .iter()
                    .filter_map(|(_, value)| *value)
                    .fold(None, fold)
            }
        };
        expected.push(selected);
    }
    let output = execute(
        &mut Over::framed(code, kind, frame, if frame == 1 { 3 } else { 10 }, proctime),
        batch.clone(),
        proctime,
    );
    assert_eq!(output.num_rows(), rows);
    assert_eq!(
        output.column(3).data_type(),
        if kind == 103 {
            &DataType::Int64
        } else {
            batch.column(2).data_type()
        }
    );
    let times = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut seen = BTreeSet::new();
    for index in 0..output.num_rows() {
        let row = usize::try_from(times.value(index) - 1000).unwrap();
        assert!(row < rows && seen.insert(row));
        let column = output.column(3);
        let actual = if column.is_null(index) {
            None
        } else {
            Some(if kind == 103 {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index) as i32
            } else {
                match code {
                    8 => column
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(index),
                    9 => column
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .unwrap()
                        .value(index),
                    10 => i32::from(
                        column
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(index),
                    ),
                    _ => unreachable!(),
                }
            })
        };
        assert_eq!(
            actual, expected[row],
            "code={code}, kind={kind}, frame={frame}, row={row}"
        );
    }
}

pub(super) fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("over_primitive_values");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for (name, code) in [("date", 8), ("time", 9), ("boolean", 10)] {
                let batch = input(rows, keys, code);
                for kind in [1, 2, 5, 6, 103] {
                    for proctime in [false, true] {
                        for frame in 0..4 {
                            if proctime && frame == 2 {
                                continue;
                            }
                            let offset = if frame == 1 { 3 } else { 10 };
                            check(&batch, rows, keys, code, kind, frame, proctime);
                            validate(&batch, code, kind, frame, offset, proctime);
                            let create = || Over::framed(code, kind, frame, offset, proctime);
                            let mut probe = create();
                            let (output, allocations) =
                                measure(|| execute(&mut probe, batch.clone(), proctime));
                            let label = format!("over_primitive_values/{name}/rows{rows}/keys{keys}/kind{kind}/frame{frame}/proctime{proctime}");
                            report(&label, batch.columns(), output.columns(), allocations);
                            group.bench_function(label, |b| {
                                b.iter_batched_ref(
                                    create,
                                    |operator| {
                                        black_box(execute(operator, batch.clone(), proctime))
                                    },
                                    BatchSize::SmallInput,
                                )
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
