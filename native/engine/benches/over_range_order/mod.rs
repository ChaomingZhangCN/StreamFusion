use super::*;
mod strings;

fn fixture(groups: usize, keys: usize) -> (RecordBatch, RecordBatch, i64) {
    let times: Vec<i64> = if groups == 3 {
        vec![100, 200, 400]
    } else {
        (1..=24)
            .map(|key| ((key as i64) << 32) | (key as i64 ^ 7))
            .collect()
    };
    let interval = if groups == 3 { 1000 } else { 24i64 << 32 };
    let final_time = if groups == 3 {
        2000
    } else {
        times[23] + interval + 1
    };
    let two = if groups == 3 { 2 } else { 7 };
    let mut values = Vec::new();
    let mut partitions = Vec::new();
    let mut timestamps = Vec::new();
    for (group, &time) in times.iter().enumerate() {
        for key in 0..keys {
            partitions.push(key as i64);
            timestamps.push(time);
            values.push(match group {
                0 => Some(99_999_999_999_999_999_999_999_999_999_999_999_999i128),
                1 => Some(-1),
                index if index == two => Some(2),
                _ => None,
            });
        }
    }
    let batch = |partitions: Vec<i64>, timestamps: Vec<i64>, values: Vec<Option<i128>>| {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(partitions)) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(timestamps)) as ArrayRef),
            (
                "v",
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    (
        batch(partitions, timestamps, values),
        batch(
            (0..keys).map(|key| key as i64).collect(),
            vec![final_time; keys],
            vec![Some(7); keys],
        ),
        interval,
    )
}

fn execute(
    operator: &mut Over,
    seed: &RecordBatch,
    last: &RecordBatch,
    kind: i64,
    interval: i64,
    checkpoint: bool,
    pending: bool,
) -> RecordBatch {
    operator.push(seed.clone());
    let seed_time = seed
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    operator.flush(seed_time.value(seed_time.len() - 1));
    if pending {
        operator.push(last.clone());
    }
    if checkpoint {
        *operator = Over::restore_framed(5800, kind, 2, interval, false, &operator.snapshot());
    }
    if !pending {
        operator.push(last.clone());
    }
    let time = last
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    operator.flush(time)
}

pub(super) fn bench(c: &mut Criterion) {
    strings::bench(c);
    let mut group = c.benchmark_group("over_range_retraction_order");
    for groups in [3, 24] {
        for keys in [1, 64] {
            let (seed, last, interval) = fixture(groups, keys);
            group.throughput(Throughput::Elements(
                (seed.num_rows() + last.num_rows()) as u64,
            ));
            for kind in [0, 22, 4] {
                for checkpoint in [false, true] {
                    for pending in [false, true] {
                        let result = execute(
                            &mut Over::framed(5800, kind, 2, interval, false),
                            &seed,
                            &last,
                            kind,
                            interval,
                            checkpoint,
                            pending,
                        );
                        let values = result
                            .column(3)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap();
                        assert_eq!(result.num_rows(), keys);
                        if kind == 0 {
                            let expected = if groups == 3 && checkpoint { 7 } else { 8 };
                            assert!(values.iter().all(|value| value == Some(expected)));
                        } else {
                            assert_eq!(values.null_count(), keys);
                        }
                        group.bench_function(format!("groups{groups}_keys{keys}_kind{kind}_checkpoint{checkpoint}_pending{pending}"), |b| {
                            b.iter_batched_ref(|| Over::framed(5800, kind, 2, interval, false), |operator| {
                                black_box(execute(operator, &seed, &last, kind, interval, checkpoint, pending))
                            }, BatchSize::SmallInput)
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
