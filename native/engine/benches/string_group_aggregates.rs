use arrow::array::{ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::{sync::Arc, time::Duration};
use streamfusion::bench::{GroupBy, LocalGroupBy};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn batch(rows: usize, keys: usize, width: usize, retract: bool) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 7 != 0).then(|| format!("{:x}", i / keys % 16).repeat(width))
            }))) as ArrayRef,
        ),
        (
            "separator",
            Arc::new(StringArray::from(vec!["::"; rows])) as ArrayRef,
        ),
        (
            "$row_kind$",
            Arc::new(Int8Array::from(vec![if retract { 3 } else { 0 }; rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn string_groups(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("listagg");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for width in [8usize, 128] {
                let seed = batch(rows, keys, width, false);
                let retract = batch(rows / 2, keys, width, true);
                for updating in [false, true] {
                    let mut original = GroupBy::listagg(updating);
                    original.update(&seed);
                    let snapshot = original.snapshot();
                    let mut restored = GroupBy::restore_listagg(updating, &snapshot);
                    let next = if updating { &retract } else { &seed };
                    assert_eq!(original.update(next), restored.update(next));
                    let mut allocation_probe = GroupBy::listagg(updating);
                    if updating {
                        allocation_probe.update(&seed);
                    }
                    let (output, allocations) = measure(|| allocation_probe.update(next));
                    report(
                        &format!(
                            "listagg/rows={rows}/keys={keys}/width={width}/retract={updating}"
                        ),
                        next.columns(),
                        output.columns(),
                        allocations,
                    );
                    group.throughput(Throughput::Elements(
                        if updating { rows / 2 } else { rows } as u64
                    ));
                    group.bench_function(
                        format!("rows={rows}/keys={keys}/width={width}/retract={updating}"),
                        |b| {
                            b.iter_batched_ref(
                                || {
                                    let mut agg = GroupBy::listagg(updating);
                                    if updating {
                                        agg.update(&seed);
                                    }
                                    agg
                                },
                                |agg| black_box(agg.update(black_box(next))),
                                BatchSize::LargeInput,
                            );
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

fn distinct_string_groups(c: &mut Criterion) {
    let mut group = c.benchmark_group("listagg_distinct");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for width in [8usize, 128] {
                let seed = batch(rows, keys, width, false);
                let retractions = batch(rows / 2, keys, width, true);
                for updating in [false, true] {
                    let next = if updating { &retractions } else { &seed };
                    let mut original = GroupBy::distinct_listagg(updating);
                    let mut expected = std::collections::BTreeMap::<i64, Vec<String>>::new();
                    let mut multiplicities =
                        std::collections::BTreeMap::<(i64, String), i64>::new();
                    let mut records = std::collections::BTreeMap::<i64, i64>::new();
                    let mut actual = std::collections::BTreeMap::<i64, Option<String>>::new();
                    for input in [&seed, next] {
                        let input_keys = input
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let values = input
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let retract = input
                            .column(3)
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap();
                        for row in 0..input.num_rows() {
                            let key = input_keys.value(row);
                            let retract = retract.value(row) == 3;
                            *records.entry(key).or_default() += if retract { -1 } else { 1 };
                            let entries = expected.entry(key).or_default();
                            if let Some(value) = values.iter().nth(row).unwrap() {
                                let count =
                                    multiplicities.entry((key, value.to_owned())).or_default();
                                if retract {
                                    *count -= 1;
                                    if *count == 0 {
                                        entries.retain(|old| old != value);
                                    }
                                } else {
                                    *count += 1;
                                    if *count == 1 {
                                        entries.push(value.to_owned());
                                    }
                                }
                            }
                        }
                        let output = original.update(input);
                        let output_keys = output
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let values = output
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let kinds = output
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap();
                        for row in 0..output.num_rows() {
                            if kinds.value(row) == 3 {
                                actual.remove(&output_keys.value(row));
                            } else if kinds.value(row) != 1 {
                                actual.insert(
                                    output_keys.value(row),
                                    values.iter().nth(row).unwrap().map(str::to_owned),
                                );
                            }
                        }
                    }
                    let expected: std::collections::BTreeMap<_, _> = expected
                        .into_iter()
                        .filter(|(key, _)| records[key] > 0)
                        .map(|(key, values)| (key, (!values.is_empty()).then(|| values.join("::"))))
                        .collect();
                    assert_eq!(actual, expected);
                    let mut restored =
                        GroupBy::restore_distinct_listagg(updating, &original.snapshot());
                    assert_eq!(original.update(&seed), restored.update(&seed));
                    let mut allocation_probe = GroupBy::distinct_listagg(updating);
                    if updating {
                        allocation_probe.update(&seed);
                    }
                    let (output, allocations) = measure(|| allocation_probe.update(next));
                    let label = format!(
                        "listagg_distinct/rows={rows}/keys={keys}/width={width}/retract={updating}"
                    );
                    report(&label, next.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(next.num_rows() as u64));
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            || {
                                let mut agg = GroupBy::distinct_listagg(updating);
                                if updating {
                                    agg.update(&seed);
                                }
                                agg
                            },
                            |agg| black_box(agg.update(black_box(next))),
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}

fn two_phase_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("listagg_local_global");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for width in [8usize, 128] {
                let input = batch(rows, keys, width, false);
                for separator_column in [-1, 2] {
                    let separator = if separator_column < 0 { "," } else { "::" };
                    let mut expected = std::collections::BTreeMap::<i64, (Vec<String>, i64)>::new();
                    let values = input
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for row in 0..rows {
                        let entry = expected.entry((row % keys) as i64).or_default();
                        entry.1 += 1;
                        if let Some(value) = values.iter().nth(row).unwrap() {
                            entry.0.push(value.to_owned());
                        }
                    }
                    let mut local = LocalGroupBy::listagg(separator_column);
                    let mut global = GroupBy::listagg_merge();
                    for offset in (0..rows).step_by(17) {
                        local.update(&input.slice(offset, (rows - offset).min(17)));
                        global.update(&local.flush());
                    }
                    let output = global.flush();
                    assert_eq!(output.num_rows(), expected.len());
                    let output_keys = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let output_values = output
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let counts = output
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for row in 0..output.num_rows() {
                        let (values, count) = &expected[&output_keys.value(row)];
                        assert_eq!(
                            output_values.iter().nth(row).unwrap(),
                            (!values.is_empty())
                                .then(|| values.join(separator))
                                .as_deref()
                        );
                        assert_eq!(counts.value(row), *count);
                    }
                    let mut restored = GroupBy::restore_listagg_merge(&global.snapshot());
                    local.update(&input);
                    let partials = local.flush();
                    global.update(&partials);
                    restored.update(&partials);
                    assert_eq!(global.flush(), restored.flush());
                    let mut local = LocalGroupBy::listagg(separator_column);
                    let mut global = GroupBy::listagg_merge();
                    let (output, allocations) = measure(|| {
                        local.update(&input);
                        global.update(&local.flush());
                        global.flush()
                    });
                    let label = format!("listagg_local_global/rows={rows}/keys={keys}/width={width}/separator_column={separator_column}");
                    report(&label, input.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            || {
                                (
                                    LocalGroupBy::listagg(separator_column),
                                    GroupBy::listagg_merge(),
                                )
                            },
                            |(local, global)| {
                                local.update(black_box(&input));
                                global.update(&local.flush());
                                black_box(global.flush())
                            },
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}

fn json_groups(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_aggregate");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for width in [8usize, 128] {
                let make = |retract: bool| {
                    RecordBatch::try_from_iter(vec![
                        (
                            "k",
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| (i % keys) as i64),
                            )) as ArrayRef,
                        ),
                        (
                            "v",
                            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                                (i % 7 != 0).then(|| format!("\"{}\"", "v".repeat(width)))
                            }))) as ArrayRef,
                        ),
                        (
                            "field",
                            Arc::new(StringArray::from_iter_values(
                                (0..rows).map(|i| format!("field-{i}")),
                            )) as ArrayRef,
                        ),
                        (
                            "$row_kind$",
                            Arc::new(Int8Array::from(vec![if retract { 3 } else { 0 }; rows]))
                                as ArrayRef,
                        ),
                    ])
                    .unwrap()
                };
                let seed = make(false);
                let retractions = make(true);
                for kind in [26, 29] {
                    for retract in [false, true] {
                        let next = if retract { &retractions } else { &seed };
                        let mut original = GroupBy::json_aggregate(kind);
                        original.update(&seed);
                        let mut restored =
                            GroupBy::restore_json_aggregate(kind, &original.snapshot());
                        assert_eq!(original.update(&retractions), restored.update(&retractions));
                        let mut allocation_probe = GroupBy::json_aggregate(kind);
                        if retract {
                            allocation_probe.update(&seed);
                        }
                        let (output, allocations) = measure(|| allocation_probe.update(next));
                        report(
                            &format!("json_aggregate/rows={rows}/keys={keys}/width={width}/kind={kind}/retract={retract}"),
                            next.columns(), output.columns(), allocations,
                        );
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(format!("rows={rows}/keys={keys}/width={width}/kind={kind}/retract={retract}"), |b| {
                            b.iter_batched_ref(|| {
                                let mut agg = GroupBy::json_aggregate(kind);
                                if retract { agg.update(&seed); }
                                agg
                            }, |agg| black_box(agg.update(black_box(next))), BatchSize::LargeInput);
                        });
                    }
                }
            }
        }
    }
    group.finish();
}

fn string_checkpoints(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_aggregate_checkpoint");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    for kind in [24, 25, 26, 29, 32, 33] {
        for (rows, keys, width) in [(16, 1, 8), (1024, 64, 128)] {
            let make_input = |prefix: &str| {
                let input = batch(rows, keys, width, false);
                if kind < 26 || kind >= 32 {
                    return input;
                }
                let mut columns = input.columns().to_vec();
                let values = columns[1].as_any().downcast_ref::<StringArray>().unwrap();
                columns[1] = Arc::new(StringArray::from_iter(
                    values.iter().map(|v| v.map(|v| format!("\"{v}\""))),
                ));
                columns[2] = Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("{prefix}{i}")),
                ));
                RecordBatch::try_new(input.schema(), columns).unwrap()
            };
            let input = make_input("seed");
            let next = make_input("next");
            let create = || {
                if kind < 26 || kind >= 32 {
                    if kind >= 32 {
                        GroupBy::distinct_listagg(kind == 33)
                    } else {
                        GroupBy::listagg(kind == 25)
                    }
                } else {
                    GroupBy::json_aggregate(kind)
                }
            };
            let restore = |snapshot: &[Vec<u8>]| {
                if kind < 26 || kind >= 32 {
                    if kind >= 32 {
                        GroupBy::restore_distinct_listagg(kind == 33, snapshot)
                    } else {
                        GroupBy::restore_listagg(kind == 25, snapshot)
                    }
                } else {
                    GroupBy::restore_json_aggregate(kind, snapshot)
                }
            };
            let mut original = create();
            original.update(&input);
            let snapshot = original.snapshot();
            let mut expected = restore(&snapshot);
            let mut restored = restore(&snapshot);
            assert_eq!(expected.update(&next), restored.update(&next));
            let ((snapshot, _), allocations) = measure(|| {
                let snapshot = original.snapshot();
                let restored = restore(&snapshot);
                (snapshot, restored)
            });
            assert!(snapshot.iter().map(Vec::len).sum::<usize>() > 0);
            let label = format!(
                "string_aggregate_checkpoint/rows={rows}/keys={keys}/width={width}/kind={kind}"
            );
            // Checkpoints return byte vectors and state, not Arrow output buffers.
            report(&label, &[], &[], allocations);
            group.bench_function(label, |b| {
                b.iter_batched_ref(
                    || (),
                    |_| {
                        let snapshot = original.snapshot();
                        let restored = restore(&snapshot);
                        black_box((snapshot, restored))
                    },
                    BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    string_groups,
    distinct_string_groups,
    two_phase_strings,
    json_groups,
    string_checkpoints
);
criterion_main!(benches);
