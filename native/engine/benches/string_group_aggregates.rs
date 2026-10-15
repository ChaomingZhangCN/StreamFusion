use arrow::array::{ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::{sync::Arc, time::Duration};
use streamfusion::bench::GroupBy;

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

criterion_group!(benches, string_groups);
criterion_main!(benches);
