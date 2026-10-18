use super::*;
use streamfusion_bridge::{string_cache_array, string_cache_field};

fn fixture(rows: usize, keys: usize, width: usize, malformed: bool) -> RecordBatch {
    let text = if malformed {
        "?".repeat(width)
    } else {
        "😀".repeat(width)
    };
    let values = StringArray::from_iter(
        (0..rows)
            .map(|row| (row % 5 != 0 && (keys == 1 || row % keys != 0)).then_some(text.as_str())),
    );
    let units: Vec<Option<Vec<u16>>> = (0..rows)
        .map(|row| {
            (!values.is_null(row)).then(|| {
                if malformed {
                    vec![0xd800 + (row % 2) as u16; width]
                } else {
                    text.encode_utf16().collect()
                }
            })
        })
        .collect();
    let cache = string_cache_array(units.iter().map(|value| value.as_deref()));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int64, false),
            arrow::datatypes::Field::new("rt", DataType::Int64, false),
            arrow::datatypes::Field::new("s", DataType::Utf8, true),
            string_cache_field(2, cache.data_type().clone()),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % keys) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 1000 + row as i64),
            )),
            Arc::new(values),
            cache,
        ],
    )
    .unwrap()
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_string_view_checkpoints");
    for rows in [64, 4096] {
        for keys in [1, 64] {
            for width in [1, 256] {
                for malformed in [false, true] {
                    let batch = fixture(rows, keys, width, malformed);
                    for kind in [1, 2, 5, 6] {
                        for (frame, proctime) in [(1, false), (1, true), (2, false)] {
                            let offset = if frame == 1 { 3 } else { 100 };
                            let mut seeded = Over::framed(3, kind, frame, offset, proctime);
                            black_box(execute(&mut seeded, batch.clone(), proctime));
                            let saved = seeded.snapshot();
                            let mut once =
                                Over::restore_framed(3, kind, frame, offset, proctime, &saved);
                            let normalized = once.snapshot();
                            let mut twice =
                                Over::restore_framed(3, kind, frame, offset, proctime, &normalized);
                            let mut continued = batch.slice(rows - 1, 1);
                            let mut columns = continued.columns().to_vec();
                            columns[1] =
                                Arc::new(Int64Array::from(vec![1000 + rows as i64 + 1000]));
                            continued = RecordBatch::try_new(continued.schema(), columns).unwrap();
                            assert_eq!(
                                execute(&mut once, continued.clone(), proctime),
                                execute(&mut twice, continued, proctime),
                                "cold view restore is idempotent"
                            );
                            let id = format!("kind{kind}_rows{rows}_keys{keys}_width{width}_malformed{malformed}_frame{frame}_proctime{proctime}");
                            group.bench_function(format!("snapshot_{id}"), |b| {
                                b.iter_batched_ref(
                                    || {
                                        Over::restore_framed(
                                            3, kind, frame, offset, proctime, &saved,
                                        )
                                    },
                                    |operator| black_box(operator.snapshot()),
                                    BatchSize::SmallInput,
                                )
                            });
                            group.bench_function(format!("restore_{id}"), |b| {
                                b.iter(|| {
                                    black_box(Over::restore_framed(
                                        3, kind, frame, offset, proctime, &saved,
                                    ))
                                })
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
