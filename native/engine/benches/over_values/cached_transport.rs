use super::*;
use streamfusion_bridge::{string_cache_array, string_cache_field};

pub(super) fn bench(c: &mut Criterion) {
    bench_frames(c, false, false);
    bench_frames(c, true, false);
    bench_frames(c, true, true);
}

fn bench_frames(c: &mut Criterion, bounded: bool, extrema: bool) {
    let mut group = c.benchmark_group(if extrema {
        "over_bounded_string_cache_extrema"
    } else if bounded {
        "over_bounded_string_cache_first_last"
    } else {
        "over_running_string_cache_transport"
    });
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for width in [1, 256] {
                for profile in ["binary", "cached", "mixed", "malformed"] {
                    let wide = "😀".repeat(width);
                    let malformed = "?".repeat(width);
                    let values = StringArray::from_iter((0..rows).map(|row| {
                        (row % 5 != 0 && (keys == 1 || row % keys != 0)).then_some(
                            if profile == "malformed" {
                                malformed.as_str()
                            } else {
                                match row % 4 {
                                    0 => "",
                                    1 => "😀",
                                    2 => "\u{e000}",
                                    _ => &wide,
                                }
                            },
                        )
                    }));
                    let units: Vec<Option<Vec<u16>>> = (0..rows)
                        .map(|row| {
                            (!values.is_null(row)
                                && (profile == "cached"
                                    || profile == "malformed"
                                    || (profile == "mixed" && row % 2 == 0)))
                                .then(|| {
                                    if profile == "malformed" {
                                        vec![0xd800 + (row % 2) as u16; width]
                                    } else {
                                        values.value(row).encode_utf16().collect()
                                    }
                                })
                        })
                        .collect();
                    let cache = string_cache_array(units.iter().map(|value| value.as_deref()));
                    let batch = RecordBatch::try_new(
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
                    .unwrap();
                    let kinds: &[i64] = if extrema {
                        &[1, 2]
                    } else if bounded {
                        &[5, 6]
                    } else {
                        &[1, 2, 5, 6]
                    };
                    for &kind in kinds {
                        for proctime in [false, true] {
                            for &frame in if bounded { &[1i64, 2][..] } else { &[0i64][..] } {
                                if proctime && frame == 2 {
                                    continue;
                                }
                                let offset = if frame == 1 { 3 } else { 100 };
                                let whole = execute(
                                    &mut Over::framed(3, kind, frame, offset, proctime),
                                    batch.clone(),
                                    proctime,
                                );
                                let mut fragmented = Over::framed(3, kind, frame, offset, proctime);
                                let mut results = Vec::new();
                                for start in (0..rows).step_by(17) {
                                    let part = batch.slice(start, 17.min(rows - start));
                                    results.push(execute(&mut fragmented, part, proctime));
                                }
                                assert_eq!(
                                    whole,
                                    concat_batches(&whole.schema(), &results).unwrap()
                                );
                                assert_eq!(whole.num_rows(), rows);
                                group.bench_function(format!("kind{kind}_rows{rows}_keys{keys}_width{width}_{profile}_frame{frame}_proctime{proctime}"), |b| {
                                b.iter_batched_ref(|| Over::framed(3, kind, frame, offset, proctime),
                                    |operator| {
                                        if proctime { black_box(operator.push_proctime(batch.clone())) }
                                        else { operator.push(batch.clone()); black_box(operator.flush(i64::MAX)) }
                                    }, BatchSize::SmallInput)
                            });
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
