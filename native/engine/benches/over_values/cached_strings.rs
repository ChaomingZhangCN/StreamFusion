use super::*;
use streamfusion::bench::StringOverReduction;

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_string_cache_reduction");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for width in [1, 256] {
            let wide = "😀".repeat(width);
            let values = StringArray::from_iter((0..rows).map(|row| {
                (row % 5 != 0).then_some(match row % 4 {
                    0 => "",
                    1 => "😀",
                    2 => "\u{e000}",
                    _ => &wide,
                })
            }));
            for profile in ["binary", "cached", "mixed", "malformed"] {
                let malformed = "?".repeat(width);
                let values = if profile == "malformed" {
                    StringArray::from_iter(
                        (0..rows).map(|row| (row % 5 != 0).then_some(malformed.as_str())),
                    )
                } else {
                    values.clone()
                };
                let caches: Vec<_> = (0..rows)
                    .map(|row| {
                        (!values.is_null(row)
                            && (profile == "cached"
                                || profile == "malformed"
                                || (profile == "mixed" && row % 2 == 0)))
                            .then(|| {
                                if profile == "malformed" {
                                    vec![0xd800 + (row % 2) as u16; width]
                                } else {
                                    values.value(row).encode_utf16().collect::<Vec<_>>()
                                }
                            })
                    })
                    .collect();
                for kind in [1, 2, 5, 6] {
                    let mut whole = StringOverReduction::new(kind);
                    whole.update(&values, &caches);
                    let mut fragmented = StringOverReduction::new(kind);
                    for start in (0..rows).step_by(17) {
                        let len = 17.min(rows - start);
                        let slice = values.slice(start, len);
                        fragmented.update(&slice, &caches[start..start + len]);
                    }
                    assert_eq!(whole.value(), fragmented.value());
                    group.bench_function(
                        format!("kind{kind}_rows{rows}_width{width}_{profile}"),
                        |b| {
                            b.iter_batched_ref(
                                || StringOverReduction::new(kind),
                                |state| {
                                    state.update(&values, &caches);
                                    black_box(state.value())
                                },
                                BatchSize::SmallInput,
                            )
                        },
                    );
                }
            }
        }
    }
    group.finish();
}
