use super::*;
use streamfusion_bridge::{
    string_cache_array, string_cache_column, string_cache_field, JavaStringCacheColumn,
};

fn fixture(keys: usize, peers: usize, width: usize) -> (RecordBatch, RecordBatch) {
    let values = [
        "😀".repeat(width),
        "\u{e000}".repeat(width),
        "a".repeat(width),
        "😀".repeat(width),
        "😀".repeat(width),
        "\u{e100}".repeat(width),
    ];
    let mut partitions = Vec::new();
    let mut times = Vec::new();
    let mut texts = Vec::new();
    let mut units = Vec::new();
    for (group, time) in [100, 200, 400, 1050, 1060, 1070].into_iter().enumerate() {
        for key in 0..keys {
            for _ in 0..peers {
                partitions.push(key as i64);
                times.push(time);
                texts.push(Some(values[group].as_str()));
                units.push((group != 5).then(|| values[group].encode_utf16().collect::<Vec<_>>()));
            }
        }
    }
    let batch = |partitions: Vec<i64>,
                 times: Vec<i64>,
                 texts: Vec<Option<&str>>,
                 units: Vec<Option<Vec<u16>>>| {
        let cache = string_cache_array(units.iter().map(|value| value.as_deref()));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("rt", DataType::Int64, false),
                Field::new("v", DataType::Utf8, true),
                string_cache_field(2, cache.data_type().clone()),
            ])),
            vec![
                Arc::new(Int64Array::from(partitions)),
                Arc::new(Int64Array::from(times)),
                Arc::new(StringArray::from(texts)),
                cache,
            ],
        )
        .unwrap()
    };
    (
        batch(partitions, times, texts, units),
        batch(
            (0..keys).map(|key| key as i64).collect(),
            vec![1500; keys],
            vec![None; keys],
            vec![None; keys],
        ),
    )
}

fn execute(
    operator: &mut Over,
    seed: &RecordBatch,
    last: &RecordBatch,
    kind: i64,
    checkpoint: bool,
    pending: bool,
) -> RecordBatch {
    operator.push(seed.clone());
    operator.flush(1070);
    if pending {
        operator.push(last.clone());
    }
    if checkpoint {
        *operator = Over::restore_framed(3, kind, 2, 1000, false, &operator.snapshot());
    }
    if !pending {
        operator.push(last.clone());
    }
    operator.flush(1500)
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_string_range_retraction_order");
    for keys in [1, 64] {
        for peers in [1, 32] {
            for width in [1, 256] {
                let (seed, last) = fixture(keys, peers, width);
                group.throughput(Throughput::Elements(
                    (seed.num_rows() + last.num_rows()) as u64,
                ));
                for kind in [1, 2] {
                    for checkpoint in [false, true] {
                        for pending in [false, true] {
                            let out = execute(
                                &mut Over::framed(3, kind, 2, 1000, false),
                                &seed,
                                &last,
                                kind,
                                checkpoint,
                                pending,
                            );
                            assert_eq!(out.num_rows(), keys);
                            let expected = if kind == 1 && !checkpoint && width == 1 {
                                "😀".repeat(width)
                            } else {
                                "\u{e100}".repeat(width)
                            };
                            let values = out
                                .column(3)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            assert!(values.iter().all(|value| value == Some(expected.as_str())));
                            let expected_cache = (kind == 1 && !checkpoint && width == 1)
                                .then(|| expected.encode_utf16().collect::<Vec<_>>());
                            let caches =
                                JavaStringCacheColumn::new(string_cache_column(&out, 3).unwrap());
                            for row in 0..keys {
                                assert_eq!(caches.value(row), expected_cache.as_deref());
                            }
                            let id = format!("keys{keys}_peers{peers}_width{width}_kind{kind}_checkpoint{checkpoint}_pending{pending}");
                            group.bench_function(id, |b| {
                                b.iter_batched_ref(
                                    || Over::framed(3, kind, 2, 1000, false),
                                    |operator| {
                                        black_box(execute(
                                            operator, &seed, &last, kind, checkpoint, pending,
                                        ))
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
