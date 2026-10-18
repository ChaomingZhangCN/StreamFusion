use super::*;
use streamfusion_bridge::{string_cache_array, string_cache_field};

// Distinct released-Flink string hashes in one 64-bucket bin. Cached/binary comparison
// is non-transitive, so retiring "a" makes JDK tree iteration observable.
const VALUES: [&str; 24] = [
    "\u{e100}3002",
    "\u{e100}3050",
    "😀3105",
    "\u{e100}3128",
    "\u{e100}3170",
    "😀3180",
    "\u{e100}3239",
    "😀3258",
    "😀3396",
    "😀3468",
    "\u{e000}3484",
    "\u{e100}3638",
    "😀3813",
    "\u{e100}3827",
    "\u{e000}3847",
    "😀3858",
    "\u{e000}3862",
    "😀3873",
    "😀3918",
    "\u{e000}3988",
    "😀4077",
    "\u{e100}4118",
    "😀4128",
    "\u{e100}4136",
];

fn fixture(steps: usize, keys: usize) -> RecordBatch {
    let texts = (0..steps * keys)
        .map(|row| match (row / keys) % 80 {
            0 => Some("a"),
            index @ 1..=24 => Some(VALUES[index - 1]),
            _ => None,
        })
        .collect::<Vec<_>>();
    let caches = texts
        .iter()
        .map(|text| {
            text.filter(|text| !text.starts_with('\u{e100}'))
                .map(|text| text.encode_utf16().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let cache = string_cache_array(caches.iter().map(|units| units.as_deref()));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int64, false),
            arrow::datatypes::Field::new("rt", DataType::Int64, false),
            arrow::datatypes::Field::new("v", DataType::Utf8, true),
            string_cache_field(2, cache.data_type().clone()),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..steps * keys).map(|row| (row % keys) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..steps * keys).map(|row| 100 + (row / keys) as i64),
            )),
            Arc::new(StringArray::from(texts)),
            cache,
        ],
    )
    .unwrap()
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_string_tree_retirement");
    for steps in [80, 1024] {
        for keys in [1, 64] {
            let batch = fixture(steps, keys);
            group.throughput(Throughput::Elements(batch.num_rows() as u64));
            for proctime in [false, true] {
                let result = execute(
                    &mut Over::framed(3, 1, 1, 32, proctime),
                    batch.clone(),
                    proctime,
                );
                let values = result
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for key in 0..keys {
                    assert_eq!(values.value(33 * keys + key), "😀3258");
                }
                group.bench_function(format!("steps{steps}_keys{keys}_proctime{proctime}"), |b| {
                    b.iter_batched_ref(
                        || Over::framed(3, 1, 1, 32, proctime),
                        |operator| black_box(execute(operator, batch.clone(), proctime)),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}
