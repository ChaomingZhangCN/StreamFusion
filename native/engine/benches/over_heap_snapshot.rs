use arrow::array::{Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use jni::objects::GlobalRef;
use std::sync::Arc;
use streamfusion::bench::Over;
use streamfusion_bridge::{string_cache_array, string_cache_field};

struct Kernel(GlobalRef);
impl Kernel {
    fn new(vm: &jni::JavaVM) -> Self {
        let mut env = vm.attach_current_thread().unwrap();
        let result = env.new_object("tech/streamfusion/bench/OverHeapProbeKernel", "()V", &[]);
        if env.exception_check().unwrap() {
            env.exception_describe().unwrap();
        }
        Self(env.new_global_ref(result.unwrap()).unwrap())
    }
    fn call(&self, method: &str) {
        let mut env = streamfusion_bridge::bridge::JVM
            .get()
            .unwrap()
            .attach_current_thread()
            .unwrap();
        let result = env.call_method(self.0.as_obj(), method, "()V", &[]);
        if env.exception_check().unwrap() {
            env.exception_describe().unwrap();
        }
        result.unwrap();
    }
}
impl Drop for Kernel {
    fn drop(&mut self) {
        self.call("close");
    }
}

fn fixture(rows: usize, keys: usize, width: usize, nulls: bool) -> (RecordBatch, RecordBatch) {
    let key = |index: usize| format!("key-{index}-{}", "x".repeat(width));
    let mut names = Vec::new();
    let mut times = Vec::new();
    let mut values = Vec::new();
    for (time, value) in [
        (100, 99_999_999_999_999_999_999_999_999_999_999_999_999i128),
        (200, -1),
        (400, 2),
    ] {
        for index in 0..keys {
            names.push(key(index));
            times.push(time);
            values.push((!nulls || index != 0).then_some(value));
        }
    }
    let batch = |names: Vec<String>, times: Vec<i64>, values: Vec<Option<i128>>| {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(StringArray::from(names)) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
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
    let seed = batch(names, times, values);
    let last = batch(
        (0..rows).map(|row| key(row % keys)).collect(),
        vec![2000; rows],
        (0..rows)
            .map(|row| (!nulls || row % keys != 0).then_some(7))
            .collect(),
    );
    (seed, last)
}

fn string_fixture(
    rows: usize,
    keys: usize,
    width: usize,
    nulls: bool,
) -> (RecordBatch, RecordBatch) {
    let (seed, last) = fixture(rows, keys, width, nulls);
    let convert = |source: RecordBatch, final_batch: bool| {
        let texts = (0..source.num_rows())
            .map(|row| {
                if source.column(2).is_null(row) {
                    return None;
                }
                Some(
                    if final_batch {
                        "tail"
                    } else {
                        ["😀", "\u{e000}", "\u{e100}"][row / keys]
                    }
                    .repeat(width.max(1)),
                )
            })
            .collect::<Vec<_>>();
        let units = texts
            .iter()
            .map(|text| {
                text.as_ref()
                    .filter(|text| !text.starts_with('\u{e100}'))
                    .map(|text| text.encode_utf16().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let cache = string_cache_array(units.iter().map(|units| units.as_deref()));
        let mut fields = source.schema().fields()[..2]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            "v",
            arrow::datatypes::DataType::Utf8,
            true,
        )));
        fields.push(Arc::new(string_cache_field(2, cache.data_type().clone())));
        RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            vec![
                source.column(0).clone(),
                source.column(1).clone(),
                Arc::new(StringArray::from(texts)),
                cache,
            ],
        )
        .unwrap()
    };
    (convert(seed, false), convert(last, true))
}

fn setup(vm: &jni::JavaVM, seed: &RecordBatch, mode: &str, strings: bool) -> (Over, Kernel) {
    let kernel = Kernel::new(vm);
    let mut operator = Over::framed(
        if strings { 3 } else { 5800 },
        if strings { 1 } else { 0 },
        2,
        1000,
        false,
    );
    operator.heap_snapshot_probe(kernel.0.clone());
    operator.push(seed.clone());
    operator.flush(400);
    if mode != "none" {
        kernel.call("beginSnapshot");
    }
    if mode == "completed" {
        kernel.call("completeSnapshot");
    }
    (operator, kernel)
}

fn run(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare JVM fixture classes");
    let property = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx512m")
        .option("-Duser.timezone=UTC")
        .option("--add-opens=java.base/java.lang=ALL-UNNAMED")
        .option("--add-opens=java.base/java.util=ALL-UNNAMED")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut group = c.benchmark_group("over_heap_snapshot");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for width in [0, 256] {
                for nulls in [false, true] {
                    for strings in [false, true] {
                        let (seed, last) = if strings {
                            string_fixture(rows, keys, width, nulls)
                        } else {
                            fixture(rows, keys, width, nulls)
                        };
                        for mode in ["none", "completed", "held", "between"] {
                            let mut state = setup(&vm, &seed, mode, strings);
                            state.0.push(last.clone());
                            if mode == "between" {
                                state.1.call("completeSnapshot");
                            }
                            let result = state.0.flush(2000);
                            if strings {
                                let values = result
                                    .column(3)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                for row in 0..rows {
                                    if nulls && row % keys == 0 {
                                        assert!(values.is_null(row));
                                    } else {
                                        assert_eq!(values.value(row), "tail".repeat(width.max(1)));
                                    }
                                }
                            } else {
                                let values = result
                                    .column(3)
                                    .as_any()
                                    .downcast_ref::<Decimal128Array>()
                                    .unwrap();
                                for row in 0..rows {
                                    if nulls && row % keys == 0 {
                                        assert!(values.is_null(row));
                                    } else {
                                        assert_eq!(
                                            values.value(row),
                                            (rows / keys * 7
                                                + usize::from(!matches!(mode, "held" | "between")))
                                                as i128
                                        );
                                    }
                                }
                            }
                            drop(state);
                            group.bench_function(
                            format!("strings{strings}_rows{rows}_keys{keys}_width{width}_nulls{nulls}_{mode}"),
                            |b| {
                                b.iter_batched_ref(
                                    || setup(&vm, &seed, mode, strings),
                                        |(operator, kernel)| {
                                        operator.push(last.clone());
                                            if mode == "between" {
                                                kernel.call("completeSnapshot");
                                            }
                                        black_box(operator.flush(2000))
                                    },
                                    BatchSize::PerIteration,
                                )
                            },
                        );
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
