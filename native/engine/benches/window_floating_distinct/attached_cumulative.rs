use super::*;
use arrow::array::StringArray;
use arrow::compute::{concat_batches, filter_record_batch};
use jni::objects::{GlobalRef, JValue};

fn callback(vm: &jni::JavaVM) -> GlobalRef {
    let mut env = vm.attach_current_thread().unwrap();
    let zone = env.new_string("UTC").unwrap();
    let object = env
        .new_object(
            "tech/streamfusion/operator/WindowCalendar",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&zone)],
        )
        .unwrap();
    env.new_global_ref(object).unwrap()
}

pub(super) fn bench(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH").expect("Run bin/bench-native.py");
    let option = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&option)
        .option("-Xms128m")
        .option("-Xmx512m")
        .option("-Duser.timezone=UTC")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let calendar = callback(&vm);
    let mut group = c.benchmark_group("attached_cumulative_partials");
    for rows in [64, 512] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            let input = RecordBatch::try_from_iter(vec![
                (
                    "slice_end",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|row| (row / keys % 6 + 1) as i64 * 1000),
                    )) as ArrayRef,
                ),
                (
                    "key0",
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|row| format!("key{}", row % keys)),
                    )) as ArrayRef,
                ),
                (
                    "partial0",
                    Arc::new(Int64Array::from_iter_values((0..rows).map(|row| {
                        if row % 7 == 0 {
                            0
                        } else {
                            (row / keys % 3 + 1) as i64
                        }
                    }))) as ArrayRef,
                ),
            ])
            .unwrap();
            let mut expected = BTreeMap::new();
            for row in 0..rows {
                let end = (row / keys % 6 + 1) as i64 * 1000;
                let start = (end - 1) / 2000 * 2000;
                let count = if row % 7 == 0 {
                    0
                } else {
                    (row / keys % 3 + 1) as i64
                };
                *expected
                    .entry((format!("key{}", row % keys), start, end))
                    .or_insert(0_i64) += count;
            }
            let late = filter_record_batch(
                &input,
                &arrow::array::BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row / keys % 6 == 0)),
                ),
            )
            .unwrap();
            for rocks in [false, true] {
                if rocks && !cfg!(feature = "rocksdb-state") {
                    continue;
                }
                for checkpoint in [false, true] {
                    if rocks && checkpoint {
                        continue;
                    }
                    let setup = || {
                        let directory = rocks.then(|| tempfile::tempdir().unwrap());
                        let mut operator = Tumbling::fixed_calendar(2000, 2000, true, 0);
                        #[cfg(feature = "rocksdb-state")]
                        if let Some(directory) = &directory {
                            operator = Tumbling::persistent_fixed_calendar(
                                directory.path().join("db").to_str().unwrap(),
                                include_str!("../fixtures/rocks-options.json"),
                                2000,
                                2000,
                                true,
                            );
                        }
                        operator = operator.account_calendar_state(64 * 1024 * 1024);
                        operator.calendar(calendar.clone());
                        (operator, directory)
                    };
                    let run = |state: &mut (Tumbling, Option<tempfile::TempDir>)| {
                        for start in (0..rows).step_by(rows / 4) {
                            state.0.update_partial(&input.slice(start, rows / 4));
                            if checkpoint {
                                state.0 = Tumbling::restore_fixed_calendar(
                                    2000,
                                    2000,
                                    true,
                                    &state.0.snapshot_fixed_calendar(),
                                )
                                .account_calendar_state(64 * 1024 * 1024);
                                state.0.calendar(calendar.clone());
                            }
                        }
                        let first = state.0.flush(1000);
                        state.0.update_partial(&late);
                        let last = state.0.flush(6000);
                        assert_eq!(state.0.late_drops(), late.num_rows() as u64);
                        assert_eq!(state.0.flush(6000).num_rows(), 0);
                        state.0.assert_calendar_accounting();
                        concat_batches(&first.schema(), &[first.clone(), last]).unwrap()
                    };
                    let mut checked = setup();
                    let (output, allocations) = measure(|| run(&mut checked));
                    let names = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let starts = output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let ends = output
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let counts = output
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let actual: BTreeMap<_, _> = (0..output.num_rows())
                        .map(|row| {
                            (
                                (
                                    names.value(row).to_owned(),
                                    starts.value(row),
                                    ends.value(row),
                                ),
                                counts.value(row),
                            )
                        })
                        .collect();
                    assert_eq!(actual, expected);
                    assert_eq!(output.num_rows(), expected.len());
                    let label = format!("attached_cumulative_partials/rows{rows}_keys{keys}_rocks{rocks}_checkpoint{checkpoint}");
                    report(&label, input.columns(), output.columns(), allocations);
                    group.bench_function(label, |b| {
                        b.iter_batched_ref(
                            setup,
                            |state| black_box(run(state)),
                            BatchSize::PerIteration,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}
