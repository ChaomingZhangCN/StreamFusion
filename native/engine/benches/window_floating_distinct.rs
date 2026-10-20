use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::collections::{BTreeMap, BTreeSet};
#[path = "window_floating_distinct/attached_cumulative.rs"]
mod attached_cumulative;
use std::sync::Arc;
use streamfusion::bench::Tumbling;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixture(rows: usize, keys: usize, code: i64) -> RecordBatch {
    let present = |row: usize| row % 7 != 0 && (keys == 1 || row % keys != 0);
    let slot = |row: usize| row / keys % 10;
    let values: ArrayRef = if code == 6 {
        let bits = [
            0x7fc00001, 0xffc00002, 0, 0x80000000, 0x7f800000, 0xff800000, 1, 0x7f7fffff,
            0x3f800000, 0x7fc00003,
        ];
        Arc::new(Float32Array::from_iter((0..rows).map(|row| {
            present(row).then(|| f32::from_bits(bits[slot(row)]))
        })))
    } else {
        let bits = [
            0x7ff8000000000001,
            0xfff8000000000002,
            0,
            0x8000000000000000,
            0x7ff0000000000000,
            0xfff0000000000000,
            1,
            0x7fefffffffffffff,
            0x3ff0000000000000,
            0x7ff8000000000003,
        ];
        Arc::new(Float64Array::from_iter((0..rows).map(|row| {
            present(row).then(|| f64::from_bits(bits[slot(row)]))
        })))
    };
    RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from_value(100, rows)) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % keys) as i64),
            )) as ArrayRef,
        ),
        ("value0", values),
    ])
    .unwrap()
}

fn check(input: &RecordBatch, output: &RecordBatch, windows: usize) {
    let keys = input
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut expected: BTreeMap<i64, BTreeSet<u64>> = BTreeMap::new();
    for row in 0..input.num_rows() {
        let set = expected.entry(keys.value(row)).or_default();
        if input.column(2).is_null(row) {
            continue;
        }
        let bits = if let Some(values) = input.column(2).as_any().downcast_ref::<Float32Array>() {
            let bits = values.value(row).to_bits();
            if bits & 0x7fffffff > 0x7f800000 {
                0x7fc00000
            } else {
                bits as u64
            }
        } else {
            let bits = input
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
                .to_bits();
            if bits & 0x7fffffffffffffff > 0x7ff0000000000000 {
                0x7ff8000000000000
            } else {
                bits
            }
        };
        set.insert(bits);
    }
    assert_eq!(output.num_rows(), expected.len() * windows);
    let output_keys = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = output
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut occurrences: BTreeMap<i64, usize> = BTreeMap::new();
    for row in 0..output.num_rows() {
        let key = output_keys.value(row);
        assert_eq!(counts.value(row), expected[&key].len() as i64);
        *occurrences.entry(key).or_default() += 1;
    }
    assert!(occurrences.values().all(|&count| count == windows));
}

fn benchmark(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("window_floating_distinct");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in if rows == 64 {
            vec![1, 64]
        } else {
            vec![1, 64, rows]
        } {
            for code in [6, 1] {
                let input = fixture(rows, keys, code);
                for (shape, slide, cumulative, windows) in [
                    ("tumble", 1000, false, 1),
                    ("hop", 500, false, 2),
                    ("cumulate", 500, true, 2),
                ] {
                    for partial in [false, true] {
                        let setup = || {
                            let create = || {
                                Tumbling::with_window_budget(
                                    1000,
                                    slide,
                                    cumulative,
                                    code,
                                    vec![7],
                                    64 * 1024 * 1024,
                                )
                            };
                            (create(), partial.then(create))
                        };
                        let run = |state: &mut (Tumbling, Option<Tumbling>)| {
                            for start in (0..rows).step_by(rows / 4) {
                                let batch = input.slice(start, rows / 4);
                                if let Some(global) = &mut state.1 {
                                    state.0.update_local(&batch);
                                    global.update_partial(&state.0.drain_partial());
                                } else {
                                    state.0.update(&batch);
                                }
                            }
                            if let Some(global) = &mut state.1 {
                                global.flush(1000)
                            } else {
                                state.0.flush(1000)
                            }
                        };
                        let mut checked = setup();
                        let (output, allocations) = measure(|| run(&mut checked));
                        check(&input, &output, windows);
                        let label = format!("window_floating_distinct/code{code}/{shape}/rows{rows}_keys{keys}_partial{partial}");
                        report(&label, input.columns(), output.columns(), allocations);
                        group.bench_function(label, |b| {
                            b.iter_batched_ref(
                                setup,
                                |state| black_box(run(state)),
                                BatchSize::SmallInput,
                            )
                        });
                    }
                }
            }
        }
    }
    group.finish();
    attached_cumulative::bench(c);
}
criterion_group!(benches, benchmark);
criterion_main!(benches);
