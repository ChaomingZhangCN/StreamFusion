use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::collections::BTreeSet;
use std::sync::Arc;
use streamfusion::bench::PersistentOver;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
const OPTIONS: &str = include_str!("fixtures/rocks-options.json");

fn input(rows: usize, code: i64) -> RecordBatch {
    let present = |row: usize| row % 7 != 0;
    let slot = |row: usize| row / 8 % 3;
    let values: ArrayRef = if code == 3 {
        let strings = [String::new(), "x\0中".repeat(64), "😀".to_owned()];
        Arc::new(StringArray::from_iter(
            (0..rows).map(|row| present(row).then(|| strings[slot(row)].as_str())),
        ))
    } else {
        Arc::new(timestamp_array((0..rows).map(|row| {
            present(row).then(|| TimestampValue::from_nanos([-1, 0, 1][slot(row)]).unwrap())
        })))
    };
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % 4) as i64),
            )) as ArrayRef,
        ),
        ("value", values),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 1000 + (row / 4) as i64 * 400),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn validate(batch: &RecordBatch, rows: usize) {
    assert_eq!(batch.num_rows(), rows);
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let times = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut expected = Vec::new();
    let mut memberships = [
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    ];
    for row in 0..rows {
        let set = &mut memberships[row % 4];
        if row % 7 != 0 {
            set.insert(row / 8 % 3);
        }
        expected.push(set.len() as i64);
    }
    let mut seen = BTreeSet::new();
    for index in 0..batch.num_rows() {
        let row = ((times.value(index) - 1000) / 400) as usize * 4 + keys.value(index) as usize;
        assert!(row < rows && seen.insert(row));
        assert!(!counts.is_null(index));
        assert_eq!(counts.value(index), expected[row], "row={row}");
    }
}

fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("persistent_over_distinct");
    for rows in [32, 512] {
        group.throughput(Throughput::Elements(rows as u64));
        for (name, code) in [("string", 3), ("timestamp9", 1009)] {
            let input = input(rows, code);
            for row_frame in [false, true] {
                for restore in [false, true] {
                    let setup = || {
                        let directory = tempfile::tempdir().unwrap();
                        let operator = PersistentOver::distinct_count(
                            directory.path().join("db").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                            code,
                            row_frame,
                        );
                        (Some(operator), directory)
                    };
                    let run = |state: &mut (Option<PersistentOver>, tempfile::TempDir)| {
                        let split = rows / 2;
                        let operator = state.0.as_mut().unwrap();
                        operator.push(&input.slice(0, split));
                        let watermark = input
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(split - 1);
                        let first = operator.advance(watermark);
                        if restore {
                            let checkpoint = state.1.path().join("checkpoint");
                            let generation = operator.checkpoint(checkpoint.to_str().unwrap());
                            state.0.take();
                            state.0 = Some(PersistentOver::restore_distinct_count(
                                state.1.path().join("restored").to_str().unwrap(),
                                input.schema(),
                                OPTIONS,
                                checkpoint.to_str().unwrap(),
                                generation,
                                code,
                                row_frame,
                            ));
                        }
                        let operator = state.0.as_mut().unwrap();
                        operator.push(&input.slice(split, rows - split));
                        let last = operator.flush();
                        concat_batches(&first.schema(), &[first.clone(), last]).unwrap()
                    };
                    let mut checked = setup();
                    let (output, allocations) = measure(|| run(&mut checked));
                    validate(&output, rows);
                    let label = format!("persistent_over_distinct/{name}/rows{rows}/rows_frame{row_frame}/restore{restore}");
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

criterion_group!(benches, bench);
criterion_main!(benches);
