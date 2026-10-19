use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
use arrow::array::{Date32Array, Float32Array, Float64Array, Time32MillisecondArray};
use datafusion::common::ScalarValue;
use std::collections::BTreeMap;
use streamfusion::bench::GroupBy;
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

fn boxed_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            a.is_nan() && b.is_nan() || a.to_bits() == b.to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            a.is_nan() && b.is_nan() || a.to_bits() == b.to_bits()
        }
        _ => a == b,
    }
}

fn exact_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        _ => a == b,
    }
}

fn float_value(value: i64) -> f64 {
    match value % 16 {
        0 => f64::from_bits(0x7ff8000000000001),
        1 => -0.0,
        2 => 0.0,
        3 => f64::from_bits(0xfff8000000000002),
        _ => value as f64 * 0.25,
    }
}

fn fixture(rows: usize, keys: usize, cardinality: usize, ty: &str) -> (RecordBatch, i64) {
    let values: Vec<_> = (0..rows)
        .map(|row| {
            (row % 7 != 0 && (keys == 1 || row % keys != 0))
                .then_some((row / keys % cardinality) as i64)
        })
        .collect();
    let (code, value): (i64, ArrayRef) = match ty {
        "tinyint" => (
            5,
            Arc::new(Int8Array::from_iter(
                values.iter().map(|v| v.map(|v| v as i8)),
            )),
        ),
        "smallint" => (
            4,
            Arc::new(Int16Array::from_iter(
                values.iter().map(|v| v.map(|v| v as i16)),
            )),
        ),
        "int" => (
            2,
            Arc::new(Int32Array::from_iter(
                values.iter().map(|v| v.map(|v| v as i32)),
            )),
        ),
        "bigint" => (0, Arc::new(Int64Array::from(values.clone()))),
        "decimal_wide" => (
            5803,
            Arc::new(
                Decimal128Array::from_iter(
                    values
                        .iter()
                        .map(|v| v.map(|v| 10_000_000_000_000_000_000i128 + i128::from(v) * 1001)),
                )
                .with_precision_and_scale(38, 3)
                .unwrap(),
            ),
        ),
        "float" => (
            6,
            Arc::new(Float32Array::from_iter(
                values.iter().map(|v| v.map(|v| float_value(v) as f32)),
            )),
        ),
        "double" => (
            1,
            Arc::new(Float64Array::from_iter(
                values.iter().map(|v| v.map(float_value)),
            )),
        ),
        "decimal" => (
            4002,
            Arc::new(
                Decimal128Array::from_iter(values.iter().map(|v| v.map(|v| i128::from(v) * 100)))
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ),
        ),
        "date" => (
            8,
            Arc::new(Date32Array::from_iter(
                values.iter().map(|v| v.map(|v| v as i32)),
            )),
        ),
        "time" => (
            9,
            Arc::new(Time32MillisecondArray::from_iter(
                values.iter().map(|v| v.map(|v| v as i32)),
            )),
        ),
        "boolean" => (
            10,
            Arc::new(BooleanArray::from_iter(
                values.iter().map(|v| v.map(|v| v % 2 == 0)),
            )),
        ),
        "timestamp9" => (
            1009,
            Arc::new(timestamp_array(
                values
                    .iter()
                    .map(|v| v.map(|v| TimestampValue::new(-1, v as u32).unwrap())),
            )),
        ),
        _ => {
            let width = if ty == "string8" { 8 } else { 256 };
            (
                3,
                Arc::new(StringArray::from_iter(
                    values.iter().map(|v| v.map(|v| format!("{v:0width$}"))),
                )),
            )
        }
    };
    (
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| (row % keys) as i64),
                )) as ArrayRef,
            ),
            ("v", value),
        ])
        .unwrap(),
        code,
    )
}

fn check(seed: &RecordBatch, input: &RecordBatch, code: i64, retract: bool) {
    let mut expected: BTreeMap<i64, (usize, Vec<(ScalarValue, usize)>)> = BTreeMap::new();
    for (batch, deletion) in
        [(seed, false), (input, true)]
            .into_iter()
            .take(if retract { 2 } else { 1 })
    {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let value = ScalarValue::try_from_array(batch.column(1), row).unwrap();
            let group = expected.entry(key).or_default();
            if deletion {
                group.0 -= 1;
            } else {
                group.0 += 1;
            }
            if !value.is_null() {
                let position = group.1.iter().position(|(old, _)| boxed_equal(old, &value));
                if deletion {
                    let index = position.expect("input retracts an existing value");
                    group.1[index].1 -= 1;
                    if group.1[index].1 == 0 {
                        group.1.remove(index);
                    }
                } else if let Some(index) = position {
                    group.1[index].1 += 1;
                } else {
                    group.1.push((value, 1));
                }
            }
            if group.0 == 0 {
                expected.remove(&key);
            }
        }
    }
    let mut agg = GroupBy::new(vec![19, 20], vec![code; 2], vec![1; 2], vec![0]);
    let mut actual = BTreeMap::new();
    let mut outputs = vec![agg.update(seed)];
    if retract {
        outputs.push(agg.update(input));
    }
    for output in outputs {
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = output
            .column_by_name("$row_kind$")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            match kinds.value(row) {
                0 | 2 => {
                    actual.insert(
                        keys.value(row),
                        (
                            ScalarValue::try_from_array(output.column(1), row).unwrap(),
                            ScalarValue::try_from_array(output.column(2), row).unwrap(),
                        ),
                    );
                }
                3 => {
                    actual.remove(&keys.value(row));
                }
                _ => {}
            }
        }
    }
    assert_eq!(actual.len(), expected.len());
    for (key, (_, values)) in expected {
        let (first, last) = &actual[&key];
        if values.is_empty() {
            assert!(first.is_null() && last.is_null());
        } else {
            assert!(exact_equal(first, &values.first().unwrap().0));
            assert!(exact_equal(last, &values.last().unwrap().0));
        }
    }
}

fn mixed_aggregate(code: i64, records: bool) -> GroupBy {
    let mut kinds = vec![19, 20, 7];
    let mut types = vec![code; 3];
    let mut columns = vec![1; 3];
    if records {
        kinds.push(3);
        types.push(0);
        columns.push(-1);
    }
    GroupBy::new(kinds, types, columns, vec![0])
}

fn record_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => a == b,
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a == b,
        _ => a == b,
    }
}

// Independent arrival/multiplicity state predicts every emitted preimage and postimage.
fn check_mixed(
    seed: &RecordBatch,
    deletion: &RecordBatch,
    code: i64,
    retract: bool,
    records: bool,
) {
    let null = ScalarValue::try_from(seed.column(1).data_type()).unwrap();
    let mut states: BTreeMap<i64, (i64, Vec<(ScalarValue, usize)>, Vec<ScalarValue>)> =
        BTreeMap::new();
    let mut aggregate = mixed_aggregate(code, records);
    for (batch, delete) in [(seed, false), (deletion, true)]
        .into_iter()
        .take(if retract { 2 } else { 1 })
    {
        let mut expected = Vec::new();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let value = ScalarValue::try_from_array(batch.column(1), row).unwrap();
            let state = states.entry(key).or_default();
            let previous = (!state.2.is_empty()).then(|| state.2.clone());
            state.0 += if delete { -1 } else { 1 };
            if !value.is_null() {
                let index = state.1.iter().position(|(old, _)| boxed_equal(old, &value));
                if delete {
                    let index = index.expect("existing membership");
                    state.1[index].1 -= 1;
                    if state.1[index].1 == 0 {
                        state.1.remove(index);
                    }
                } else if let Some(index) = index {
                    state.1[index].1 += 1;
                } else {
                    state.1.push((value, 1));
                }
            }
            if state.0 == 0 {
                if let Some(previous) = previous {
                    expected.push((key, 3, previous));
                }
                states.remove(&key);
                continue;
            }
            let mut current = vec![
                state
                    .1
                    .first()
                    .map_or_else(|| null.clone(), |v| v.0.clone()),
                state.1.last().map_or_else(|| null.clone(), |v| v.0.clone()),
                ScalarValue::Int64(Some(state.1.len() as i64)),
            ];
            if records {
                current.push(ScalarValue::Int64(Some(state.0)));
            }
            match previous {
                None => expected.push((key, 0, current.clone())),
                Some(previous)
                    if !previous
                        .iter()
                        .zip(&current)
                        .all(|(a, b)| record_equal(a, b)) =>
                {
                    expected.push((key, 1, previous));
                    expected.push((key, 2, current.clone()));
                }
                _ => {}
            }
            state.2 = current;
        }
        let output = aggregate.update(batch);
        assert_eq!(output.num_rows(), expected.len());
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = output
            .column_by_name("$row_kind$")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        for (row, (key, kind, values)) in expected.iter().enumerate() {
            assert_eq!(keys.value(row), *key);
            assert_eq!(kinds.value(row), *kind);
            for (column, value) in values.iter().enumerate() {
                let actual = ScalarValue::try_from_array(output.column(column + 1), row).unwrap();
                assert!(
                    exact_equal(&actual, value),
                    "row {row}, column {column}: {actual:?} != {value:?}"
                );
            }
        }
    }
}

fn bench(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("distinct_ordered_values");
    for rows in [64, 4096] {
        for keys in [1, 64] {
            for cardinality in [4, 128] {
                for ty in [
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "decimal_wide",
                    "float",
                    "double",
                    "decimal",
                    "date",
                    "time",
                    "boolean",
                    "timestamp9",
                    "string8",
                    "string256",
                ] {
                    let (seed, code) = fixture(rows, keys, cardinality, ty);
                    let deleted = seed.slice(0, rows / 2);
                    let mut fields = deleted.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new("$row_kind$", DataType::Int8, false)));
                    let mut columns = deleted.columns().to_vec();
                    columns.push(Arc::new(Int8Array::from_value(3, deleted.num_rows())));
                    let deletion =
                        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
                    for retract in [false, true] {
                        let input = if retract { &deletion } else { &seed };
                        check(&seed, input, code, retract);
                        group.throughput(Throughput::Elements(input.num_rows() as u64));
                        let create = || {
                            let mut aggregate =
                                GroupBy::new(vec![19, 20], vec![code; 2], vec![1; 2], vec![0]);
                            if retract {
                                aggregate.update(&seed);
                            }
                            aggregate
                        };
                        let mut probe = create();
                        let (output, allocations) = measure(|| probe.update(input));
                        let name = format!(
                            "{ty}/rows{rows}/keys{keys}/cardinality{cardinality}/retract{retract}"
                        );
                        report(
                            &format!("distinct_ordered_values/{name}"),
                            input.columns(),
                            output.columns(),
                            allocations,
                        );
                        group.bench_function(name, |b| {
                            b.iter_batched_ref(
                                create,
                                |aggregate| black_box(aggregate.update(black_box(input))),
                                BatchSize::SmallInput,
                            );
                        });
                    }
                }
            }
        }
    }
    for rows in [64, 4096] {
        for ty in ["date", "time", "double", "string256"] {
            let (seed, code) = fixture(rows, 64, 4, ty);
            let deleted = seed.slice(0, rows / 2);
            let mut fields = deleted.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("$row_kind$", DataType::Int8, false)));
            let mut columns = deleted.columns().to_vec();
            columns.push(Arc::new(Int8Array::from_value(3, deleted.num_rows())));
            let deletion = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            for records in [false, true] {
                for retract in [false, true] {
                    check_mixed(&seed, &deletion, code, retract, records);
                    let input = if retract { &deletion } else { &seed };
                    group.throughput(Throughput::Elements(input.num_rows() as u64));
                    let create = || {
                        let mut aggregate = mixed_aggregate(code, records);
                        if retract {
                            aggregate.update(&seed);
                        }
                        aggregate
                    };
                    let mut probe = create();
                    let (output, allocations) = measure(|| probe.update(input));
                    let name = format!("mixed/{ty}/rows{rows}/records{records}/retract{retract}");
                    report(
                        &format!("distinct_ordered_values/{name}"),
                        input.columns(),
                        output.columns(),
                        allocations,
                    );
                    group.bench_function(name, |b| {
                        b.iter_batched_ref(
                            create,
                            |aggregate| black_box(aggregate.update(black_box(input))),
                            BatchSize::SmallInput,
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
