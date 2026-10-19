use super::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

fn check(input: &RecordBatch, output: &RecordBatch, kind: i64, frame: i64, offset: i64) {
    let keys = input
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let times = input
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = input
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let result = output
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let DataType::Decimal128(_, input_scale) = values.data_type() else {
        unreachable!()
    };
    let DataType::Decimal128(_, result_scale) = result.data_type() else {
        unreachable!()
    };
    let factor = 10_i128.pow((result_scale - input_scale) as u32);
    let mut history: BTreeMap<i64, VecDeque<usize>> = BTreeMap::new();
    let mut expected = Vec::new();
    for row in 0..input.num_rows() {
        let rows = history.entry(keys.value(row)).or_default();
        rows.push_back(row);
        while (frame == 1 && rows.len() > offset as usize + 1)
            || (frame == 2 && times.value(row) - times.value(*rows.front().unwrap()) > offset)
        {
            rows.pop_front();
        }
        let set: BTreeSet<_> = rows
            .iter()
            .filter(|&&index| !values.is_null(index))
            .map(|&index| values.value(index))
            .collect();
        let sum: i128 = set.iter().sum();
        let value = if set.is_empty() && kind != 122 {
            None
        } else if kind == 104 && !set.is_empty() {
            let numerator = sum * factor;
            let count = set.len() as i128;
            Some((numerator.abs() + count / 2) / count * numerator.signum())
        } else {
            Some(sum)
        };
        expected.push(value);
    }
    let output_times = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut seen = BTreeSet::new();
    for row in 0..output.num_rows() {
        let index = (output_times.value(row) - times.value(0)) as usize;
        assert!(seen.insert(index));
        assert_eq!(
            (!result.is_null(row)).then(|| result.value(row)),
            expected[index],
            "kind={kind} frame={frame} row={index}"
        );
    }
    assert_eq!(seen.len(), input.num_rows());
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("over_decimal_distinct_reductions");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for code in [3202, 5800, 5837] {
                let original = fixture(rows, keys, code);
                let precision = ((code - 2000) / 100) as u8;
                let scale = ((code - 2000) % 100) as i8;
                let values = Decimal128Array::from_iter((0..rows).map(|row| {
                    (!original.column(2).is_null(row))
                        .then_some([125, 125, 250, -25, 0, -250][row / (2 * keys) % 6])
                }))
                .with_precision_and_scale(precision, scale)
                .unwrap();
                let mut columns = original.columns().to_vec();
                columns[2] = Arc::new(values);
                let input = RecordBatch::try_new(original.schema(), columns).unwrap();
                for kind in [100, 122, 104] {
                    for proctime in [false, true] {
                        for frame in 0..4 {
                            if proctime && frame == 2 {
                                continue;
                            }
                            let offset = if frame == 1 { 3 } else { 10 };
                            validate(&input, code, kind, frame, offset, proctime);
                            let mut operator = Over::framed(code, kind, frame, offset, proctime);
                            let (output, allocations) =
                                measure(|| execute(&mut operator, input.clone(), proctime));
                            check(&input, &output, kind, frame, offset);
                            let label = format!("over_decimal_distinct_reductions/code{code}/rows{rows}_keys{keys}_kind{kind}_frame{frame}_proctime{proctime}");
                            report(&label, input.columns(), output.columns(), allocations);
                            group.bench_function(label, |b| {
                                b.iter_batched_ref(
                                    || Over::framed(code, kind, frame, offset, proctime),
                                    |operator| {
                                        black_box(execute(operator, input.clone(), proctime))
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
