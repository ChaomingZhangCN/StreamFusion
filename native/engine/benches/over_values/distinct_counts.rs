use super::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
enum Value {
    Number(i128),
    Text(String),
}

pub(super) fn check(input: &RecordBatch, output: &RecordBatch, frame: i64, offset: i64) {
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
    let column = input.column(2);
    let values: Vec<Option<Value>> = match column.data_type() {
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| Value::Text(array.value(row).to_owned())))
                .collect()
        }
        DataType::Decimal128(_, _) => {
            let array = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| Value::Number(array.value(row))))
                .collect()
        }
        ty if streamfusion_bridge::timestamp::is_timestamp(ty) => {
            let array =
                streamfusion_bridge::timestamp::TimestampColumn::try_new(column.as_ref()).unwrap();
            (0..column.len())
                .map(|row| {
                    (!array.is_null(row)).then(|| Value::Number(array.value(row).unwrap().nanos()))
                })
                .collect()
        }
        _ => unreachable!(),
    };
    let mut membership: BTreeMap<i64, BTreeSet<Value>> = BTreeMap::new();
    let mut history: BTreeMap<i64, VecDeque<usize>> = BTreeMap::new();
    let mut expected = Vec::new();
    for row in 0..input.num_rows() {
        let key = keys.value(row);
        let count = if frame == 0 || frame == 3 {
            let set = membership.entry(key).or_default();
            if let Some(value) = &values[row] {
                set.insert(value.clone());
            }
            set.len()
        } else {
            let records = history.entry(key).or_default();
            records.push_back(row);
            while if frame == 1 {
                records.len() > offset as usize + 1
            } else {
                records
                    .front()
                    .is_some_and(|&first| times.value(row) - times.value(first) > offset)
            } {
                records.pop_front();
            }
            records
                .iter()
                .filter_map(|&index| values[index].clone())
                .collect::<BTreeSet<_>>()
                .len()
        };
        expected.push(count as i64);
    }
    let result_times = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = output
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut seen = BTreeSet::new();
    assert_eq!(output.num_rows(), input.num_rows());
    for index in 0..output.num_rows() {
        let row = usize::try_from(result_times.value(index) - times.value(0)).unwrap();
        assert!(row < input.num_rows() && seen.insert(row));
        assert!(!counts.is_null(index));
        assert_eq!(
            counts.value(index),
            expected[row],
            "frame={frame}, row={row}"
        );
    }
}
