use super::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    let values = input
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let mut membership: BTreeMap<i64, BTreeSet<i128>> = BTreeMap::new();
    let mut history: BTreeMap<i64, VecDeque<usize>> = BTreeMap::new();
    let mut expected = Vec::new();
    for row in 0..input.num_rows() {
        let key = keys.value(row);
        let count = if frame == 0 || frame == 3 {
            let set = membership.entry(key).or_default();
            if !values.is_null(row) {
                set.insert(values.value(row));
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
                .filter(|&&index| !values.is_null(index))
                .map(|&index| values.value(index))
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
