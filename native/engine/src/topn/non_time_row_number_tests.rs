use super::*;

fn batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![("v", Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

fn ranker() -> TopNRanker {
    let mut ranker = TopNRanker::new(
        vec![],
        vec![SortColumn {
            index: 0,
            ascending: true,
            nulls_first: true,
        }],
        i64::MAX,
        true,
        false,
    );
    ranker.enable_non_time_row_number();
    ranker
}

fn changes(batch: &RecordBatch) -> Vec<(i8, Option<i64>, i64)> {
    if batch.num_rows() == 0 {
        return vec![];
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = row_kind_column(batch).unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                kinds.value(i),
                (!values.is_null(i)).then(|| values.value(i)),
                ranks.value(i),
            )
        })
        .collect()
}

#[test]
fn inserted_row_keeps_insert_kind_and_existing_rows_keep_identity() {
    let mut ranker = ranker();
    assert_eq!(
        changes(
            &ranker
                .push(&batch(vec![Some(30), Some(10), Some(10)]), 0)
                .unwrap()
        ),
        vec![
            (0, Some(30), 1),
            (0, Some(10), 1),
            (1, Some(30), 1),
            (2, Some(30), 2),
            (0, Some(10), 2),
            (1, Some(30), 2),
            (2, Some(30), 3)
        ]
    );
    assert_eq!(
        changes(&ranker.push(&batch(vec![None]), 0).unwrap()),
        vec![
            (0, None, 1),
            (1, Some(10), 1),
            (2, Some(10), 2),
            (1, Some(10), 2),
            (2, Some(10), 3),
            (1, Some(30), 3),
            (2, Some(30), 4)
        ]
    );
}

#[test]
fn all_rows_survive_raw_restore_and_continue_ranking() {
    let mut original = ranker();
    original
        .push(&batch((0..2048).map(Some).collect()), 0)
        .unwrap();
    assert_eq!(
        original
            .groups
            .iter()
            .map(|(_, rows)| rows.len())
            .sum::<usize>(),
        2048
    );
    let snapshots: Vec<_> = original.snapshot_partitions(128).into_values().collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![],
        vec![],
        original.sort_columns.clone(),
        i64::MAX,
        true,
        false,
        &snapshots,
        0,
    );
    restored.enable_non_time_row_number();
    let suffix = batch(vec![Some(2048), Some(-1), None, Some(0)]);
    assert_eq!(
        changes(&original.push(&suffix, 0).unwrap()),
        changes(&restored.push(&suffix, 0).unwrap())
    );
    assert_eq!(
        restored
            .groups
            .iter()
            .map(|(_, rows)| rows.len())
            .sum::<usize>(),
        2052
    );
}

#[test]
fn empty_input_and_memory_budget_use_the_existing_operator_contract() {
    let mut empty = ranker();
    assert_eq!(empty.push(&batch(vec![]), 0).unwrap().num_rows(), 0);
    let mut limited = ranker().with_memory_budget(64).unwrap();
    assert!(limited.push(&batch(vec![Some(1), Some(2)]), 0).is_err());
}
