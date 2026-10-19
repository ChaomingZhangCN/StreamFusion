use super::*;
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};

fn batch(times: &[i64], valid: &[bool]) -> RecordBatch {
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(
            valid
                .iter()
                .map(|&present| present.then_some(DECIMAL128_MAX - 1))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(38, 38)
        .unwrap(),
    );
    let strings: ArrayRef = Arc::new(StringArray::from(
        valid
            .iter()
            .map(|&present| present.then_some("😀\u{e000}".repeat(128)))
            .collect::<Vec<_>>(),
    ));
    let timestamps: ArrayRef = Arc::new(timestamp_array(valid.iter().map(|&present| {
        present.then(|| TimestampValue::new(253_402_300_799_999, 999_999).unwrap())
    })));
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(vec![1; times.len()])) as ArrayRef,
        ),
        ("rt", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
        ("d", decimals),
        ("s", strings),
        ("t", timestamps),
    ])
    .unwrap()
}

#[test]
fn count_reads_membership_across_frames_and_canonical_restore() {
    for frame in 0..4 {
        for proctime in [false, true] {
            if proctime && frame == 2 {
                continue;
            }
            let offset = if frame == 1 { 1 } else { 100 };
            let mut before = OverWindowAggregator::new(
                vec![5838, 3, 1009],
                vec![3; 3],
                1,
                vec![2, 3, 4],
                vec![0],
                frame,
                offset,
                proctime,
            );
            let first = batch(&[100, 100], &[true, false]);
            let out = if proctime {
                before.push_proctime(first, 0).unwrap()
            } else {
                before.push(first, 0).unwrap();
                before.flush(100, 0).unwrap()
            };
            for column in 5..8 {
                let actual = out
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(actual.values().as_ref(), &[1, 1]);
            }
            let snapshots = before
                .snapshot_partitions(8, &[-1])
                .into_values()
                .collect::<Vec<_>>();
            let mut restored = OverWindowAggregator::restore_partitions(
                vec![5838, 3, 1009],
                vec![3; 3],
                1,
                vec![2, 3, 4],
                vec![0],
                frame,
                offset,
                proctime,
                &snapshots,
                0,
                0,
            );
            let second = batch(&[200, 300], &[true, false]);
            let out = if proctime {
                restored.push_proctime(second, 0).unwrap()
            } else {
                restored.push(second, 0).unwrap();
                restored.flush(300, 0).unwrap()
            };
            let expected: &[i64] = if matches!(frame, 0 | 3) {
                &[2, 2]
            } else if frame == 2 {
                &[2, 1]
            } else {
                &[1, 1]
            };
            for column in 5..8 {
                let actual = out
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(
                    actual.values().as_ref(),
                    expected,
                    "frame {frame}, proctime {proctime}"
                );
            }
        }
    }
}

#[test]
fn typed_count_membership_obeys_frame_retention() {
    for frame in 0..4 {
        for proctime in [false, true] {
            if proctime && frame == 2 {
                continue;
            }
            let mut over = OverWindowAggregator::new(
                vec![5838, 3, 1009],
                vec![3; 3],
                1,
                vec![2, 3, 4],
                vec![0],
                frame,
                1000,
                proctime,
            )
            .with_state_retention(2000);
            let first = batch(&[100], &[true]);
            if proctime {
                over.push_proctime(first, 5000).unwrap();
            } else {
                over.push(first, 5000).unwrap();
                over.flush(100, 5000).unwrap();
            }
            let second = batch(&[200], &[true]);
            let out = if proctime {
                over.push_proctime(second, 8000).unwrap()
            } else {
                over.push(second, 8000).unwrap();
                over.flush(200, 8000).unwrap()
            };
            for column in 5..8 {
                let count = out
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(
                    count,
                    if frame == 2 { 2 } else { 1 },
                    "frame {frame}, proctime {proctime}"
                );
            }
        }
    }
}

fn reductions(
    times: &[i64],
    integers: Vec<Option<i64>>,
    decimals: Vec<Option<i128>>,
    precision: u8,
    scale: i8,
) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(vec![1; times.len()])) as ArrayRef,
        ),
        ("rt", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
        ("n", Arc::new(Int64Array::from(integers)) as ArrayRef),
        (
            "d",
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn running_mixed_reductions_restore_raw_sums_counts_and_result_scales() {
    for frame in [0, 3] {
        for proctime in [false, true] {
            let create = || {
                OverWindowAggregator::new(
                    vec![0, 3202, 3202, 0, 3202],
                    vec![3, 0, 22, 4, 4],
                    1,
                    vec![2, 3, 3, 2, 3],
                    vec![0],
                    frame,
                    0,
                    proctime,
                )
            };
            let mut over = create();
            let batches = [
                reductions(&[100], vec![None], vec![None], 12, 2),
                reductions(
                    &[200, 300, 400],
                    vec![Some(10), Some(20), None],
                    vec![Some(125), Some(250), None],
                    12,
                    2,
                ),
                reductions(&[500], vec![Some(7)], vec![Some(-25)], 12, 2),
            ];
            for (index, batch) in batches.into_iter().enumerate() {
                let watermark = column_i64(&batch, "rt")
                    .values()
                    .iter()
                    .copied()
                    .max()
                    .unwrap();
                let out = if proctime {
                    over.push_proctime(batch, 0).unwrap()
                } else {
                    over.push(batch, 0).unwrap();
                    over.flush(watermark, 0).unwrap()
                };
                let row = out.num_rows() - 1;
                let scalar = |column| ScalarValue::try_from_array(out.column(column), row).unwrap();
                let count = [0, 2, 3][index];
                assert_eq!(scalar(4), ScalarValue::Int64(Some(count)));
                assert_eq!(
                    scalar(5),
                    ScalarValue::Decimal128([None, Some(375), Some(350)][index], 38, 2)
                );
                assert_eq!(
                    scalar(6),
                    ScalarValue::Decimal128(Some([0, 375, 350][index]), 38, 2)
                );
                assert_eq!(
                    scalar(7),
                    ScalarValue::Int64([None, Some(15), Some(12)][index])
                );
                assert_eq!(
                    scalar(8),
                    ScalarValue::Decimal128([None, Some(1_875_000), Some(1_166_667)][index], 38, 6)
                );
                let snapshots = over
                    .snapshot_partitions(8, &[-1])
                    .into_values()
                    .collect::<Vec<_>>();
                over = OverWindowAggregator::restore_partitions(
                    vec![0, 3202, 3202, 0, 3202],
                    vec![3, 0, 22, 4, 4],
                    1,
                    vec![2, 3, 3, 2, 3],
                    vec![0],
                    frame,
                    0,
                    proctime,
                    &snapshots,
                    0,
                    0,
                );
            }
        }
    }
}

#[test]
fn bounded_decimal_folds_restore_and_retract_the_full_previous_frame() {
    for (frame, proctime) in [(1, false), (1, true), (2, false)] {
        let offset = if frame == 1 { 1 } else { 100 };
        let mut over = OverWindowAggregator::new(
            vec![3202; 3],
            vec![0, 22, 4],
            1,
            vec![3; 3],
            vec![0],
            frame,
            offset,
            proctime,
        );
        let first = reductions(
            &[100, 200, 300],
            vec![None; 3],
            vec![None, Some(125), Some(250)],
            12,
            2,
        );
        let out = if proctime {
            over.push_proctime(first, 0).unwrap()
        } else {
            over.push(first, 0).unwrap();
            over.flush(300, 0).unwrap()
        };
        assert_eq!(
            ScalarValue::try_from_array(out.column(4), 2).unwrap(),
            ScalarValue::Decimal128(Some(375), 38, 2)
        );
        let snapshots = over
            .snapshot_partitions(8, &[-1])
            .into_values()
            .collect::<Vec<_>>();
        let mut restored = OverWindowAggregator::restore_partitions(
            vec![3202; 3],
            vec![0, 22, 4],
            1,
            vec![3; 3],
            vec![0],
            frame,
            offset,
            proctime,
            &snapshots,
            0,
            0,
        );
        let second = reductions(&[400], vec![None], vec![Some(-25)], 12, 2);
        let out = if proctime {
            restored.push_proctime(second, 0).unwrap()
        } else {
            restored.push(second, 0).unwrap();
            restored.flush(400, 0).unwrap()
        };
        assert_eq!(
            ScalarValue::try_from_array(out.column(4), 0).unwrap(),
            ScalarValue::Decimal128(Some(225), 38, 2)
        );
        assert_eq!(
            ScalarValue::try_from_array(out.column(5), 0).unwrap(),
            ScalarValue::Decimal128(Some(225), 38, 2)
        );
        assert_eq!(
            ScalarValue::try_from_array(out.column(6), 0).unwrap(),
            ScalarValue::Decimal128(Some(1_125_000), 38, 6)
        );
    }
}

#[test]
fn decimal_average_result_overflow_does_not_replace_the_checkpointed_sum() {
    for frame in [0, 3] {
        let mut over =
            OverWindowAggregator::new(vec![5800], vec![4], 1, vec![3], vec![0], frame, 0, false);
        over.push(
            reductions(&[100], vec![None], vec![Some(DECIMAL128_MAX - 1)], 38, 0),
            0,
        )
        .unwrap();
        let out = over.flush(100, 0).unwrap();
        assert!(out.column(4).is_null(0));
        let snapshots = over
            .snapshot_partitions(8, &[-1])
            .into_values()
            .collect::<Vec<_>>();
        let mut restored = OverWindowAggregator::restore_partitions(
            vec![5800],
            vec![4],
            1,
            vec![3],
            vec![0],
            frame,
            0,
            false,
            &snapshots,
            0,
            0,
        );
        restored
            .push(
                reductions(&[200], vec![None], vec![Some(-(DECIMAL128_MAX - 1))], 38, 0),
                0,
            )
            .unwrap();
        let out = restored.flush(200, 0).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(out.column(4), 0).unwrap(),
            ScalarValue::Decimal128(Some(0), 38, 6)
        );
    }
}

#[test]
fn decimal_extrema_and_first_last_keep_input_precision_through_every_frame_restore() {
    for (precision, scale) in [(12, 2), (38, 0), (38, 38)] {
        let code = 2000 + precision as i64 * 100 + scale as i64;
        for frame in 0..4 {
            for proctime in [false, true] {
                if proctime && frame == 2 {
                    continue;
                }
                let offset = if frame == 1 { 1 } else { 100 };
                let mut before = OverWindowAggregator::new(
                    vec![code; 4],
                    vec![1, 2, 5, 6],
                    1,
                    vec![3; 4],
                    vec![0],
                    frame,
                    offset,
                    proctime,
                );
                let first = reductions(
                    &[100, 200, 300, 300],
                    vec![None; 4],
                    vec![None, Some(250), Some(-100), Some(-100)],
                    precision,
                    scale,
                );
                let out = if proctime {
                    before.push_proctime(first, 0).unwrap()
                } else {
                    before.push(first, 0).unwrap();
                    before.flush(300, 0).unwrap()
                };
                for column in 4..8 {
                    assert_eq!(
                        out.column(column).data_type(),
                        &DataType::Decimal128(precision, scale)
                    );
                    assert!(out.column(column).is_null(0));
                }
                let snapshots = before
                    .snapshot_partitions(8, &[-1])
                    .into_values()
                    .collect::<Vec<_>>();
                let mut after = OverWindowAggregator::restore_partitions(
                    vec![code; 4],
                    vec![1, 2, 5, 6],
                    1,
                    vec![3; 4],
                    vec![0],
                    frame,
                    offset,
                    proctime,
                    &snapshots,
                    0,
                    0,
                );
                let next = reductions(
                    &[400, 500],
                    vec![None; 2],
                    vec![None, Some(125)],
                    precision,
                    scale,
                );
                let out = if proctime {
                    after.push_proctime(next, 0).unwrap()
                } else {
                    after.push(next, 0).unwrap();
                    after.flush(500, 0).unwrap()
                };
                let expected = if matches!(frame, 0 | 3) {
                    [-100, 250, 250, 125]
                } else {
                    [125; 4]
                };
                for (slot, value) in expected.into_iter().enumerate() {
                    assert_eq!(
                        ScalarValue::try_from_array(out.column(4 + slot), 1).unwrap(),
                        ScalarValue::Decimal128(Some(value), precision, scale)
                    );
                }
            }
        }
    }
}

#[test]
fn timestamp_values_keep_wide_years_fractional_order_and_frame_state() {
    let earliest = TimestampValue::new(-62_135_596_800_000, 1).unwrap();
    let middle = TimestampValue::new(0, 2).unwrap();
    let latest = TimestampValue::new(253_402_300_799_999, 999_999).unwrap();
    let make = |times: &[i64], values: Vec<Option<TimestampValue>>| {
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![1; times.len()])) as ArrayRef,
            ),
            ("rt", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
            ("v", Arc::new(timestamp_array(values)) as ArrayRef),
        ])
        .unwrap()
    };
    for frame in 0..4 {
        for proctime in [false, true] {
            if proctime && frame == 2 {
                continue;
            }
            let offset = if frame == 1 { 1 } else { 100 };
            let mut over = OverWindowAggregator::new(
                vec![1009; 4],
                vec![1, 2, 5, 6],
                1,
                vec![2; 4],
                vec![0],
                frame,
                offset,
                proctime,
            );
            let first = make(
                &[100, 200, 300, 300],
                vec![None, Some(middle), Some(earliest), Some(earliest)],
            );
            let out = if proctime {
                over.push_proctime(first, 0).unwrap()
            } else {
                over.push(first, 0).unwrap();
                over.flush(300, 0).unwrap()
            };
            for column in 3..7 {
                assert!(out.column(column).is_null(0));
            }
            let snapshots = over
                .snapshot_partitions(8, &[-1])
                .into_values()
                .collect::<Vec<_>>();
            let mut restored = OverWindowAggregator::restore_partitions(
                vec![1009; 4],
                vec![1, 2, 5, 6],
                1,
                vec![2; 4],
                vec![0],
                frame,
                offset,
                proctime,
                &snapshots,
                0,
                0,
            );
            let next = make(&[400, 500], vec![None, Some(latest)]);
            let out = if proctime {
                restored.push_proctime(next, 0).unwrap()
            } else {
                restored.push(next, 0).unwrap();
                restored.flush(500, 0).unwrap()
            };
            let expected = if matches!(frame, 0 | 3) {
                [earliest, latest, middle, latest]
            } else {
                [latest; 4]
            };
            for (slot, value) in expected.into_iter().enumerate() {
                assert_eq!(
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(
                        out.column(3 + slot).as_ref()
                    )
                    .unwrap()
                    .value(1)
                    .unwrap(),
                    value
                );
            }
        }
    }
}

#[test]
fn typed_extrema_first_last_follow_idle_cleanup_and_range_retention_policy() {
    for frame in 0..4 {
        for proctime in [false, true] {
            if proctime && frame == 2 {
                continue;
            }
            let first = TimestampValue::new(-62_135_596_800_000, 1).unwrap();
            let next = TimestampValue::new(253_402_300_799_999, 999_999).unwrap();
            let make = |time, value| {
                RecordBatch::try_from_iter(vec![
                    ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                    ("rt", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
                    ("v", Arc::new(timestamp_array([Some(value)])) as ArrayRef),
                ])
                .unwrap()
            };
            let mut over = OverWindowAggregator::new(
                vec![1009; 4],
                vec![1, 2, 5, 6],
                1,
                vec![2; 4],
                vec![0],
                frame,
                1000,
                proctime,
            )
            .with_state_retention(2000);
            if proctime {
                over.push_proctime(make(100, first), 5000).unwrap();
            } else {
                over.push(make(100, first), 5000).unwrap();
                over.flush(100, 5000).unwrap();
            }
            let out = if proctime {
                over.push_proctime(make(200, next), 8000).unwrap()
            } else {
                over.push(make(200, next), 8000).unwrap();
                over.flush(200, 8000).unwrap()
            };
            let expected = if frame == 2 {
                [first, next, first, next]
            } else {
                [next; 4]
            };
            for (slot, value) in expected.into_iter().enumerate() {
                assert_eq!(
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(
                        out.column(3 + slot).as_ref()
                    )
                    .unwrap()
                    .value(0)
                    .unwrap(),
                    value
                );
            }
        }
    }
}

fn string_values(times: &[i64], values: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(vec![1; times.len()])) as ArrayRef,
        ),
        ("rt", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
        ("v", Arc::new(StringArray::from(values)) as ArrayRef),
    ])
    .unwrap()
}

#[test]
fn strings_keep_binary_order_nulls_empty_values_and_frame_checkpoint_state() {
    for frame in 0..4 {
        for proctime in [false, true] {
            if proctime && frame == 2 {
                continue;
            }
            let offset = if frame == 1 { 1 } else { 100 };
            let mut before = OverWindowAggregator::new(
                vec![3; 4],
                vec![1, 2, 5, 6],
                1,
                vec![2; 4],
                vec![0],
                frame,
                offset,
                proctime,
            );
            let input = string_values(
                &[100, 200, 300, 300],
                vec![None, Some("😀"), Some("\u{e000}"), Some("\u{e000}")],
            );
            let out = if proctime {
                before.push_proctime(input, 0).unwrap()
            } else {
                before.push(input, 0).unwrap();
                before.flush(300, 0).unwrap()
            };
            for column in 3..7 {
                assert!(out.column(column).is_null(0));
            }
            // Both values are reachable in every frame at the first U+E000 row. Byte ordering
            // differs from Java cached String ordering, so the non-BMP value is the maximum.
            assert_eq!(
                ScalarValue::try_from_array(out.column(3), 2).unwrap(),
                ScalarValue::Utf8(Some("\u{e000}".into()))
            );
            assert_eq!(
                ScalarValue::try_from_array(out.column(4), 2).unwrap(),
                ScalarValue::Utf8(Some("😀".into()))
            );
            let partitions = before
                .snapshot_partitions(8, &[-1])
                .into_values()
                .collect::<Vec<_>>();
            let mut after = OverWindowAggregator::restore_partitions(
                vec![3; 4],
                vec![1, 2, 5, 6],
                1,
                vec![2; 4],
                vec![0],
                frame,
                offset,
                proctime,
                &partitions,
                0,
                0,
            );
            let input = string_values(&[400, 500], vec![None, Some("")]);
            let out = if proctime {
                after.push_proctime(input, 0).unwrap()
            } else {
                after.push(input, 0).unwrap();
                after.flush(500, 0).unwrap()
            };
            let expected = if matches!(frame, 0 | 3) {
                ["", "😀", "😀", ""]
            } else {
                [""; 4]
            };
            for (slot, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    ScalarValue::try_from_array(out.column(3 + slot), 1).unwrap(),
                    ScalarValue::Utf8(Some(expected.into())),
                    "frame {frame}, proctime {proctime}"
                );
            }
        }
    }
}

#[test]
fn string_winners_compare_before_copy_and_bounded_folds_share_frame_ownership() {
    let mut state = OverAggState::new(1, &DataType::Utf8);
    state.fold_text("a");
    let RunningAgg::OverStringValue {
        value: Some(first), ..
    } = &state.agg
    else {
        panic!()
    };
    let first = first.clone();
    state.fold_text(&"z".repeat(4096));
    let RunningAgg::OverStringValue {
        value: Some(current),
        ..
    } = &state.agg
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&first, current));
    let shared: Arc<str> = Arc::from("");
    state.fold_shared_text(&shared);
    let RunningAgg::OverStringValue {
        value: Some(current),
        ..
    } = &state.agg
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&shared, current));
}

#[test]
fn string_owned_memory_grows_shrinks_evicts_and_recomputes_after_restore() {
    let long = "😀".repeat(4096);
    let projected = |time, value| {
        let batch = string_values(&[time], vec![Some(value)]);
        RecordBatch::try_from_iter(vec![
            ("key0", batch.column(0).clone()),
            ("rt", batch.column(1).clone()),
            ("value0", batch.column(2).clone()),
        ])
        .unwrap()
    };
    let mut running = OverAggregator::new(vec![3], vec![6]);
    running.track = true;
    running.update(&projected(100, "a"));
    let small = running.bytes;
    running.update(&projected(200, &long));
    assert_eq!(running.bytes, small + long.len() - 1);
    running.update(&projected(300, ""));
    assert_eq!(running.bytes, small - 1);
    let mut bounded = BoundedOverAggregator::new(vec![3], vec![1], true, 1);
    bounded.track = true;
    bounded.update(&projected(100, &long));
    let wide = bounded.bytes;
    bounded.update(&projected(200, ""));
    assert!(bounded.bytes < wide);
    let measured = bounded.bytes;
    bounded.recompute_bytes();
    assert_eq!(bounded.bytes, measured);
    let raw = bounded.snapshot(None);
    let mut restored =
        BoundedOverAggregator::restore(vec![3], vec![1], true, 1, &raw, &mut HashMap::default());
    restored.recompute_bytes();
    let restored_wide = restored.bytes;
    assert!(restored_wide >= long.len());
    restored.track = true;
    restored.update(&projected(300, ""));
    assert!(restored.bytes < restored_wide - long.len());
    let after_eviction = restored.bytes;
    restored.recompute_bytes();
    assert_eq!(restored.bytes, after_eviction);
    let key = restored.keys.keys().next().unwrap().clone();
    let mut inner = OverInner::Bounded(restored);
    inner.clear_key(&key.0);
    assert_eq!(inner.state_bytes(), 0);
    let key = running.keys.keys().next().unwrap().clone();
    let mut inner = OverInner::Aggregates(running);
    inner.clear_key(&key.0);
    assert_eq!(inner.state_bytes(), 0);
}

#[test]
fn range_map_tracks_pending_capacity_restore_conventions_and_cleanup() {
    let mut over = OverWindowAggregator::new(
        vec![5800; 3],
        vec![0, 22, 4],
        1,
        vec![3; 3],
        vec![0],
        2,
        1000,
        false,
    )
    .with_memory_budget(1 << 20)
    .unwrap();
    let first = reductions(
        &[100, 200, 400, 2000],
        vec![None; 4],
        vec![Some(DECIMAL128_MAX), Some(-1), Some(2), Some(7)],
        38,
        0,
    );
    over.push(first, 0).unwrap();
    assert!(
        over.inner.state_bytes() > 0,
        "pending timestamp map must be budgeted"
    );
    over.flush(400, 0).unwrap();
    let measured = over.inner.state_bytes();
    over.inner.start_tracking();
    assert_eq!(over.inner.state_bytes(), measured);
    let snapshots: Vec<Vec<u8>> = over.snapshot_partitions(128, &[-1]).into_values().collect();
    for canonical in [false, true] {
        let mut after = OverWindowAggregator::restore_partitions(
            vec![5800; 3],
            vec![0, 22, 4],
            1,
            vec![3; 3],
            vec![0],
            2,
            1000,
            false,
            &snapshots,
            0,
            0,
        )
        .with_memory_budget(1 << 20)
        .unwrap();
        if canonical {
            after.restore_canonical_range_order(&snapshots).unwrap();
        }
        let out = after.flush(2000, 0).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(out.column(4), 0).unwrap(),
            ScalarValue::Decimal128(Some(if canonical { 8 } else { 7 }), 38, 0)
        );
        let measured = after.inner.state_bytes();
        after.inner.start_tracking();
        assert_eq!(after.inner.state_bytes(), measured);
        after.flush(3501, 0).unwrap();
        assert_eq!(after.inner.state_bytes(), 0);
    }
}

#[test]
fn cached_running_string_reductions_match_released_heap_operator_traces() {
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../src/test/resources/over-string-cache-oracle.json"
    ))
    .unwrap();
    let text = [
        Some("😀"),
        Some("\u{e000}"),
        Some("\u{e100}"),
        Some("😀"),
        None,
        Some("\u{e000}"),
        Some("tail"),
    ];
    for frame in [0, 3] {
        for proctime in [false, true] {
            let expected = &oracle[format!("{frame}:{proctime}:false")];
            for recovered in [false, true] {
                let mut states = [1, 2, 5, 6].map(|kind| OverAggState::new(kind, &DataType::Utf8));
                let mut rows = Vec::new();
                for (step, value) in text.iter().enumerate() {
                    if recovered && step == 2 {
                        for state in &mut states {
                            let saved = state.emit();
                            state.agg.restore_value(&saved);
                        }
                    }
                    if let Some(value) = value {
                        let cache = (step < 2 || step == 3)
                            .then(|| value.encode_utf16().collect::<Vec<_>>());
                        for state in &mut states {
                            state.fold_cached_text(value, cache.as_deref());
                        }
                    }
                    if !recovered || step >= 2 {
                        let row = states
                            .iter()
                            .map(|state| match state.emit() {
                                ScalarValue::Utf8(value) => value,
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>();
                        rows.push(vec![row]);
                    }
                }
                assert_eq!(
                    serde_json::to_value(rows).unwrap(),
                    expected[usize::from(recovered)]
                );
            }
        }
    }
}

#[test]
fn cached_string_winners_keep_original_utf16_and_reject_losers_before_copy() {
    let mut min = OverAggState::new(1, &DataType::Utf8);
    let mut max = OverAggState::new(2, &DataType::Utf8);
    for state in [&mut min, &mut max] {
        state.fold_cached_text("?", Some(&[0xd800]));
        state.fold_cached_text("?", Some(&[0xd801]));
    }
    let RunningAgg::OverStringValue {
        java_utf16: Some(first),
        ..
    } = &min.agg
    else {
        panic!()
    };
    let first = first.clone();
    assert_eq!(first.as_ref(), &[0xd800]);
    let RunningAgg::OverStringValue {
        java_utf16: Some(last),
        ..
    } = &max.agg
    else {
        panic!()
    };
    assert_eq!(last.as_ref(), &[0xd801]);
    min.fold_cached_text("?", Some(&[0xdfff; 4096]));
    let RunningAgg::OverStringValue {
        java_utf16: Some(current),
        ..
    } = &min.agg
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&first, current));
    assert_eq!(min.text_bytes(), 1 + 2 + 4 * std::mem::size_of::<usize>());
    let saved = min.emit();
    min.agg.restore_value(&saved);
    let RunningAgg::OverStringValue { java_utf16, .. } = &min.agg else {
        panic!()
    };
    assert!(java_utf16.is_none());
    assert_eq!(min.text_bytes(), 1 + 2 * std::mem::size_of::<usize>());
}

#[test]
fn cached_companions_use_metadata_and_sliced_offsets() {
    let cache = string_cache_array([
        Some(&[][..]),
        None,
        Some(&[0xd800][..]),
        Some(&[0xd83d, 0xde00][..]),
    ]);
    let sliced = cache.slice(1, 3);
    let reader = JavaStringCacheColumn::new(&sliced);
    assert_eq!(reader.value(0), None);
    assert_eq!(reader.value(1), Some(&[0xd800][..]));
    assert_eq!(reader.value(2), Some(&[0xd83d, 0xde00][..]));
    let batch = RecordBatch::try_from_iter(vec![(
        "$string_java_0$",
        Arc::new(StringArray::from(vec!["ordinary SQL field"])) as ArrayRef,
    )])
    .unwrap();
    assert_eq!(data_arity(&batch), 1);
    assert!(!has_string_caches(&batch));
    assert_eq!(strip_string_caches(&batch), batch);
}

#[test]
fn bounded_first_last_share_cache_ownership_budget_payloads_and_restore_binary_frames() {
    let units = [0xd83d, 0xde00];
    let cache = string_cache_array([Some(&units[..]), Some(&units[..])]);
    let fields = vec![
        Field::new("rt", DataType::Int64, false),
        Field::new("value0", DataType::Utf8, true),
        Field::new("value1", DataType::Utf8, true),
        string_cache_field(1, cache.data_type().clone()),
        string_cache_field(2, cache.data_type().clone()),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![100, 150])),
            Arc::new(StringArray::from(vec!["😀", "😀"])),
            Arc::new(StringArray::from(vec!["😀", "😀"])),
            cache.clone(),
            cache,
        ],
    )
    .unwrap();
    let mut inner = BoundedOverAggregator::new(vec![3; 2], vec![5, 6], false, 100);
    inner.track = true;
    let out = inner.update(&batch);
    assert_eq!(
        JavaStringCacheColumn::new(string_cache_column(&out, 0).unwrap()).value(1),
        Some(&units[..])
    );
    let row = &inner.keys.values().next().unwrap()[0];
    let owned = row.java_cache(0).unwrap().clone();
    let mut winner = OverAggState::new(5, &DataType::Utf8);
    winner.fold_shared_cached_text(row.text_value(0).unwrap(), row.java_cache(0));
    let RunningAgg::OverStringValue {
        java_utf16: Some(shared),
        ..
    } = &winner.agg
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&owned, shared));
    let measured = inner.bytes;
    inner.recompute_bytes();
    assert_eq!(inner.bytes, measured);
    let snapshot = inner.snapshot(None);
    let mut restored = BoundedOverAggregator::restore(
        vec![3; 2],
        vec![5, 6],
        false,
        100,
        &snapshot,
        &mut HashMap::default(),
    );
    assert!(restored
        .keys
        .values()
        .flatten()
        .all(|row| row.java_cache(0).is_none()));
    restored.track = true;
    restored.recompute_bytes();
    assert!(restored.bytes < measured);
    inner.offset = 0;
    inner.evict();
    let measured = inner.bytes;
    inner.recompute_bytes();
    assert_eq!(inner.bytes, measured);
}

#[test]
fn mixed_decimal_and_cached_string_folds_share_expired_timestamp_membership() {
    let texts = [
        Some("😀"),
        Some("\u{e000}"),
        Some("a"),
        Some("😀"),
        Some("😀"),
        Some("\u{e100}"),
        None,
    ];
    let units = texts
        .iter()
        .enumerate()
        .map(|(index, text)| {
            (index != 5)
                .then(|| text.map(|text| text.encode_utf16().collect::<Vec<_>>()))
                .flatten()
        })
        .collect::<Vec<_>>();
    let cache = string_cache_array(units.iter().map(|value| value.as_deref()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("value0", DataType::Decimal128(38, 0), true),
            Field::new("value1", DataType::Utf8, true),
            string_cache_field(2, cache.data_type().clone()),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![
                100, 200, 400, 1050, 1060, 1070, 1500,
            ])),
            Arc::new(
                Decimal128Array::from(vec![1, 2, 3, 4, 5, 6, 7])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(StringArray::from(texts.to_vec())),
            cache,
        ],
    )
    .unwrap();
    let mut inner = BoundedOverAggregator::new(vec![5800, 3], vec![0, 1], false, 1000);
    inner.track = true;
    let out = inner.update(&batch);
    assert_eq!(
        ScalarValue::try_from_array(out.column(0), 6).unwrap(),
        ScalarValue::Decimal128(Some(22), 38, 0)
    );
    assert_eq!(
        ScalarValue::try_from_array(out.column(1), 6).unwrap(),
        ScalarValue::Utf8(Some("😀".into()))
    );
    assert_eq!(
        JavaStringCacheColumn::new(string_cache_column(&out, 1).unwrap()).value(6),
        Some(&[0xd83d, 0xde00][..])
    );
    let keys = inner
        .range_order
        .as_ref()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .keys();
    assert_eq!(keys.len(), 4);
    assert!(keys.iter().all(|&time| time >= 1050));
    let measured = inner.bytes;
    inner.recompute_bytes();
    assert_eq!(inner.bytes, measured);
}

#[test]
fn selected_string_caches_survive_later_batches_without_cache_columns() {
    let cached = string_cache_array([Some(&[97u16][..]), Some(&[122u16][..])]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("rt", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        string_cache_field(2, cached.data_type().clone()),
    ]));
    let seed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0, 0])),
            Arc::new(Int64Array::from(vec![100, 101])),
            Arc::new(StringArray::from(vec!["a", "z"])),
            cached,
        ],
    )
    .unwrap();
    let tail = RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(vec![102, 103])) as ArrayRef),
        (
            "v",
            Arc::new(StringArray::from(vec![Some("m"), None])) as ArrayRef,
        ),
    ])
    .unwrap();
    assert!(!has_string_caches(&tail));
    for frame in 0..4 {
        let mut aggregate = OverWindowAggregator::new(
            vec![3; 4],
            vec![1, 2, 5, 6],
            1,
            vec![2; 4],
            vec![0],
            frame,
            10,
            false,
        );
        aggregate.push(seed.clone(), 0).unwrap();
        aggregate.flush(101, 0).unwrap();
        aggregate.push(tail.clone(), 0).unwrap();
        let output = aggregate.flush(103, 0).unwrap();
        assert_eq!(output.num_rows(), 2);
        for (slot, text, units) in [
            (0, "a", Some(&[97u16][..])),
            (1, "z", Some(&[122u16][..])),
            (2, "a", Some(&[97u16][..])),
            (3, "m", None),
        ] {
            let values = output
                .column(slot + 3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..2 {
                assert_eq!(values.value(row), text, "frame {frame} slot {slot}");
                let cache = string_cache_column(&output, slot + 3).map(JavaStringCacheColumn::new);
                assert_eq!(
                    cache.as_ref().and_then(|cache| cache.value(row)),
                    units,
                    "frame {frame} slot {slot} row {row}"
                );
            }
        }
    }
}

#[test]
fn distinct_text_counts_use_binary_membership_and_account_retained_keys() {
    let long = "😀".repeat(4096);
    let projected = |time, value| {
        let batch = string_values(&[time], vec![Some(value)]);
        RecordBatch::try_from_iter(vec![
            ("key0", batch.column(0).clone()),
            ("rt", batch.column(1).clone()),
            ("value0", batch.column(2).clone()),
        ])
        .unwrap()
    };
    let mut running = OverAggregator::new(vec![3], vec![103]);
    running.track = true;
    let first = running.update(&projected(100, &long));
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(running.bytes >= long.len());
    let once = running.bytes;
    running.update(&projected(200, &long));
    assert_eq!(running.bytes, once);
    running.update(&projected(300, ""));
    running.update(&projected(400, "?"));
    let measured = running.bytes;
    running.recompute_bytes();
    assert_eq!(running.bytes, measured);
    let raw = running.snapshot(None);
    let mut restored = OverAggregator::restore(vec![3], vec![103], &raw, &mut HashMap::default());
    restored.track = true;
    restored.recompute_bytes();
    assert!(restored.bytes >= long.len());
    let restored_bytes = restored.bytes;
    let continuation = restored.update(&projected(500, "?"));
    assert_eq!(
        continuation
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(restored.bytes, restored_bytes);
    let key = restored.keys.keys().next().unwrap().clone();
    let mut inner = OverInner::Aggregates(restored);
    inner.clear_key(&key.0);
    assert_eq!(inner.state_bytes(), 0);
}

#[test]
fn distinct_numeric_membership_grows_the_tracked_footprint() {
    let mut running = OverAggregator::new(vec![0], vec![103]);
    running.track = true;
    let input = |start: i64, end: i64| {
        RecordBatch::try_from_iter(vec![
            (
                "rt",
                Arc::new(Int64Array::from_iter_values(start..end)) as ArrayRef,
            ),
            (
                "value0",
                Arc::new(Int64Array::from_iter_values(start..end)) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    running.update(&input(0, 1));
    let small = running.bytes;
    running.update(&input(1, 1024));
    assert!(running.bytes > small + 1024 * std::mem::size_of::<i64>());
    let measured = running.bytes;
    running.recompute_bytes();
    assert_eq!(running.bytes, measured);
}
