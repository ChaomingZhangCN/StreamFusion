use super::*;
use arrow::array::{Decimal128Array, Int32Array, Int8Array, ListArray, MapArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use std::collections::HashMap;

fn fixture(kind: &str, rows: usize, width: usize, list: bool) -> RecordBatch {
    let mut offsets = vec![0i32];
    let mut keys = Vec::new();
    for row in 0..rows + 1 {
        if row % 7 != 0 && row % 13 != 0 {
            for item in 0..width {
                keys.push((item % 5 != 0).then_some((row * width + item) as i64));
            }
        }
        offsets.push(keys.len() as i32);
    }
    let count = keys.len();
    let (key, value): (ArrayRef, ArrayRef) = match kind {
        "integer" => (
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int32Array::from_iter(
                (0..count).map(|i| (i % 11 != 0).then_some(i as i32 - 100)),
            )),
        ),
        "decimal_timestamp" => {
            (
                Arc::new(
                    Decimal128Array::from_iter(keys.iter().map(|i| {
                        i.map(|i| -12345678901234567890123456789000000000i128 + i as i128)
                    }))
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
                ),
                Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                    (0..count).map(|i| {
                        (i % 11 != 0).then(|| {
                            streamfusion_bridge::timestamp::TimestampValue::from_nanos(
                                i as i128 * 123456789 - 999999999,
                            )
                            .unwrap()
                        })
                    }),
                )),
            )
        }
        "row_string" => (
            Arc::new(StructArray::new(
                vec![
                    Arc::new(Field::new("n", DataType::Int64, true)),
                    Arc::new(
                        Field::new("$row_kind$", DataType::Int8, false).with_metadata(
                            HashMap::from([("streamfusion.nested-row-kind".into(), "1".into())]),
                        ),
                    ),
                ]
                .into(),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(Int8Array::from_iter_values(
                        (0..count).map(|i| (i % 4) as i8),
                    )),
                ],
                Some(NullBuffer::from_iter(keys.iter().map(Option::is_some))),
            )),
            Arc::new(StringArray::from_iter(
                (0..count).map(|i| (i % 11 != 0).then(|| format!("é中😀-{i}"))),
            )),
        ),
        _ => unreachable!(),
    };
    let entries = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("key", key.data_type().clone(), true)),
            key,
        ),
        (
            Arc::new(Field::new("value", value.data_type().clone(), true)),
            value,
        ),
    ]));
    let entry = Arc::new(Field::new("items", entries.data_type().clone(), false));
    let offsets = OffsetBuffer::new(offsets.into());
    let nulls = Some(NullBuffer::from_iter((0..rows + 1).map(|i| i % 13 != 0)));
    let array: ArrayRef = if list {
        Arc::new(ListArray::new(entry, offsets, entries, nulls))
    } else {
        Arc::new(MapArray::new(
            entry,
            offsets,
            (*entries).clone(),
            nulls,
            false,
        ))
    };
    let mut field = Field::new("m", array.data_type().clone(), true);
    if list {
        field = field.with_metadata(HashMap::from([(
            "streamfusion.map-list".into(),
            "1".into(),
        )]));
    }
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])
        .unwrap()
        .slice(1, rows)
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bridge/map_udf_payloads");
    for rows in [16, 1024] {
        for width in [4, 64] {
            for kind in ["integer", "decimal_timestamp", "row_string"] {
                for list in [false, true] {
                    let input = fixture(kind, rows, width, list);
                    for cached in [false, true] {
                        let label =
                            format!("{kind}/rows{rows}_width{width}_list{list}_cached{cached}");
                        let output = roundtrip(&input, cached);
                        assert_eq!(output, input);
                        drop(output);
                        let (output, allocations) = measure(|| roundtrip(&input, cached));
                        report(
                            &format!("bridge/map_udf_payloads/{label}"),
                            input.columns(),
                            output.columns(),
                            allocations,
                        );
                        group.bench_function(label, |b| {
                            b.iter(|| {
                                std::hint::black_box(roundtrip(
                                    std::hint::black_box(&input),
                                    cached,
                                ))
                            })
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
