//! Small-functionality admission: prepared Arrow enters and output is consumed entirely in Rust.
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::{fs::File, hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::Tumbling;

fn operation(shape: &str) -> Tumbling {
    let count = shape.starts_with("count-");
    let shape = shape.strip_prefix("count-").unwrap_or(shape);
    Tumbling::with_window_budget(
        10000,
        if shape == "tumble" { 10000 } else { 2000 },
        shape == "cumulate",
        if count { 0 } else { 3 },
        if count { vec![3, 3, 3] } else { vec![1, 2, 3] },
        1024 * 1024 * 1024,
    )
}

fn canonical_input(batch: RecordBatch, count: bool) -> RecordBatch {
    let ts = streamfusion_bridge::timestamp::TimestampColumn::try_new(
        batch.column(if count { 4 } else { 2 }).as_ref(),
    )
    .expect("timestamp input");
    let millis: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..batch.num_rows()).map(|i| ts.value(i).unwrap().millis()),
    ));
    RecordBatch::try_from_iter(vec![
        ("ts", millis),
        ("value0", batch.column(1).clone()),
        ("value1", batch.column(if count { 2 } else { 1 }).clone()),
        (
            "value2",
            if count {
                batch.column(3).clone()
            } else {
                Arc::new(Int64Array::from_value(1, batch.num_rows())) as ArrayRef
            },
        ),
        ("key0", batch.column(0).clone()),
    ])
    .unwrap()
}

fn consume(batch: &RecordBatch) -> (usize, i64) {
    if batch.column_by_name("result0").unwrap().data_type() == &DataType::Int64 {
        let columns: Vec<_> = [
            "key0",
            "result0",
            "result1",
            "result2",
            "window_start",
            "window_end",
        ]
        .into_iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect();
        let mut hash = 0_i64;
        for row in 0..batch.num_rows() {
            let row_hash = columns
                .iter()
                .enumerate()
                .fold(0_i64, |h, (index, column)| {
                    let value = if column.is_null(row) {
                        0
                    } else {
                        column.value(row)
                    };
                    h.wrapping_mul(31).wrapping_add(if index >= 4 {
                        value.wrapping_mul(31)
                    } else {
                        value
                    })
                });
            hash = hash.wrapping_add(row_hash);
        }
        return black_box((batch.num_rows(), hash));
    }

    let key = batch
        .column_by_name("key0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min = batch
        .column_by_name("result0")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let max = batch
        .column_by_name("result1")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let count = batch
        .column_by_name("result2")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let start = batch
        .column_by_name("window_start")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let end = batch
        .column_by_name("window_end")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut hash = 0_i64;
    for row in 0..batch.num_rows() {
        let values = [
            if key.is_null(row) { 0 } else { key.value(row) },
            if min.is_null(row) {
                0
            } else {
                min.value(row).len() as i64
            },
            if max.is_null(row) {
                0
            } else {
                max.value(row).len() as i64
            },
            count.value(row),
            start.value(row).wrapping_mul(31),
            end.value(row).wrapping_mul(31),
        ];
        let row_hash = values
            .into_iter()
            .fold(0_i64, |h, v| h.wrapping_mul(31).wrapping_add(v));
        hash = hash.wrapping_add(row_hash);
    }
    black_box((batch.num_rows(), hash))
}

fn oracle(batch: &RecordBatch, path: &str) {
    let names = [
        "key0",
        "result0",
        "result1",
        "result2",
        "window_start",
        "window_end",
    ];
    let arrays: Vec<ArrayRef> = names
        .into_iter()
        .map(|name| {
            let array = batch.column_by_name(name).unwrap();
            if name.starts_with("window_") {
                arrow::compute::cast(array, &DataType::Timestamp(TimeUnit::Millisecond, None))
                    .unwrap()
            } else {
                array.clone()
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(
        names
            .into_iter()
            .zip(&arrays)
            .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let output = RecordBatch::try_new(schema, arrays).unwrap();
    let mut writer =
        FileWriter::try_new(File::create(path).unwrap(), output.schema().as_ref()).unwrap();
    writer.write(&output).unwrap();
    writer.finish().unwrap();
}

fn consume_drop(batch: &RecordBatch) -> (usize, i64) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let text = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let kinds = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int8Array>()
        .unwrap();
    let mut hash = 0_i64;
    for row in 0..batch.num_rows() {
        let row_hash = i64::from(kinds.value(row))
            .wrapping_mul(31)
            .wrapping_add(ids.value(row))
            .wrapping_mul(31)
            .wrapping_add(if text.is_null(row) {
                0
            } else {
                text.value(row).len() as i64
            });
        hash = hash.wrapping_add(row_hash);
    }
    black_box((batch.num_rows(), hash))
}

fn drop_trials(
    input: &RecordBatch,
    batches: &[RecordBatch],
    warmups: usize,
    runs: usize,
    output: Option<&String>,
) {
    if let Some(path) = output {
        let batch = streamfusion::bench::drop_update_before(input);
        let mut writer =
            FileWriter::try_new(File::create(path).unwrap(), batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    for trial in 0..warmups + runs {
        drop(streamfusion::bench::drop_update_before(&batches[0]));
        let clock = Instant::now();
        let (mut rows, mut hash) = (0, 0_i64);
        for batch in batches {
            let output = streamfusion::bench::drop_update_before(black_box(batch));
            let (count, checksum) = consume_drop(&output);
            rows += count;
            hash = hash.wrapping_add(checksum);
        }
        black_box((rows, hash));
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"input_rows\":{},\"output_rows\":{rows},\"checksum\":{hash}}}",trial<warmups,input.num_rows());
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() >= 3, "usage: feature_admission INPUT.arrow tumble|hop|cumulate|drop|count-tumble|count-hop|count-cumulate [warmups=5] [runs=7] [output.arrow]");
    let path = &args[1];
    let shape = &args[2];
    assert!([
        "tumble",
        "hop",
        "cumulate",
        "drop",
        "count-tumble",
        "count-hop",
        "count-cumulate"
    ]
    .contains(&shape.as_str()));
    let warmups: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(7);
    let lengths: Vec<usize> =
        serde_json::from_reader(File::open(format!("{path}.batches.json")).unwrap()).unwrap();
    let mut reader = FileReader::try_new(File::open(path).unwrap(), None).unwrap();
    let raw = reader.next().unwrap().unwrap();
    let input = if shape == "drop" {
        raw
    } else {
        canonical_input(raw, shape.starts_with("count-"))
    };
    assert!(reader.next().is_none());
    let mut offset = 0;
    let batches: Vec<_> = lengths
        .into_iter()
        .map(|len| {
            let batch = input.slice(offset, len);
            offset += len;
            batch
        })
        .collect();
    assert_eq!(offset, input.num_rows());
    assert!(!batches.is_empty());
    if shape == "drop" {
        drop_trials(&input, &batches, warmups, runs, args.get(5));
        return;
    }
    // Oracle serialization and all input preparation are outside every operation timer.
    if let Some(path) = args.get(5) {
        let mut op = operation(shape);
        op.update(&batches[0]);
        for batch in &batches {
            op.update(batch);
        }
        oracle(&op.flush(i64::MAX), path);
    }
    for trial in 0..warmups + runs {
        let mut op = operation(shape);
        op.update(&batches[0]);
        let clock = Instant::now();
        for batch in &batches {
            op.update(black_box(batch));
        }
        let output = op.flush(i64::MAX);
        let (rows, hash) = consume(&output);
        drop(output);
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"input_rows\":{},\"output_rows\":{rows},\"checksum\":{hash}}}",trial<warmups,input.num_rows());
    }
}
