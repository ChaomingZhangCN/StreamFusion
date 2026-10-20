//! Processing-time COUNT admission on prepared Arrow, including frame/state/output work.
use arrow::{
    array::{Array, ArrayRef, Int64Array, ListArray, MapArray},
    datatypes::{Field, Schema},
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use std::{fs::File, hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::Over;

fn consume(batch: &RecordBatch) -> (usize, i64) {
    let key = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maps = batch.column(2);
    let count = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut hash = 0i64;
    for row in 0..batch.num_rows() {
        let width = if maps.is_null(row) {
            0
        } else if let Some(map) = maps.as_any().downcast_ref::<MapArray>() {
            i64::from(map.value_length(row))
        } else {
            i64::from(
                maps.as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value_length(row),
            )
        };
        // Java consumes k, map size, the fixed zero proctime placeholder, then COUNT.
        hash = hash.wrapping_add(
            key.value(row)
                .wrapping_mul(31)
                .wrapping_add(width)
                .wrapping_mul(31)
                .wrapping_mul(31)
                .wrapping_add(count.value(row)),
        );
    }
    black_box((batch.num_rows(), hash))
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "over_map_count INPUT.arrow rows|range|bounded [warmups=3] [runs=7] [output.arrow]"
    );
    let frame = match args[2].as_str() {
        "rows" => 3,
        "range" => 0,
        "bounded" => 1,
        _ => panic!("frame"),
    };
    let warmups: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(3);
    let runs: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(7);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(input.num_columns(), 3);
    let times =
        streamfusion_bridge::timestamp::TimestampColumn::try_new(input.column(2).as_ref()).unwrap();
    assert!((0..input.num_rows()).all(|row| times
        .value(row)
        .is_ok_and(|time| time.millis() == 0 && time.nanos() == 0)));
    let canonical = RecordBatch::try_from_iter(vec![
        ("k", input.column(0).clone()),
        ("pt", input.column(2).clone()),
        ("m", input.column(1).clone()),
    ])
    .unwrap();
    let lengths: Vec<usize> =
        serde_json::from_slice(&std::fs::read(format!("{}.batches.json", args[1])).unwrap())
            .unwrap();
    let mut offset = 0;
    let batches = lengths
        .into_iter()
        .map(|length| {
            let batch = canonical.slice(offset, length);
            offset += length;
            batch
        })
        .collect::<Vec<_>>();
    assert_eq!(offset, canonical.num_rows());
    if let Some(path) = args.get(5) {
        let mut operator = Over::framed(0, 3, frame, 3, true).with_budget(1024 * 1024 * 1024);
        operator.push_proctime(batches[0].clone());
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            "count",
            arrow::datatypes::DataType::Int64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let mut writer = FileWriter::try_new(File::create(path).unwrap(), schema.as_ref()).unwrap();
        for batch in &batches {
            let output = operator.push_proctime(batch.clone());
            let arrays: Vec<ArrayRef> = vec![
                output.column(0).clone(),
                output.column(2).clone(),
                output.column(1).clone(),
                output.column(3).clone(),
            ];
            writer
                .write(&RecordBatch::try_new(schema.clone(), arrays).unwrap())
                .unwrap();
        }
        writer.finish().unwrap();
    }
    for trial in 0..warmups + runs {
        let mut operator = Over::framed(0, 3, frame, 3, true).with_budget(1024 * 1024 * 1024);
        operator.push_proctime(batches[0].clone());
        let (mut rows, mut hash) = (0, 0i64);
        let clock = Instant::now();
        for batch in &batches {
            let output = operator.push_proctime(black_box(batch.clone()));
            let (count, checksum) = consume(&output);
            rows += count;
            hash = hash.wrapping_add(checksum);
        }
        black_box((rows, hash));
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{rows},\"checksum\":{hash}}}",trial<warmups);
    }
}
