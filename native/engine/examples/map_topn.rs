//! Top-N admission on prepared Arrow maps: state, output allocation and full-value consumption.
use arrow::{
    array::{Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray, StructArray},
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::AppendTopN;

fn map_hash(map: &ListArray, row: usize) -> i64 {
    if map.is_null(row) {
        return 0;
    }
    let entries = map.values().as_any().downcast_ref::<StructArray>().unwrap();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let mut result = 1i64;
    for i in map.value_offsets()[row] as usize..map.value_offsets()[row + 1] as usize {
        let key = if keys.is_null(i) {
            0
        } else {
            keys.value(i)
                .as_bytes()
                .iter()
                .fold(1i64, |h, b| h.wrapping_mul(31).wrapping_add(i64::from(*b)))
        };
        let value = if values.is_null(i) {
            0
        } else {
            i64::from(values.value(i)).wrapping_add(1)
        };
        result = result
            .wrapping_mul(31)
            .wrapping_add(31i64.wrapping_add(key).wrapping_mul(31).wrapping_add(value));
    }
    result
}
fn consume(batch: &RecordBatch) -> (usize, i64) {
    if batch.num_rows() == 0 {
        return (0, 0);
    }
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maps = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let ranks = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let mut sum = 0i64;
    for row in 0..batch.num_rows() {
        let hash = keys
            .value(row)
            .wrapping_mul(31)
            .wrapping_add(ids.value(row))
            .wrapping_mul(31)
            .wrapping_add(map_hash(maps, row))
            .wrapping_mul(31)
            .wrapping_add(ranks.value(row))
            .wrapping_mul(31)
            .wrapping_add(i64::from(kinds.value(row)));
        sum = sum.wrapping_add(hash);
    }
    black_box((batch.num_rows(), sum))
}
fn operator() -> AppendTopN {
    AppendTopN::new(vec![0], vec![(1, false)], 3, true, false).with_budget(1 << 30)
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "map_topn INPUT.arrow high|low [warmups=5] [runs=9]"
    );
    let warmups: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(9);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(input.num_columns(), 3);
    let batches = (0..input.num_rows())
        .step_by(1024)
        .map(|start| input.slice(start, 1024.min(input.num_rows() - start)))
        .collect::<Vec<_>>();
    let bytes = std::fs::read(format!("{}.counts.bin", args[1])).unwrap();
    let expected: Vec<i32> = bytes
        .chunks_exact(4)
        .map(|v| i32::from_le_bytes(v.try_into().unwrap()))
        .collect();
    let mut actual = vec![0i32; input.num_rows() * 12];
    assert_eq!(expected.len(), actual.len());
    let mut verify = operator();
    for batch in &batches {
        let output = verify.push(batch);
        if output.num_rows() == 0 {
            continue;
        }
        let ids = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ranks = output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = output
            .column(4)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let input_keys = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            let id = ids.value(row) as usize;
            let source = if args[2] == "high" {
                id
            } else {
                input.num_rows() - 1 - id
            };
            assert_eq!(keys.value(row), input_keys.value(source));
            assert_eq!(
                output.column(2).slice(row, 1).as_ref(),
                input.column(2).slice(source, 1).as_ref(),
                "retained payload {id}"
            );
            let rank = ranks.value(row) as usize;
            assert!((1..=3).contains(&rank));
            actual[id * 12 + kinds.value(row) as usize * 3 + rank - 1] += 1;
        }
    }
    assert_eq!(actual, expected, "full row identity/rank/RowKind histogram");
    drop(verify);
    for trial in 0..warmups + runs {
        let mut op = operator();
        let clock = Instant::now();
        let mut count = 0;
        let mut checksum = 0i64;
        for batch in &batches {
            let result = consume(&op.push(black_box(batch)));
            count += result.0;
            checksum = checksum.wrapping_add(result.1);
        }
        black_box((count, checksum));
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{count},\"checksum\":{checksum}}}",trial<warmups);
    }
}
