//! Binary ELT functionality admission starts and stops inside Rust on prepared Arrow.
use arrow::{
    array::{Array, BinaryArray},
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::CalcProgram;
fn program() -> CalcProgram {
    CalcProgram::new(
        vec![40, 0, 0, 0],
        vec![0, 0, 1, 2],
        vec![3, 0, 0, 0],
        vec![],
        vec![],
        vec![],
    )
}
fn consume(batch: &RecordBatch) -> (usize, i64) {
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let hash = (0..values.len())
        .map(|row| {
            if values.is_null(row) {
                -1
            } else {
                i64::from(values.value_length(row))
            }
        })
        .fold(0i64, i64::wrapping_add);
    black_box((batch.num_rows(), hash))
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "binary_elt INPUT.arrow [warmups=5] [runs=9] [output.arrow]"
    );
    let warmups: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(9);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let batches = (0..input.num_rows())
        .step_by(1024)
        .map(|start| input.slice(start, 1024.min(input.num_rows() - start)))
        .collect::<Vec<_>>();
    if let Some(path) = args.get(4) {
        let mut op = program();
        op.run(batches[0].clone());
        let first = op.run(batches[0].clone());
        let mut writer =
            FileWriter::try_new(File::create(path).unwrap(), first.schema().as_ref()).unwrap();
        writer.write(&first).unwrap();
        for batch in &batches[1..] {
            writer.write(&op.run(batch.clone())).unwrap();
        }
        writer.finish().unwrap();
    }
    for trial in 0..warmups + runs {
        let mut op = program();
        op.run(batches[0].clone());
        let (mut rows, mut hash) = (0, 0i64);
        let clock = Instant::now();
        for batch in &batches {
            let output = op.run(black_box(batch.clone()));
            let (n, h) = consume(&output);
            rows += n;
            hash = hash.wrapping_add(h);
        }
        black_box((rows, hash));
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{rows},\"checksum\":{hash}}}",trial<warmups);
    }
}
