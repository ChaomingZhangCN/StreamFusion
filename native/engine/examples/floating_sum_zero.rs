//! SUM0 functionality admission starts on prepared Arrow and consumes the result inside Rust.
use arrow::{array::ArrayRef, ipc::reader::FileReader};
use datafusion::common::ScalarValue;
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::ZeroSum;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "floating_sum_zero INPUT.arrow float|double [batch_rows=1024] [warmups=5] [runs=9]"
    );
    let code = match args[2].as_str() {
        "float" => 6,
        "double" => 1,
        _ => panic!("unknown type"),
    };
    let batch_rows: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1024);
    let warmups: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(9);
    let mut input = FileReader::try_new(File::open(&args[1]).unwrap(), None).unwrap();
    let input = input.next().unwrap().unwrap();
    let column = input.column(0);
    let batches: Vec<ArrayRef> = (0..input.num_rows())
        .step_by(batch_rows)
        .map(|start| column.slice(start, batch_rows.min(input.num_rows() - start)))
        .collect();
    for trial in 0..warmups + runs {
        let mut sum = ZeroSum::new(code);
        let clock = Instant::now();
        for batch in &batches {
            sum.update(black_box(batch));
        }
        let bits = match sum.value() {
            ScalarValue::Float32(Some(value)) => i64::from(value.to_bits()),
            ScalarValue::Float64(Some(value)) => value.to_bits() as i64,
            other => panic!("unexpected SUM0 result {other:?}"),
        };
        black_box(bits);
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"bits\":{bits},\"rows\":{}}}", trial < warmups, input.num_rows());
    }
}
