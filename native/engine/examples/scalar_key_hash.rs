//! Prepared Arrow key projection/hash admission; full released-Flink hashes are verified outside clocks.
use arrow::ipc::reader::FileReader;
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::flink_key_hashes;
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "scalar_key_hash INPUT.arrow [warmups=5] [runs=9]"
    );
    let warmups: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(9);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let golden = std::fs::read(format!("{}.hashes.bin", args[1])).unwrap();
    let expected: Vec<i32> = golden
        .chunks_exact(4)
        .map(|v| i32::from_le_bytes(v.try_into().unwrap()))
        .collect();
    assert_eq!(expected, flink_key_hashes(&input, &[0, 1], &[-1, -1]));
    let batches = (0..input.num_rows())
        .step_by(1024)
        .map(|start| input.slice(start, 1024.min(input.num_rows() - start)))
        .collect::<Vec<_>>();
    for trial in 0..warmups + runs {
        let clock = Instant::now();
        let mut hash = 0i64;
        for batch in &batches {
            for value in flink_key_hashes(black_box(batch), &[0, 1], &[-1, -1]) {
                hash = hash.wrapping_add(i64::from(value));
            }
        }
        black_box(hash);
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{},\"checksum\":{hash}}}",trial<warmups,input.num_rows());
    }
}
