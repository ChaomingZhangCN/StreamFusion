use arrow::array::{Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::sync::Arc;
use streamfusion::bench::Over;

mod over_counts;
mod over_range_order;
mod over_reductions;
mod over_values;

criterion_group!(
    benches,
    over_counts::bench,
    over_reductions::bench,
    over_range_order::bench,
    over_values::bench
);
criterion_main!(benches);
