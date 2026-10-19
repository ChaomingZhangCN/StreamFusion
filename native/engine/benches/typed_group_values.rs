use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int8Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::LocalGroupBy;

mod typed_distinct;
const ROWS: usize = 4096;
criterion_group!(benches, typed_distinct::bench_typed_distinct);
criterion_main!(benches);
