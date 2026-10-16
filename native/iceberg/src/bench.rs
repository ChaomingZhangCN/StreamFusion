//! Native single-file encoding boundary; the end-to-end benchmark retains JNI and Java IO costs.
use crate::writer::{Encoder, WriterSettings};
use arrow::array::RecordBatch;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::Schema;

pub fn encode(schema: &Schema, input: &RecordBatch, selected: &[u32]) -> Bytes {
    futures::executor::block_on(async {
        let io = FileIO::new_with_memory();
        let settings = WriterSettings {
            compression: "zstd".into(),
            compression_level: Some(3),
            row_group_bytes: 128 << 20,
            page_bytes: 1 << 20,
            page_rows: 20000,
            dictionary_bytes: 2 << 20,
            dictionary_enabled: true,
            version: 1,
        };
        let mut encoder = Encoder::new(
            input.schema(),
            schema.clone(),
            settings,
            io.new_output("benchmark.parquet").unwrap(),
        )
        .await;
        encoder.write(input, selected).await;
        let result = encoder.finish().await;
        assert_eq!(result.row_count as usize, selected.len());
        io.new_input("benchmark.parquet")
            .unwrap()
            .read()
            .await
            .unwrap()
    })
}
