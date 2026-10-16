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

pub fn route(input: &RecordBatch, destinations: &[i32]) -> Vec<RecordBatch> {
    crate::routing::Routing::new(input.clone(), destinations)
        .batches
        .into_iter()
        .map(|(_, batch)| batch)
        .collect()
}

pub struct ReadFixture {
    io: FileIO,
    schema: Schema,
    size: u64,
}

impl ReadFixture {
    pub fn new(schema: &Schema, bytes: Bytes) -> Self {
        let io = FileIO::new_with_memory();
        let size = bytes.len() as u64;
        crate::reader::runtime()
            .block_on(io.new_output("read.parquet").unwrap().write(bytes))
            .unwrap();
        Self {
            io,
            schema: schema.clone(),
            size,
        }
    }

    pub fn read(&self, project_ids: &[i32], batch_rows: usize, skip: u64) -> Vec<RecordBatch> {
        self.read_as(project_ids, batch_rows, skip, None)
    }

    pub fn read_as(
        &self,
        project_ids: &[i32],
        batch_rows: usize,
        skip: u64,
        output_schema: Option<arrow::datatypes::SchemaRef>,
    ) -> Vec<RecordBatch> {
        crate::reader::runtime().block_on(async {
            let task = crate::reader::ScanTask {
                schema: self.schema.clone(),
                path: "read.parquet".into(),
                file_size: self.size,
                start: 0,
                length: self.size,
                project_ids: project_ids.to_vec(),
                deletes: vec![],
                case_sensitive: true,
            };
            let mut reader =
                crate::reader::Reader::new(self.io.clone(), task, batch_rows, skip).unwrap();
            if let Some(schema) = output_schema {
                reader = reader.with_output_schema(schema);
            }
            let mut batches = vec![];
            while let Some(batch) = reader.next().await.unwrap() {
                batches.push(batch);
            }
            batches
        })
    }
}
