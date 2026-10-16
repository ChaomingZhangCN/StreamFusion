use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::SchemaRef;
use iceberg::io::OutputFile;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::writer::file_writer::{
    FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder,
};
use iceberg::writer::CurrentFileStatus;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriterSettings {
    pub compression: String,
    pub compression_level: Option<i32>,
    pub row_group_bytes: usize,
    pub page_bytes: usize,
    pub page_rows: usize,
    pub dictionary_bytes: usize,
    pub dictionary_enabled: bool,
    pub version: u8,
}

impl WriterSettings {
    fn properties(&self) -> WriterProperties {
        let compression = match self.compression.as_str() {
            "uncompressed" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "zstd" => Compression::ZSTD(
                ZstdLevel::try_new(self.compression_level.unwrap_or(3)).expect("Zstd level"),
            ),
            "gzip" => Compression::GZIP(
                GzipLevel::try_new(self.compression_level.unwrap_or(6) as u32).expect("Gzip level"),
            ),
            "lz4" => Compression::LZ4_RAW,
            other => panic!("Unadmitted Iceberg codec: {other}"),
        };
        WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(Some(self.row_group_bytes))
            .set_data_page_size_limit(self.page_bytes)
            .set_data_page_row_count_limit(self.page_rows)
            .set_dictionary_page_size_limit(self.dictionary_bytes)
            .set_dictionary_enabled(self.dictionary_enabled)
            .set_writer_version(match self.version {
                1 => WriterVersion::PARQUET_1_0,
                2 => WriterVersion::PARQUET_2_0,
                _ => panic!("Unadmitted Parquet writer version"),
            })
            .build()
    }
}

pub(crate) struct Encoder {
    pub input_schema: SchemaRef,
    write_schema: SchemaRef,
    writer: Option<ParquetWriter>,
}

#[derive(Serialize)]
pub(crate) struct FileResult {
    pub row_count: u64,
    pub size: u64,
    pub nan_counts: std::collections::HashMap<i32, u64>,
    pub split_offsets: Vec<i64>,
}

impl Encoder {
    pub(crate) async fn new(
        input_schema: SchemaRef,
        schema: IcebergSchema,
        settings: WriterSettings,
        output: OutputFile,
    ) -> Self {
        let schema = Arc::new(schema);
        let write_schema = Arc::new(schema.as_ref().try_into().expect("Iceberg Arrow schema"));
        let writer = ParquetWriterBuilder::new(settings.properties(), schema)
            .build(output)
            .await
            .expect("Iceberg Parquet writer");
        Self {
            input_schema,
            write_schema,
            writer: Some(writer),
        }
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch, selected: &[u32]) {
        if selected.is_empty() {
            return;
        }
        let contiguous = selected.windows(2).all(|w| w[1] == w[0] + 1);
        let indices = UInt32Array::from(selected.to_vec());
        let columns = self
            .write_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column = if contiguous {
                    batch.column(i).slice(selected[0] as usize, selected.len())
                } else {
                    take(batch.column(i), &indices, None).expect("Iceberg Arrow selection")
                };
                if column.data_type() == field.data_type() {
                    column
                } else if streamfusion_bridge::timestamp::is_timestamp(column.data_type())
                    && streamfusion_bridge::timestamp::is_timestamp(field.data_type())
                {
                    streamfusion_bridge::timestamp::cast_timestamp(&column, field.data_type())
                        .expect("Iceberg timestamp column conversion")
                } else {
                    cast(&column, field.data_type()).expect("Iceberg physical column conversion")
                }
            })
            .collect::<Vec<_>>();
        let selected = RecordBatch::try_new(self.write_schema.clone(), columns)
            .expect("Iceberg selected record batch");
        self.writer
            .as_mut()
            .expect("Iceberg writer closed")
            .write(&selected)
            .await
            .expect("Iceberg native Parquet write");
    }

    pub(crate) fn size(&self) -> usize {
        self.writer
            .as_ref()
            .expect("Iceberg writer closed")
            .current_written_size()
    }

    pub(crate) async fn finish(&mut self) -> FileResult {
        let files = self
            .writer
            .take()
            .expect("Iceberg writer closed")
            .close()
            .await
            .expect("Iceberg native Parquet finalization");
        assert_eq!(
            files.len(),
            1,
            "A Java-selected file must yield exactly one data file"
        );
        let file = files[0].build().expect("Iceberg native file statistics");
        FileResult {
            row_count: file.record_count(),
            size: file.file_size_in_bytes(),
            nan_counts: file.nan_value_counts().clone(),
            split_offsets: file.split_offsets().unwrap_or_default().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::io::FileIO;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn selections_preserve_order_nulls_ids_and_metrics() {
        futures::executor::block_on(async {
            let schema = IcebergSchema::builder()
                .with_fields([
                    NestedField::optional(17, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(38, "value", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap();
            let input_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
                    Arc::new(StringArray::from(vec![Some("first"), Some("second"), None])),
                ],
            )
            .unwrap();
            let settings = WriterSettings {
                compression: "zstd".into(),
                compression_level: None,
                row_group_bytes: 128 << 20,
                page_bytes: 1 << 20,
                page_rows: 20000,
                dictionary_bytes: 2 << 20,
                dictionary_enabled: true,
                version: 1,
            };
            let io = FileIO::new_with_memory();
            let mut writer = Encoder::new(
                input_schema,
                schema,
                settings,
                io.new_output("file").unwrap(),
            )
            .await;
            writer.write(&batch, &[]).await;
            writer.write(&batch, &[2, 0, 2]).await;
            writer.write(&batch, &[1]).await;
            assert!(writer.size() > 0);
            let result = writer.finish().await;
            assert_eq!(result.row_count, 4);
            let bytes = io.new_input("file").unwrap().read().await.unwrap();
            assert_eq!(bytes.len() as u64, result.size);
            assert_eq!(result.split_offsets.len(), 1);
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let columns = builder.metadata().file_metadata().schema_descr().columns();
            assert_eq!(columns[0].self_type().get_basic_info().id(), 17);
            assert_eq!(columns[1].self_type().get_basic_info().id(), 38);
            let rows = builder.build().unwrap().next().unwrap().unwrap();
            let ids = rows
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(
                ids.iter().collect::<Vec<_>>(),
                vec![Some(30), Some(10), Some(30), None]
            );
            let strings = rows
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![None, Some("first"), None, Some("second")]
            );
            assert_eq!(strings.null_count(), 2);
        });
    }
}
