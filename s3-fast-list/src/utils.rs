use std::sync::Arc;
use tokio::io::AsyncWrite;
use arrow_array::array::ArrayRef;
use arrow_array::array::{UInt8Array, UInt64Array, StringArray};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding, GzipLevel};
use log::warn;
use crate::core::{ObjectKey, ObjectProps};

pub struct AsyncParquetOutput<W> {
    schema_ref: SchemaRef,
    writer: AsyncArrowWriter<W>,
}

impl<W: AsyncWrite + Unpin + Send> AsyncParquetOutput<W> {

    pub fn new(buf_wr: W) -> Self {

        // define fields
        let field_key = Field::new("Key", DataType::Utf8, false);
        let field_size = Field::new("Size", DataType::UInt64, false);
        let field_last_modified = Field::new("LastModified", DataType::UInt64, false);
        let field_etag = Field::new("ETag", DataType::Utf8, false);
        let field_diff_flag = Field::new("DiffFlag", DataType::UInt8, false);

        // define schema
        let schema_ref = Arc::new(Schema::new(vec![field_key, field_size, field_last_modified, field_etag, field_diff_flag]));

        // define writer props
        let writer_props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::GZIP(GzipLevel::try_new(6).unwrap()))
            .build();

        // build writer
        let writer = AsyncArrowWriter::try_new(buf_wr, Arc::clone(&schema_ref), Some(writer_props.clone())).unwrap();

        Self {
            schema_ref: schema_ref,
            writer: writer,
        }

    }

    pub async fn write(&mut self, v: Vec<(ObjectKey, ObjectProps)>, diff_flag: u8) -> tokio::io::Result<()> {

        if v.len() == 0 {
            return Ok(());
        }

        let mut vec_key: Vec<&str> = Vec::new();
        let mut vec_size: Vec<u64> = Vec::new();
        let mut vec_last_modified: Vec<u64> = Vec::new();
        let mut vec_etag: Vec<String> = Vec::new();
        let mut vec_diff_flag: Vec<u8> = Vec::new();

        let _: Vec<_> = v.iter().map(|(key, props)| {
            vec_key.push(key.as_str());
            vec_size.push(props.size());
            vec_last_modified.push(props.last_modified());
            vec_etag.push(props.etag_string());
            vec_diff_flag.push(diff_flag);
        }).collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec_key)) as ArrayRef,
            Arc::new(UInt64Array::from(vec_size)) as ArrayRef,
            Arc::new(UInt64Array::from(vec_last_modified)) as ArrayRef,
            Arc::new(StringArray::from(vec_etag)) as ArrayRef,
            Arc::new(UInt8Array::from(vec_diff_flag)) as ArrayRef
        ];

        let batch = RecordBatch::try_new(Arc::clone(&self.schema_ref), columns).unwrap();
        let res = self.writer.write(&batch).await;
        if res.is_ok() {
            return Ok(());
        } else {
            warn!("parquet writer write op failed {:?}", res.unwrap());
        }

        Ok(())
    }

    pub async fn close(self) -> tokio::io::Result<()> {
        let _ = self.writer.close().await;

        Ok(())
    }
}
