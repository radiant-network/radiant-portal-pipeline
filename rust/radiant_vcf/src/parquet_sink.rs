//! Parquet files for one Iceberg table partition, written with the arrow-rs `parquet` crate.
//!
//! Port of the write half of `radiant/tasks/iceberg/utils.py::_write_file` (itself a copy of PyIceberg's): the
//! rows are cast to the table's Arrow schema without field ids, so that `add_files` accepts the files at commit
//! time, and the writer options come from the same table properties (`_get_parquet_writer_kwargs`).
//! The files are written to a local directory; the Python side uploads them and commits them.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Writer options of one table, resolved on the Python side from its Iceberg properties.
#[derive(Clone)]
pub struct SinkConfig {
    /// The table's Arrow schema, without field ids. Every batch is cast to it.
    pub schema: SchemaRef,
    pub directory: PathBuf,
    /// File name prefix; files are `<prefix>-<n>.parquet`.
    pub prefix: String,
    /// `write.parquet.compression-codec`: `zstd`, `snappy` or `uncompressed`.
    pub compression: String,
    /// `write.parquet.compression-level`.
    pub compression_level: Option<i32>,
    /// `write.parquet.page-size-bytes`.
    pub data_page_size: usize,
    /// `write.parquet.dict-size-bytes`.
    pub dictionary_page_size: usize,
    /// `write.parquet.page-row-limit`, passed by PyIceberg as pyarrow's `write_batch_size`.
    pub write_batch_size: usize,
    /// `write.parquet.row-group-limit`.
    pub row_group_rows: usize,
    /// Encoded size at which a row group is closed: the bound on the memory a writer holds.
    pub row_group_bytes: usize,
    /// `write.target-file-size-bytes`: a new file is started once the current one reaches it.
    pub target_file_size: usize,
}

/// One closed Parquet file.
#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
}

pub struct ParquetSink {
    cfg: SinkConfig,
    props: WriterProperties,
    writer: Option<(ArrowWriter<File>, PathBuf, u64)>,
    files: Vec<WrittenFile>,
}

fn compression(codec: &str, level: Option<i32>) -> Result<Compression, String> {
    match codec.to_ascii_lowercase().as_str() {
        "zstd" => Ok(Compression::ZSTD(match level {
            Some(l) => ZstdLevel::try_new(l).map_err(|e| e.to_string())?,
            None => ZstdLevel::default(),
        })),
        "snappy" => Ok(Compression::SNAPPY),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!(
            "Parquet compression codec {other:?} is not built into radiant_vcf"
        )),
    }
}

impl ParquetSink {
    pub fn new(cfg: SinkConfig) -> Result<Self, String> {
        let props = WriterProperties::builder()
            .set_compression(compression(&cfg.compression, cfg.compression_level)?)
            .set_data_page_size_limit(cfg.data_page_size)
            .set_dictionary_page_size_limit(cfg.dictionary_page_size)
            .set_write_batch_size(cfg.write_batch_size)
            .set_max_row_group_row_count(Some(cfg.row_group_rows))
            .set_max_row_group_bytes(Some(cfg.row_group_bytes))
            .set_created_by("radiant_vcf (parquet-rs)".to_string())
            .build();
        std::fs::create_dir_all(&cfg.directory).map_err(|e| format!("{}: {e}", cfg.directory.display()))?;
        Ok(Self {
            cfg,
            props,
            writer: None,
            files: Vec::new(),
        })
    }

    /// Appends rows. A batch without rows opens no file.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = conform(batch, &self.cfg.schema)?;
        if self.writer.is_none() {
            let path = self
                .cfg
                .directory
                .join(format!("{}-{:05}.parquet", self.cfg.prefix, self.files.len()));
            let file = File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            let w = ArrowWriter::try_new(file, self.cfg.schema.clone(), Some(self.props.clone()))
                .map_err(|e| e.to_string())?;
            self.writer = Some((w, path, 0));
        }
        let (w, _, rows) = self.writer.as_mut().expect("opened above");
        w.write(&batch).map_err(|e| e.to_string())?;
        *rows += batch.num_rows() as u64;
        if w.bytes_written() + w.in_progress_size() >= self.cfg.target_file_size {
            self.close_current()?;
        }
        Ok(())
    }

    fn close_current(&mut self) -> Result<(), String> {
        if let Some((w, path, rows)) = self.writer.take() {
            w.close().map_err(|e| e.to_string())?;
            let bytes = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
            self.files.push(WrittenFile {
                path: path.to_string_lossy().into_owned(),
                rows,
                bytes,
            });
        }
        Ok(())
    }

    /// Closes the open file and returns every file written.
    pub fn finish(&mut self) -> Result<Vec<WrittenFile>, String> {
        self.close_current()?;
        Ok(std::mem::take(&mut self.files))
    }
}

/// Writes every batch `next` yields to the sink of its table (`tables[i]` for the i-th batch), until `next` gives
/// None. Returns the files of each sink.
pub fn drain(
    sinks: &mut [(String, ParquetSink)],
    tables: &[&str],
    mut next: impl FnMut() -> Result<Option<Vec<RecordBatch>>, String>,
) -> Result<Vec<(String, Vec<WrittenFile>)>, String> {
    let idx = tables
        .iter()
        .map(|t| {
            sinks
                .iter()
                .position(|(k, _)| k == t)
                .ok_or_else(|| format!("no Parquet sink for table {t}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    while let Some(batches) = next()? {
        for (i, batch) in idx.iter().zip(batches.iter()) {
            let (key, sink) = &mut sinks[*i];
            sink.write(batch).map_err(|e| format!("table {key}: {e}"))?;
        }
    }
    sinks.iter_mut().map(|(k, s)| Ok((k.clone(), s.finish()?))).collect()
}

/// The batch's columns, picked by name and cast to the types of `schema` (`Utf8` to `LargeUtf8`, `List` to
/// `LargeList`...). `RecordBatch::try_new` then refuses a null in a required column.
pub fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| -> Result<ArrayRef, String> {
            let col = batch
                .column_by_name(f.name())
                .ok_or_else(|| format!("missing column {}", f.name()))?;
            if col.data_type() == f.data_type() {
                Ok(col.clone())
            } else {
                cast(col, f.data_type()).map_err(|e| format!("column {}: {e}", f.name()))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn cfg(dir: PathBuf, target: usize) -> SinkConfig {
        SinkConfig {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::LargeUtf8, true),
            ])),
            directory: dir,
            prefix: "t".into(),
            compression: "zstd".into(),
            compression_level: None,
            data_page_size: 1 << 20,
            dictionary_page_size: 2 << 20,
            write_batch_size: 20_000,
            row_group_rows: 1 << 20,
            row_group_bytes: 64 << 20,
            target_file_size: target,
        }
    }

    fn batch(n: i32, null_id: bool) -> RecordBatch {
        let ids: Int32Array = (0..n).map(|i| if null_id && i == 0 { None } else { Some(i) }).collect();
        RecordBatch::try_from_iter(vec![
            ("extra", Arc::new(Int32Array::from(vec![0; n as usize])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec![Some("x"); n as usize]))),
            ("id", Arc::new(ids)),
        ])
        .unwrap()
    }

    #[test]
    fn casts_and_rolls_files() {
        let dir = std::env::temp_dir().join(format!("radiant_vcf_sink_{}", std::process::id()));
        let mut sink = ParquetSink::new(cfg(dir.clone(), 1)).unwrap();
        sink.write(&batch(10, false)).unwrap();
        sink.write(&batch(0, false)).unwrap();
        sink.write(&batch(5, false)).unwrap();
        let files = sink.finish().unwrap();
        assert_eq!(files.iter().map(|f| f.rows).collect::<Vec<_>>(), vec![10, 5]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn refuses_null_in_required_column() {
        let schema = cfg(PathBuf::new(), 1).schema;
        assert!(conform(&batch(3, true), &schema).is_err());
        assert_eq!(conform(&batch(3, false), &schema).unwrap().schema(), schema);
    }
}
