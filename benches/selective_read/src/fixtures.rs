use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::*;
use arrow::compute::{cast, concat_batches, max, max_string, min, min_string};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tpchgen::generators::LineItem;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MIB: u64 = 1024 * 1024;
pub const BATCH_ROWS: usize = 8192;
pub const GROUP_ROWS: usize = 131072;
pub const FILE_ROWS: usize = GROUP_ROWS * 8;

pub fn original_schema() -> SchemaRef {
    let fields = [
        ("l_orderkey", DataType::Int64),
        ("l_partkey", DataType::Int64),
        ("l_suppkey", DataType::Int64),
        ("l_linenumber", DataType::Int32),
        ("l_quantity", DataType::Decimal128(15, 2)),
        ("l_extendedprice", DataType::Decimal128(15, 2)),
        ("l_discount", DataType::Decimal128(15, 2)),
        ("l_tax", DataType::Decimal128(15, 2)),
        ("l_returnflag", DataType::Utf8),
        ("l_linestatus", DataType::Utf8),
        ("l_shipdate", DataType::Date32),
        ("l_commitdate", DataType::Date32),
        ("l_receiptdate", DataType::Date32),
        ("l_shipinstruct", DataType::Utf8),
        ("l_shipmode", DataType::Utf8),
        ("l_comment", DataType::Utf8),
    ];
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, ty)| Field::new(name, ty, false))
            .collect::<Vec<_>>(),
    ))
}

pub fn wide_schema() -> SchemaRef {
    let mut fields = original_schema().fields().to_vec();
    fields.extend(
        (0..64).map(|j| Arc::new(Field::new(format!("payload_{j:02}"), DataType::Int64, true))),
    );
    Arc::new(Schema::new(fields))
}

pub fn source_batch(rows: &[LineItem<'_>]) -> Result<RecordBatch> {
    macro_rules! column {
        ($array:ident, $field:ident) => {
            Arc::new($array::from_iter_values(rows.iter().map(|r| r.$field))) as ArrayRef
        };
    }
    let decimal = |values: Vec<i128>| -> Result<ArrayRef> {
        Ok(Arc::new(
            Decimal128Array::from(values).with_precision_and_scale(15, 2)?,
        ))
    };
    let columns = vec![
        column!(Int64Array, l_orderkey),
        column!(Int64Array, l_partkey),
        column!(Int64Array, l_suppkey),
        column!(Int32Array, l_linenumber),
        decimal(
            rows.iter()
                .map(|r| i128::from(r.l_quantity) * 100)
                .collect(),
        )?,
        decimal(
            rows.iter()
                .map(|r| i128::from(r.l_extendedprice.into_inner()))
                .collect(),
        )?,
        decimal(
            rows.iter()
                .map(|r| i128::from(r.l_discount.into_inner()))
                .collect(),
        )?,
        decimal(
            rows.iter()
                .map(|r| i128::from(r.l_tax.into_inner()))
                .collect(),
        )?,
        column!(StringArray, l_returnflag),
        column!(StringArray, l_linestatus),
        Arc::new(Date32Array::from_iter_values(
            rows.iter().map(|r| r.l_shipdate.to_unix_epoch()),
        )),
        Arc::new(Date32Array::from_iter_values(
            rows.iter().map(|r| r.l_commitdate.to_unix_epoch()),
        )),
        Arc::new(Date32Array::from_iter_values(
            rows.iter().map(|r| r.l_receiptdate.to_unix_epoch()),
        )),
        column!(StringArray, l_shipinstruct),
        column!(StringArray, l_shipmode),
        column!(StringArray, l_comment),
    ];
    Ok(RecordBatch::try_new(original_schema(), columns)?)
}

pub fn payload(order: i64, line: i32, column: usize) -> Option<i64> {
    let digest = Sha256::digest(format!("dar-wide-v1/{order}/{line}/{column:02}").as_bytes());
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&digest[..8]);
    let value = u64::from_le_bytes(bytes);
    (!value.is_multiple_of(17))
        .then_some(((value & 0x7fffffffffffffff) as i64) - 0x4000000000000000)
}

pub fn add_payloads(batch: &RecordBatch) -> Result<RecordBatch> {
    let orders = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("orderkey type")?;
    let lines = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("linenumber type")?;
    let mut columns = batch.columns().to_vec();
    columns.extend((0..64).map(|j| {
        Arc::new(Int64Array::from_iter(
            (0..batch.num_rows()).map(|r| payload(orders.value(r), lines.value(r), j)),
        )) as ArrayRef
    }));
    Ok(RecordBatch::try_new(wide_schema(), columns)?)
}

pub fn normalize(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| cast(array, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

pub fn writer_settings() -> Value {
    json!({
        "row_group_rows": GROUP_ROWS, "groups_per_file": 8, "input_batch_rows": BATCH_ROWS,
        "version": "PARQUET_1_0", "compression": "ZSTD(3)",
        "data_page_bytes": 1048576, "data_page_rows": 20000, "write_batch_rows": 1024,
        "dictionary": true, "dictionary_page_bytes": 1048576,
        "statistics": "Page", "statistics_truncate_length": null,
        "column_index_truncate_length": null, "offset_index": true, "bloom_filter": false,
        "max_row_group_bytes": null, "created_by": "dar-selective-read-v1 parquet-rs 58.4.0"
    })
}

fn writer_properties() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_max_row_group_row_count(Some(GROUP_ROWS))
        .set_max_row_group_bytes(None)
        .set_data_page_size_limit(MIB as usize)
        .set_data_page_row_count_limit(20000)
        .set_write_batch_size(1024)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(MIB as usize)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_statistics_truncate_length(None)
        .set_column_index_truncate_length(None)
        .set_offset_index_disabled(false)
        .set_bloom_filter_enabled(false)
        .set_created_by("dar-selective-read-v1 parquet-rs 58.4.0".into())
        .build())
}

pub struct Budget {
    used: AtomicU64,
    limit: u64,
}

impl Budget {
    pub fn new(limit: u64) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit,
        }
    }

    pub fn write(self: &Arc<Self>, path: &Path, bytes: &[u8]) -> Result<()> {
        let mut output = BudgetFile::new(path, self.clone())?;
        output.write_all(bytes)?;
        output.flush()?;
        Ok(())
    }
}

struct BudgetFile {
    file: File,
    budget: Arc<Budget>,
}

impl BudgetFile {
    fn new(path: &Path, budget: Arc<Budget>) -> Result<Self> {
        Ok(Self {
            file: File::options().write(true).create_new(true).open(path)?,
            budget,
        })
    }
}

impl Write for BudgetFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len() as u64;
        self.budget
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(count)
                    .filter(|next| *next <= self.budget.limit)
            })
            .map_err(|_| io::Error::other("fixture output disk budget exceeded"))?;
        match self.file.write(bytes) {
            Ok(written) => {
                self.budget
                    .used
                    .fetch_sub(count - written as u64, Ordering::SeqCst);
                Ok(written)
            }
            Err(error) => {
                self.budget.used.fetch_sub(count, Ordering::SeqCst);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Clone)]
struct Bounds {
    min: ScalarValue,
    max: ScalarValue,
    nulls: u64,
}

struct Stats {
    schema: SchemaRef,
    columns: Vec<Option<Bounds>>,
    rows: u64,
}

impl Stats {
    fn new(schema: SchemaRef) -> Self {
        Self {
            columns: vec![None; schema.fields().len()],
            schema,
            rows: 0,
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            return Err("unexpected batch schema".into());
        }
        self.rows += batch.num_rows() as u64;
        for (slot, array) in self.columns.iter_mut().zip(batch.columns()) {
            let next = bounds(array.as_ref())?;
            if let Some(previous) = slot {
                if !next.min.is_null() && (previous.min.is_null() || next.min < previous.min) {
                    previous.min = next.min;
                }
                if !next.max.is_null() && (previous.max.is_null() || next.max > previous.max) {
                    previous.max = next.max;
                }
                previous.nulls += next.nulls;
            } else {
                *slot = Some(next);
            }
        }
        Ok(())
    }

    fn json(&self) -> Result<Value> {
        let mut minimum = Map::new();
        let mut maximum = Map::new();
        let mut nulls = Map::new();
        for (field, bounds) in self.schema.fields().iter().zip(&self.columns) {
            let bounds = bounds
                .as_ref()
                .ok_or("statistics missing for an empty file")?;
            minimum.insert(field.name().clone(), scalar_json(&bounds.min)?);
            maximum.insert(field.name().clone(), scalar_json(&bounds.max)?);
            nulls.insert(field.name().clone(), json!(bounds.nulls));
        }
        Ok(
            json!({"numRecords": self.rows, "minValues": minimum, "maxValues": maximum, "nullCount": nulls}),
        )
    }
}

fn bounds(array: &dyn Array) -> Result<Bounds> {
    macro_rules! numeric {
        ($array:ty, $variant:ident) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or("array type mismatch")?;
            (
                ScalarValue::$variant(min(values)),
                ScalarValue::$variant(max(values)),
            )
        }};
    }
    let (low, high) = match array.data_type() {
        DataType::Int64 => numeric!(Int64Array, Int64),
        DataType::Int32 => numeric!(Int32Array, Int32),
        DataType::Date32 => numeric!(Date32Array, Date32),
        DataType::Decimal128(p, s) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or("decimal type mismatch")?;
            (
                ScalarValue::Decimal128(min(values), *p, *s),
                ScalarValue::Decimal128(max(values), *p, *s),
            )
        }
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("string type mismatch")?;
            (
                ScalarValue::Utf8(min_string(values).map(str::to_owned)),
                ScalarValue::Utf8(max_string(values).map(str::to_owned)),
            )
        }
        other => return Err(format!("unsupported fixture statistics type: {other:?}").into()),
    };
    Ok(Bounds {
        min: low,
        max: high,
        nulls: array.null_count() as u64,
    })
}

fn scalar_json(value: &ScalarValue) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    Ok(match value {
        ScalarValue::Int64(Some(v)) => json!(v),
        ScalarValue::Int32(Some(v)) => json!(v),
        ScalarValue::Utf8(Some(v)) => json!(v),
        ScalarValue::Date32(Some(v)) => json!(
            chrono::NaiveDate::from_num_days_from_ce_opt(719163 + v)
                .ok_or("invalid date")?
                .to_string()
        ),
        ScalarValue::Decimal128(Some(v), 15, 2) => {
            let text = format!(
                "{}{}.{:02}",
                if *v < 0 { "-" } else { "" },
                v.abs() / 100,
                v.abs() % 100
            );
            serde_json::from_str(&text)?
        }
        other => return Err(format!("unsupported statistics scalar: {other:?}").into()),
    })
}

pub struct TableWriter {
    path: PathBuf,
    schema: SchemaRef,
    budget: Arc<Budget>,
    current: Option<ArrowWriter<BudgetFile>>,
    stats: Stats,
    files: Vec<Value>,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl TableWriter {
    pub fn new(path: &Path, schema: SchemaRef, budget: Arc<Budget>) -> Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            path: path.to_owned(),
            stats: Stats::new(schema.clone()),
            schema,
            budget,
            current: None,
            files: Vec::new(),
            pending: Vec::new(),
            pending_rows: 0,
        })
    }

    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = (BATCH_ROWS - self.pending_rows).min(batch.num_rows() - offset);
            self.pending.push(batch.slice(offset, length));
            self.pending_rows += length;
            offset += length;
            if self.pending_rows == BATCH_ROWS {
                self.flush_batch()?;
            }
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let batch = if self.pending.len() == 1 {
            self.pending.pop().ok_or("missing buffered batch")?
        } else {
            concat_batches(&self.schema, &self.pending)?
        };
        self.pending.clear();
        self.pending_rows = 0;
        if self.current.is_none() {
            let path = self
                .path
                .join(format!("part-{:05}.parquet", self.files.len()));
            self.current = Some(ArrowWriter::try_new(
                BudgetFile::new(&path, self.budget.clone())?,
                self.schema.clone(),
                Some(writer_properties()?),
            )?);
        }
        self.stats.update(&batch)?;
        self.current
            .as_mut()
            .ok_or("missing writer")?
            .write(&batch)?;
        if self.stats.rows == FILE_ROWS as u64 {
            self.finish_file()?;
        }
        Ok(())
    }

    fn finish_file(&mut self) -> Result<()> {
        let Some(writer) = self.current.take() else {
            return Ok(());
        };
        writer.close()?;
        let name = format!("part-{:05}.parquet", self.files.len());
        let path = self.path.join(&name);
        let stats = self.stats.json()?;
        let mut inspected = inspect_file(&path, self.schema.clone(), &stats)?;
        inspected["path"] = json!(name);
        self.files.push(inspected);
        self.stats = Stats::new(self.schema.clone());
        Ok(())
    }

    pub fn finish(mut self, delta: Option<(&str, &str)>) -> Result<Value> {
        self.flush_batch()?;
        self.finish_file()?;
        let rows: u64 = self.files.iter().filter_map(|v| v["rows"].as_u64()).sum();
        let bytes: u64 = self.files.iter().filter_map(|v| v["bytes"].as_u64()).sum();
        let schema = delta_schema(&self.schema)?;
        let mut log_manifest = Value::Null;
        if let Some((profile, id)) = delta {
            let table_id = Uuid::new_v5(&Uuid::NAMESPACE_URL,
                format!("https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{profile}/{id}").as_bytes());
            let mut actions = vec![
                json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
                json!({"metaData": {"id": table_id.to_string(), "format": {"provider": "parquet", "options": {}},
                    "schemaString": serde_json::to_string(&schema)?, "partitionColumns": [], "configuration": {}, "createdTime": 0}}),
            ];
            for file in &self.files {
                actions.push(json!({"add": {
                    "path": file["path"], "partitionValues": {}, "size": file["bytes"],
                    "modificationTime": 0, "dataChange": true,
                    "stats": serde_json::to_string(&file["delta_stats"])?
                }}));
            }
            let mut text = actions
                .iter()
                .map(serde_json::to_string)
                .collect::<std::result::Result<Vec<_>, _>>()?
                .join("\n");
            text.push('\n');
            fs::create_dir(self.path.join("_delta_log"))?;
            let log_path = self.path.join("_delta_log/00000000000000000000.json");
            self.budget.write(&log_path, text.as_bytes())?;
            log_manifest = json!({"path": "_delta_log/00000000000000000000.json", "bytes": text.len(), "sha256": hash_bytes(text.as_bytes())});
        }
        Ok(
            json!({"rows": rows, "bytes": bytes, "file_count": self.files.len(), "schema": schema,
            "files": self.files, "delta_log": log_manifest}),
        )
    }
}

fn delta_schema(schema: &Schema) -> Result<Value> {
    let fields = schema.fields().iter().map(|field| {
        let kind = match field.data_type() {
            DataType::Int64 => "long", DataType::Int32 => "integer", DataType::Utf8 => "string",
            DataType::Date32 => "date", DataType::Decimal128(15, 2) => "decimal(15,2)",
            other => return Err(format!("unsupported schema field: {other:?}").into()),
        };
        Ok(json!({"name": field.name(), "type": kind, "nullable": field.is_nullable(), "metadata": {}}))
    }).collect::<Result<Vec<_>>>()?;
    Ok(json!({"type": "struct", "fields": fields}))
}

pub fn parquet_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(path)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "parquet"));
    paths.sort();
    if paths.is_empty() {
        return Err(format!("no Parquet files at {}", path.display()).into());
    }
    Ok(paths)
}

pub fn read_batches(path: &Path) -> Result<ParquetRecordBatchReader> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(BATCH_ROWS)
        .build()?)
}

pub fn inspect_file(path: &Path, schema: SchemaRef, expected_stats: &Value) -> Result<Value> {
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(path)?,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )?;
    if reader.schema() != &schema {
        return Err("on-disk Arrow schema changed".into());
    }
    let metadata = reader.metadata();
    let version = metadata.file_metadata().version();
    let created_by = metadata.file_metadata().created_by();
    if version != 1 || created_by != Some("dar-selective-read-v1 parquet-rs 58.4.0") {
        return Err("unexpected Parquet writer identity".into());
    }
    let columns = metadata.column_index().ok_or("missing column indexes")?;
    let offsets = metadata.offset_index().ok_or("missing offset indexes")?;
    let file_bytes = fs::metadata(path)?.len();
    let mut groups = Vec::new();
    let mut first_row = 0;
    for (g, group) in metadata.row_groups().iter().enumerate() {
        if group.num_rows() <= 0
            || group.num_rows() > GROUP_ROWS as i64
            || (g + 1 < metadata.num_row_groups() && group.num_rows() != GROUP_ROWS as i64)
        {
            return Err("unexpected row-group boundary".into());
        }
        let mut chunks = Vec::new();
        for (c, chunk) in group.columns().iter().enumerate() {
            let index = &columns[g][c];
            if matches!(index, ColumnIndexMetaData::NONE) {
                return Err("missing page statistics".into());
            }
            let locations = offsets[g][c].page_locations();
            if locations.is_empty() || locations.len() != index.num_pages() as usize {
                return Err("page index/offset count mismatch".into());
            }
            let mut pages = Vec::new();
            for (p, location) in locations.iter().enumerate() {
                let end = locations
                    .get(p + 1)
                    .map_or(group.num_rows(), |next| next.first_row_index);
                if (p == 0 && location.first_row_index != 0)
                    || end <= location.first_row_index
                    || end > group.num_rows()
                    || location.offset < 0
                    || location.compressed_page_size <= 0
                    || location.offset as u64 + location.compressed_page_size as u64 > file_bytes
                {
                    return Err("invalid physical page boundary".into());
                }
                pages.push(json!({"first_row": location.first_row_index, "rows": end - location.first_row_index,
                    "offset": location.offset, "compressed_bytes": location.compressed_page_size,
                    "nulls": index.null_count(p), "all_null": index.is_null_page(p),
                    "bounds": page_bounds(index, p)?}));
            }
            let statistics = chunk.statistics().ok_or("missing row-group statistics")?;
            chunks.push(json!({
                "column": schema.field(c).name(), "physical_type": chunk.column_type().to_string(),
                "compression": chunk.compression().to_string(),
                "encodings": chunk.encodings().map(|v| v.to_string()).collect::<Vec<_>>(),
                "compressed_bytes": chunk.compressed_size(), "uncompressed_bytes": chunk.uncompressed_size(),
                "dictionary_page_offset": chunk.dictionary_page_offset(),
                "column_index": {"offset": chunk.column_index_offset(), "length": chunk.column_index_length(),
                    "boundary_order": index.get_boundary_order().map(|v| v.to_string())},
                "offset_index": {"offset": chunk.offset_index_offset(), "length": chunk.offset_index_length()},
                "statistics": {"nulls": statistics.null_count_opt(),
                    "min_hex": statistics.min_bytes_opt().map(hex), "max_hex": statistics.max_bytes_opt().map(hex)},
                "pages": pages
            }));
        }
        groups.push(json!({"first_row": first_row, "rows": group.num_rows(), "columns": chunks}));
        first_row += group.num_rows();
    }
    if metadata.num_row_groups() > 8 {
        return Err("too many row groups in a data file".into());
    }
    if first_row != metadata.file_metadata().num_rows()
        || Some(first_row as u64) != expected_stats["numRecords"].as_u64()
    {
        return Err("Parquet footer row count differs from Delta statistics".into());
    }
    let created_by = created_by.map(str::to_owned);
    let mut actual_stats = Stats::new(schema.clone());
    for batch in reader.with_batch_size(BATCH_ROWS).build()? {
        actual_stats.update(&batch?)?;
    }
    let actual_stats = actual_stats.json()?;
    if &actual_stats != expected_stats {
        return Err(format!(
            "Delta statistics disagree with full Parquet read: {}",
            path.display()
        )
        .into());
    }
    Ok(
        json!({"rows": actual_stats["numRecords"], "bytes": file_bytes,
        "parquet_version": version, "created_by": created_by,
        "sha256": hash_file(path)?, "delta_stats": actual_stats, "row_groups": groups}),
    )
}

fn page_bounds(index: &ColumnIndexMetaData, page: usize) -> Result<Value> {
    Ok(match index {
        ColumnIndexMetaData::INT32(v) => {
            json!({"min": v.min_value(page), "max": v.max_value(page)})
        }
        ColumnIndexMetaData::INT64(v) => {
            json!({"min": v.min_value(page), "max": v.max_value(page)})
        }
        ColumnIndexMetaData::BYTE_ARRAY(v) | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(v) => {
            json!({"min_hex": v.min_value(page).map(hex), "max_hex": v.max_value(page).map(hex)})
        }
        other => return Err(format!("unexpected fixture page index type: {other:?}").into()),
    })
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(result, "{byte:02x}");
    }
    result
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; MIB as usize];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    Ok(hex(&digest.finalize()))
}

pub fn limit_memory(bytes: u64) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: both calls receive an initialized rlimit with a valid lifetime.
        if unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut limit) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        limit.rlim_cur = bytes.min(limit.rlim_max).min(limit.rlim_cur);
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = bytes;
        Err("the frozen generator profile requires Linux".into())
    }
}

pub fn available_disk(path: &Path) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: statvfs writes the provided structure on success; the path is NUL terminated.
        if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let stats = unsafe { stats.assume_init() };
        Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err("disk preflight requires Linux".into())
    }
}

pub fn peak_rss() -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes this structure on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let usage = unsafe { usage.assume_init() };
        Ok(u64::try_from(usage.ru_maxrss)? * 1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("resource accounting requires Linux".into())
    }
}
