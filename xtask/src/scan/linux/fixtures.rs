use std::{fs, path::Path, sync::Arc};

use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use snafu::ResultExt;

use crate::error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, JsonSnafu, ParquetSnafu, Result};

pub(super) const FLOAT64_VALUES: [f64; 2] = [1.9651349465042103, 1.9651349465042105];

pub(super) fn create_float64_table(table: &Path) -> Result<RecordBatch> {
    let log = table.join("_delta_log");
    fs::create_dir_all(&log).context(FileSnafu { path: &log })?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let data_file = table.join("part.parquet");
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(FLOAT64_VALUES.to_vec())),
        ],
    )
    .context(ArrowSnafu { path: &data_file })?;
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&data_file).context(FileSnafu { path: &data_file })?,
        schema,
        None,
    )
    .context(ParquetSnafu { path: &data_file })?;
    writer
        .write(&data)
        .context(ParquetSnafu { path: &data_file })?;
    writer.close().context(ParquetSnafu { path: &data_file })?;
    let fields: Vec<_> = [("id", "integer"), ("value", "double")]
        .into_iter()
        .map(|(name, kind)| json!({"name": name, "type": kind, "nullable": true, "metadata": {}}))
        .collect();
    let actions = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "float64-predicates", "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": [], "configuration": {},
        }}),
        json!({"add": {
            "path": "part.parquet", "partitionValues": {},
            "size": fs::metadata(&data_file).context(FileSnafu { path: &data_file })?.len(),
            "modificationTime": 0, "dataChange": true,
        }}),
    ];
    write_log(&log, &actions)?;
    Ok(data)
}

pub(super) fn create_repeated_table(
    source: &Path,
    destination: &Path,
    file_count: usize,
) -> Result<()> {
    let source_log = source.join("_delta_log/00000000000000000000.json");
    let actions: Vec<Value> = fs::read_to_string(&source_log)
        .context(FileSnafu { path: &source_log })?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .context(JsonSnafu {
            subject: "fixture log actions",
        })?;
    let template = actions
        .iter()
        .find_map(|action| action.get("add"))
        .ok_or_else(|| {
            InvalidInputSnafu {
                message: "Spark fixture has no data file to repeat",
            }
            .build()
        })?;
    let path = template["path"].as_str().ok_or_else(|| {
        InvalidInputSnafu {
            message: "Spark fixture data file has no path",
        }
        .build()
    })?;
    let source_file = source.join(path);
    let log = destination.join("_delta_log");
    fs::create_dir_all(&log).context(FileSnafu { path: &log })?;
    let mut repeated: Vec<_> = actions
        .iter()
        .filter(|action| action.get("protocol").is_some() || action.get("metaData").is_some())
        .cloned()
        .collect();
    // Repeat one Spark file so lifecycle checks use many small, independently readable files.
    for index in 0..file_count {
        let name = format!("part-{index:03}.parquet");
        let destination_file = destination.join(&name);
        fs::copy(&source_file, &destination_file).context(FileSnafu {
            path: &destination_file,
        })?;
        let mut add = template.clone();
        add["path"] = json!(name);
        repeated.push(json!({"add": add}));
    }
    write_log(&log, &repeated)
}

fn write_log(log_directory: &Path, actions: &[Value]) -> Result<()> {
    let log = log_directory.join("00000000000000000000.json");
    let contents: String = actions.iter().map(|action| format!("{action}\n")).collect();
    fs::write(&log, contents).context(FileSnafu { path: &log })
}

#[cfg(test)]
mod tests;
