use super::{Result, State, require, table};
use crate::selective_read::{array, text};
use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Schema};
use arrow_select::{concat::concat_batches, filter::filter_record_batch};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

fn parquet_file(directory: &Path) -> Result<PathBuf> {
    let paths = fs::read_dir(directory)?
        .map(|e| Ok(e?.path()))
        .collect::<Result<Vec<_>>>()?;
    let paths: Vec<_> = paths
        .into_iter()
        .filter(|p| p.extension().is_some_and(|s| s == "parquet"))
        .collect();
    require(paths.len() == 1, "expected one corpus Parquet file")?;
    Ok(paths[0].clone())
}

fn normalized(batch: &RecordBatch) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let mut kind = field.data_type();
            while let DataType::Dictionary(_, value) = kind {
                kind = value;
            }
            let kind = match kind {
                DataType::LargeUtf8 | DataType::Utf8View => &DataType::Utf8,
                _ => kind,
            };
            field.as_ref().clone().with_data_type(kind.clone())
        })
        .collect::<Vec<_>>();
    table::cast(batch, Arc::new(Schema::new(fields)))
}

fn ids(batch: &RecordBatch) -> Result<&Int32Array> {
    batch
        .column_by_name("id")
        .and_then(|a| a.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| "invalid corpus id column".into())
}

fn exported(record: &Value, output: &Path, expected: &RecordBatch) -> Result<()> {
    require(
        record["status"] == "success",
        "reader capability query failed",
    )?;
    let queries = array(&record["queries"])?;
    require(!queries.is_empty(), "missing capability queries")?;
    let expected = table::sort(&normalized(expected)?, "id")?;
    for query in queries {
        let actual = table::stream(
            &output.join("reader").join(text(&query["result"])?),
            Some(8192),
        )?;
        table::equal(&table::sort(&normalized(&actual)?, "id")?, &expected)?;
        require(
            query["output_rows"] == expected.num_rows() && query["completion_ns"].is_null(),
            "invalid capability row count or timing",
        )?;
    }
    Ok(())
}

impl State {
    pub(super) fn capability_export(&mut self, request: &Value) -> Result<Value> {
        let original = self.harness.join("../../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/deletion_vectors");
        let source = table::parquet(
            &mut self.decoder,
            &[parquet_file(&original.join("table"))?],
            &[],
        )?;
        let saved = FileReader::try_new(File::open(original.join("expected.arrow"))?, None)?;
        let schema = saved.schema();
        let live = concat_batches(&schema, &saved.collect::<std::result::Result<Vec<_>, _>>()?)?;
        require(
            source.num_rows() == 12 && live.num_rows() == 9,
            "invalid external DV corpus row counts",
        )?;
        let source_ids: BTreeSet<_> = ids(&source)?.iter().collect();
        let live_ids: BTreeSet<_> = ids(&live)?.iter().collect();
        require(
            source_ids
                .difference(&live_ids)
                .copied()
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([Some(2), Some(5), Some(12)]),
            "external DV rows changed",
        )?;
        let mut expected = if request["deleted"].as_bool().ok_or("invalid deleted flag")? {
            live
        } else {
            source.clone()
        };
        if !request["ids"].is_null() {
            let wanted = array(&request["ids"])?;
            let selected = BooleanArray::from(
                ids(&expected)?
                    .iter()
                    .map(|id| wanted.contains(&json!(id)))
                    .collect::<Vec<_>>(),
            );
            expected = filter_record_batch(&expected, &selected)?;
        }
        match request["semantics"].as_str() {
            Some("nulls") => {
                let value = source
                    .column_by_name("value")
                    .ok_or("missing corpus value")?;
                let label = source
                    .column_by_name("label")
                    .ok_or("missing corpus label")?;
                let selected = BooleanArray::from(
                    (0..source.num_rows())
                        .map(|i| value.is_null(i) || label.is_null(i))
                        .collect::<Vec<_>>(),
                );
                expected = filter_record_batch(&source, &selected)?.project(&[2, 0, 1])?;
            }
            Some("duplicate") => expected = source.slice(1, 1),
            _ => require(
                request["semantics"].is_null(),
                "unknown capability semantics",
            )?,
        }
        exported(
            &request["record"],
            Path::new(text(&request["output"])?),
            &expected,
        )?;
        Ok(json!(expected.num_rows()))
    }

    pub(super) fn capability_stream(&mut self, request: &Value) -> Result<Value> {
        let output = Path::new(text(&request["output"])?);
        let source = output.join("no-dv");
        let schema = table::parquet(&mut self.decoder, &[parquet_file(&source)?], &[])?.schema();
        let log = "_delta_log/00000000000000000000.json";
        let mut actions = fs::read_to_string(source.join(log))?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        actions.retain(|a| a.get("protocol").is_some() || a.get("metaData").is_some());
        let output = output.join("stream-table");
        fs::create_dir(&output)?;
        fs::create_dir(output.join("_delta_log"))?;
        for index in 0..32 {
            let path = output.join(format!("{index:03}.parquet"));
            let data = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..8192)),
                    Arc::new(Int32Array::from(vec![1; 8192])),
                    Arc::new(StringArray::from(vec![
                        if index < 31 { "1" } else { "bad" };
                        8192
                    ])),
                ],
            )?;
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path)?,
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build(),
                ),
            )?;
            writer.write(&data)?;
            writer.close()?;
            actions.push(json!({"add":{"path":format!("{index:03}.parquet"),"size":fs::metadata(&path)?.len(),"partitionValues":{},"modificationTime":0,"dataChange":true}}));
        }
        let mut file = File::create_new(output.join(log))?;
        for action in actions {
            writeln!(file, "{action}")?;
        }
        Ok(Value::Null)
    }
}

pub(super) fn corrupt(request: &Value) -> Result<Value> {
    let mut values = table::stream(Path::new(text(&request["input"])?), None)?;
    require(values.num_rows() > 0, "cannot corrupt empty export")?;
    let index = values.schema().index_of("l_orderkey")?;
    let column = values
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("invalid order key")?;
    require(!column.is_null(0), "cannot corrupt null order key")?;
    let changed = column.value(0).checked_add(1).ok_or("order key overflow")?;
    let column = Int64Array::from_iter(
        column
            .iter()
            .enumerate()
            .map(|(i, value)| if i == 0 { Some(changed) } else { value }),
    );
    let mut columns = values.columns().to_vec();
    columns[index] = Arc::new(column);
    values = RecordBatch::try_new(values.schema(), columns)?;
    table::write(Path::new(text(&request["output"])?), &values)?;
    Ok(Value::Null)
}

#[cfg(test)]
mod tests;
