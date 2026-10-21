//! Isolated numeric and byte-array fixtures; independent value checks stay in Python.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int64Array, StringBuilder,
    },
    buffer::NullBuffer,
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema},
    ipc::{
        CompressionType,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    dv,
    fixtures::{MIB, Result, hash_file},
};

// SplitMix64 fixes the sequence across platforms without depending on Python's RNG.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn bytes(&mut self, size: usize) -> Vec<u8> {
        let mut value = vec![0; size];
        for chunk in value.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes()[..chunk.len()]);
        }
        value
    }

    fn decimal(&mut self, precision: u8) -> i128 {
        let bound = 10u128.pow(u32::from(precision)) - 1;
        let width = 2 * bound + 1;
        let threshold = width.wrapping_neg() % width;
        loop {
            let value = (u128::from(self.next()) << 64) | u128::from(self.next());
            if value >= threshold {
                let offset = value % width;
                return if offset < bound {
                    -((bound - offset) as i128)
                } else {
                    (offset - bound) as i128
                };
            }
        }
    }
}

fn provenance() -> Value {
    json!({"implementation":"rust", "random":"splitmix64-v1", "arrow":"58.4.0", "parquet":"58.4.0"})
}

fn object(root: &Path, path: &Path) -> Result<Value> {
    Ok(
        json!({"path":path.strip_prefix(root)?, "bytes":fs::metadata(path)?.len(), "sha256":hash_file(path)?}),
    )
}

fn parquet(path: &Path, batch: &RecordBatch, properties: WriterProperties) -> Result<()> {
    let mut writer =
        ArrowWriter::try_new(File::create_new(path)?, batch.schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

fn properties(
    rows: usize,
    page_bytes: usize,
    page_rows: usize,
) -> parquet::file::properties::WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_max_row_group_row_count(Some(rows))
        .set_max_row_group_bytes(None)
        .set_data_page_size_limit(page_bytes)
        .set_data_page_row_count_limit(page_rows)
        .set_write_batch_size(1024)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::UNCOMPRESSED)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_created_by("dar-page-fixtures-v1 parquet-rs 58.4.0".into())
}

fn references(
    output: &Path,
    batches: BTreeMap<String, Vec<RecordBatch>>,
    compressed: bool,
) -> Result<Value> {
    let mut records = Map::new();
    for (name, batches) in batches {
        let path = output.join(format!("{name}.arrow"));
        let options = IpcWriteOptions::default()
            .try_with_compression(compressed.then_some(CompressionType::ZSTD))?;
        let mut writer = StreamWriter::try_new_with_options(
            File::create_new(&path)?,
            &batches[0].schema(),
            options,
        )?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        let mut record = object(output, &path)?;
        record["rows"] = json!(batches.iter().map(RecordBatch::num_rows).sum::<usize>());
        records.insert(name, record);
    }
    Ok(Value::Object(records))
}

fn log(
    output: &Path,
    schema: &Schema,
    id: &str,
    deletion_vectors: bool,
    additions: Vec<Value>,
) -> Result<Value> {
    let fields = schema.fields().iter().map(|field| {
        let data_type = match field.data_type() {
            DataType::Int32 => "integer".to_owned(),
            DataType::Int64 => "long".to_owned(),
            DataType::Float32 => "float".to_owned(),
            DataType::Float64 => "double".to_owned(),
            DataType::Utf8 => "string".to_owned(),
            DataType::Binary => "binary".to_owned(),
            DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
            _ => unreachable!("page fixture type"),
        };
        json!({"name":field.name(), "type":data_type, "nullable":field.is_nullable(), "metadata":{}})
    }).collect::<Vec<_>>();
    let protocol = if deletion_vectors {
        json!({"minReaderVersion":3, "minWriterVersion":7, "readerFeatures":["deletionVectors"], "writerFeatures":["deletionVectors","invariants","appendOnly"]})
    } else {
        json!({"minReaderVersion":1, "minWriterVersion":2})
    };
    let actions = [
        json!({"protocol":protocol}),
        json!({"metaData":{"id":id,
        "format":{"provider":"parquet","options":{}}, "schemaString":json!({"type":"struct","fields":fields}).to_string(),
        "partitionColumns":[], "configuration":{}}}),
    ];
    let directory = output.join("_delta_log");
    fs::create_dir(&directory)?;
    let path = directory.join("00000000000000000000.json");
    let mut file = File::create_new(&path)?;
    for action in actions.into_iter().chain(additions) {
        writeln!(file, "{action}")?;
    }
    object(output, &path)
}

fn save(output: &Path, value: Value) -> Result<()> {
    let mut file = File::create_new(output.join("manifest.json"))?;
    serde_json::to_writer_pretty(&mut file, &value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn numeric_types(decimals: bool) -> Vec<(&'static str, DataType)> {
    if decimals {
        vec![
            ("decimal20", DataType::Decimal128(20, 4)),
            ("decimal38", DataType::Decimal128(38, 10)),
        ]
    } else {
        vec![
            ("int32", DataType::Int32),
            ("int64", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
        ]
    }
}

fn numeric_batch(decimals: bool, file: usize, rows: usize) -> Result<RecordBatch> {
    let mut fields = vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("is_match", DataType::Int32, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            (file * rows..(file + 1) * rows).map(|i| i as i64),
        )),
        Arc::new(Int32Array::from_iter_values(
            (file * rows..(file + 1) * rows).map(|i| i32::from(i % 8191 == 0)),
        )),
    ];
    for (name, dtype) in numeric_types(decimals) {
        for column in 0..8 {
            let nullable = column % 2 == 0;
            let mut rng = Random((450 + file * 32 + fields.len() - 2) as u64);
            let nulls = nullable.then(|| {
                NullBuffer::from((0..rows).map(|_| rng.next() & 1 != 0).collect::<Vec<_>>())
            });
            let values: ArrayRef = match dtype {
                DataType::Int32 => Arc::new(Int32Array::new(
                    (0..rows)
                        .map(|_| rng.next() as i32)
                        .collect::<Vec<_>>()
                        .into(),
                    nulls,
                )),
                DataType::Int64 => Arc::new(Int64Array::new(
                    (0..rows)
                        .map(|_| rng.next() as i64)
                        .collect::<Vec<_>>()
                        .into(),
                    nulls,
                )),
                DataType::Float32 => Arc::new(Float32Array::new(
                    (0..rows)
                        .map(|_| f32::from_bits(rng.next() as u32))
                        .collect::<Vec<_>>()
                        .into(),
                    nulls,
                )),
                DataType::Float64 => Arc::new(Float64Array::new(
                    (0..rows)
                        .map(|_| f64::from_bits(rng.next()))
                        .collect::<Vec<_>>()
                        .into(),
                    nulls,
                )),
                DataType::Decimal128(precision, scale) => Arc::new(
                    Decimal128Array::new(
                        (0..rows)
                            .map(|_| rng.decimal(precision))
                            .collect::<Vec<_>>()
                            .into(),
                        nulls,
                    )
                    .with_precision_and_scale(precision, scale)?,
                ),
                _ => unreachable!("numeric fixture type"),
            };
            fields.push(Field::new(
                format!("{name}_{column}"),
                dtype.clone(),
                nullable,
            ));
            columns.push(values);
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn numeric(output: &Path, decimals: bool, rows: usize) -> Result<()> {
    fs::create_dir(output)?;
    let mut files = Vec::new();
    let mut additions = Vec::new();
    let mut expected: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();
    let mut schema = None;
    for file in 0..2 {
        let batch = numeric_batch(decimals, file, rows)?;
        schema = Some(batch.schema());
        let path = output.join(format!("part-{file}.parquet"));
        parquet(
            &path,
            &batch,
            properties(rows, 4 * MIB as usize, rows).build(),
        )?;
        let record = object(output, &path)?;
        additions.push(json!({"add":{"path":record["path"], "size":record["bytes"], "partitionValues":{}, "modificationTime":0, "dataChange":true}}));
        files.push(record);
        let selected = BooleanArray::from(
            (file * rows..(file + 1) * rows)
                .map(|i| i % 8191 == 0)
                .collect::<Vec<_>>(),
        );
        for (index, (name, _)) in numeric_types(decimals).iter().enumerate() {
            let projection = std::iter::once(0)
                .chain(2 + 8 * index..2 + 8 * (index + 1))
                .collect::<Vec<_>>();
            expected
                .entry((*name).into())
                .or_default()
                .push(filter_record_batch(
                    &batch.project(&projection)?,
                    &selected,
                )?);
        }
    }
    let log = log(
        output,
        &schema.unwrap(),
        if decimals {
            "decimal-pages-451"
        } else {
            "numeric-pages-450"
        },
        false,
        additions,
    )?;
    let references = references(output, expected, false)?;
    save(
        output,
        json!({"format":"numeric-page-fixture-v1", "generator":provenance(), "seed":450, "rows_per_file":rows,
        "row_group_rows":rows, "page_target_bytes":4*MIB, "page_row_limit":rows, "files":files, "log_sha256":log["sha256"], "references":references}),
    )
}

fn length_rows(case: &str) -> Result<usize> {
    Ok(match case {
        "long" => 16384,
        "short" | "selected-long-batch" | "mixed-bad-first" | "mixed-alternating" => 65536,
        "selected-long" => 8192,
        "length-heavy" => 262144,
        _ => return Err("unknown length fixture case".into()),
    })
}

fn value_size(case: &str, row: usize, column: usize, rng: &mut Random) -> usize {
    match case {
        "mixed-bad-first" | "mixed-alternating" => {
            let page = row / 8192;
            let skewed = if case == "mixed-bad-first" {
                page < 4
            } else {
                (page + column).is_multiple_of(2)
            };
            if skewed {
                if row.is_multiple_of(997) {
                    524288
                } else {
                    row % 2
                }
            } else {
                512 + (rng.next() % 64) as usize
            }
        }
        "long" => 1536 + (rng.next() % 1024) as usize,
        "short" => 8 + (rng.next() % 56) as usize,
        "selected-long" | "selected-long-batch" => {
            if row.is_multiple_of(997) {
                262144
            } else {
                row % 2
            }
        }
        "length-heavy" => (rng.next() % 2) as usize,
        _ => unreachable!("validated length case"),
    }
}

fn length_batch(case: &str, file: usize, rows: usize) -> Result<RecordBatch> {
    let mut fields = vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("grouped", DataType::Int32, false),
        Field::new("scattered", DataType::Int32, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            (file * rows..(file + 1) * rows).map(|i| i as i64),
        )),
        Arc::new(Int32Array::from_iter_values(
            (0..rows).map(|i| i32::from(i % 2048 < 8)),
        )),
        Arc::new(Int32Array::from_iter_values(
            (0..rows).map(|i| i32::from(i % 997 == 0)),
        )),
    ];
    for column in 0..8 {
        let mut rng = Random((452 + 16 * file + column) as u64);
        let nullable = column % 2 == 1;
        let string = column % 4 < 2;
        let mut strings = StringBuilder::new();
        let mut binary = BinaryBuilder::new();
        for row in 0..rows {
            let size = value_size(case, row, column, &mut rng);
            let value = rng.bytes(size);
            if nullable && row % 7 == 0 {
                if string {
                    strings.append_null();
                } else {
                    binary.append_null();
                }
            } else if string {
                let mut text = String::with_capacity(size + 2);
                for byte in &value[..size / 2] {
                    text.push(b"0123456789abcdef"[usize::from(byte >> 4)] as char);
                    text.push(b"0123456789abcdef"[usize::from(byte & 15)] as char);
                }
                if size > 2 {
                    text.push('\u{e9}');
                }
                strings.append_value(text);
            } else {
                binary.append_value(value);
            }
        }
        fields.push(Field::new(
            format!("payload_{column}"),
            if string {
                DataType::Utf8
            } else {
                DataType::Binary
            },
            nullable,
        ));
        columns.push(if string {
            Arc::new(strings.finish())
        } else {
            Arc::new(binary.finish())
        });
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn length(
    output: &Path,
    case: &str,
    inspector: &Path,
    compression: &str,
    rows: usize,
) -> Result<()> {
    fs::create_dir(output)?;
    let page_bytes = (if case.starts_with("mixed-") { 8 } else { 4 }) * MIB as usize;
    let mut objects = Vec::new();
    let mut layouts = Vec::new();
    let mut additions = Vec::new();
    let mut expected: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();
    let mut schema = None;
    for file in 0..2 {
        let batch = length_batch(case, file, rows)?;
        schema = Some(batch.schema());
        let mut settings =
            properties(rows / 2, page_bytes, 8192).set_compression(if compression == "zstd" {
                Compression::ZSTD(ZstdLevel::try_new(3)?)
            } else {
                Compression::UNCOMPRESSED
            });
        for field in batch.schema().fields().iter().skip(3) {
            settings = settings.set_column_encoding(
                field.name().as_str().into(),
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
            );
        }
        let path = output.join(format!("part-{file}.parquet"));
        parquet(&path, &batch, settings.build())?;
        let inspected = Command::new(inspector).arg("inspect").arg(&path).output()?;
        if !inspected.status.success() {
            return Err(format!(
                "page inspection failed: {}",
                String::from_utf8_lossy(&inspected.stderr)
            )
            .into());
        }
        let mut layout: Value = serde_json::from_slice(&inspected.stdout)?;
        layout["file"] = json!(path.file_name().unwrap());
        layouts.push(layout);
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("delta-length-452/{case}/{file}").as_bytes(),
        );
        let dv_path = output.join(format!("deletion_vector_{uuid}.bin"));
        let (bytes, mut descriptor) =
            dv::payload(&(0..rows as u64).step_by(103).collect::<Vec<_>>())?;
        File::create_new(&dv_path)?.write_all(&bytes)?;
        descriptor["pathOrInlineDv"] = json!(z85::encode(uuid.as_bytes()));
        let record = object(output, &path)?;
        additions.push(json!({"add":{"path":record["path"], "size":record["bytes"], "partitionValues":{},
            "modificationTime":0, "dataChange":true, "deletionVector":descriptor, "stats":json!({"numRecords":rows,"tightBounds":false}).to_string()}}));
        objects.extend([record, object(output, &dv_path)?]);
        let projection = batch.project(&std::iter::once(0).chain(3..11).collect::<Vec<_>>())?;
        for selection in ["grouped", "scattered", "dense"] {
            let selected = BooleanArray::from(
                (0..rows)
                    .map(|row| {
                        row % 103 != 0
                            && match selection {
                                "grouped" => row % 2048 < 8,
                                "scattered" => row.is_multiple_of(997),
                                _ => true,
                            }
                    })
                    .collect::<Vec<_>>(),
            );
            expected
                .entry(selection.into())
                .or_default()
                .push(filter_record_batch(&projection, &selected)?);
        }
    }
    let id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("delta-length-452/{case}").as_bytes(),
    );
    objects.push(log(
        output,
        &schema.unwrap(),
        &id.to_string(),
        true,
        additions,
    )?);
    let references = references(output, expected, true)?;
    save(
        output,
        json!({"format":"delta-length-network-v1", "case":case, "rows_per_file":rows, "columns":8, "seed":452,
        "compression":compression, "compression_level":if compression == "zstd" { json!(3) } else { Value::Null },
        "row_group_rows":rows/2, "page_target_bytes":page_bytes, "page_row_limit":8192, "generator":provenance(),
        "objects":objects, "references":references, "layout":layouts}),
    )
}

pub fn run(command: &str, args: &[String]) -> Result<()> {
    let mut args = args.iter();
    let mut output = None;
    let mut inspector = None;
    let mut case = None;
    let mut compression = "NONE";
    let mut decimals = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" if output.is_none() => {
                output = Some(PathBuf::from(args.next().ok_or("missing output")?))
            }
            "--decimals" if command == "numeric-pages" && !decimals => decimals = true,
            "--binary" if command == "length-pages" && inspector.is_none() => {
                inspector = Some(fs::canonicalize(args.next().ok_or("missing inspector")?)?)
            }
            "--case" if command == "length-pages" && case.is_none() => {
                case = Some(args.next().ok_or("missing case")?.as_str())
            }
            "--compression" if command == "length-pages" => {
                compression = args.next().ok_or("missing compression")?
            }
            _ => return Err(format!("unknown or repeated {command} option: {arg}").into()),
        }
    }
    let output = output.ok_or("--output NEW_DIRECTORY is required")?;
    if command == "numeric-pages" {
        numeric(&output, decimals, 524288)
    } else {
        let case = case.ok_or("--case is required")?;
        let rows = length_rows(case)?;
        if !["NONE", "zstd"].contains(&compression) {
            return Err("compression must be NONE or zstd".into());
        }
        length(
            &output,
            case,
            &inspector.ok_or("--binary INSPECTOR is required")?,
            compression,
            rows,
        )
    }
}

#[cfg(test)]
mod tests;
