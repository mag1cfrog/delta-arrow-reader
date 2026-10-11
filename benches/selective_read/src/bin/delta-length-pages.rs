//! Local decoder proof for #452. Reported bytes cover page bodies, not query I/O.

#[path = "../delta_length.rs"]
mod delta_length;

use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray},
    compute::{cast, concat_batches, filter_record_batch},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection},
    },
    basic::{Compression, Encoding, PageType, Type},
    file::{
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
        reader::FileReader,
        serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn hash_bytes(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn fixture(case: &str) -> Result<RecordBatch> {
    let rows = if case == "length-heavy" { 131072 } else { 4099 };
    let mut text = Vec::with_capacity(rows);
    let mut binary = Vec::with_capacity(rows);
    for row in 0..rows {
        let size = match case {
            "short" => row * 7919 % 64,
            "long" => 256 + row * 7919 % 1024,
            "selected-long" => {
                if row % 997 == 0 {
                    262144
                } else {
                    row % 2
                }
            }
            "length-heavy" => ((row * 7919) ^ (row >> 4)) % 2,
            "all-null" => 0,
            _ => return Err("unknown fixture".into()),
        };
        let value = format!("row{row}:\u{00e9}\u{1f642}");
        text.push(value.repeat(size / value.len()) + &"x".repeat(size % value.len()));
        binary.push(
            (0..size)
                .map(|i| (row ^ i.wrapping_mul(31)) as u8)
                .collect::<Vec<_>>(),
        );
    }
    let nullable = |row: usize| case != "all-null" && !row.is_multiple_of(7);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        Arc::new(StringArray::from_iter_values(text.iter())),
        Arc::new(StringArray::from_iter(
            text.iter()
                .enumerate()
                .map(|(i, s)| nullable(i).then_some(s)),
        )),
        Arc::new(BinaryArray::from_iter_values(binary.iter())),
        Arc::new(BinaryArray::from_iter(
            binary
                .iter()
                .enumerate()
                .map(|(i, b)| nullable(i).then_some(b)),
        )),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("nullable_text", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, false),
        Field::new("nullable_binary", DataType::Binary, true),
    ]));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn write_fixture(path: &Path, batch: &RecordBatch) -> Result<()> {
    let mut properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(2049))
        .set_data_page_row_count_limit(1024)
        .set_data_page_size_limit(4 * 1024 * 1024);
    for field in &batch.schema().fields()[1..] {
        properties = properties.set_column_encoding(
            field.name().as_str().into(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        );
    }
    let mut writer = ArrowWriter::try_new(
        File::create_new(path)?,
        batch.schema(),
        Some(properties.build()),
    )?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

fn partial_file(input: &Path, output: &Path, selected: &[bool]) -> Result<Vec<Value>> {
    let original = fs::read(input)?;
    let mut partial = original.clone();
    let reader = SerializedFileReader::new_with_options(
        File::open(input)?,
        ReadOptionsBuilder::new().with_page_index().build(),
    )?;
    let metadata = reader.metadata();
    let indexes = metadata.offset_index().ok_or("missing offset indexes")?;
    let mut first = 0;
    let mut records = Vec::new();
    for (group, group_index) in indexes.iter().enumerate() {
        let row_group = reader.get_row_group(group)?;
        for (column, column_index) in group_index.iter().enumerate().skip(1) {
            let column_meta = metadata.row_group(group).column(column);
            assert_eq!(column_meta.compression(), Compression::UNCOMPRESSED);
            assert_eq!(column_meta.column_type(), Type::BYTE_ARRAY);
            assert_eq!(column_meta.column_descr().max_rep_level(), 0);
            let nullable = match column_meta.column_descr().max_def_level() {
                0 => false,
                1 => true,
                _ => return Err("nested definitions".into()),
            };
            let mut pages = row_group.get_column_page_reader(column)?;
            for (page_index, location) in column_index.page_locations().iter().enumerate() {
                // Full reads here inspect trusted generated fixtures. They are not timed.
                let page = pages.get_next_page()?.ok_or("missing page")?;
                assert_eq!(page.page_type(), PageType::DATA_PAGE);
                assert_eq!(page.encoding(), Encoding::DELTA_LENGTH_BYTE_ARRAY);
                let body = page.buffer();
                let end =
                    usize::try_from(location.offset + i64::from(location.compressed_page_size))?;
                let start = end.checked_sub(body.len()).ok_or("page boundary")?;
                assert_eq!(&original[start..end], body.as_ref());
                let row_start = first + usize::try_from(location.first_row_index)?;
                let rows = page.num_values() as usize;
                let next_row = column_index
                    .page_locations()
                    .get(page_index + 1)
                    .map_or(row_group.metadata().num_rows(), |next| next.first_row_index);
                assert_eq!(next_row - location.first_row_index, rows as i64);
                let selected: Vec<_> = selected[row_start..row_start + rows]
                    .iter()
                    .enumerate()
                    .filter_map(|(row, keep)| keep.then_some(row))
                    .collect();
                let plan = delta_length::discover(body.len(), rows, nullable, &selected, |range| {
                    Ok(body[range].to_vec())
                })
                .map_err(|error| format!("{error:?}"))?;
                partial[start..end].fill(0);
                partial[start..start + plan.prefix.len()].copy_from_slice(&plan.prefix);
                for range in &plan.ranges {
                    partial[start + range.start..start + range.end]
                        .copy_from_slice(&body[range.clone()]);
                }
                records.push(json!({"row_group":group,"column":column,"first_row":row_start,"rows":rows,
                    "nullable":nullable,"selected_rows":selected.len(),
                    "page_type":"DATA_PAGE","encoding":"DELTA_LENGTH_BYTE_ARRAY","compression":"UNCOMPRESSED",
                    "body_bytes":body.len(),"structural_bytes":plan.payload_start,"selected_payload_bytes":plan.selected_bytes,
                    "prefix_bytes":plan.prefix.len(),"discovery_rounds":plan.probes,"payload_requests":plan.ranges.len(),
                    "fetched_body_bytes":plan.prefix.len() + plan.ranges.iter().map(|r| r.len()).sum::<usize>()}));
            }
            assert!(pages.get_next_page()?.is_none());
        }
        first += usize::try_from(row_group.metadata().num_rows())?;
    }
    assert_eq!(first, selected.len());
    fs::write(output, partial)?;
    Ok(records)
}

fn check(path: &Path, expected: &RecordBatch, selection: &BooleanArray, views: bool) -> Result<()> {
    let schema = if views {
        Arc::new(Schema::new(
            expected
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let dtype = match field.data_type() {
                        DataType::Utf8 => DataType::Utf8View,
                        DataType::Binary => DataType::BinaryView,
                        dtype => dtype.clone(),
                    };
                    field.as_ref().clone().with_data_type(dtype)
                })
                .collect::<Vec<_>>(),
        ))
    } else {
        expected.schema()
    };
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(path)?,
        ArrowReaderOptions::new().with_schema(schema.clone()),
    )?
    .with_row_selection(RowSelection::from_filters(std::slice::from_ref(selection)))
    .with_batch_size(257)
    .build()?;
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    let batch = concat_batches(&schema, &batches)?;
    let columns = batch
        .columns()
        .iter()
        .zip(expected.schema().fields())
        .map(|(column, field)| cast(column, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(&RecordBatch::try_new(expected.schema(), columns)?, expected);
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 2 {
        return Err("delta-length-pages NEW_OUTPUT_DIRECTORY".into());
    }
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut results = Vec::new();
    for case in ["short", "long", "selected-long", "length-heavy", "all-null"] {
        let input = fixture(case)?;
        let path = output.join(format!("{case}.parquet"));
        write_fixture(&path, &input)?;
        let fixture_hash = hash_bytes(fs::read(&path)?);
        for shape in ["grouped", "scattered", "dense", "empty"] {
            // A deleted-row mask exercises physical row positions across pages/groups.
            // Delta DV loading and predicate evaluation belong to the later query runner.
            let selected: Vec<_> = (0..input.num_rows())
                .map(|row| {
                    (shape == "dense"
                        || (shape == "grouped" && row % 2049 < 17)
                        || (shape == "scattered" && row % 997 == 0))
                        && row % 103 != 0
                })
                .collect();
            let filter = BooleanArray::from(selected.clone());
            let expected = filter_record_batch(&input, &filter)?;
            let partial = output.join(format!("{case}-{shape}-selected-only.parquet"));
            let pages = partial_file(&path, &partial, &selected)?;
            for file in [&path, &partial] {
                for views in [false, true] {
                    check(file, &expected, &filter, views)?;
                }
            }
            results.push(json!({"case":case,"selection":shape,"input_rows":input.num_rows(),"output_rows":expected.num_rows(),
                "fixture_sha256":fixture_hash,"pages":pages,"correctness":"passed"}));
        }
    }
    let report = json!({"format":"delta-length-decoder-proof-v1","scope":"local correctness and page-body byte accounting; not network timings",
        "parquet":"58.4.0","source_sha256":hash_bytes(concat!(include_str!("delta-length-pages.rs"),include_str!("../delta_length.rs"))),
        "lockfile_sha256":hash_bytes(include_str!("../../Cargo.lock")),"cases":results});
    fs::write(
        output.join("results.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "Verified {} cases; {}",
        results.len(),
        output.join("results.json").display()
    );
    Ok(())
}
