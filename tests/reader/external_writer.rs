//! Frozen Spark/Delta tables and Spark-read Arrow oracles, with no writer in CI.

use std::{error::Error, fs, fs::File, path::PathBuf, sync::Arc};

use arrow::{
    array::{BooleanArray, Int32Array, StringArray},
    compute::{
        cast, concat_batches, filter_record_batch, is_not_null, is_null,
        kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq},
        sort_to_indices, take_record_batch,
    },
    datatypes::{DataType, Fields, SchemaRef},
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};
use delta_arrow_reader::{
    DeltaComparison as C, DeltaPredicate as P, DeltaScalar as S, DeltaScanExecutionOptions,
    DeltaTable, DeltaTableBuilder, ParquetReaderBackend, WarmupMode,
};
use futures_util::TryStreamExt;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use serde_json::Value;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/reader/fixtures/external_writer/corpus"
);
const BACKENDS: [ParquetReaderBackend; 2] = [
    ParquetReaderBackend::Direct,
    ParquetReaderBackend::DeltaKernel,
];

struct Fixture {
    directory: PathBuf,
    info: Value,
    expected: RecordBatch,
}

fn fixtures() -> TestResult<Vec<Fixture>> {
    let root = PathBuf::from(CORPUS);
    let manifest: Value = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
    manifest["fixtures"]
        .as_array()
        .ok_or("missing fixture list")?
        .iter()
        .map(|info| {
            let directory = root.join(info["name"].as_str().ok_or("missing fixture name")?);
            let oracle = FileReader::try_new(File::open(directory.join("expected.arrow"))?, None)?;
            let schema = oracle.schema();
            let expected = concat_batches(&schema, &oracle.collect::<Result<Vec<_>, _>>()?)?;
            Ok(Fixture {
                directory,
                info: info.clone(),
                expected,
            })
        })
        .collect()
}

// Spark's IPC oracle has plain strings; DataFusion also exposes views and
// dictionary-encoded partition columns. Check logical types before casting so
// normalization cannot hide a wrong numeric type or a renamed nested field.
fn assert_fields(actual: &Fields, expected: &Fields) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.name(), expected.name());
        assert_eq!(actual.is_nullable(), expected.is_nullable());
        let actual_type = match actual.data_type() {
            DataType::Dictionary(_, value) => value.as_ref(),
            value => value,
        };
        match (actual_type, expected.data_type()) {
            (DataType::Utf8View, DataType::Utf8) => {}
            (DataType::Struct(actual), DataType::Struct(expected)) => {
                assert_fields(actual, expected)
            }
            (actual, expected) => assert_eq!(actual, expected),
        }
    }
}

fn canonical(batch: &RecordBatch, schema: &SchemaRef) -> TestResult<RecordBatch> {
    assert_fields(batch.schema().fields(), schema.fields());
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| cast(column, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(Arc::clone(schema), columns)?;
    let indices = sort_to_indices(batch.column_by_name("id").ok_or("missing id")?, None, None)?;
    Ok(take_record_batch(&batch, &indices)?)
}

fn compare(column: &str, op: C, value: i32) -> P {
    P::Compare {
        column: column.into(),
        op,
        value: S::Int32(value),
    }
}

// Each mask comes from Arrow kernels on a verified full scan, independently
// of reader pruning, row filtering, and SQL expression evaluation.
fn predicates(batch: &RecordBatch) -> TestResult<Vec<(&'static str, P, BooleanArray)>> {
    let id = batch.column_by_name("id").ok_or("missing id")?;
    let value = batch.column_by_name("value").ok_or("missing value")?;
    let threshold = Int32Array::new_scalar(30);
    let mut cases = vec![
        (
            "value = 30",
            compare("value", C::Eq, 30),
            eq(value, &threshold)?,
        ),
        (
            "value <> 30",
            compare("value", C::NotEq, 30),
            neq(value, &threshold)?,
        ),
        (
            "value < 30",
            compare("value", C::Lt, 30),
            lt(value, &threshold)?,
        ),
        (
            "value <= 30",
            compare("value", C::LtEq, 30),
            lt_eq(value, &threshold)?,
        ),
        (
            "value > 30",
            compare("value", C::Gt, 30),
            gt(value, &threshold)?,
        ),
        (
            "value >= 30",
            compare("value", C::GtEq, 30),
            gt_eq(value, &threshold)?,
        ),
        (
            "value IS NULL",
            P::IsNull {
                column: "value".into(),
            },
            is_null(value)?,
        ),
        (
            "value IS NOT NULL",
            P::IsNotNull {
                column: "value".into(),
            },
            is_not_null(value)?,
        ),
        (
            "id > 0",
            compare("id", C::Gt, 0),
            gt(id, &Int32Array::new_scalar(0))?,
        ),
        (
            "id = 0",
            compare("id", C::Eq, 0),
            eq(id, &Int32Array::new_scalar(0))?,
        ),
        (
            "id = 2",
            compare("id", C::Eq, 2),
            eq(id, &Int32Array::new_scalar(2))?,
        ),
    ];
    if let Some(region) = batch.column_by_name("region") {
        cases.push((
            "region = 'east'",
            P::Compare {
                column: "region".into(),
                op: C::Eq,
                value: S::Utf8("east".into()),
            },
            eq(region, &StringArray::new_scalar("east"))?,
        ));
        cases.push((
            "region IS NULL",
            P::IsNull {
                column: "region".into(),
            },
            is_null(region)?,
        ));
        cases.push((
            "region IS NOT NULL",
            P::IsNotNull {
                column: "region".into(),
            },
            is_not_null(region)?,
        ));
    }
    Ok(cases)
}

async fn read(
    table: &DeltaTable,
    backend: ParquetReaderBackend,
    predicate: Option<P>,
    id_only: bool,
) -> TestResult<RecordBatch> {
    let mut builder = table
        .scan()
        .with_target_partitions(2)?
        .with_execution_options(DeltaScanExecutionOptions::new().with_parquet_backend(backend));
    if let Some(predicate) = predicate {
        builder = builder.with_predicate(predicate);
    }
    if id_only {
        builder = builder.with_projection(["id"]);
    }
    let scan = builder.build().await?;
    let schema = scan.schema();
    let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
    Ok(concat_batches(&schema, &batches)?)
}

#[tokio::test]
async fn snapshots_and_predicates_match_spark_and_arrow() -> TestResult {
    for fixture in fixtures()? {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let table = DeltaTableBuilder::new(fixture.directory.join("table").to_string_lossy())
                .with_warmup(warmup)
                .load_table()
                .await?;
            assert_eq!(
                table.version(),
                fixture.info["snapshot_version"]
                    .as_u64()
                    .ok_or("missing version")?
            );
            assert_fields(table.schema().fields(), fixture.expected.schema().fields());
            for backend in BACKENDS {
                let context = format!("{} {warmup:?} {backend:?}", fixture.directory.display());
                let full = read(&table, backend, None, false).await?;
                let full = canonical(&full, &fixture.expected.schema())?;
                assert_eq!(full, fixture.expected, "{context}");
                for (sql, predicate, mask) in predicates(&full)? {
                    let expected = filter_record_batch(&full, &mask)?;
                    for id_only in [false, true] {
                        let expected = if id_only {
                            expected.project(&[0])?
                        } else {
                            expected.clone()
                        };
                        let actual =
                            read(&table, backend, Some(predicate.clone()), id_only).await?;
                        assert_eq!(
                            canonical(&actual, &expected.schema())?,
                            expected,
                            "{context}, {sql}, id_only={id_only}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn corpus_contains_the_recorded_physical_features() -> TestResult {
    let fixtures = fixtures()?;
    assert_eq!(fixtures.len(), 3);
    let mut bytes = fs::metadata(PathBuf::from(CORPUS).join("manifest.json"))?.len();
    for fixture in fixtures {
        assert_eq!(
            fixture.expected.num_rows() as u64,
            fixture.info["expected_row_count"]
                .as_u64()
                .ok_or("missing expected count")?
        );
        let mut physical_rows = 0;
        let groups = fixture.info["row_groups_per_file"]
            .as_object()
            .ok_or("missing files")?;
        for (path, count) in groups {
            let file =
                SerializedFileReader::new(File::open(fixture.directory.join("table").join(path))?)?;
            assert_eq!(
                file.num_row_groups() as u64,
                count.as_u64().ok_or("missing group count")?
            );
            physical_rows += file.metadata().file_metadata().num_rows();
            if fixture.info["name"] == "nested_mapping" {
                for field in file
                    .metadata()
                    .file_metadata()
                    .schema_descr()
                    .root_schema()
                    .get_fields()
                {
                    assert!(field.name().starts_with("col-"));
                    assert!(field.get_basic_info().has_id());
                }
            }
        }
        assert_eq!(
            physical_rows as u64,
            fixture.info["physical_row_count"]
                .as_u64()
                .ok_or("missing physical count")?
        );
        for (path, info) in fixture.info["files"]
            .as_object()
            .ok_or("missing inventory")?
        {
            let size = fs::metadata(fixture.directory.join(path))?.len();
            assert_eq!(size, info["bytes"].as_u64().ok_or("missing file size")?);
            bytes += size;
        }
        match fixture.info["name"].as_str() {
            Some("partitioned") => {
                assert!(groups.len() >= 3);
                assert!(
                    groups
                        .values()
                        .any(|count| count.as_u64().is_some_and(|count| count > 1))
                );
                assert_eq!(
                    fixture.info["partition_columns"],
                    serde_json::json!(["region"])
                );
            }
            Some("nested_mapping") => assert_eq!(fixture.info["snapshot_version"], 1),
            Some("deletion_vectors") => {
                assert_eq!(physical_rows, 12);
                assert_eq!(fixture.expected.num_rows(), 9);
                assert!(
                    !fixture.info["deletion_vectors"]
                        .as_array()
                        .ok_or("missing DVs")?
                        .is_empty()
                );
            }
            _ => return Err("unexpected fixture".into()),
        }
    }
    assert!(bytes <= 256 * 1024);
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn sql_matches_spark_for_both_backends_and_string_representations() -> TestResult {
    use datafusion::prelude::{SessionConfig, SessionContext};
    use delta_arrow_reader::datafusion::{IntraFileRepartitioning, ScanOptions, register_table};

    for fixture in fixtures()? {
        let mut queries = vec![("SELECT * FROM fixture".to_owned(), fixture.expected.clone())];
        for (condition, _, mask) in predicates(&fixture.expected)? {
            queries.push((
                format!("SELECT id FROM fixture WHERE {condition}"),
                filter_record_batch(&fixture.expected, &mask)?.project(&[0])?,
            ));
        }
        let table = DeltaTableBuilder::new(fixture.directory.join("table").to_string_lossy())
            .load_table()
            .await?;
        for backend in BACKENDS {
            for use_arrow_view_types in [false, true] {
                let context = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_batch_size(7)
                        .with_target_partitions(3)
                        .with_repartition_file_min_size(1),
                );
                register_table(
                    &context,
                    "fixture",
                    table.clone(),
                    ScanOptions {
                        execution_options: DeltaScanExecutionOptions::new()
                            .with_parquet_backend(backend),
                        target_partitions: Some(3),
                        intra_file_repartitioning: IntraFileRepartitioning::Always,
                        use_arrow_view_types,
                    },
                )?;
                for (sql, expected) in &queries {
                    let frame = context.sql(sql).await?;
                    let schema = Arc::new(frame.schema().as_arrow().clone());
                    let batches = frame.collect().await?;
                    let actual = concat_batches(&schema, &batches)?;
                    assert_eq!(
                        canonical(&actual, &expected.schema())?,
                        *expected,
                        "{} {backend:?} views={use_arrow_view_types}: {sql}",
                        fixture.directory.display()
                    );
                }
            }
        }
    }
    Ok(())
}
