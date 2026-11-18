//! File origin is opt-in, hidden from stars, and follows the existing Delta scan.

use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, StringArray, StringViewArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::{MemTable, TableProvider, listing::ListingTableUrl},
    execution::SessionStateBuilder,
    physical_plan::collect,
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaTableBuilder, ParquetReaderBackend,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{
    SparkSqlOptions, SparkSqlQuery, configure_spark_sql, plan_query,
};
use sail_plan::resolver::PlanResolver;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/delta_reads.rs"]
mod delta_reads;
#[path = "support/file_metadata.rs"]
mod file_metadata_fixture;
#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;
#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use delta_reads::{RecordingLocalStore, TestResult};
use reader_support::RealParquetDeltaTable;

fn context() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(64),
                ),
        )
        .build(),
    )
}

async fn run(ctx: &SessionContext, sql: &str, ansi: bool) -> TestResult<Value> {
    spark_results::capture_executed_query_result(
        ctx,
        &json!({"sql":sql,"ansi":ansi,"ordered":true}),
    )
    .await
    .map_err(|error| error as Box<dyn std::error::Error>)
}

async fn register(
    ctx: &SessionContext,
    fixture: &RealParquetDeltaTable,
    name: &str,
    backend: ParquetReaderBackend,
) -> TestResult<String> {
    let uri = ListingTableUrl::parse(fixture.path().to_string_lossy())?
        .as_str()
        .to_owned();
    let table = DeltaTableBuilder::new(&uri).load_table().await?;
    ctx.register_table(
        name,
        Arc::new(DeltaTableProvider::try_new(
            table,
            ScanOptions {
                execution_options: DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                target_partitions: Some(1),
                ..Default::default()
            },
        )?),
    )?;
    Ok(uri)
}

#[tokio::test]
async fn file_metadata_partitioned_files_and_deletion_vectors() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_partition_values_and_deletion_vector(
        "spark-file-origin",
    )?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let ctx = context();
        let uri = register(&ctx, &fixture, "files", backend).await?;
        let west = format!("{uri}part-00001.parquet");
        let east = format!("{uri}part-00002.parquet");
        let partition = plan_query(
            &ctx,
            "SELECT region FROM files",
            &SparkSqlOptions::default(),
        )
        .await?
        .create_physical_plan(&ctx)
        .await?;
        assert_eq!(
            partition.schema().field(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
        let batches = collect(partition, ctx.task_ctx()).await?;
        let mut regions = Vec::new();
        for batch in &batches {
            spark_results::validate_batch(batch.schema().as_ref(), batch)
                .map_err(|error| error as Box<dyn std::error::Error>)?;
            for row in 0..batch.num_rows() {
                regions.push(arrow::util::display::array_value_to_string(
                    batch.column(0).as_ref(),
                    row,
                )?);
            }
        }
        regions.sort();
        assert_eq!(regions, ["us-east", "us-east", "us-west"]);
        let query = plan_query(
            &ctx,
            "SELECT input_file_name() AS path FROM files",
            &SparkSqlOptions::default(),
        )
        .await?;
        let physical = query.create_physical_plan(&ctx).await?;
        assert_eq!(physical.schema().field(0).data_type(), &DataType::Utf8View);
        for batch in collect(physical, ctx.task_ctx()).await? {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or("file path must remain a string view")?;
            let unique: std::collections::BTreeSet<_> = array.iter().flatten().collect();
            assert_eq!(
                array
                    .data_buffers()
                    .iter()
                    .map(|buffer| buffer.len())
                    .sum::<usize>(),
                unique.iter().map(|path| path.len()).sum::<usize>()
            );
            for i in 1..array.len() {
                if array.value(i) == array.value(i - 1) {
                    assert_eq!(array.views()[i], array.views()[i - 1]);
                }
            }
        }
        for ansi in [false, true] {
            let result = run(&ctx, "SELECT _metadata.file_name AS name,id,input_file_name() AS path,_metadata.file_path AS metadata_path FROM files ORDER BY id", ansi).await?;
            assert_eq!(
                result["rows"],
                json!([
                    ["part-00001.parquet", "2", west, west],
                    ["part-00002.parquet", "3", east, east],
                    ["part-00002.parquet", "4", east, east]
                ])
            );
            for index in [0, 2, 3] {
                assert_eq!(result["schema"]["fields"][index]["type"], "string");
            }
            assert_eq!(run(&ctx, &format!("SELECT id FROM files WHERE _metadata.file_path = '{east}' AND region = 'us-east' ORDER BY id"), ansi).await?["rows"], json!([["3"],["4"]]));
            assert_eq!(run(&ctx, "SELECT input_file_name() AS path,count(*) AS n FROM files GROUP BY input_file_name() ORDER BY path", ansi).await?["rows"], json!([[west,"1"],[east,"2"]]));
            assert_eq!(run(&ctx, "SELECT _metadata.file_path AS path,count(*) AS n FROM files GROUP BY _metadata.file_path ORDER BY path", ansi).await?["rows"], json!([[west,"1"],[east,"2"]]));
            for sql in [
                "SELECT * FROM files ORDER BY id",
                "SELECT f.* FROM files f ORDER BY id",
            ] {
                let result = run(&ctx, sql, ansi).await?;
                let names: Vec<_> = result["schema"]["fields"]
                    .as_array()
                    .ok_or("fields")?
                    .iter()
                    .map(|field| field["name"].as_str().ok_or("field name"))
                    .collect::<Result<_, _>>()?;
                assert_eq!(names, ["id", "customer_name", "region"]);
            }
            for sql in [
                "SELECT _metadata.file_path FROM files",
                "SELECT f._metadata.file_name FROM files f",
                "SELECT input_file_name() FROM files",
                "SELECT _metadata FROM files",
            ] {
                assert_eq!(
                    run(&ctx, sql, ansi).await?["rows"]
                        .as_array()
                        .ok_or("rows")?
                        .len(),
                    3
                );
            }
            for sql in ["DESCRIBE files", "SHOW COLUMNS FROM files"] {
                let result = run(&ctx, sql, ansi).await?;
                assert!(!result["rows"].to_string().contains("__dar_file"));
                assert!(!result["rows"].to_string().contains("_metadata"));
            }
            let explain = run(
                &ctx,
                "EXPLAIN SELECT input_file_name(), _metadata.file_path FROM files",
                ansi,
            )
            .await?;
            assert!(!explain["rows"].as_array().ok_or("explain rows")?.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn file_metadata_mapping_subqueries_aliases_and_joins() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_column_mapping("spark-origin-mapped")?;
    let ctx = context();
    let uri = register(&ctx, &fixture, "files", ParquetReaderBackend::Direct).await?;
    let path = format!("{uri}{}", fixture.data_file_path());
    ctx.register_table(
        "mem",
        Arc::new(MemTable::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )?]],
        )?),
    )?;
    for ansi in [false, true] {
        assert_eq!(
            run(
                &ctx,
                "SELECT id,_metadata.file_path FROM files WHERE customer_name = 'alice'",
                ansi
            )
            .await?["rows"],
            json!([["1", path]])
        );
        for sql in [
            "SELECT input_file_name() AS path FROM (SELECT id FROM files) x ORDER BY id",
            "WITH x AS (SELECT id FROM files) SELECT input_file_name() AS path FROM x ORDER BY id",
            "SELECT input_file_name() AS path FROM files f(i,c) ORDER BY i",
        ] {
            assert_eq!(
                run(&ctx, sql, ansi).await?["rows"],
                json!([[path], [path], [path]])
            );
        }
        assert_eq!(
            run(
                &ctx,
                "SELECT input_file_name() AS path FROM files f JOIN mem m ON f.id=m.id",
                ansi
            )
            .await?["rows"],
            json!([[path]])
        );
        let error = plan_query(
            &ctx,
            "SELECT input_file_name() FROM files a JOIN files b ON a.id=b.id",
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await
        .err()
        .ok_or("multiple sources accepted")?;
        assert!(
            error
                .to_string()
                .contains("MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION")
        );
        assert_eq!(run(&ctx, "SELECT a._metadata.file_path,b._metadata.file_name FROM files a JOIN files b ON a.id=b.id ORDER BY a.id", ansi).await?["rows"], json!([[path,fixture.data_file_path()],[path,fixture.data_file_path()],[path,fixture.data_file_path()]]));
        assert_eq!(run(&ctx, "SELECT named_struct('file_path','alias') AS _metadata,_metadata.file_path AS path FROM files ORDER BY id", ansi).await?["rows"], json!([[["alias"],"alias"],[["alias"],"alias"],[["alias"],"alias"]]));
    }
    let stored = file_metadata_fixture::stored_metadata_fixture()?;
    register(&ctx, &stored, "stored", ParquetReaderBackend::Direct).await?;
    for ansi in [false, true] {
        assert_eq!(
            run(
                &ctx,
                "SELECT id,_metadata.file_path AS path FROM stored ORDER BY id",
                ansi
            )
            .await?["rows"],
            json!([["1", "ordinary-a"], ["2", null], ["3", "ordinary-c"]])
        );
        assert!(
            plan_query(
                &ctx,
                "SELECT _metadata.file_name FROM stored",
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
    }
    Ok(())
}

async fn without_file_adapter(
    ctx: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> TestResult<SparkSqlQuery> {
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let sail_sql_analyzer::statement::ReadOnlyStatement::Query(query) =
        sail_sql_analyzer::statement::from_ast_read_only_statement(ast)?
    else {
        return Err("expected query".into());
    };
    let named = PlanResolver::new(ctx, Arc::new(options.clone()))
        .resolve_named_plan(query)
        .await?;
    Ok(SparkSqlQuery {
        logical_plan: named.plan,
        column_names: named.fields,
    })
}

#[tokio::test]
async fn file_metadata_unrequested_keeps_logical_raw_schemas_and_read_requests() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_partition_values_and_deletion_vector(
        "spark-origin-spy",
    )?;
    let (uri, store) = RecordingLocalStore::register(fixture.path())?;
    let table = DeltaTableBuilder::new(uri).load_table().await?;
    let provider = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let schema = provider.schema();
    let ctx = context();
    ctx.register_table("files", Arc::new(provider))?;
    for ansi in [false, true] {
        for sql in [
            "SELECT * FROM files ORDER BY id",
            "SELECT customer_name FROM files WHERE id > 2 ORDER BY customer_name",
            "SELECT count(*) AS n FROM files",
            "SELECT DISTINCT region FROM files ORDER BY region",
            "SELECT id FROM files UNION ALL SELECT 9 AS id",
            "WITH x AS (SELECT id FROM files) SELECT * FROM x ORDER BY id",
        ] {
            let options = SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            };
            let expected = without_file_adapter(&ctx, sql, &options).await?;
            let actual = plan_query(&ctx, sql, &options).await?;
            assert_eq!(
                actual.logical_plan.schema(),
                expected.logical_plan.schema(),
                "{sql}"
            );
            assert_eq!(actual.column_names, expected.column_names);
            let mut baseline = None;
            for query in [expected, actual] {
                store.reads.lock().map_err(|_| "read lock")?.clear();
                let physical = query.create_physical_plan(&ctx).await?;
                let output_schema = physical.schema();
                let scans = collect_scan_metrics(physical.as_ref());
                let batches = collect(physical, ctx.task_ctx()).await?;
                let mut reads: Vec<_> = store
                    .reads
                    .lock()
                    .map_err(|_| "read lock")?
                    .iter()
                    .map(|read| (read.path.clone(), read.range.clone()))
                    .collect();
                reads.sort_by_key(|(path, range)| (path.clone(), range.start, range.end));
                if let Some((previous_schema, previous_batches, previous_reads, previous_metrics)) =
                    &baseline
                {
                    assert_eq!(&output_schema, previous_schema, "{sql}");
                    assert_eq!(&batches, previous_batches, "{sql}");
                    assert_eq!(&reads, previous_reads, "{sql}");
                    assert_eq!(
                        scans[0].snapshot().reader_metrics,
                        *previous_metrics,
                        "{sql}"
                    );
                } else {
                    baseline = Some((
                        output_schema,
                        batches,
                        reads,
                        scans[0].snapshot().reader_metrics,
                    ));
                }
            }
        }
    }
    let native = ctx
        .sql("SELECT * FROM files ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(native[0].schema(), schema);
    assert_eq!(native.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let spoof_schema = Arc::new(Schema::new(vec![Field::new(
        "__dar_file_path",
        DataType::Utf8,
        false,
    )]));
    ctx.register_table(
        "spoof",
        Arc::new(MemTable::try_new(
            spoof_schema.clone(),
            vec![vec![RecordBatch::try_new(
                spoof_schema,
                vec![Arc::new(StringArray::from(vec!["forged"]))],
            )?]],
        )?),
    )?;
    assert_eq!(
        run(&ctx, "SELECT input_file_name() FROM spoof", false).await?["rows"],
        json!([[""]])
    );
    Ok(())
}

fn replace_captured_file_root(value: &mut Value, captured: &str, actual: &str) {
    match value {
        Value::String(text) if text.starts_with(captured) => {
            *text = format!("{actual}{}", &text[captured.len()..]);
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| replace_captured_file_root(value, captured, actual)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| replace_captured_file_root(value, captured, actual)),
        _ => {}
    }
}

#[tokio::test]
async fn file_metadata_spark_reference_contracts() -> TestResult {
    let request: Value =
        serde_json::from_str(include_str!("fixtures/file-metadata/requests.json"))?;
    let records = include_str!("fixtures/file-metadata/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_only"], true);
    assert_eq!(records[0]["reads_expected_outputs"], false);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    assert_eq!(records.len() - 1, 28);
    let fixture = RealParquetDeltaTable::new_with_two_files_and_deletion_vector(
        "spark-origin-reference",
        &[],
    )?;
    let ctx = context();
    let uri = register(&ctx, &fixture, "file_source", ParquetReaderBackend::Direct).await?;
    let stored = file_metadata_fixture::stored_metadata_fixture()?;
    register(&ctx, &stored, "stored_source", ParquetReaderBackend::Direct).await?;
    let input = &request["corpora"][0]["metadata"]["inputs"][0];
    let batch = spark_inputs::decode_recorded_batch(input)
        .map_err(|error| error as Box<dyn std::error::Error>)?;
    ctx.register_table(
        "mem_source",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    let mut observed = std::collections::BTreeSet::new();
    let (mut strict_success, mut policy_controls, mut rejections) = (0, 0, 0);
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("ANSI")?;
        assert!(observed.insert((id, ansi)), "duplicate reference");
        let requested = request["corpora"][0]["cases"]
            .as_array()
            .ok_or("cases")?
            .iter()
            .find(|requested| requested["id"] == case["id"] && requested["ansi"] == case["ansi"])
            .ok_or("unrequested reference")?;
        assert_eq!(case["sql"], requested["sql"]);
        assert_eq!(case["ordered"], requested["ordered"]);
        assert_eq!(case["file_source"]["delta_python_package_available"], false);
        let sql = case["sql"].as_str().ok_or("reference SQL")?;
        if id == "file_join_rejected" {
            assert_eq!(case["status"], "error");
            assert_eq!(
                case["spark_error_details"]["condition"],
                "MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION"
            );
            let error = plan_query(
                &ctx,
                sql,
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await
            .err()
            .ok_or("multiple sources accepted")?;
            assert!(
                error
                    .to_string()
                    .contains("MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION")
            );
            rejections += 1;
            continue;
        }
        assert_eq!(case["status"], "ok");
        let mut expected = case["expected"]["rows"].clone();
        replace_captured_file_root(
            &mut expected,
            case["file_source"]["root_uri"]
                .as_str()
                .ok_or("captured root")?,
            &uri,
        );
        let actual = run(&ctx, sql, ansi).await?;
        if id == "file_mem_join" {
            // Spark evaluates this control on the streamed memory side of its broadcast join.
            assert!(
                case["spark_plans"]["executed"]
                    .as_str()
                    .ok_or("join plan")?
                    .contains("BuildLeft")
            );
            assert!(
                expected
                    .as_array()
                    .ok_or("observed join rows")?
                    .iter()
                    .all(|row| row[1] == "")
            );
            let mut standalone = records
                .iter()
                .find(|row| row["id"] == "file_origin" && row["ansi"] == case["ansi"])
                .ok_or("standalone origin reference")?["expected"]["rows"]
                .clone();
            replace_captured_file_root(
                &mut standalone,
                case["file_source"]["root_uri"]
                    .as_str()
                    .ok_or("captured root")?,
                &uri,
            );
            assert_eq!(
                actual["rows"], standalone,
                "policy control must retain exact per-ID file origins"
            );
            assert_ne!(
                actual["rows"], expected,
                "retain the observed optimizer-policy difference"
            );
            policy_controls += 1;
        } else {
            assert_eq!(actual["rows"], expected, "{id}, ansi={ansi}");
            strict_success += 1;
        }
        let mut actual_schema = actual["schema"].clone();
        let mut expected_schema = case["schema_stages"]["executed"].clone();
        spark_results::remove_nullability_and_metadata(&mut actual_schema);
        spark_results::remove_nullability_and_metadata(&mut expected_schema);
        assert_eq!(actual_schema, expected_schema, "{id}, ansi={ansi}");
    }
    assert_eq!(observed.len(), 28);
    assert_eq!((strict_success, policy_controls, rejections), (24, 2, 2));
    println!(
        "FILE_METADATA_REFERENCES strict_success={strict_success} policy_controls={policy_controls} rejections={rejections}"
    );
    Ok(())
}

#[tokio::test]
async fn file_metadata_empty_tables_and_internal_name_collisions() -> TestResult {
    let empty = RealParquetDeltaTable::new_default("spark-origin-empty")?;
    std::fs::write(
        empty.path().join("_delta_log/00000000000000000001.json"),
        "",
    )?;
    let ctx = context();
    register(&ctx, &empty, "empty_files", ParquetReaderBackend::Direct).await?;
    for ansi in [false, true] {
        assert_eq!(
            run(
                &ctx,
                "SELECT input_file_name(),_metadata.file_path,_metadata.file_name FROM empty_files",
                ansi
            )
            .await?["rows"],
            json!([])
        );
        assert_eq!(
            run(&ctx, "SELECT input_file_name()", ansi).await?["rows"],
            json!([[""]])
        );
    }
    let collision = file_metadata_fixture::stored_metadata_fixture()?;
    let log = collision
        .path()
        .join("_delta_log/00000000000000000000.json");
    let mut actions = std::fs::read_to_string(&log)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = actions
        .iter_mut()
        .find_map(|action| action.get_mut("metaData"))
        .ok_or("metadata action")?;
    let mut schema: Value =
        serde_json::from_str(metadata["schemaString"].as_str().ok_or("schema string")?)?;
    schema["fields"][1]["name"] = json!("__dar_file_path");
    metadata["schemaString"] = json!(schema.to_string());
    std::fs::write(
        log,
        actions
            .iter()
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )?;
    let uri = register(&ctx, &collision, "collision", ParquetReaderBackend::Direct).await?;
    let path = format!("{uri}{}", collision.data_file_path());
    for ansi in [false, true] {
        assert_eq!(
            run(
                &ctx,
                "SELECT input_file_name(),_metadata.file_path FROM collision ORDER BY id",
                ansi
            )
            .await?["rows"],
            json!([[path, path], [path, path], [path, path]])
        );
        for field in ["file_size", "file_modification_time"] {
            assert!(
                plan_query(
                    &ctx,
                    &format!("SELECT _metadata.{field} FROM collision"),
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    }
                )
                .await
                .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn file_metadata_unmatched_outer_rows_have_empty_filename_and_null_metadata() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("spark-origin-outer-join")?;
    let ctx = context();
    register(&ctx, &fixture, "files", ParquetReaderBackend::Direct).await?;
    for ansi in [false, true] {
        for from in [
            "(SELECT 99 AS id) m LEFT OUTER JOIN files f ON f.id=m.id",
            "files f RIGHT OUTER JOIN (SELECT 99 AS id) m ON f.id=m.id",
            "files f FULL OUTER JOIN (SELECT 99 AS id) m ON f.id=m.id",
        ] {
            let sql = format!(
                "SELECT m.id,input_file_name() AS file,f._metadata.file_path AS path,f._metadata.file_name AS name FROM {from} WHERE f.id IS NULL"
            );
            let query = plan_query(
                &ctx,
                &sql,
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&ctx).await?;
            assert_eq!(physical.schema().field(1).data_type(), &DataType::Utf8View);
            assert!(!physical.schema().field(1).is_nullable());
            assert!(physical.schema().field(2).is_nullable());
            assert!(physical.schema().field(3).is_nullable());
            assert_eq!(
                run(&ctx, &sql, ansi).await?["rows"],
                json!([["99", "", null, null]])
            );
        }
    }
    Ok(())
}
