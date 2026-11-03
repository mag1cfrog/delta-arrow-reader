//! MAP construction retains exact Spark fields before and after scalar inlining.

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, encode_value, validate_batch};

#[tokio::test]
async fn map_constructor_nullability_matches_spark() -> TestResult {
    let reference = std::env::var_os("DAR_MAP_CONSTRUCTOR_REFERENCE").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/map-constructor-nullability-spark.jsonl"),
        std::path::PathBuf::from,
    );
    let raw = std::fs::read_to_string(reference)?;
    let records = raw.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["map_encoding"], "entry_pairs");
    assert_eq!(records.len() - 1, 102);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?)));
        assert!(case.get("expected").is_some() ^ case.get("error").is_some());
    }
    let mut output = std::env::var_os("DAR_MAP_CONSTRUCTOR_OUTPUT")
        .map(|path| std::fs::OpenOptions::new().write(true).create_new(true).open(path))
        .transpose()?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)))
            .build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let mut actual = json!({});
            let mut phase = "resolve";
            let observed: TestResult = async {
                let query = plan_query(&session, case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions { ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(), ..SparkSqlOptions::default() }).await?;
                actual["public_schema"] = capture_schema(query.logical_plan.schema().fields(),
                    Some(&query.column_names))?;
                actual["logical_plan"] = json!(format!("{}", query.logical_plan.display_indent_schema()));
                phase = "physical";
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                actual["executed_schema_before_collect"] = capture_schema(schema.fields(), None)?;
                actual["physical_plan"] = json!(format!("{physical:?}"));
                phase = "execute";
                let batches = collect(Arc::clone(&physical), session.task_ctx()).await?;
                actual["executed_schema_after_collect"] = capture_schema(physical.schema().fields(), None)?;
                let mut rows = Vec::new();
                let mut batch_observations = Vec::new();
                for batch in batches {
                    phase = "validate_batch";
                    validate_batch(schema.as_ref(), &batch)?;
                    batch_observations.push(json!({"schema":capture_schema(batch.schema().fields(), None)?,
                        "rows":batch.num_rows()}));
                    for row in 0..batch.num_rows() {
                        rows.push(batch.columns().iter().map(|column| encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?);
                    }
                }
                rows.sort_by_key(|row| json!(row).to_string());
                actual["batches"] = json!(batch_observations);
                actual["rows"] = json!(rows);
                Ok(())
            }.await;
            let matches = match &observed {
                Ok(()) if case.get("error").is_none() => {
                    actual["public_schema"] == case["public_schema"]
                        && actual["executed_schema_before_collect"] == case["executed_schema_before_collect"]
                        && actual["executed_schema_after_collect"] == case["executed_schema_after_collect"]
                        && actual["rows"] == case["expected"]["rows"]
                }
                Ok(()) => false,
                Err(error) => {
                    let engine_error = error.downcast_ref::<DataFusionError>().is_some();
                    actual["error"] = json!({"message":error.to_string(), "phase":phase,
                        "engine_error":engine_error});
                    case["error"]["condition"].as_str().is_some_and(|condition|
                        engine_error && error.to_string().contains(&format!("[{condition}]")))
                }
            };
            if let Some(output) = &mut output {
                writeln!(output, "{}", json!({"id":case["id"], "input":case["input"],
                    "ansi":case["ansi"], "batch_size":batch_size, "matches":matches,
                    "expected":case, "actual":actual}))?;
            }
            if !matches {
                differences.push(format!("{} {} ANSI={} batch={batch_size}: expected {case}, got {actual}",
                    case["id"],case["input"],case["ansi"]));
            }
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 204);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_setup_preserves_native_array_constructor() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build());
    for sql in ["SELECT make_array(1,NULL) AS r", "SELECT make_array() AS r"] {
        let frame = native.sql(sql).await?;
        let expected_logical = frame.schema().clone();
        let physical = frame.create_physical_plan().await?;
        let expected_schema = physical.schema();
        let expected_batches = collect(physical, native.task_ctx()).await?;
        let frame = configured.sql(sql).await?;
        assert_eq!(*frame.schema(), expected_logical, "{sql}");
        let physical = frame.create_physical_plan().await?;
        assert_eq!(physical.schema(), expected_schema, "{sql}");
        assert_eq!(collect(physical, configured.task_ctx()).await?, expected_batches, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn map_constructor_preserves_required_numeric_cast_failures() -> TestResult {
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size)))
            .build());
        for (sql, cause) in [
            ("SELECT MAP('a',1,2,2) AS r", "Cannot cast string 'a' to value of Int32 type"),
            ("SELECT MAP(1,'invalid',2,2) AS r", "Cannot cast string 'invalid' to value of Int32 type"),
        ] {
            let query = plan_query(&session, sql, &SparkSqlOptions {
                ansi_mode: true, ..SparkSqlOptions::default()
            }).await?;
            let error = query.create_physical_plan(&session).await
                .expect_err("MAP must retain its required numeric cast failure");
            let DataFusionError::ArrowError(arrow::error::ArrowError::CastError(actual), _) =
                error.find_root() else {
                    return Err(format!("{sql}: unexpected failure: {error}").into());
                };
            assert_eq!(actual, cause, "{sql}, batch={batch_size}");
        }
    }
    Ok(())
}
