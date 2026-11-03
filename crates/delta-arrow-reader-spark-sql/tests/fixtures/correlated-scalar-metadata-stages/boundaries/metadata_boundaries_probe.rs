//! Root runs this immutable probe against a frozen head; mismatches retain actual rows.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-metadata-boundary-probes/sources/spark_inputs.rs"]
mod spark_inputs;
#[expect(dead_code, reason = "This probe uses only the frozen schema and row encoders.")]
#[path = "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-metadata-boundary-probes/sources/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, encode_value, validate_batch};

#[tokio::test]
async fn outer_star_and_hidden_sort_preserve_recorded_metadata_boundaries() -> TestResult {
    let raw = std::fs::read_to_string(
        "/home/hanbo/.local/share/delta-arrow-reader-review/spark-window-completion-2026-11-02/correlated-scalar-metadata-boundary-probes/boundaries-spark.jsonl",
    )?;
    let records = raw.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 12);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?)));
        assert!(case.get("expected").is_some() ^ case.get("error").is_some());
    }
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
                actual["executed_schema"] = capture_schema(schema.fields(), None)?;
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
                        && actual["executed_schema"] == case["executed_schema_before_collect"]
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
            eprintln!("metadata_boundary_probe: {}", json!({"id":case["id"],"input":case["input"],
                "ansi":case["ansi"],"batch_size":batch_size,"matches":matches,
                "expected":case,"actual":actual}));
            if !matches {
                differences.push(format!("{} {} ANSI={} batch={batch_size}: expected {case}, got {actual}",
                    case["id"],case["input"],case["ansi"]));
            }
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 24);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
