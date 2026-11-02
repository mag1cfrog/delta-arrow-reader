//! Public declarations and physical refinement are separate Spark observations.

use std::sync::Arc;

use arrow::array::{Int32Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

fn records() -> TestResult<Vec<Value>> {
    // Selected records are verbatim from the two frozen Spark captures named in
    // the header. The three declaration records are rejection controls below.
    let records = include_str!("fixtures/schema-observations-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 25);
    Ok(records)
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_batch_size(batch_size)),
        )
        .build(),
    )
}

async fn check_refinement_cases(optimizer_control: bool) -> TestResult {
    let records = records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let mut checked = 0;
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            if if optimizer_control {
                id != "optimizer_narrowing"
            } else {
                !id.ends_with("_literal_valid")
            } {
                continue;
            }
            checked += 1;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..Default::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(query.logical_plan.schema().fields().len(), 1);
            assert_eq!(physical.schema().fields().len(), 1);
            assert!(query.logical_plan.schema().field(0).is_nullable());
            assert!(!physical.schema().field(0).is_nullable());
            assert_eq!(physical.schema().field(0).name(), &query.column_names[0]);
            if optimizer_control {
                assert_eq!(
                    case["expected"]["schema"],
                    case["schema_stages"]["analyzed"]
                );
                assert_eq!(
                    case["schema_stages"]["executed"]["fields"][0]["nullable"],
                    false
                );
            }
            let actual = capture_spark_query_result(&session, case).await;
            if actual.as_ref().ok() != Some(&case["expected"]) {
                differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["input"], case["ansi"], case["expected"]
                ));
            }
        }
        assert_eq!(checked, if optimizer_control { 6 } else { 16 });
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn optimizer_refinement_preserves_public_declarations() -> TestResult {
    check_refinement_cases(true).await
}

#[tokio::test]
async fn literal_casts_preserve_public_declarations() -> TestResult {
    check_refinement_cases(false).await
}

#[tokio::test]
async fn wrong_constructor_declarations_and_missing_metadata_remain_visible() -> TestResult {
    let records = records()?;
    let session = session(32);
    let children: Fields = vec![Arc::new(Field::new("id", DataType::Int32, true))].into();
    let schema = Arc::new(Schema::new(vec![
        Field::new("selected_id", DataType::Int32, true),
        Field::new("selected_struct", DataType::Struct(children.clone()), false),
        Field::new("left_id", DataType::Int32, true),
        Field::new("right_id", DataType::Int32, true),
    ]));
    // Deliberately wrong declarations model the faults, without making today's
    // named_struct or USING implementation a required failing behavior.
    session.register_batch(
        "fault_declarations",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![99])),
                Arc::new(StructArray::try_new(
                    children,
                    vec![Arc::new(Int32Array::from(vec![99]))],
                    None,
                )?),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )?,
    )?;
    for (id, projection, rows) in [
        ("nested_scalar", "selected_id", json!([["99"]])),
        ("struct_column", "selected_struct", json!([[["99"]]])),
        ("using_hidden", "left_id,right_id", json!([["0", "0"]])),
    ] {
        let reference = records
            .iter()
            .find(|r| r["id"] == id)
            .ok_or("missing control")?;
        let case = json!({"sql":format!("SELECT {projection} FROM fault_declarations"),
            "ansi":true,"expected":{"schema":reference["expected"]["schema"],"rows":rows}});
        let actual = capture_spark_query_result(&session, &case).await?;
        assert_eq!(actual["rows"], case["expected"]["rows"]);
        assert_ne!(actual["schema"], reference["expected"]["schema"], "{id}");
        assert_ne!(actual, case["expected"], "{id}");
    }
    Ok(())
}
