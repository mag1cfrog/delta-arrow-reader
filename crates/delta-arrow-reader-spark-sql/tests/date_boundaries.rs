//! DATE casts and literals accept the same string boundaries as Spark.

use std::error::Error;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn execute_date_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..Default::default()
        },
    )
    .await?;
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(query.column_names, ["r"]);
    assert_eq!(
        query.logical_plan.schema().field(0).data_type(),
        schema.field(0).data_type()
    );
    let result_type = match schema.field(0).data_type() {
        DataType::Date32 => "date",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        other => return Err(format!("unexpected DATE test result type: {other}").into()),
    };
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        let values = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(if values.is_null(row) {
                assert!(schema.field(0).is_nullable());
                json!([null])
            } else {
                json!([array_value_to_string(values.as_ref(), row)?])
            });
        }
    }
    Ok(json!({"type": result_type, "rows": rows}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_string_boundaries_match_spark() -> TestResult {
    let records = include_str!("fixtures/date-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 517);
    assert_eq!(
        records
            .iter()
            .filter(|r| r.get("retained_difference").is_some())
            .count(),
        2
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        for case in &records[1..] {
            let actual = execute_date_query(&session, case).await;
            let expected = &case["expected"];
            if case["retained_difference"] == "timestamp_separator_632" {
                assert_eq!(expected["condition"], "INVALID_TYPED_LITERAL");
                assert_eq!(
                    actual?,
                    json!({"type":"string", "rows":[["2026-01-02 01:02:03.123456"]]})
                );
                continue;
            }
            let matches = match &actual {
                Ok(actual) => {
                    expected["status"] == "ok"
                        && actual["type"] == expected["type"]
                        && actual["rows"] == expected["rows"]
                }
                Err(error) => {
                    let message = error.to_string();
                    match expected["condition"].as_str() {
                        Some("CAST_INVALID_INPUT") => message.contains("[CAST_INVALID_INPUT]"),
                        Some("INVALID_TYPED_LITERAL") => {
                            message.contains("invalid argument: found")
                                || message.contains("invalid argument: DateValue")
                        }
                        _ => false,
                    }
                }
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
