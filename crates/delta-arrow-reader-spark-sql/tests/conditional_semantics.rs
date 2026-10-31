//! Conditional expressions must reject invalid conditions before choosing a branch.
//! Shared Spark captures also cover temporal fallback values and result types.

use std::error::Error;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn execute_reference_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().ok_or("missing timezone")?.into(),
        ..SparkSqlOptions::default()
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await?;
    if case["expected"]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
        return Err("invalid CASE condition reached physical planning".into());
    }
    if case["expected"]["condition"]
        == "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN"
    {
        return Err("invalid scalar subquery reached physical planning".into());
    }
    let logical_nullable = query.logical_plan.schema().field(0).is_nullable();
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    let result_type = match schema.field(0).data_type() {
        DataType::Date32 => "date",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz",
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => {
            assert_eq!(timezone, &options.session_timezone);
            "timestamp"
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string",
        DataType::Int32 => "integer",
        DataType::Int64 => "long",
        DataType::Boolean => "boolean",
        DataType::Null => "void",
        DataType::Float64 => "double",
        DataType::Decimal128(10, 2) => "decimal(10,2)",
        other => return Err(format!("unexpected result type: {other:?}").into()),
    };
    let batches = collect(plan, session.task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        let nulls = column.logical_nulls();
        for row in 0..batch.num_rows() {
            let value = if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                assert!(logical_nullable && schema.field(0).is_nullable());
                Value::Null
            } else if matches!(column.data_type(), DataType::Timestamp(_, _)) {
                column
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row)
                    .to_string()
                    .into()
            } else {
                array_value_to_string(column.as_ref(), row)?.into()
            };
            rows.push(vec![value]);
        }
    }
    Ok(json!({"type": result_type, "rows": rows, "names": query.column_names}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_conditional_results_and_admission() -> TestResult {
    let records = include_str!("fixtures/conditional-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 563);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        for case in &records[1..] {
            let actual = execute_reference_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    actual["type"] == expected["schema"]["fields"][0]["type"]
                        && actual["rows"] == expected["rows"]
                        && actual["names"] == json!(["r"])
                }
                Err(error) if expected["status"] == "error" => {
                    match expected["condition"].as_str() {
                        Some(
                            "CAST_INVALID_INPUT"
                            | "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
                        ) => {
                            let error = error.to_string();
                            error.contains("[CAST_INVALID_INPUT]")
                            || ((case["id"] == "case_live_cast" || case["id"].as_str().is_some_and(|id| id.starts_with("eval_")))
                                && error.contains("Cast error: Cannot cast string 'bad' to value of Int32 type"))
                            || (case["id"]
                                .as_str()
                                .is_some_and(|id| id.contains("_timestamp"))
                                && error.starts_with(
                                    "Execution error: error in SQL parser: found b at 0:1",
                                ))
                        }
                        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
                            error.to_string().contains("two values expected")
                        }
                        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => error
                            .to_string()
                            .contains("CASE WHEN condition must be BOOLEAN"),
                        Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => error
                            .to_string()
                            .contains("Scalar subquery returned more than one row"),
                        Some(
                            "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
                        ) => error
                            .to_string()
                            .contains("Scalar subquery must return exactly one column"),
                        Some("DIVIDE_BY_ZERO") => {
                            error.to_string().contains("[DIVIDE_BY_ZERO]")
                                || error
                                    .to_string()
                                    .contains("Arrow error: Divide by zero error")
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matches {
                differences.push(format!("{} ANSI={} timezone={} batch={batch_size}: expected {expected}, got {actual:?}", case["id"], case["ansi"], case["timezone"]));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
