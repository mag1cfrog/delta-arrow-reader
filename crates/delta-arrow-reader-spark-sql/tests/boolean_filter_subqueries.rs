//! Correlated EXISTS filters discard unused scalar queries before preparing them.
//! Projections, noncorrelated queries and required inputs retain their errors.

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

#[tokio::test]
async fn boolean_filter_subqueries_match_spark_preparation_boundaries() -> TestResult {
    let records = include_str!("fixtures/boolean-filter-subqueries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
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
            let actual = execute_subquery_case(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    let fields = expected["schema"]["fields"]
                        .as_array()
                        .ok_or("missing reference fields")?
                        .iter()
                        .map(|field| json!({"name":field["name"],"type":field["type"]}))
                        .collect::<Vec<_>>();
                    actual["rows"] == expected["rows"] && actual["fields"] == json!(fields)
                }
                Err(error) if expected["status"] == "error" => {
                    required_subquery_error_matches(case, &error.to_string())
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn execute_subquery_case(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    assert_eq!(schema.fields().len(), query.column_names.len());
    let fields = schema
        .fields()
        .iter()
        .zip(&query.column_names)
        .enumerate()
        .map(|(index, (field, name))| {
            assert_eq!(field.name(), name);
            assert_eq!(
                field.data_type(),
                query.logical_plan.schema().field(index).data_type()
            );
            let data_type = match field.data_type() {
                DataType::Boolean => "boolean",
                DataType::Int64 => "long",
                other => return Err(format!("unexpected result type: {other}").into()),
            };
            Ok(json!({"name":name,"type":data_type}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        if column.is_null(row) {
                            assert!(query.logical_plan.schema().field(index).is_nullable());
                            assert!(schema.field(index).is_nullable());
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(column.as_ref(), row).map(Value::String)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(json!({"rows":rows,"fields":fields}))
}

fn required_subquery_error_matches(case: &Value, error: &str) -> bool {
    match case["expected"]["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => {
            let Some(id) = case["id"].as_str() else {
                return false;
            };
            if id.contains("double") {
                error.contains("Cannot cast string \"bad\" to Float64")
            } else if id.contains("decimal") {
                error.contains("Cannot cast Some(\"bad\") to DECIMAL(10,2)")
            } else {
                error.contains("Cannot cast string 'bad' to value of Int32 type")
            }
        }
        Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") => {
            error.contains("Cannot cast string 'bad-local' to value of Int32 type")
        }
        Some("DIVIDE_BY_ZERO") => error.contains("Divide by zero error"),
        Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => {
            error.contains("Scalar subquery returned more than one row")
        }
        Some("UNRESOLVED_COLUMN.WITH_SUGGESTION") => {
            error.contains("missing_column") && error.contains("cannot resolve attribute")
        }
        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
            error.contains("Spark `abs` function requires 1 argument, got 0")
        }
        _ => false,
    }
}

#[tokio::test]
async fn native_sql_keeps_subquery_preparation_errors() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let result: datafusion::error::Result<_> = async {
        session
            .sql(
                "SELECT k, EXISTS(SELECT id FROM (VALUES (0),(1)) t(id) \
                 WHERE id=o.k AND false \
                 AND (SELECT CAST('bad' AS INT) FROM (VALUES (0)) s(x))>0) AS r \
                 FROM (VALUES (0),(1)) o(k)",
            )
            .await?
            .collect()
            .await
    }
    .await;
    let error = result
        .expect_err("native SQL must retain its scalar-query preparation error")
        .to_string();
    assert!(error.contains("Cannot cast string 'bad'"), "{error}");
    Ok(())
}
