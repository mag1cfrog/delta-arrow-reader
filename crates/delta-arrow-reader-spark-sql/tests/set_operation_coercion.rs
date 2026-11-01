//! Set inputs share a result type before duplicate elimination and comparison.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn set_operation_coercion_matches_spark() -> TestResult {
    let records = include_str!("fixtures/set-operation-coercion-spark.jsonl")
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
            let actual = execute_set_operation(&session, case).await;
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
                    set_coercion_error_matches(case, &error.to_string())
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} timezone={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"], case["timezone"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn execute_set_operation(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().unwrap_or("UTC").into(),
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
                DataType::Null => "void".into(),
                DataType::Boolean => "boolean".into(),
                DataType::Int8 => "byte".into(),
                DataType::Int16 => "short".into(),
                DataType::Int32 => "integer".into(),
                DataType::Int64 => "long".into(),
                DataType::Float32 => "float".into(),
                DataType::Float64 => "double".into(),
                DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string".into(),
                DataType::Binary => "binary".into(),
                DataType::Date32 => "date".into(),
                DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".into(),
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => {
                    assert_eq!(
                        timezone.as_ref(),
                        case["timezone"].as_str().unwrap_or("UTC")
                    );
                    "timestamp".into()
                }
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
                    .map(
                        |(index, column)| -> Result<Value, arrow::error::ArrowError> {
                            if column
                                .logical_nulls()
                                .is_some_and(|nulls| nulls.is_null(row))
                            {
                                assert!(query.logical_plan.schema().field(index).is_nullable());
                                assert!(schema.field(index).is_nullable());
                                Ok(Value::Null)
                            } else {
                                let mut value = if matches!(
                                    column.data_type(),
                                    DataType::Timestamp(TimeUnit::Microsecond, _)
                                ) {
                                    column
                                        .as_primitive::<TimestampMicrosecondType>()
                                        .value(row)
                                        .to_string()
                                } else {
                                    array_value_to_string(column.as_ref(), row)?
                                };
                                if matches!(
                                    column.data_type(),
                                    DataType::Float32 | DataType::Float64
                                ) && !value.contains(['.', 'e', 'E'])
                                {
                                    value.push_str(".0");
                                }
                                Ok(Value::String(value))
                            }
                        },
                    )
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"rows":rows,"fields":fields}))
}

fn set_coercion_error_matches(case: &Value, error: &str) -> bool {
    match case["expected"]["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => match case["id"].as_str() {
            Some("invalid_boolean") => {
                error.contains("[CAST_INVALID_INPUT] Cannot cast STRING to BOOLEAN")
            }
            Some("invalid_date") => {
                error.contains("[CAST_INVALID_INPUT]")
                    && error.contains("'bad'")
                    && error.contains("\"DATE\"")
            }
            Some("invalid_timestamp") => {
                // Timestamp parsing retains its native diagnostic until the error-contract work.
                error.contains("error in SQL parser: found b at 0:1")
            }
            Some("invalid_float") => error.contains("Cannot cast string \"bad\" to Float64"),
            Some("integer_overflow") => {
                error.contains("Cannot cast string '9223372036854775808' to value of Int64 type")
            }
            Some(id) if id.starts_with("three_inputs_") => {
                error.contains("Cannot cast string '1.0' to value of Int64 type")
            }
            _ => error.contains("Cannot cast string 'bad' to value of Int64 type"),
        },
        Some("INCOMPATIBLE_COLUMN_TYPE") => {
            error.contains("Incompatible STRING result types")
                || error.contains("Incompatible set-operation column types")
        }
        Some("PARSE_SYNTAX_ERROR") => error.contains("found BY"),
        Some("NUM_COLUMNS_MISMATCH") => error.contains("same number of columns"),
        _ => false,
    }
}

#[tokio::test]
async fn set_coercion_preserves_output_names_and_field_metadata() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let metadata = [("description".to_string(), "synthetic count".to_string())]
        .into_iter()
        .collect();
    let field = Field::new("value", DataType::Int32, true).with_metadata(metadata);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
    )?;
    session.register_batch("counts", batch)?;
    let query = plan_query(
        &session,
        "SELECT value FROM counts INTERSECT SELECT CAST(1 AS BIGINT)",
        &SparkSqlOptions::default(),
    )
    .await?;
    assert_eq!(query.column_names, ["value"]);
    let physical = query.create_physical_plan(&session).await?;
    assert_eq!(physical.schema().field(0).name(), "value");
    assert_eq!(physical.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(physical.schema().field(0).metadata(), field.metadata());
    let rows = collect(physical, session.task_ctx()).await?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(array_value_to_string(rows[0].column(0).as_ref(), 0)?, "1");
    Ok(())
}

#[tokio::test]
async fn native_sql_keeps_its_set_operation_coercion() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT '01' AS value UNION ALL SELECT 1",
        "SELECT CAST(1 AS INT) AS value INTERSECT SELECT CAST(1 AS BIGINT)",
    ] {
        let expected = native.sql(sql).await?.collect().await?;
        let actual = configured.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
