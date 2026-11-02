//! VALUES must choose the common input type before evaluating Decimal rows.

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

fn spark_type_name(data_type: &DataType) -> TestResult<String> {
    Ok(match data_type {
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Null => "void".into(),
        other => return Err(format!("unexpected VALUES result type: {other:?}").into()),
    })
}

async fn execute_values_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        ..SparkSqlOptions::default()
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await?;
    if case["expected"]["condition"]
        .as_str()
        .is_some_and(|condition| {
            matches!(
                condition,
                "INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE"
                    | "INVALID_INLINE_TABLE.NUM_COLUMNS_MISMATCH"
            )
        })
    {
        return Err("invalid VALUES input reached physical planning".into());
    }
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    let types = schema
        .fields()
        .iter()
        .map(|field| spark_type_name(field.data_type()))
        .collect::<TestResult<Vec<_>>>()?;
    for (logical, physical) in query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .zip(schema.fields())
    {
        assert_eq!(logical.data_type(), physical.data_type());
    }
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                if column
                    .logical_nulls()
                    .is_some_and(|nulls| nulls.is_null(row))
                {
                    assert!(schema.field(index).is_nullable());
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                    values.push(Value::Null);
                } else {
                    let value = array_value_to_string(column.as_ref(), row)?;
                    values.push(if column.data_type().is_floating() {
                        json!(value.parse::<f64>()?)
                    } else {
                        value.into()
                    });
                }
            }
            rows.push(values);
        }
    }
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        query.column_names.iter().collect::<Vec<_>>()
    );
    Ok(json!({"types":types, "names":query.column_names, "rows":rows}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_values_decimal_coercion() -> TestResult {
    let records = include_str!("fixtures/values-decimal-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 143);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["retain_fraction_digits_first"], "false");
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
            let expected = &case["expected"];
            let actual = execute_values_query(&session, case).await;
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    let fields = expected["schema"]["fields"]
                        .as_array()
                        .ok_or("missing Spark fields")?;
                    let mut rows = expected["rows"]
                        .as_array()
                        .ok_or("missing Spark rows")?
                        .clone();
                    for row in &mut rows {
                        for (value, field) in row
                            .as_array_mut()
                            .ok_or("invalid Spark row")?
                            .iter_mut()
                            .zip(fields)
                        {
                            if !value.is_null()
                                && matches!(field["type"].as_str(), Some("float" | "double"))
                            {
                                *value = json!(
                                    value
                                        .as_str()
                                        .ok_or("invalid Spark float")?
                                        .parse::<f64>()?
                                );
                            }
                        }
                    }
                    let mut actual_rows = actual["rows"]
                        .as_array()
                        .ok_or("missing native rows")?
                        .clone();
                    if case["ordered"] == false {
                        rows.sort_by_cached_key(Value::to_string);
                        actual_rows.sort_by_cached_key(Value::to_string);
                    }
                    actual["types"] == json!(fields.iter().map(|f| &f["type"]).collect::<Vec<_>>())
                        && actual["names"]
                            == json!(fields.iter().map(|f| &f["name"]).collect::<Vec<_>>())
                        && actual_rows == rows
                }
                Err(error) if expected["status"] == "error" => {
                    let message = error.to_string();
                    match expected["condition"].as_str() {
                        Some("INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE") => {
                            message.contains("Incompatible VALUES Decimal input type")
                        }
                        Some("INVALID_INLINE_TABLE.NUM_COLUMNS_MISMATCH") => {
                            message.contains("Inconsistent data length across values list")
                        }
                        Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") => {
                            message.contains("Cannot cast Some(\"bad\") to DECIMAL(4,2)")
                                || message
                                    .contains("Cannot cast string 'bad' to value of Int32 type")
                        }
                        Some("REMAINDER_BY_ZERO") => message.contains("[REMAINDER_BY_ZERO]"),
                        _ => false,
                    }
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
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
