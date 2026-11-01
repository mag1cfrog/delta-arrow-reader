//! Equal rows retain their required occurrence counts in ALL set operations.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn set_operation_duplicate_counts_match_spark() -> TestResult {
    let records = include_str!("fixtures/set-operation-duplicates-spark.jsonl")
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
        register_float_inputs(&session, &records[0], batch_size)?;
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
                    set_duplicate_error_matches(case, &error.to_string())
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

async fn execute_set_operation(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
                DataType::Null => "void".into(),
                DataType::Int32 => "integer".into(),
                DataType::Int64 => "long".into(),
                DataType::Float32 => "float".into(),
                DataType::Float64 => "double".into(),
                DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string".into(),
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
                                let value = match column.data_type() {
                                    DataType::Float32 => {
                                        column.as_primitive::<Float32Type>().value(row).to_string()
                                    }
                                    DataType::Float64 => {
                                        column.as_primitive::<Float64Type>().value(row).to_string()
                                    }
                                    _ => array_value_to_string(column.as_ref(), row)?,
                                };
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

fn set_duplicate_error_matches(case: &Value, error: &str) -> bool {
    match case["expected"]["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => {
            error.contains("Cannot cast string 'bad' to value of Int64 type")
        }
        Some("NUM_COLUMNS_MISMATCH") => error.contains("same number of columns"),
        _ => false,
    }
}

fn register_float_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let values = input["rows"]
            .as_array()
            .ok_or("missing input rows")?
            .iter()
            .map(|value| value.as_str().map(str::parse::<f64>).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let column: ArrayRef = match input["type"].as_str() {
            Some("float") => Arc::new(Float32Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v as f32))
                    .collect::<Vec<_>>(),
            )),
            Some("double") => Arc::new(Float64Array::from(values)),
            _ => return Err("unexpected input type".into()),
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column])?;
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (partition, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[partition % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}
