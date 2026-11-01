//! Spark comparisons share coercion while NULLIF retains its first input type and value.

use std::error::Error;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
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
        DataType::Int8 => "byte".into(),
        DataType::Int16 => "short".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Date32 => "date".into(),
        DataType::Null => "void".into(),
        other => return Err(format!("unexpected comparison result type: {other:?}").into()),
    })
}

async fn execute_comparison_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
    if case["expected"]["condition"] == "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES" {
        return Err("incomparable inputs reached physical planning".into());
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
                    let value = match column.data_type() {
                        DataType::Float32 => {
                            let value = column.as_primitive::<Float32Type>().value(row);
                            if value.is_nan() {
                                "nan".into()
                            } else {
                                format!("{:08x}", value.to_bits())
                            }
                        }
                        DataType::Float64 => {
                            let value = column.as_primitive::<Float64Type>().value(row);
                            if value.is_nan() {
                                "nan".into()
                            } else {
                                format!("{:016x}", value.to_bits())
                            }
                        }
                        _ => array_value_to_string(column.as_ref(), row)?,
                    };
                    values.push(value.into());
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
async fn match_spark_nullif_and_string_comparisons() -> TestResult {
    let mut records = include_str!("fixtures/nullif-types-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 625);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records
            .iter()
            .filter(|case| case.get("retained_difference").is_some())
            .count(),
        0
    );
    let strings = include_str!("fixtures/string-comparison-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(strings.len(), 685);
    assert_eq!(strings[0]["spark_version"], "4.2.0");
    assert_eq!(
        strings
            .iter()
            .filter(|case| case.get("retained_difference").is_some())
            .count(),
        1
    );
    records.extend(strings.into_iter().skip(1));
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
            let actual = execute_comparison_query(&session, case).await;
            if case["retained_difference"] == "known_null_right_623" {
                // #623 separately handles a statically NULL right input.
                assert_eq!(case["id"], "null_right_known_bad");
                assert_eq!(case["ansi"], true);
                assert_eq!(expected["rows"], json!([["bad"]]));
                assert!(actual.is_err_and(|error| {
                    error
                        .to_string()
                        .contains("Cannot cast string 'bad' to value of Int64 type")
                }));
                continue;
            }
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    let fields = expected["schema"]["fields"]
                        .as_array()
                        .ok_or("missing Spark fields")?;
                    actual["types"] == json!(fields.iter().map(|f| &f["type"]).collect::<Vec<_>>())
                        && actual["names"] == json!(["r"])
                        && actual["rows"] == expected["rows"]
                }
                Err(error) if expected["condition"] == "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES" => {
                    error.to_string().contains("is not comparable")
                }
                Err(error) if expected["condition"] == "CAST_INVALID_INPUT" => {
                    let message = error.to_string();
                    message.contains("[CAST_INVALID_INPUT]")
                        || (case["id"] == "bad_left_null_right"
                            && message.contains("Cannot cast string 'bad' to value of Int32 type"))
                        || (message.contains("Cannot cast string")
                            && message.contains("to value of Int64 type"))
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
    let native = SessionContext::new();
    let rows = native
        .sql("SELECT NULLIF(1,CAST(2 AS DOUBLE)) AS r")
        .await?
        .collect()
        .await?;
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(rows[0].column(0).data_type(), &DataType::Float64);
    assert!(!rows[0].column(0).is_null(0));
    assert_eq!(
        rows[0].column(0).as_primitive::<Float64Type>().value(0),
        1.0
    );
    let native_fraction = async {
        native
            .sql("SELECT '1.5'=CAST(1 AS INT) AS r")
            .await?
            .collect()
            .await
    }
    .await;
    assert!(
        native_fraction.is_err_and(|error| error.to_string().contains("Cannot cast string '1.5'"))
    );
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
