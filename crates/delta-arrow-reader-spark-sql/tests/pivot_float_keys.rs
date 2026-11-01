//! PIVOT compares floating keys without changing aggregate inputs or native SQL.

use std::collections::BTreeSet;
use std::error::Error;

use arrow::array::{Array, AsArray, ListArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/float_inputs.rs"]
mod float_inputs;
use float_inputs::register_float_inputs;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn pivot_float_keys_match_spark() -> TestResult {
    let records = include_str!("fixtures/pivot-float-keys-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 128);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
    }
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
        register_float_inputs(&session, &records[0], batch_size, 2)?;
        for case in &records[1..] {
            let actual = execute_pivot(&session, case).await;
            if actual.as_ref().ok() != Some(&case["expected"]) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn execute_pivot(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            assert_eq!(field.name(), &query.column_names[index]);
            let logical = query.logical_plan.schema().field(index);
            assert_eq!(logical.data_type(), field.data_type());
            assert_eq!(logical.is_nullable(), field.is_nullable());
            assert_eq!(logical.metadata(), field.metadata());
            Ok(
                json!({"name":field.name(), "type":spark_schema_type(field.data_type())?,
                  "nullable":field.is_nullable(), "metadata":field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Utf8 => json!("string"),
        DataType::List(field) => json!({"type":"array",
            "elementType":spark_schema_type(field.data_type())?,"containsNull":field.is_nullable()}),
        other => return Err(format!("unexpected PIVOT output type: {other}").into()),
    })
}

fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::List(_) => {
            let values = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("expected list")?
                .value(row);
            json!(
                (0..values.len())
                    .map(|index| encode_value(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Float32 => {
            let value = column.as_primitive::<Float32Type>().value(row);
            json!(if value.is_nan() {
                "nan".into()
            } else {
                format!("{:08x}", value.to_bits())
            })
        }
        DataType::Float64 => {
            let value = column.as_primitive::<Float64Type>().value(row);
            json!(if value.is_nan() {
                "nan".into()
            } else {
                format!("{:016x}", value.to_bits())
            })
        }
        _ => json!(array_value_to_string(column, row)?),
    })
}

#[tokio::test]
async fn spark_pivot_planning_does_not_change_native_float_equality() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/pivot-float-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_float_inputs(session, &metadata, 32, 1)?;
    }
    for kind in ["float", "double"] {
        execute_pivot(&configured, &json!({"ansi":true,"sql":format!(
            "SELECT * FROM (SELECT k FROM {kind}_mixed) PIVOT(COUNT(*) FOR (k) IN (CAST('-0.0' AS {kind})))"
        )})).await?;
        for operator in ["=", "IS NOT DISTINCT FROM"] {
            let sql = format!(
                "SELECT COUNT(*) AS n FROM {kind}_mixed WHERE k {operator} CAST('-0.0' AS {kind})"
            );
            let expected = native.sql(&sql).await?.collect().await?;
            let actual = configured.sql(&sql).await?.collect().await?;
            assert_eq!(actual, expected, "{sql}");
        }
    }
    Ok(())
}
