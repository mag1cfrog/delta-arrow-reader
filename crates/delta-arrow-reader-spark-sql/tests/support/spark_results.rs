//! Capture full query rows and schemas for comparison with independent Spark records.

use std::error::Error;

use arrow::array::{Array, AsArray, ListArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::util::display::array_value_to_string;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
use serde_json::{Value, json};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) async fn capture_spark_query_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
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
        other => return Err(format!("unexpected Spark output type: {other}").into()),
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
