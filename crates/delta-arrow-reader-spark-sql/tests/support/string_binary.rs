//! Independent Spark observations for the bounded C06 string and binary scope.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
use serde_json::{Value, json};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn decode_hex(value: &str) -> TestResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err("odd binary encoding length".into());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
        .collect()
}

pub(crate) fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    representation: usize,
    batch_size: usize,
) -> TestResult {
    let inputs = metadata
        .get("registered_inputs")
        .or_else(|| metadata.get("inputs"))
        .and_then(Value::as_array)
        .ok_or("missing inputs")?;
    for input in inputs {
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, field) in input["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .enumerate()
        {
            let values: Vec<_> = rows.iter().map(|row| &row[index]).collect();
            let array: ArrayRef = match field["type"].as_str().ok_or("missing type")? {
                "string" => Arc::new(StringArray::from(
                    values.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
                )),
                "integer" => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(v.as_str().ok_or("missing integer")?.parse()?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                )),
                "long" => Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(v.as_str().ok_or("missing long")?.parse()?))
                            }
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                )),
                "binary" => {
                    let decoded = values
                        .iter()
                        .map(|v| v.as_str().map(decode_hex).transpose())
                        .collect::<TestResult<Vec<_>>>()?;
                    Arc::new(BinaryArray::from(
                        decoded.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                    ))
                }
                other => return Err(format!("unexpected input type: {other}").into()),
            };
            let target = match (array.data_type(), representation) {
                (DataType::Utf8, 1) => DataType::LargeUtf8,
                (DataType::Utf8, 2) => DataType::Utf8View,
                (DataType::Binary, 1) => DataType::LargeBinary,
                (DataType::Binary, 2) => DataType::BinaryView,
                (data_type, _) => data_type.clone(),
            };
            let array = cast(&array, &target)?;
            let field_metadata: HashMap<String, String> =
                serde_json::from_value(field["metadata"].clone())?;
            fields.push(
                Field::new(
                    field["name"].as_str().ok_or("missing name")?,
                    target,
                    field["nullable"].as_bool().ok_or("missing nullability")?,
                )
                .with_metadata(field_metadata),
            );
            columns.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut partitions = vec![Vec::new(); 2];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}

fn capture_schema(fields: &[Arc<Field>], names: Option<&[String]>) -> TestResult<Value> {
    Ok(
        json!({"type":"struct", "fields": fields.iter().enumerate().map(|(i, field)| {
        let data_type = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => "binary",
            DataType::Int32 => "integer",
            DataType::Int64 => "long",
            DataType::Float64 => "double",
            DataType::Boolean => "boolean",
            DataType::Null => "void",
            other => return Err(format!("unexpected output type: {other}").into()),
        };
        Ok(json!({"name": names.map_or(field.name(), |names| &names[i]),
            "type":data_type,"nullable":field.is_nullable(),"metadata":field.metadata()}))
    }).collect::<TestResult<Vec<_>>>()?}),
    )
}

// Retain fields from each completed planning stage when a later stage fails.
// Existing callers keep capture_result's Result-based interface below.
pub(crate) async fn capture_observation(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let query = match plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(query) => query,
        Err(error) => {
            return Ok(json!({
                "error":format!("resolve: {error}"),
                "error_stage":"resolve",
                "analyzed_schema":null,
                "physical_schema":null,
                "analyzed_arrow_schema":null,
                "physical_arrow_schema":null,
            }));
        }
    };
    let analyzed_schema = capture_schema(
        query.logical_plan.schema().fields(),
        Some(&query.column_names),
    )?;
    let mut observation = json!({
        "analyzed_schema":analyzed_schema,
        "analyzed_arrow_schema":format!("{:?}",query.logical_plan.schema()),
        "physical_schema":null,
        "physical_arrow_schema":null,
    });
    let physical = match query.create_physical_plan(session).await {
        Ok(physical) => physical,
        Err(error) => {
            observation["error"] = json!(format!("physical: {error}"));
            observation["error_stage"] = json!("physical");
            return Ok(observation);
        }
    };
    let schema = physical.schema();
    observation["physical_schema"] = capture_schema(schema.fields(), None)?;
    observation["physical_arrow_schema"] = json!(format!("{schema:?}"));
    let batches = match collect(physical, session.task_ctx()).await {
        Ok(batches) => batches,
        Err(error) => {
            observation["error"] = json!(format!("execute: {error}"));
            observation["error_stage"] = json!("execute");
            return Ok(observation);
        }
    };
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema(), schema);
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                let nulls = column.logical_nulls();
                let value = if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    assert!(schema.field(index).is_nullable());
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                    Value::Null
                } else if matches!(
                    column.data_type(),
                    DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                        | DataType::FixedSizeBinary(_)
                ) {
                    let binary = cast(column, &DataType::Binary)?;
                    let binary = binary
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or("expected binary")?;
                    json!(
                        binary
                            .value(row)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>()
                    )
                } else {
                    json!(array_value_to_string(column.as_ref(), row)?)
                };
                values.push(value);
            }
            rows.push(values);
        }
    }
    observation["rows"] = json!(rows);
    Ok(observation)
}

pub(crate) async fn capture_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let observation = capture_observation(session, case).await?;
    if let Some(error) = observation["error"].as_str() {
        return Err(error.to_owned().into());
    }
    Ok(observation)
}
