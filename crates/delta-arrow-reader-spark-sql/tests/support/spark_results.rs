//! Capture full query rows and schemas for comparison with independent Spark records.

use std::error::Error;

use arrow::array::{Array, AsArray, ListArray, StructArray};
use arrow::datatypes::{DataType, Fields, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
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
    Ok(capture_spark_query_observation(session, case).await?["result"].take())
}

pub(crate) async fn capture_spark_query_observation(
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
    let resolved = query.logical_plan.schema().fields();
    let public_schema = capture_schema(resolved, Some(&query.column_names))?;
    let physical_schema = capture_schema(schema.fields(), None)?;
    if let Some(expected) = case
        .get("executed_schema_after_collect")
        .or_else(|| case.get("executed_schema_before_collect"))
    {
        if physical_schema != *expected {
            return Err(format!(
                "executed schema: expected {expected}, got {physical_schema}"
            )
            .into());
        }
    } else {
        validate_physical_schema(resolved, &query.column_names, schema.fields())?;
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
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
    Ok(json!({"result":{"schema":public_schema,"rows":rows},
        "physical_schema":physical_schema}))
}

pub(crate) fn validate_physical_schema(
    resolved: &Fields,
    names: &[String],
    physical: &Fields,
) -> TestResult {
    if resolved.len() != names.len() || physical.len() != resolved.len() {
        return Err("resolved and physical field counts differ from output names".into());
    }
    for ((logical, name), field) in resolved.iter().zip(names).zip(physical) {
        // Only top-level nullability can narrow; nested declarations stay exact.
        if field.name() != name
            || logical.data_type() != field.data_type()
            || logical.metadata() != field.metadata()
            || (field.is_nullable() && !logical.is_nullable())
        {
            return Err(format!(
                "resolved field {logical:?} (public name {name:?}) differs from physical field {field:?}"
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_batch(schema: &Schema, batch: &RecordBatch) -> TestResult {
    if batch.schema().as_ref() != schema {
        return Err("returned batch schema differs from physical schema".into());
    }
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.logical_null_count() != 0 {
            return Err(format!(
                "nonnullable physical field {:?} returned NULL",
                field.name()
            )
            .into());
        }
        column.to_data().validate_full()?;
    }
    Ok(())
}

pub(crate) fn capture_schema(fields: &Fields, names: Option<&[String]>) -> TestResult<Value> {
    Ok(json!({"type":"struct","fields":fields.iter().enumerate()
        .map(|(index, field)| Ok(json!({
            "name":names.map_or(field.name(), |names| &names[index]),
            "type":spark_schema_type(field.data_type())?,
            "nullable":field.is_nullable(),"metadata":field.metadata()})))
        .collect::<TestResult<Vec<_>>>()?}))
}

pub(crate) fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Null => json!("void"),
        DataType::Int8 => json!("byte"),
        DataType::Int16 => json!("short"),
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Boolean => json!("boolean"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Decimal128(precision, scale) => json!(format!("decimal({precision},{scale})")),
        DataType::Utf8 => json!("string"),
        DataType::List(field) => json!({"type":"array",
            "elementType":spark_schema_type(field.data_type())?,"containsNull":field.is_nullable()}),
        DataType::Struct(fields) => json!({"type":"struct", "fields":fields.iter()
            .map(|field| Ok(json!({"name":field.name(), "type":spark_schema_type(field.data_type())?,
                "nullable":field.is_nullable(), "metadata":field.metadata()})))
            .collect::<TestResult<Vec<_>>>()?}),
        other => return Err(format!("unexpected Spark output type: {other}").into()),
    })
}

pub(crate) fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Null => Value::Null,
        DataType::List(field) => {
            let values = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("expected list")?
                .value(row);
            if !field.is_nullable() && values.logical_null_count() != 0 {
                return Err("nonnullable list element returned NULL".into());
            }
            json!(
                (0..values.len())
                    .map(|index| encode_value(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Struct(fields) => {
            let values = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("expected struct")?;
            json!(
                values
                    .columns()
                    .iter()
                    .zip(fields)
                    .map(|(column, field)| {
                        let value = encode_value(column.as_ref(), row)?;
                        if value.is_null() && !field.is_nullable() {
                            return Err(format!(
                                "nonnullable struct field {:?} returned NULL",
                                field.name()
                            )
                            .into());
                        }
                        Ok(value)
                    })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, NullArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn null_values_are_distinct_from_empty_strings() -> TestResult {
        let untyped: ArrayRef = Arc::new(NullArray::new(1));
        let typed: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let text: ArrayRef = Arc::new(StringArray::from(vec![""]));
        assert_eq!(encode_value(untyped.as_ref(), 0)?, Value::Null);
        assert_eq!(encode_value(typed.as_ref(), 0)?, Value::Null);
        assert_eq!(encode_value(text.as_ref(), 0)?, json!(""));

        let nested = StructArray::from(vec![
            (
                Arc::new(Field::new("untyped", DataType::Null, true)),
                untyped.clone(),
            ),
            (Arc::new(Field::new("typed", DataType::Int32, true)), typed),
            (Arc::new(Field::new("text", DataType::Utf8, false)), text),
        ]);
        assert_eq!(encode_value(&nested, 0)?, json!([null, null, ""]));

        let lists = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, 1, 1].into()),
            untyped,
            None,
        )?;
        assert_eq!(encode_value(&lists, 0)?, json!([null]));
        assert_eq!(encode_value(&lists, 1)?, json!([]));
        Ok(())
    }
}
