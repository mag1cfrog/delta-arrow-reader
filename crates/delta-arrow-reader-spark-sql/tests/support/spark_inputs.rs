//! Register Arrow providers from the schemas and values recorded by Spark fixtures.

use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray, new_empty_array};
use arrow::datatypes::{DataType, Decimal128Type, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde_json::Value;

use super::spark_results::TestResult;

fn decode_recorded_data_type(value: &Value) -> TestResult<DataType> {
    Ok(match value.as_str() {
        Some("void") => DataType::Null,
        Some("float") => DataType::Float32,
        Some("double") => DataType::Float64,
        Some("byte") => DataType::Int8,
        Some("short") => DataType::Int16,
        Some("integer") => DataType::Int32,
        Some("long") => DataType::Int64,
        Some("string") => DataType::Utf8,
        Some("binary") => DataType::Binary,
        Some(decimal) if decimal.starts_with("decimal(") && decimal.ends_with(')') => {
            let (precision, scale) = decimal[8..decimal.len() - 1]
                .split_once(',')
                .ok_or("missing Decimal precision and scale")?;
            DataType::Decimal128(precision.parse()?, scale.parse()?)
        }
        None if value["type"] == "array" => DataType::List(Arc::new(Field::new(
            "item",
            decode_recorded_data_type(&value["elementType"])?,
            value["containsNull"]
                .as_bool()
                .ok_or("missing containsNull")?,
        ))),
        None if value["type"] == "struct" => DataType::Struct(
            value["fields"]
                .as_array()
                .ok_or("missing fields")?
                .iter()
                .map(|field| {
                    Ok(Field::new(
                        field["name"].as_str().ok_or("missing field name")?,
                        decode_recorded_data_type(&field["type"])?,
                        field["nullable"].as_bool().ok_or("missing nullability")?,
                    )
                    .with_metadata(serde_json::from_value(field["metadata"].clone())?))
                })
                .collect::<TestResult<Vec<_>>>()?
                .into(),
        ),
        _ => return Err(format!("unexpected recorded type: {value}").into()),
    })
}

fn decode_recorded_value(value: &Value, data_type: &DataType) -> TestResult<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::try_from(data_type)?);
    }
    Ok(match data_type {
        DataType::Float32 => ScalarValue::Float32(Some(f32::from_bits(u32::from_str_radix(
            value.as_str().ok_or("missing FLOAT bits")?,
            16,
        )?))),
        DataType::Float64 => ScalarValue::Float64(Some(f64::from_bits(u64::from_str_radix(
            value.as_str().ok_or("missing DOUBLE bits")?,
            16,
        )?))),
        DataType::Utf8 => {
            ScalarValue::Utf8(Some(value.as_str().ok_or("missing string")?.to_owned()))
        }
        DataType::Binary => {
            let hex = value.as_str().ok_or("missing binary hex")?;
            if hex.len() % 2 != 0
                || !hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err("invalid lowercase binary hex".into());
            }
            ScalarValue::Binary(Some(
                (0..hex.len())
                    .step_by(2)
                    .map(|index| u8::from_str_radix(&hex[index..index + 2], 16))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        DataType::Int32 => {
            ScalarValue::Int32(Some(value.as_str().ok_or("missing integer")?.parse()?))
        }
        DataType::Int8 => ScalarValue::Int8(Some(value.as_str().ok_or("missing byte")?.parse()?)),
        DataType::Int16 => {
            ScalarValue::Int16(Some(value.as_str().ok_or("missing short")?.parse()?))
        }
        DataType::Int64 => ScalarValue::Int64(Some(value.as_str().ok_or("missing long")?.parse()?)),
        DataType::Decimal128(precision, scale) => ScalarValue::Decimal128(
            Some(arrow::compute::kernels::cast_utils::parse_decimal::<
                Decimal128Type,
            >(
                value.as_str().ok_or("missing Decimal value")?,
                *precision,
                *scale,
            )?),
            *precision,
            *scale,
        ),
        DataType::List(field) => ScalarValue::List(ScalarValue::new_list(
            &value
                .as_array()
                .ok_or("missing list")?
                .iter()
                .map(|item| decode_recorded_value(item, field.data_type()))
                .collect::<TestResult<Vec<_>>>()?,
            field.data_type(),
            field.is_nullable(),
        )),
        DataType::Struct(fields) => {
            let values = value.as_array().ok_or("missing struct values")?;
            assert_eq!(values.len(), fields.len());
            let columns = values
                .iter()
                .zip(fields)
                .map(|(value, field)| {
                    Ok(decode_recorded_value(value, field.data_type())?.to_array_of_size(1)?)
                })
                .collect::<TestResult<Vec<_>>>()?;
            ScalarValue::Struct(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                None,
            )?))
        }
        _ => return Err(format!("unexpected input type: {data_type}").into()),
    })
}

pub(crate) fn decode_recorded_batch(input: &Value) -> TestResult<RecordBatch> {
    let DataType::Struct(fields) = decode_recorded_data_type(&input["schema"])? else {
        return Err("expected input schema".into());
    };
    let rows = input["rows"].as_array().ok_or("missing rows")?;
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if rows.is_empty() {
                Ok(new_empty_array(field.data_type()))
            } else {
                let values = rows
                    .iter()
                    .map(|row| decode_recorded_value(&row[index], field.data_type()))
                    .collect::<TestResult<Vec<_>>>()?;
                Ok(ScalarValue::iter_to_array(values)?)
            }
        })
        .collect::<TestResult<Vec<ArrayRef>>>()?;
    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn register_recorded_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    register_recorded_inputs_with_partition_count(session, metadata, batch_size, 2)
}

pub(crate) fn register_recorded_inputs_with_partition_count(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    partition_count: usize,
) -> TestResult {
    assert!(
        partition_count > 0,
        "input partition count must be positive"
    );
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let batch = decode_recorded_batch(input)?;
        let schema = batch.schema();
        let mut partitions = vec![Vec::new(); partition_count];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % partition_count]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}

#[test]
fn recorded_binary_values_check_hex_and_preserve_bytes() -> TestResult {
    use serde_json::json;

    assert_eq!(
        decode_recorded_data_type(&json!("binary"))?,
        DataType::Binary
    );
    for (encoded, bytes) in [("00ff6162", vec![0, 255, 97, 98]), ("", vec![])] {
        assert_eq!(
            decode_recorded_value(&json!(encoded), &DataType::Binary)?,
            ScalarValue::Binary(Some(bytes))
        );
    }
    assert_eq!(
        decode_recorded_value(&Value::Null, &DataType::Binary)?,
        ScalarValue::Binary(None)
    );
    for encoded in [
        json!("0"),
        json!("gg"),
        json!("0G"),
        json!("\u{00e9}"),
        json!(1),
    ] {
        assert!(decode_recorded_value(&encoded, &DataType::Binary).is_err());
    }
    Ok(())
}

#[test]
fn recorded_void_children_decode_as_null() -> TestResult {
    let data_type = decode_recorded_data_type(&serde_json::json!({
        "type": "array", "elementType": "void", "containsNull": true
    }))?;
    let DataType::List(field) = &data_type else {
        return Err("expected list".into());
    };
    assert_eq!(field.data_type(), &DataType::Null);
    let value = decode_recorded_value(&serde_json::json!([null]), &data_type)?;
    let ScalarValue::List(values) = value else {
        return Err("expected list value".into());
    };
    assert_eq!(values.value(0).data_type(), &DataType::Null);
    assert_eq!(values.value(0).len(), 1);
    Ok(())
}
