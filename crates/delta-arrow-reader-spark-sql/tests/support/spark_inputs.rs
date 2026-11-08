//! Register Arrow providers from the schemas and values recorded by Spark fixtures.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, MapArray, StructArray, new_empty_array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Decimal128Type, Field, IntervalUnit, Schema, TimeUnit};
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
        Some("boolean") => DataType::Boolean,
        Some("string") => DataType::Utf8,
        Some("binary") => DataType::Binary,
        Some("date") => DataType::Date32,
        // A Delta TIMESTAMP column reaches the frontend as UTC-annotated microseconds.
        Some("timestamp") => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Some("timestamp_ntz") => DataType::Timestamp(TimeUnit::Microsecond, None),
        // Spark exports every year-month qualifier as one Arrow month count.
        Some("interval year" | "interval year to month" | "interval month") => {
            DataType::Interval(IntervalUnit::YearMonth)
        }
        // The recorded day-time columns are DAY TO SECOND microsecond counts.
        Some("interval day to second") => DataType::Duration(TimeUnit::Microsecond),
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
        None if value["type"] == "map" => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", decode_recorded_data_type(&value["keyType"])?, false),
                        Field::new(
                            "values",
                            decode_recorded_data_type(&value["valueType"])?,
                            value["valueContainsNull"]
                                .as_bool()
                                .ok_or("missing valueContainsNull")?,
                        ),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
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
        DataType::Date32 => {
            ScalarValue::Date32(Some(value.as_str().ok_or("missing day count")?.parse()?))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => ScalarValue::TimestampMicrosecond(
            Some(value.as_str().ok_or("missing microsecond count")?.parse()?),
            timezone.clone(),
        ),
        DataType::Interval(IntervalUnit::YearMonth) => ScalarValue::IntervalYearMonth(Some(
            value.as_str().ok_or("missing month count")?.parse()?,
        )),
        DataType::Duration(TimeUnit::Microsecond) => ScalarValue::DurationMicrosecond(Some(
            value.as_str().ok_or("missing microsecond count")?.parse()?,
        )),
        DataType::Boolean => ScalarValue::Boolean(Some(match value.as_str() {
            Some("True" | "true") => true,
            Some("False" | "false") => false,
            _ => return Err(format!("unexpected recorded boolean: {value}").into()),
        })),
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
        DataType::Map(field, ordered) => {
            let recorded_entries = value.as_array().ok_or("missing map entries")?;
            let entries = if recorded_entries.is_empty() {
                new_empty_array(field.data_type())
            } else {
                ScalarValue::iter_to_array(
                    recorded_entries
                        .iter()
                        .map(|entry| decode_recorded_value(entry, field.data_type()))
                        .collect::<TestResult<Vec<_>>>()?,
                )?
            };
            ScalarValue::Map(Arc::new(MapArray::try_new(
                field.clone(),
                OffsetBuffer::from_lengths([recorded_entries.len()]),
                entries
                    .as_struct_opt()
                    .ok_or("map entries are not a Struct")?
                    .clone(),
                None,
                *ordered,
            )?))
        }
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

/// Retains hidden Arrow storage for inputs marked `native_slice`.
#[allow(
    dead_code,
    reason = "Integration tests select individual shared input registrations."
)]
pub(crate) fn register_recorded_inputs_with_slices(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    register_recorded_inputs_with_slices_impl(session, metadata, batch_size, None)
}

#[allow(dead_code, reason = "Only C06 callers request additive receipts.")]
pub(crate) async fn register_recorded_inputs_with_slices_and_receipts(
    session: &SessionContext, metadata: &Value, batch_size: usize,
) -> TestResult<Value> {
    let mut registrations = Vec::new();
    register_recorded_inputs_with_slices_impl(session, metadata, batch_size, Some(&mut registrations))?;
    super::spark_results::c06_receipts::provider_readbacks(
        session, registrations, super::spark_results::encode_value,
    ).await
}

fn register_recorded_inputs_with_slices_impl(
    session: &SessionContext, metadata: &Value, batch_size: usize,
    mut receipts: Option<&mut Vec<Value>>,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let mut storage = input.clone();
        let visible_rows = input["rows"].as_array().ok_or("missing rows")?.len();
        if input["native_slice"] == true {
            let rows = storage["rows"].as_array_mut().ok_or("missing rows")?;
            rows.insert(0, metadata["native"]["sliced_hidden_rows"][0].clone());
            rows.push(metadata["native"]["sliced_hidden_rows"][1].clone());
        }
        let storage = decode_recorded_batch(&storage)?;
        let batch = storage.slice(usize::from(input["native_slice"] == true), visible_rows);
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (chunk, offset) in (0..visible_rows).step_by(batch_size).enumerate() {
            partitions[chunk % 2].push(batch.slice(offset, batch_size.min(visible_rows - offset)));
        }
        if let Some(receipts) = receipts.as_deref_mut() {
            let mut receipt=super::spark_results::c06_receipts::registration(
                input, &storage, &batch, &partitions, super::spark_results::encode_value,
            )?;
            receipt["registration"]["slice"]=serde_json::json!({
                "offset":usize::from(input["native_slice"] == true),"length":visible_rows,
                "storage_rows":storage.num_rows()});
            receipts.push(receipt);
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
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
fn recorded_booleans_preserve_null_and_reject_other_values() -> TestResult {
    use serde_json::json;

    assert_eq!(
        decode_recorded_data_type(&json!("boolean"))?,
        DataType::Boolean
    );
    for (encoded, value) in [
        ("True", true),
        ("true", true),
        ("False", false),
        ("false", false),
    ] {
        assert_eq!(
            decode_recorded_value(&json!(encoded), &DataType::Boolean)?,
            ScalarValue::Boolean(Some(value))
        );
    }
    assert_eq!(
        decode_recorded_value(&Value::Null, &DataType::Boolean)?,
        ScalarValue::Boolean(None)
    );
    for encoded in [json!("yes"), json!("1"), json!(1), json!(true)] {
        assert!(decode_recorded_value(&encoded, &DataType::Boolean).is_err());
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

#[test]
fn recorded_temporal_values_keep_days_and_microseconds() -> TestResult {
    use serde_json::json;

    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let wall_clock = DataType::Timestamp(TimeUnit::Microsecond, None);
    assert_eq!(decode_recorded_data_type(&json!("date"))?, DataType::Date32);
    assert_eq!(decode_recorded_data_type(&json!("timestamp"))?, utc);
    assert_eq!(
        decode_recorded_data_type(&json!("timestamp_ntz"))?,
        wall_clock
    );
    assert_eq!(
        decode_recorded_value(&json!("-719162"), &DataType::Date32)?,
        ScalarValue::Date32(Some(-719_162))
    );
    assert_eq!(
        decode_recorded_value(&json!("-1"), &utc)?,
        ScalarValue::TimestampMicrosecond(Some(-1), Some("UTC".into()))
    );
    assert_eq!(
        decode_recorded_value(&json!("253402300799999999"), &wall_clock)?,
        ScalarValue::TimestampMicrosecond(Some(253_402_300_799_999_999), None)
    );
    assert_eq!(
        decode_recorded_value(&Value::Null, &utc)?,
        ScalarValue::TimestampMicrosecond(None, Some("UTC".into()))
    );
    for (encoded, data_type) in [(json!(1), &DataType::Date32), (json!("1.5"), &wall_clock)] {
        assert!(decode_recorded_value(&encoded, data_type).is_err());
    }
    Ok(())
}
