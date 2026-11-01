//! Spark set keys equate both zero signs and all NaNs without changing UNION ALL.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array, StringArray};
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
async fn set_operation_float_keys_match_spark() -> TestResult {
    let records = include_str!("fixtures/set-operation-float-keys-spark.jsonl")
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
                    let mut actual_rows = actual["rows"].clone();
                    let mut expected_rows = expected["rows"].clone();
                    match case["comparison"].as_str() {
                        Some("float_equality") => {
                            equate_set_zero_signs(&mut actual_rows, &fields)?;
                            equate_set_zero_signs(&mut expected_rows, &fields)?;
                        }
                        Some("exact_bits") => {}
                        _ => return Err("missing reference comparison policy".into()),
                    }
                    actual_rows == expected_rows && actual["fields"] == json!(fields)
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

// Spark may remove normalization during adaptive planning; set equality still equates zeros.
fn equate_set_zero_signs(rows: &mut Value, fields: &[Value]) -> TestResult {
    let rows = rows.as_array_mut().ok_or("missing rows")?;
    for row in rows.iter_mut() {
        for (value, field) in row
            .as_array_mut()
            .ok_or("missing row")?
            .iter_mut()
            .zip(fields)
        {
            let positive_zero = match (field["type"].as_str(), value.as_str()) {
                (Some("float"), Some("80000000")) => Some("00000000"),
                (Some("double"), Some("8000000000000000")) => Some("0000000000000000"),
                _ => None,
            };
            if let Some(zero) = positive_zero {
                *value = json!(zero);
            }
        }
    }
    rows.sort_by_key(Value::to_string);
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
            let data_type: String = match field.data_type() {
                DataType::Float32 => "float".into(),
                DataType::Float64 => "double".into(),
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

fn register_float_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let rows = input["rows"].as_array().ok_or("missing input rows")?;
        let bits = rows
            .iter()
            .map(|row| {
                row[0]
                    .as_str()
                    .map(|value| u64::from_str_radix(value, 16))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let column: ArrayRef = match input["type"].as_str() {
            Some("float") => Arc::new(Float32Array::from(
                bits.into_iter()
                    .map(|v| v.map(|v| u32::try_from(v).map(f32::from_bits)).transpose())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Some("double") => Arc::new(Float64Array::from(
                bits.into_iter()
                    .map(|v| v.map(f64::from_bits))
                    .collect::<Vec<_>>(),
            )),
            _ => return Err("unexpected input type".into()),
        };
        let labels = Arc::new(StringArray::from(
            rows.iter().map(|row| row[1].as_str()).collect::<Vec<_>>(),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", column.data_type().clone(), true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column, labels])?;
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

#[tokio::test]
async fn union_all_preserves_zero_signs_and_nan_payloads() -> TestResult {
    for values in raw_float_inputs() {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );

        let batch = RecordBatch::try_from_iter([("k", values)])?;
        session.register_batch("input", batch.clone())?;
        let mut expected = float_row_bits(&[batch.clone(), batch])?;
        expected.sort();
        for ansi_mode in [false, true] {
            let query = plan_query(
                &session,
                "SELECT k FROM input UNION ALL SELECT k FROM input",
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let batches = collect(
                query.create_physical_plan(&session).await?,
                session.task_ctx(),
            )
            .await?;
            let mut actual = float_row_bits(&batches)?;
            actual.sort();
            assert_eq!(actual, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn floating_set_outputs_preserve_names_and_metadata() -> TestResult {
    for data_type in [DataType::Float32, DataType::Float64] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );

        let values: ArrayRef = if data_type == DataType::Float32 {
            Arc::new(Float32Array::from(vec![Some(0.0), Some(-0.0), None]))
        } else {
            Arc::new(Float64Array::from(vec![Some(0.0), Some(-0.0), None]))
        };
        let metadata = HashMap::from([("description".into(), "synthetic float".into())]);
        let field = Field::new("k", data_type.clone(), true).with_metadata(metadata.clone());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![values])?;
        session.register_batch("left_input", batch.clone())?;
        session.register_batch("right_input", batch.slice(1, 2))?;
        for (operation, row_count) in [
            ("UNION", 2),
            ("UNION ALL", 5),
            ("INTERSECT", 2),
            ("INTERSECT ALL", 2),
            ("EXCEPT", 0),
            ("EXCEPT ALL", 1),
        ] {
            let query = plan_query(
                &session,
                &format!("SELECT k FROM left_input {operation} SELECT k FROM right_input"),
                &SparkSqlOptions::default(),
            )
            .await?;
            assert_eq!(query.column_names, ["k"]);
            assert_eq!(
                query.logical_plan.schema().field(0).metadata(),
                &metadata,
                "{operation}"
            );
            let physical = query.create_physical_plan(&session).await?;
            let schema = physical.schema();
            assert_eq!(schema.field(0).name(), "k");
            assert_eq!(schema.field(0).data_type(), &data_type);
            assert_eq!(schema.field(0).metadata(), &metadata, "{operation}");
            let batches = collect(physical, session.task_ctx()).await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                row_count
            );
            if batches.iter().any(|batch| batch.column(0).null_count() > 0) {
                assert!(schema.field(0).is_nullable());
                assert!(query.logical_plan.schema().field(0).is_nullable());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_sql_keeps_its_floating_set_behavior() -> TestResult {
    for values in raw_float_inputs() {
        let native = SessionContext::new();
        let configured = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );

        let batch = RecordBatch::try_from_iter([("k", values)])?;
        for session in [&native, &configured] {
            session.register_batch("input", batch.clone())?;
        }
        for operation in [
            "UNION",
            "UNION ALL",
            "INTERSECT",
            "INTERSECT ALL",
            "EXCEPT",
            "EXCEPT ALL",
        ] {
            let sql = format!("SELECT k FROM input {operation} SELECT k FROM input");
            let expected = native.sql(&sql).await?.collect().await?;
            let actual = configured.sql(&sql).await?.collect().await?;
            let mut expected_bits = float_row_bits(&expected)?;
            let mut actual_bits = float_row_bits(&actual)?;
            expected_bits.sort();
            actual_bits.sort();
            assert_eq!(actual_bits, expected_bits, "{operation}");
        }
    }
    Ok(())
}

fn raw_float_inputs() -> [ArrayRef; 2] {
    [
        Arc::new(Float32Array::from(
            vec![
                Some(0),
                Some(0x80000000),
                Some(0x7fc01234),
                Some(0xffc04321),
                Some(0x7f800001),
                None,
            ]
            .into_iter()
            .map(|value| value.map(f32::from_bits))
            .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            vec![
                Some(0),
                Some(0x8000000000000000),
                Some(0x7ff8000000001234),
                Some(0xfff8000000004321),
                Some(0x7ff0000000000001),
                None,
            ]
            .into_iter()
            .map(|value| value.map(f64::from_bits))
            .collect::<Vec<_>>(),
        )),
    ]
}

fn float_row_bits(batches: &[RecordBatch]) -> TestResult<Vec<Option<u64>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(if column.is_null(row) {
                None
            } else {
                Some(match column.data_type() {
                    DataType::Float32 => {
                        u64::from(column.as_primitive::<Float32Type>().value(row).to_bits())
                    }
                    DataType::Float64 => column.as_primitive::<Float64Type>().value(row).to_bits(),
                    other => return Err(format!("unexpected float fixture type: {other}").into()),
                })
            });
        }
    }
    Ok(rows)
}
