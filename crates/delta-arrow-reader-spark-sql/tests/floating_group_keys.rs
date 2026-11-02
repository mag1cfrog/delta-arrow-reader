//! Floating grouping keys share Spark equality without changing aggregate payloads.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, StructArray, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

fn decode_recorded_data_type(value: &Value) -> TestResult<DataType> {
    Ok(match value.as_str() {
        Some("float") => DataType::Float32,
        Some("double") => DataType::Float64,
        Some("integer") => DataType::Int32,
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
        DataType::Int32 => {
            ScalarValue::Int32(Some(value.as_str().ok_or("missing integer")?.parse()?))
        }
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

fn register_recorded_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
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

#[tokio::test]
async fn floating_group_keys_match_spark() -> TestResult {
    let records = include_str!("fixtures/floating-group-keys-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 840);
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case).await;
            let matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                actual.as_ref().is_err_and(|error| {
                    let message = error.to_string();
                    message.contains("[CAST_INVALID_INPUT]") && message.contains("bad")
                })
            } else {
                actual.as_ref().ok() == Some(&case["expected"])
            };
            if !matches {
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

#[tokio::test]
async fn nested_key_normalization_preserves_argument_evaluation() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/floating-group-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    for ansi in [true, false] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );
        register_recorded_inputs(&session, &metadata, 2)?;
        let evaluated_rows = Arc::new(AtomicUsize::new(0));
        let observed = evaluated_rows.clone();
        session.register_udf(create_udf(
            "observed_key",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(move |args| {
                observed.fetch_add(
                    match &args[0] {
                        ColumnarValue::Array(array) => array.len(),
                        ColumnarValue::Scalar(_) => 1,
                    },
                    Ordering::SeqCst,
                );
                Ok(args[0].clone())
            }),
        ));
        let query = plan_query(
            &session,
            "SELECT COUNT(DISTINCT ARRAY(observed_key(k))) AS n FROM double_scalar_mixed",
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await?;
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 13);

        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = invoked.clone();
        session.register_udf(create_udf(
            "volatile_key",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |args| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        ));
        for expression in [
            "COUNT(DISTINCT ARRAY(volatile_key(k)))",
            "COLLECT_SET(ARRAY(volatile_key(k)))",
        ] {
            let planned = plan_query(
                &session,
                &format!("SELECT {expression} FROM double_scalar_mixed GROUP BY id%2"),
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await;
            assert!(
                planned.as_ref().is_err_and(|error| {
                    error
                        .to_string()
                        .contains("Non-deterministic expression volatile_key")
                }),
                "{planned:?}"
            );
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_planning_preserves_native_distinct_behavior() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/floating-group-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 32)?;
    }
    for kind in ["float", "double"] {
        for shape in ["scalar", "array", "struct", "nested", "struct_array"] {
            let sql = format!("SELECT COUNT(DISTINCT k) AS n FROM {kind}_{shape}_mixed");
            capture_spark_query_result(&configured, &json!({"ansi":true,"sql":sql})).await?;
            assert_eq!(
                configured.sql(&sql).await?.collect().await?,
                native.sql(&sql).await?.collect().await?,
                "{sql}"
            );
        }
    }
    Ok(())
}
