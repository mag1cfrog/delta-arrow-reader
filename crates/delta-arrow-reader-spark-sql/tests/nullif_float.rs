//! Independent Spark results for floating NULLIF and its comparison controls.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn reference_session(metadata: &Value, batch_size: usize) -> TestResult<SessionContext> {
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
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(Int32Array::from_iter_values(0..rows.len() as i32))];
        for column in 0..2 {
            let values = rows
                .iter()
                .map(|row| {
                    row[column]
                        .as_str()
                        .map(|bits| u64::from_str_radix(bits, 16))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            columns.push(match input["type"].as_str() {
                Some("float") => Arc::new(Float32Array::from(
                    values
                        .into_iter()
                        .map(|v| {
                            v.map(|bits| u32::try_from(bits).map(f32::from_bits))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                Some("double") => Arc::new(Float64Array::from(
                    values
                        .into_iter()
                        .map(|v| v.map(f64::from_bits))
                        .collect::<Vec<_>>(),
                )),
                _ => return Err("unknown input type".into()),
            });
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", columns[1].data_type().clone(), true),
            Field::new("b", columns[2].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect();
        session.register_table(
            input["table"].as_str().ok_or("missing table")?,
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
    }
    Ok(session)
}

fn encode_result(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    let value = match column.data_type() {
        DataType::Float32 => {
            let value = column.as_primitive::<Float32Type>().value(row);
            if value.is_nan() {
                "nan".to_owned()
            } else {
                format!("{:08x}", value.to_bits())
            }
        }
        DataType::Float64 => {
            let value = column.as_primitive::<Float64Type>().value(row);
            if value.is_nan() {
                "nan".to_owned()
            } else {
                format!("{:016x}", value.to_bits())
            }
        }
        _ => array_value_to_string(column, row)?,
    };
    Ok(Value::String(value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_independent_spark_float_nullif_results() -> TestResult {
    let records = include_str!("fixtures/nullif-float-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    for batch_size in [1, 3, 32] {
        let session = reference_session(&records[0], batch_size)?;
        for case in &records[1..] {
            let options = SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                ..SparkSqlOptions::default()
            };
            let outcome: TestResult = async {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &options,
                )
                .await?;
                let logical_nullable = query.logical_plan.schema().field(0).is_nullable();
                let plan = query.create_physical_plan(&session).await?;
                let schema = plan.schema();
                let batches = collect(plan, session.task_ctx()).await?;
                let expected_type = match case["expected"]["schema"]["fields"][0]["type"].as_str() {
                    Some("float") => DataType::Float32,
                    Some("double") => DataType::Float64,
                    Some("integer") => DataType::Int32,
                    Some("string") => DataType::Utf8,
                    Some("boolean") => DataType::Boolean,
                    Some("decimal(10,2)") => DataType::Decimal128(10, 2),
                    _ => {
                        return Err(
                            "unrecognized reference type or an expected error succeeded".into()
                        );
                    }
                };
                assert_eq!(
                    schema.field(0).data_type(),
                    &expected_type,
                    "{}",
                    case["id"]
                );
                assert_eq!(query.column_names, ["r"], "{}", case["id"]);
                let mut rows = Vec::new();
                for batch in batches {
                    for row in 0..batch.num_rows() {
                        if batch.column(0).is_null(row) {
                            assert!(
                                logical_nullable && schema.field(0).is_nullable(),
                                "{}: NULL must be representable",
                                case["id"]
                            );
                        }
                        rows.push(vec![encode_result(batch.column(0).as_ref(), row)?]);
                    }
                }
                assert_eq!(
                    json!(rows),
                    case["expected"]["rows"],
                    "{} ANSI={} batch={batch_size}",
                    case["id"],
                    options.ansi_mode
                );
                Ok(())
            }
            .await;
            if case["expected"]["status"] == "error" {
                let error = outcome
                    .err()
                    .ok_or("expected the live expression to fail")?
                    .to_string();
                match case["expected"]["condition"].as_str() {
                    Some("CAST_INVALID_INPUT") => {
                        let cast_type = if case["id"]
                            .as_str()
                            .ok_or("missing case ID")?
                            .starts_with("float_")
                        {
                            "Float32"
                        } else {
                            "Float64"
                        };
                        assert!(
                            error.contains("[CAST_INVALID_INPUT]") && error.contains(cast_type),
                            "{}: {error}",
                            case["id"]
                        );
                    }
                    Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => {
                        assert!(
                            error.contains("Scalar subquery returned more than one row"),
                            "{}: {error}",
                            case["id"]
                        );
                    }
                    _ => return Err("unrecognized reference error".into()),
                }
            } else {
                outcome?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn evaluate_each_volatile_nullif_operand_once() -> TestResult {
    let session = reference_session(&json!({"inputs": []}), 2)?;
    let left_rows = Arc::new(AtomicUsize::new(0));
    let right_rows = Arc::new(AtomicUsize::new(0));
    for (name, counter, zero) in [
        ("left_value", left_rows.clone(), -0.0),
        ("right_value", right_rows.clone(), 0.0),
    ] {
        session.register_udf(create_udf(
            name,
            vec![DataType::Int32],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let arrays = ColumnarValue::values_to_arrays(arguments)?;
                counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
                Ok(ColumnarValue::Array(Arc::new(
                    Float64Array::from_iter_values((0..arrays[0].len()).map(|_| zero)),
                )))
            }),
        ));
    }
    let query = plan_query(
        &session,
        "SELECT NULLIF(left_value(id),right_value(id)) AS r FROM VALUES (0),(1),(2) t(id)",
        &SparkSqlOptions::default(),
    )
    .await?;
    let plan = query.create_physical_plan(&session).await?;
    assert_eq!(left_rows.load(Ordering::SeqCst), 0);
    assert_eq!(right_rows.load(Ordering::SeqCst), 0);
    let batches = collect(plan, session.task_ctx()).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    assert!(
        batches
            .iter()
            .all(|b| b.column(0).null_count() == b.num_rows())
    );
    assert_eq!(left_rows.load(Ordering::SeqCst), 3);
    assert_eq!(right_rows.load(Ordering::SeqCst), 3);
    Ok(())
}
