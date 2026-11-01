//! Spark comparisons share coercion while NULLIF retains its first input type and value.

use std::error::Error;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};
use std::sync::Arc;

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
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".into(),
        DataType::Null => "void".into(),
        other => return Err(format!("unexpected comparison result type: {other:?}").into()),
    })
}

async fn execute_comparison_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().unwrap_or("UTC").into(),
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
        if let DataType::Timestamp(_, Some(timezone)) = physical.data_type() {
            assert_eq!(
                timezone.as_ref(),
                case["result_timezone"]
                    .as_str()
                    .unwrap_or(&options.session_timezone)
            );
        }
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
                        DataType::Timestamp(TimeUnit::Microsecond, _) => column
                            .as_primitive::<TimestampMicrosecondType>()
                            .value(row)
                            .to_string(),
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

fn register_temporal_input(session: &SessionContext, table: &str, input: &Value) -> TestResult {
    let dates: Vec<Option<i32>> = serde_json::from_value(input["date_days"].clone())?;
    let ntz: Vec<Option<i64>> = serde_json::from_value(input["ntz_us"].clone())?;
    let ltz: Vec<Option<i64>> = serde_json::from_value(input["ltz_utc_us"].clone())?;
    assert_eq!(dates.len(), 4);
    let mut columns = vec![
        (
            "k",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        ("d", Arc::new(Date32Array::from(dates))),
        ("ntz", Arc::new(TimestampMicrosecondArray::from(ntz))),
        (
            "ltz",
            Arc::new(TimestampMicrosecondArray::from(ltz).with_timezone("UTC")),
        ),
    ];
    if let Some(strings) = input.get("strings") {
        let strings: Vec<Option<String>> = serde_json::from_value(strings.clone())?;
        columns.push(("s", Arc::new(StringArray::from(strings)) as ArrayRef));
    }
    session.register_batch(table, RecordBatch::try_from_iter(columns)?)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_comparison_coercion() -> TestResult {
    let mut records = include_str!("fixtures/nullif-types-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 721);
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
        0
    );
    records.extend(strings.into_iter().skip(1));
    let temporal = include_str!("fixtures/temporal-comparison-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(temporal.len(), 1415);
    assert_eq!(temporal[0]["spark_version"], "4.2.0");
    let temporal_input = temporal[0]["external_input"].clone();
    records.extend(temporal.into_iter().skip(1));
    let string_temporal = include_str!("fixtures/string-temporal-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(string_temporal.len(), 957);
    assert_eq!(string_temporal[0]["spark_version"], "4.2.0");
    assert_eq!(
        string_temporal
            .iter()
            .filter(|case| case.get("retained_difference").is_some())
            .count(),
        3
    );
    let string_temporal_input = string_temporal[0]["external_input"].clone();
    records.extend(string_temporal.into_iter().skip(1));
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
        register_temporal_input(&session, "external_temporal", &temporal_input)?;
        register_temporal_input(&session, "external_string_temporal", &string_temporal_input)?;
        session.register_batch(
            "nullif_input",
            RecordBatch::try_from_iter([
                ("k", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
                (
                    "a",
                    Arc::new(StringArray::from(vec![Some("bad"), Some("001"), None])) as ArrayRef,
                ),
                ("b", Arc::new(Int32Array::from(vec![None; 3])) as ArrayRef),
            ])?,
        )?;
        for case in &records[1..] {
            let expected = &case["expected"];
            let actual = execute_comparison_query(&session, case).await;
            if case["retained_difference"] == "local_null_comparison_633" {
                assert_eq!(expected["condition"], "CAST_INVALID_INPUT");
                assert_eq!(
                    actual?,
                    json!({"types":["boolean"], "names":["r"], "rows":[[null],[null],[null]]})
                );
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
                Err(error)
                    if expected["condition"] == "CAST_INVALID_INPUT"
                        || expected["condition"]
                            == "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION" =>
                {
                    let timestamp_parse_error = case["error_representation"] == "timestamp_parser"
                        && error.downcast_ref::<datafusion::common::DataFusionError>().is_some_and(|error| {
                            matches!(error.find_root(), datafusion::common::DataFusionError::Execution(message)
                                if message.starts_with("error in SQL parser: found b at 0:1")
                                    || message.starts_with("error in SQL parser: found end of input expected")
                                    || message == "invalid argument: DateValue { year: 2026, month: 2, day: 30 }")
                        });
                    let message = error.to_string();
                    timestamp_parse_error
                        || message.contains("[CAST_INVALID_INPUT]")
                        || message.contains("Cannot cast string 'bad' to value of Int32 type")
                        || message.contains("Cannot cast string 'bad' to value of Date32 type")
                        || message.contains("Error parsing timestamp from 'bad'")
                        || (message.contains("Cannot cast string")
                            && message.contains("to value of Int64 type"))
                        || (case["id"]
                            .as_str()
                            .is_some_and(|id| id.ends_with("bad_left_required"))
                            && message.starts_with(
                                "Execution error: error in SQL parser: found b at 0:1",
                            ))
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
    register_temporal_input(&native, "external_temporal", &temporal_input)?;
    let rows = native
        .sql("SELECT d=ltz AS r FROM external_temporal ORDER BY k")
        .await?
        .collect()
        .await?;
    let values = rows
        .iter()
        .flat_map(|batch| batch.column(0).as_boolean().iter())
        .collect::<Vec<_>>();
    assert_eq!(values, [Some(false), Some(true), None, Some(false)]);
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
    let configured_host = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured_host] {
        let native_date_error = async {
            session
                .sql("SELECT 'bad'=DATE '2026-01-02' AS r")
                .await?
                .collect()
                .await
        }
        .await;
        assert!(native_date_error.is_err_and(|error| {
            error
                .to_string()
                .contains("Cannot cast string 'bad' to value of Date32 type")
        }));
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
