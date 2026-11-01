//! Spark comparisons share coercion while NULLIF retains its first input type and value.

use std::error::Error;

use arrow::array::{Array, ArrayRef, AsArray, Date32Array, Int32Array, TimestampMicrosecondArray};
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

fn register_temporal_input(session: &SessionContext, input: &Value) -> TestResult {
    let dates: Vec<Option<i32>> = serde_json::from_value(input["date_days"].clone())?;
    let ntz: Vec<Option<i64>> = serde_json::from_value(input["ntz_us"].clone())?;
    let ltz: Vec<Option<i64>> = serde_json::from_value(input["ltz_utc_us"].clone())?;
    assert_eq!(dates.len(), 4);
    session.register_batch(
        "external_temporal",
        RecordBatch::try_from_iter([
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
        ])?,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_comparison_coercion() -> TestResult {
    let mut records = include_str!("fixtures/nullif-types-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 625);
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
        1
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
        register_temporal_input(&session, &temporal_input)?;
        for case in &records[1..] {
            let expected = &case["expected"];
            let actual = execute_comparison_query(&session, case).await;
            if case["retained_difference"] == "known_null_right_623" {
                // #623 separately handles a statically NULL right input.
                assert_eq!(case["id"], "null_right_known_bad");
                assert_eq!(case["ansi"], true);
                assert_eq!(expected["rows"], json!([["bad"]]));
                assert!(actual.is_err_and(|error| {
                    error
                        .to_string()
                        .contains("Cannot cast string 'bad' to value of Int64 type")
                }));
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
                Err(error) if expected["condition"] == "CAST_INVALID_INPUT" => {
                    let message = error.to_string();
                    message.contains("[CAST_INVALID_INPUT]")
                        || (case["id"] == "bad_left_null_right"
                            && message.contains("Cannot cast string 'bad' to value of Int32 type"))
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
    register_temporal_input(&native, &temporal_input)?;
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

#[tokio::test]
async fn nanosecond_columns_keep_existing_comparison_behavior() -> TestResult {
    use arrow::array::Int64Array;
    use arrow::datatypes::Int64Type;
    // #627 owns mixed-unit precision; this slice preserves the previous comparisons.
    for (left_unit, right_unit, left_values, right_values, expected_equal) in [
        (
            TimeUnit::Nanosecond,
            TimeUnit::Nanosecond,
            vec![Some(1), Some(1001), None],
            vec![Some(2), Some(1001), None],
            vec![Some(false), Some(true), None],
        ),
        (
            TimeUnit::Nanosecond,
            TimeUnit::Microsecond,
            vec![Some(1), Some(1001), None],
            vec![Some(0), Some(1), None],
            vec![Some(true), Some(true), None],
        ),
        (
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
            vec![Some(0), Some(1), None],
            vec![Some(1), Some(1001), None],
            vec![Some(true), Some(true), None],
        ),
    ] {
        for (left_zone, right_zone) in [
            (None, None),
            (Some("UTC"), Some("UTC")),
            (Some("UTC"), Some("America/Phoenix")),
            (None, Some("UTC")),
        ] {
            let left_type = DataType::Timestamp(left_unit, left_zone.map(Arc::<str>::from));
            let right_type = DataType::Timestamp(right_unit, right_zone.map(Arc::<str>::from));
            let left = arrow::compute::cast(&Int64Array::from(left_values.clone()), &left_type)?;
            let right = arrow::compute::cast(&Int64Array::from(right_values.clone()), &right_type)?;
            let batch = RecordBatch::try_from_iter([("a", left), ("b", right)])?;
            for ansi_mode in [true, false] {
                let session = SessionContext::new_with_state(
                    configure_spark_sql(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(SessionConfig::new().with_target_partitions(1)),
                    )
                    .build(),
                );
                session.register_batch("nanos", batch.clone())?;
                let query = plan_query(
                    &session,
                    "SELECT a=b AS equal, NULLIF(a,b) AS nullif_value FROM nanos",
                    &SparkSqlOptions {
                        ansi_mode,
                        session_timezone: "America/Phoenix".into(),
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(query.logical_plan.schema().field(1).data_type(), &left_type);
                let output = collect(
                    query.create_physical_plan(&session).await?,
                    session.task_ctx(),
                )
                .await?;
                let actual_equal = output
                    .iter()
                    .flat_map(|b| b.column(0).as_boolean().iter())
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_equal, expected_equal,
                    "{left_type} vs {right_type}, ANSI={ansi_mode}"
                );
                let mut original = Vec::new();
                for batch in output {
                    assert_eq!(batch.column(1).data_type(), &left_type);
                    let ticks = arrow::compute::cast(batch.column(1), &DataType::Int64)?;
                    original.extend(ticks.as_primitive::<Int64Type>().iter());
                }
                let expected = left_values
                    .iter()
                    .zip(&expected_equal)
                    .map(|(value, equal)| if *equal == Some(true) { None } else { *value })
                    .collect::<Vec<_>>();
                assert_eq!(original, expected);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn nanosecond_fallback_keeps_ambiguous_timezone_errors() -> TestResult {
    use arrow::array::TimestampNanosecondArray;

    // The existing Arrow cast rejects NY's missing or repeated local time.
    for (local_ns, instant_us) in [
        (1_772_937_000_000_000_000, 1_772_955_000_000_000), // 2026-03-08 02:30
        (1_793_496_600_000_000_000, 1_793_511_000_000_000), // 2026-11-01 01:30
    ] {
        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(TimestampNanosecondArray::from(vec![local_ns])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![instant_us])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
            ),
        ])?;
        for ansi_mode in [true, false] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
            );
            session.register_batch("nanos", batch.clone())?;
            // Separate executions prevent one failing expression from hiding another.
            for expression in ["a=b", "b=a", "NULLIF(a,b)", "NULLIF(b,a)"] {
                let result: TestResult<_> = async {
                    let query = plan_query(
                        &session,
                        &format!("SELECT {expression} AS r FROM nanos"),
                        &SparkSqlOptions {
                            ansi_mode,
                            session_timezone: "America/New_York".into(),
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(collect(
                        query.create_physical_plan(&session).await?,
                        session.task_ctx(),
                    )
                    .await?)
                }
                .await;
                assert!(
                    result.as_ref().is_err_and(|error| error
                        .to_string()
                        .contains("Cannot cast timezone to different timezone")),
                    "{expression}, local={local_ns}, ANSI={ansi_mode}: {result:?}"
                );
            }
        }
    }
    Ok(())
}
