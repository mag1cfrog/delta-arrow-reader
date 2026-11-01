//! Spark references for conditional result types, chosen values and input errors.

use std::error::Error;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, StringArray};
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

async fn execute_reference_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().ok_or("missing timezone")?.into(),
        ..SparkSqlOptions::default()
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await?;
    if case["expected"]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
        return Err("invalid CASE condition reached physical planning".into());
    }
    if case["expected"]["condition"] == "DATATYPE_MISMATCH.DATA_DIFF_TYPES" {
        return Err("incompatible result inputs reached physical planning".into());
    }
    if case["expected"]["condition"]
        == "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN"
    {
        return Err("invalid scalar subquery reached physical planning".into());
    }
    let logical_nullable = query.logical_plan.schema().field(0).is_nullable();
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    assert_eq!(schema.field(0).name(), "r");
    if case["float_encoding"] == "ieee754_bits"
        || matches!(
            schema.field(0).data_type(),
            DataType::Date32 | DataType::Timestamp(..)
        )
    {
        let logical_type = query.logical_plan.schema().field(0).data_type();
        let physical_type = schema.field(0).data_type();
        if logical_type != physical_type {
            return Err(format!(
                "logical type {logical_type:?} differs from physical {physical_type:?}"
            )
            .into());
        }
    }
    let result_type = match schema.field(0).data_type() {
        DataType::Date32 => "date",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz",
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => {
            assert_eq!(timezone, &options.session_timezone);
            "timestamp"
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string",
        DataType::Int8 => "byte",
        DataType::Int16 => "short",
        DataType::Int32 => "integer",
        DataType::Int64 => "long",
        DataType::Boolean => "boolean",
        DataType::Binary => "binary",
        DataType::Null => "void",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Decimal128(_, _) => "decimal",
        other => return Err(format!("unexpected result type: {other:?}").into()),
    };
    let result_type = match schema.field(0).data_type() {
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        _ => result_type.to_string(),
    };
    let batches = collect(plan, session.task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        let nulls = column.logical_nulls();
        for row in 0..batch.num_rows() {
            let value = if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                assert!(logical_nullable && schema.field(0).is_nullable());
                Value::Null
            } else if column.data_type() == &DataType::Binary {
                column
                    .as_binary::<i32>()
                    .value(row)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
                    .into()
            } else if matches!(column.data_type(), DataType::Timestamp(_, _)) {
                column
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row)
                    .to_string()
                    .into()
            } else if case["float_encoding"] == "ieee754_bits" && column.data_type().is_floating() {
                match column.data_type() {
                    DataType::Float32 => {
                        let value = column.as_primitive::<Float32Type>().value(row);
                        if value.is_nan() {
                            "nan".into()
                        } else {
                            format!("{:08x}", value.to_bits()).into()
                        }
                    }
                    DataType::Float64 => {
                        let value = column.as_primitive::<Float64Type>().value(row);
                        if value.is_nan() {
                            "nan".into()
                        } else {
                            format!("{:016x}", value.to_bits()).into()
                        }
                    }
                    _ => unreachable!(),
                }
            } else {
                array_value_to_string(column.as_ref(), row)?.into()
            };
            rows.push(vec![value]);
        }
    }
    Ok(json!({"type": result_type, "rows": rows, "names": query.column_names}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_conditional_results_and_admission() -> TestResult {
    let mut records = include_str!("fixtures/conditional-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 1383);
    let numeric = include_str!("fixtures/conditional-numeric-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(numeric.len(), 1233);
    assert_eq!(numeric[0]["spark_version"], "4.2.0");
    assert_eq!(numeric[0]["retain_fraction_digits_first"], "false");
    let mut registered_inputs = numeric[0]["registered_inputs"]
        .as_object()
        .ok_or("missing numeric registered inputs")?
        .clone();
    records.extend(numeric.into_iter().skip(1));
    let strings = include_str!("fixtures/conditional-strings-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(strings.len(), 963);
    assert_eq!(strings[0]["spark_version"], "4.2.0");
    registered_inputs.extend(
        strings[0]["registered_inputs"]
            .as_object()
            .ok_or("missing STRING registered inputs")?
            .clone(),
    );
    records.extend(strings.into_iter().skip(1));
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
        for (name, input) in &registered_inputs {
            let rows: Vec<(i32, Option<String>, Option<String>)> =
                serde_json::from_value(input["rows"].clone())?;
            let raw_table = format!("{name}_strings");
            session.register_batch(
                &raw_table,
                RecordBatch::try_from_iter([
                    (
                        "k",
                        Arc::new(Int32Array::from(
                            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                    (
                        "l",
                        Arc::new(StringArray::from(
                            rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                    (
                        "r",
                        Arc::new(StringArray::from(
                            rows.iter().map(|r| r.2.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                ])?,
            )?;
            let cast_registered_input = |column: &str, data_type: &str| {
                if data_type == "BINARY" {
                    format!("arrow_cast({column}, 'Binary')")
                } else {
                    format!("CAST({column} AS {data_type})")
                }
            };
            let frame = session
                .sql(&format!(
                    "SELECT k,{} AS a,{} AS b FROM {raw_table}",
                    cast_registered_input(
                        "l",
                        input["left_type"].as_str().ok_or("missing left type")?
                    ),
                    cast_registered_input(
                        "r",
                        input["right_type"].as_str().ok_or("missing right type")?
                    ),
                ))
                .await?;
            session.register_table(name, frame.into_view())?;
        }
        for case in &records[1..] {
            let actual = execute_reference_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    actual["type"] == expected["schema"]["fields"][0]["type"]
                        && actual["rows"] == expected["rows"]
                        && actual["names"] == json!(["r"])
                }
                Err(error) if expected["status"] == "error" => {
                    match expected["condition"].as_str() {
                        Some(
                            "CAST_INVALID_INPUT"
                            | "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
                        ) => {
                            let error = error.to_string();
                            error.contains("[CAST_INVALID_INPUT]")
                            || (case["float_encoding"] == "ieee754_bits" && error.contains("Cast error: Cannot cast string 'bad' to value of Int32 type"))
                            || (case["id"].as_str().is_some_and(|id| id.contains("_INT_required_error") || id.contains("_integer_text_"))
                                && error.contains("Cast error: Cannot cast string") && error.contains("to value of Int64 type"))
                            || (case["id"] == "replay_decimal_division_after_case" && error.contains("Cannot cast Some(\"bad\") to DECIMAL(10,2)"))
                            || ((case["id"] == "case_live_cast" || case["id"].as_str().is_some_and(|id| id.starts_with("eval_")))
                                && error.contains("Cast error: Cannot cast string 'bad' to value of Int32 type"))
                            || (case["id"]
                                .as_str()
                                .is_some_and(|id| id.contains("_timestamp"))
                                && error.starts_with(
                                    "Execution error: error in SQL parser: found b at 0:1",
                                ))
                        }
                        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
                            error.to_string().contains("two values expected")
                        }
                        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => error
                            .to_string()
                            .contains("CASE WHEN condition must be BOOLEAN"),
                        Some("DATATYPE_MISMATCH.DATA_DIFF_TYPES") => error
                            .to_string()
                            .contains("Incompatible STRING result types"),
                        Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => error
                            .to_string()
                            .contains("Scalar subquery returned more than one row"),
                        Some(
                            "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
                        ) => error
                            .to_string()
                            .contains("Scalar subquery must return exactly one column"),
                        Some("DIVIDE_BY_ZERO") => {
                            error.to_string().contains("[DIVIDE_BY_ZERO]")
                                || error
                                    .to_string()
                                    .contains("Arrow error: Divide by zero error")
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matches {
                differences.push(format!("{} ANSI={} timezone={} batch={batch_size}: expected {expected}, got {actual:?}", case["id"], case["ansi"], case["timezone"]));
            }
        }
        let native = session
            .sql("SELECT NVL(CAST(1.23 AS DECIMAL(10,2)),CAST(2.45 AS DECIMAL(10,2))) AS d, COALESCE(CAST(NULL AS FLOAT),CAST(16777217 AS INT)) AS f, NVL(CAST(NULL AS INT),'bad') AS s")
            .await?
            .collect()
            .await?;
        assert_eq!(native.len(), 1);
        assert_eq!(native[0].num_rows(), 1);
        assert_eq!(native[0].column(0).data_type(), &DataType::Float64);
        assert_eq!(native[0].column(1).data_type(), &DataType::Float32);
        assert!(!native[0].column(0).is_null(0));
        assert!(!native[0].column(1).is_null(0));
        assert_eq!(
            native[0].column(0).as_primitive::<Float64Type>().value(0),
            1.23
        );
        assert_eq!(
            native[0].column(1).as_primitive::<Float32Type>().value(0),
            16_777_216.0
        );
        assert!(native[0].column(2).data_type().is_string());
        assert!(!native[0].column(2).is_null(0));
        assert_eq!(
            array_value_to_string(native[0].column(2).as_ref(), 0)?,
            "bad"
        );
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .take(30)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
