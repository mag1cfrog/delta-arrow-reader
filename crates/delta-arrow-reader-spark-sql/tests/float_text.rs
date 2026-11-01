//! Spark float text agrees across casts, nested values, CSV and XML.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn float_array_from_bits(sql_type: &str, bits: &[Option<u64>]) -> TestResult<ArrayRef> {
    match sql_type {
        "FLOAT" => {
            let values = bits
                .iter()
                .map(|value| {
                    value
                        .map(|bits| u32::try_from(bits).map(f32::from_bits))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Float32Array::from(values)))
        }
        "DOUBLE" => Ok(Arc::new(Float64Array::from(
            bits.iter()
                .map(|value| value.map(f64::from_bits))
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!("unexpected floating SQL type: {other}").into()),
    }
}

fn string_rows(batches: &[RecordBatch]) -> TestResult<Vec<Vec<Option<String>>>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ));
                values.push(if column.is_null(row) {
                    assert!(batch.schema().field(index).is_nullable());
                    None
                } else {
                    Some(array_value_to_string(column.as_ref(), row)?)
                });
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

async fn execute_spark_text(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
) -> TestResult<Vec<Vec<Option<String>>>> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let plan = query.create_physical_plan(session).await?;
    for (index, name) in query.column_names.iter().enumerate() {
        assert_eq!(plan.schema().field(index).name(), name);
        assert_eq!(plan.schema().field(index).data_type(), &DataType::Utf8);
    }
    string_rows(&collect(plan, session.task_ctx()).await?)
}

async fn assert_pivot_names_and_counts(session: &SessionContext, case: &Value) -> TestResult {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing pivot SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing pivot ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let names: Vec<_> = case["schema"]["fields"]
        .as_array()
        .ok_or("missing pivot schema")?
        .iter()
        .map(|field| field["name"].as_str().ok_or("missing pivot name"))
        .collect::<Result<_, _>>()?;
    assert_eq!(query.column_names, names);
    let plan = query.create_physical_plan(session).await?;
    for (index, name) in names.iter().enumerate() {
        assert_eq!(plan.schema().field(index).name().as_str(), *name);
        assert_eq!(plan.schema().field(index).data_type(), &DataType::Int64);
    }
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for column in batch.columns() {
                let column = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("pivot count is not BIGINT")?;
                values.push(if column.is_null(row) {
                    Value::Null
                } else {
                    json!(column.value(row))
                });
            }
            rows.push(values);
        }
    }
    assert_eq!(
        json!(rows),
        case["rows"],
        "{} pivot ANSI={}",
        case["sql_type"],
        case["ansi"]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_float_text_from_exact_bits() -> TestResult {
    let records = include_str!("fixtures/float-text-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["java_version"], "21.0.8");
    assert_eq!(records.len(), 447);
    let pivot_cases = records[0]["pivot_cases"]
        .as_array()
        .ok_or("missing pivot cases")?;
    assert_eq!(pivot_cases.len(), 4);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2);
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config.clone()),
            )
            .build(),
        );
        let native_session = SessionContext::new_with_config(config);
        for sql_type in ["FLOAT", "DOUBLE"] {
            let cases: Vec<_> = records[1..]
                .iter()
                .filter(|r| r["sql_type"] == sql_type)
                .collect();
            let raw_bits: Vec<Option<u64>> = cases
                .iter()
                .map(|r| {
                    r["bits"]
                        .as_str()
                        .map(|s| u64::from_str_radix(s, 16))
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            // The slice covers data and validity offsets, including signed zero and NULL.
            let padded_bits: Vec<_> = std::iter::once(Some(0))
                .chain(raw_bits)
                .chain([Some(0)])
                .collect();
            let values = float_array_from_bits(sql_type, &padded_bits)?.slice(1, cases.len());
            let input = RecordBatch::try_from_iter([
                (
                    "k",
                    Arc::new(Int32Array::from(
                        (0..cases.len() as i32).collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
                ("value", values),
            ])?;
            session.register_batch("float_inputs", input.clone())?;
            native_session.register_batch("float_inputs", input)?;
            let expected: Vec<Vec<Option<String>>> = cases
                .iter()
                .map(|r| {
                    ["text", "csv", "xml", "nested"]
                        .map(|key| r["expected"][key].as_str().map(str::to_string))
                        .to_vec()
                })
                .collect();
            let projection = "CAST(value AS STRING) AS text,TO_CSV(NAMED_STRUCT('v',value)) AS csv,TO_XML(NAMED_STRUCT('v',value)) AS xml,CAST(ARRAY(value) AS STRING) AS nested";
            for ansi_mode in [true, false] {
                for cast in ["CAST", "TRY_CAST"] {
                    let sql = format!(
                        "SELECT {} FROM float_inputs ORDER BY k",
                        projection.replacen("CAST(value", &format!("{cast}(value"), 1)
                    );
                    let actual = execute_spark_text(&session, &sql, ansi_mode).await?;
                    assert_eq!(actual.len(), expected.len());
                    for (index, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
                        if actual != expected {
                            differences.push(format!("{sql_type} {} {cast} ANSI={ansi_mode} batch={batch_size}: expected {expected:?}, got {actual:?}",cases[index]["bits"]));
                        }
                    }
                }
                assert!(
                    execute_spark_text(
                        &session,
                        &format!("SELECT {projection} FROM float_inputs WHERE false"),
                        ansi_mode
                    )
                    .await?
                    .is_empty()
                );
                for (literal, text) in [
                    ("1e10", "1.0E10"),
                    ("1e-8", "1.0E-8"),
                    ("-0.0", "-0.0"),
                    ("Infinity", "Infinity"),
                    ("NaN", "NaN"),
                ] {
                    let sql =
                        format!("SELECT CAST(CAST('{literal}' AS {sql_type}) AS STRING) AS r");
                    assert_eq!(
                        execute_spark_text(&session, &sql, ansi_mode).await?,
                        vec![vec![Some(text.to_string())]]
                    );
                }
            }
            let sql = "SELECT CAST(value AS VARCHAR) AS r FROM float_inputs ORDER BY k";
            let configured = session.sql(sql).await?.collect().await?;
            let native = native_session.sql(sql).await?.collect().await?;
            assert_eq!(configured[0].schema(), native[0].schema());
            assert_eq!(
                string_rows(&configured)?,
                string_rows(&native)?,
                "native DataFusion casts changed"
            );
            for case in pivot_cases
                .iter()
                .filter(|case| case["sql_type"] == sql_type)
            {
                let bits: Vec<Option<u64>> = case["bits"]
                    .as_array()
                    .ok_or("missing pivot input bits")?
                    .iter()
                    .map(|bits| -> TestResult<_> {
                        Ok(Some(u64::from_str_radix(
                            bits.as_str().ok_or("missing pivot input")?,
                            16,
                        )?))
                    })
                    .collect::<TestResult<_>>()?;
                session.register_batch(
                    "float_pivot_inputs",
                    RecordBatch::try_from_iter([(
                        "value",
                        float_array_from_bits(sql_type, &bits)?,
                    )])?,
                )?;
                assert_pivot_names_and_counts(&session, case).await?;
                session.deregister_table("float_pivot_inputs")?;
            }
            session.deregister_table("float_inputs")?;
            native_session.deregister_table("float_inputs")?;
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
