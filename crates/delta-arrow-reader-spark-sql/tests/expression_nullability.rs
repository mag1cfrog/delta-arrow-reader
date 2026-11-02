//! Expression and VALUES declarations preserve Spark schemas and required evaluation.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, AsArray, StringArray};
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
async fn numeric_decimal_casts_match_spark_schemas_and_errors() -> TestResult {
    check_spark_reference(
        include_str!("fixtures/numeric-decimal-cast-spark.jsonl"),
        582,
        &[],
    )
    .await
}

#[tokio::test]
async fn integer_div_matches_spark_schemas_and_errors() -> TestResult {
    check_spark_reference(
        include_str!("fixtures/integer-div-nullability-spark.jsonl"),
        218,
        &[],
    )
    .await
}

#[tokio::test]
async fn values_preserve_input_guarantees_and_recorded_gaps() -> TestResult {
    let gaps = include_str!("fixtures/values-nullability-gaps.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(gaps.len(), 11);
    check_spark_reference(
        include_str!("fixtures/values-nullability-spark.jsonl"),
        122,
        &gaps,
    )
    .await
}

#[tokio::test]
async fn arithmetic_matches_spark_schemas_and_errors() -> TestResult {
    check_spark_reference(
        include_str!("fixtures/arithmetic-nullability-spark.jsonl"),
        358,
        &[],
    )
    .await
}

async fn check_spark_reference(fixture: &str, count: usize, retained_gaps: &[Value]) -> TestResult {
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, count);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    let mut checked_gaps = BTreeSet::new();
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
        if let Some(inputs) = records[0].get("inputs") {
            register_numeric_inputs(&session, inputs)?;
        }
        for case in &records[1..] {
            let actual = capture_nullability_query(&session, case)
                .await
                .unwrap_or_else(|error| {
                    let message = error.to_string();
                    if ((message.contains("Overflow") || message.contains("overflow"))
                        && (message.contains("Decimal") || message.contains("decimal")))
                        || message.contains("too large to store in a Decimal")
                    {
                        json!({"condition":"NUMERIC_VALUE_OUT_OF_RANGE"})
                    } else if message.contains("[REMAINDER_BY_ZERO]") {
                        json!({"condition":"REMAINDER_BY_ZERO"})
                    } else if message.contains("Divide by zero") {
                        json!({"condition":"DIVIDE_BY_ZERO"})
                    } else if message.contains("Overflow happened on") {
                        json!({"condition":"ARITHMETIC_OVERFLOW"})
                    } else if message.ends_with("Execution error: dead") {
                        json!({"condition":"USER_RAISED_EXCEPTION"})
                    } else if message.contains("[CAST_INVALID_INPUT]")
                        || message.contains("Cannot cast string 'bad' to value of Int32 type")
                    {
                        json!({"condition":"INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"})
                    } else {
                        json!({"unexpected_error":message})
                    }
                });
            let expected = if let Some(gap) = retained_gaps
                .iter()
                .find(|gap| gap["id"] == case["id"] && gap["ansi"] == case["ansi"])
            {
                assert_ne!(
                    gap["actual"], case["expected"],
                    "resolved gaps must leave this ledger"
                );
                assert!(matches!(gap["issue"].as_u64(), Some(142 | 721 | 722)));
                checked_gaps.insert((
                    gap["id"].as_str().ok_or("missing gap ID")?,
                    gap["ansi"].as_bool().ok_or("missing gap mode")?,
                ));
                &gap["actual"]
            } else {
                &case["expected"]
            };
            if actual != *expected {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["ansi"], expected
                ));
            }
        }
    }
    assert_eq!(checked_gaps.len(), retained_gaps.len());
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn capture_nullability_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
    let fields = query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let physical = schema.field(index);
            assert_eq!(physical.name(), &query.column_names[index]);
            assert_eq!(physical.data_type(), field.data_type());
            assert_eq!(physical.metadata(), field.metadata());
            // Constant folding can narrow physical nullability, as it does in Spark.
            assert!(!physical.is_nullable() || field.is_nullable());
            let data_type = match field.data_type() {
                DataType::Int8 => "byte".to_owned(),
                DataType::Int16 => "short".to_owned(),
                DataType::Int32 => "integer".to_owned(),
                DataType::Int64 => "long".to_owned(),
                DataType::Boolean => "boolean".to_owned(),
                DataType::Float32 => "float".to_owned(),
                DataType::Float64 => "double".to_owned(),
                DataType::Utf8 => "string".to_owned(),
                DataType::Null => "void".to_owned(),
                DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
                other => return Err(format!("unexpected result type: {other}").into()),
            };
            Ok(json!({"name":physical.name(),"type":data_type,
                "nullable":field.is_nullable(),"metadata":field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for (index, column) in batch.columns().iter().enumerate() {
            assert!(schema.field(index).is_nullable() || column.logical_null_count() == 0);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        Ok(
                            if column
                                .logical_nulls()
                                .is_some_and(|nulls| nulls.is_null(row))
                            {
                                Value::Null
                            } else {
                                json!(match column.data_type() {
                                    DataType::Float32 => {
                                        let value = column.as_primitive::<Float32Type>().value(row);
                                        if value.is_nan() {
                                            "NaN".into()
                                        } else {
                                            format!("{:08x}", value.to_bits())
                                        }
                                    }
                                    DataType::Float64 => {
                                        let value = column.as_primitive::<Float64Type>().value(row);
                                        if value.is_nan() {
                                            "NaN".into()
                                        } else {
                                            format!("{:016x}", value.to_bits())
                                        }
                                    }
                                    _ => array_value_to_string(column.as_ref(), row)?,
                                })
                            },
                        )
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}

fn register_numeric_inputs(session: &SessionContext, inputs: &Value) -> TestResult {
    for input in inputs.as_array().ok_or("missing inputs")? {
        let data_type = match input["type"].as_str().ok_or("missing type")? {
            "byte" => DataType::Int8,
            "short" => DataType::Int16,
            "integer" => DataType::Int32,
            "long" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "decimal(8,2)" => DataType::Decimal128(8, 2),
            other => return Err(format!("unexpected fixture type: {other}").into()),
        };
        let values = StringArray::from(
            input["values"]
                .as_array()
                .ok_or("missing values")?
                .iter()
                .map(Value::as_str)
                .collect::<Vec<_>>(),
        );
        let array = arrow::compute::cast(&values, &data_type)?;
        let metadata = input
            .get("metadata")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        let field = Field::new(
            "v",
            data_type,
            input["nullable"].as_bool().ok_or("missing nullable")?,
        )
        .with_metadata(metadata);
        assert!(field.is_nullable() || array.logical_null_count() == 0);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
        session.register_table(
            input["name"].as_str().ok_or("missing name")?,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn configured_session_preserves_native_numeric_and_values_contracts() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT CAST(1 AS DECIMAL(2,0)) AS r",
        "SELECT TRY_CAST(99999 AS DECIMAL(2,0)) AS r",
        "SELECT CAST(1.25 AS DECIMAL(10,1)) AS r",
        "SELECT * FROM (VALUES(1),(2)) t(v)",
        "SELECT * FROM (VALUES(1),(NULL)) t(v)",
        "SELECT COALESCE(v,7/0) AS r FROM (VALUES(0),(1)) t(v) LIMIT 0",
        "SELECT v % 2 AS r FROM (VALUES(1),(2),(NULL)) t(v)",
        "SELECT CAST(v AS DECIMAL(8,2)) % 2 AS r FROM (VALUES(1),(2),(NULL)) t(v)",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(expected.schema(), actual.schema(), "{sql}");
        assert_eq!(expected.collect().await?, actual.collect().await?, "{sql}");
    }
    Ok(())
}
