//! Decimal comparisons must use the cast value, including its rounding and NULLs.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde::Deserialize;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
struct ComparisonReferenceInput {
    sql_type: String,
    values: Vec<Option<String>>,
}

async fn register_comparison_inputs(
    session: &SessionContext,
    inputs: &Value,
    batch_size: usize,
) -> TestResult {
    for (name, input) in inputs.as_object().ok_or("missing reference inputs")? {
        let input: ComparisonReferenceInput = serde_json::from_value(input.clone())?;
        let padded = StringArray::from_iter(
            std::iter::once(Some("0")).chain(input.values.iter().map(Option::as_deref)),
        );
        let raw_name = format!("{name}_strings");
        session.register_batch(
            &raw_name,
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int32Array::from_iter_values(
                        -1..i32::try_from(input.values.len())?,
                    )) as ArrayRef,
                ),
                ("v", Arc::new(padded) as ArrayRef),
            ])?,
        )?;
        let batches = session
            .sql(&format!(
                "SELECT id,CAST(v AS {}) AS v FROM {raw_name}",
                input.sql_type
            ))
            .await?
            .collect()
            .await?;
        let schema = batches.first().ok_or("missing input batch")?.schema();
        let batch = arrow::compute::concat_batches(&schema, &batches)?.slice(1, input.values.len());
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|start| batch.slice(start, batch_size.min(batch.num_rows() - start)))
            .collect();
        session.register_table(
            name,
            Arc::new(MemTable::try_new(batch.schema(), vec![batches])?),
        )?;
    }
    Ok(())
}

fn boolean_rows(batches: &[RecordBatch]) -> Value {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    assert_eq!(column.data_type(), &DataType::Boolean);
                    if column.is_null(row) {
                        assert!(batch.schema().field(index).is_nullable());
                        Value::Null
                    } else {
                        Value::Bool(column.as_boolean().value(row))
                    }
                })
                .collect::<Vec<_>>();
            rows.push(values);
        }
    }
    json!(rows)
}

async fn spark_comparison_rows(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let plan = query.create_physical_plan(session).await?;
    assert!(
        plan.schema()
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Boolean)
    );
    if let Some(fields) = case["expected"]["schema"]["fields"].as_array() {
        assert_eq!(
            query.column_names,
            fields
                .iter()
                .map(|field| {
                    field["name"]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or("missing reference column name")
                })
                .collect::<Result<Vec<_>, _>>()?
        );
    }
    Ok(boolean_rows(&collect(plan, session.task_ctx()).await?))
}

fn matches_reference(case: &Value, actual: &TestResult<Value>) -> bool {
    let expected = &case["expected"];
    match actual {
        Ok(rows) => expected["status"] == "ok" && rows == &expected["rows"],
        Err(error) => {
            let error = error.to_string().to_lowercase();
            let cause = match (expected["condition"].as_str(), case["target"].as_str()) {
                (Some("CAST_OVERFLOW"), Some("TINYINT")) => "out of range int8",
                (Some("CAST_OVERFLOW"), Some("SMALLINT")) => "out of range int16",
                (Some("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"), Some("DECIMAL(2,0)")) => {
                    "too large to store in a decimal128 of precision 2"
                }
                (Some("NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"), Some("DECIMAL(10,2)")) => {
                    "too large to store in a decimal128 of precision 10"
                }
                _ => return false,
            };
            expected["status"] == "error" && error.contains(cause)
        }
    }
}

#[tokio::test]
async fn decimal_cast_comparisons_match_spark() -> TestResult {
    let records = include_str!("fixtures/decimal-cast-comparisons-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 153);
    let mut differences = Vec::new();
    let mut retained_overflows = 0;
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
        register_comparison_inputs(&session, &records[0]["inputs"], batch_size).await?;
        for case in &records[1..] {
            let expected = &case["expected"];
            let actual = spark_comparison_rows(&session, case).await;
            // #661: legacy narrowing fails even without a comparison rewrite.
            let retained_overflow = match (case["ansi"].as_bool(), case["id"].as_str()) {
                (Some(false), Some("overflow_TINYINT_CAST_False")) => {
                    Some(("TINYINT", "value of 128 is out of range Int8"))
                }
                (Some(false), Some("overflow_SMALLINT_CAST_False")) => {
                    Some(("SMALLINT", "value of 100000 is out of range Int16"))
                }
                _ => None,
            };
            if let Some((target, cause)) = retained_overflow {
                retained_overflows += 1;
                assert_eq!(expected["status"], "ok");
                assert!(
                    actual
                        .expect_err("legacy cast gap changed")
                        .to_string()
                        .contains(cause)
                );
                let query = plan_query(
                    &session,
                    &format!("SELECT CAST(v AS {target}) AS value FROM overflow"),
                    &SparkSqlOptions {
                        ansi_mode: false,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let plan = query.create_physical_plan(&session).await?;
                let error = collect(plan, session.task_ctx())
                    .await
                    .expect_err("direct cast gap changed");
                assert!(error.to_string().contains(cause), "{error}");
            } else if !matches_reference(case, &actual) {
                differences.push(format!(
                    "Spark {} ANSI={} batch={batch_size}: expected={expected}; actual={actual:?}",
                    case["id"], case["ansi"]
                ));
            }
            if case["ansi"] == true {
                let native: TestResult<Value> = async {
                    let frame = session
                        .sql(case["sql"].as_str().ok_or("missing SQL")?)
                        .await?;
                    Ok(boolean_rows(&frame.collect().await?))
                }
                .await;
                if !matches_reference(case, &native) {
                    differences.push(format!(
                        "Native {} batch={batch_size}: expected={expected}; actual={native:?}",
                        case["id"]
                    ));
                }
            }
        }
    }
    assert_eq!(retained_overflows, 4);
    assert!(
        differences.is_empty(),
        "{} comparison differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
