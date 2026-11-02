//! Integer DIV retains Spark's infix names without changing its arithmetic.

use std::collections::BTreeSet;

use arrow::datatypes::{DataType, Fields};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{
    TestResult, capture_spark_query_result, encode_value, validate_batch, validate_physical_schema,
};

fn references() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/integer-div-names-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn recorded_schema(fields: &Fields, names: Option<&[String]>) -> TestResult<Value> {
    Ok(json!({"type":"struct", "fields":fields.iter().enumerate()
        .map(|(index, field)| {
            let data_type = match field.data_type() {
                DataType::Int64 => "long",
                DataType::Boolean => "boolean",
                other => return Err(format!("unexpected DIV result type: {other}").into()),
            };
            Ok(json!({"name":names.map_or(field.name(), |names| &names[index]),
                "type":data_type, "nullable":field.is_nullable(), "metadata":field.metadata()}))
        }).collect::<TestResult<Vec<_>>>()?}))
}

fn compare_scoped_fields(actual: &Value, expected: &Value) -> TestResult {
    let actual_fields = actual["fields"].as_array().ok_or("missing actual fields")?;
    let expected_fields = expected["fields"]
        .as_array()
        .ok_or("missing reference fields")?;
    if actual_fields.len() != expected_fields.len() {
        return Err(format!("field count differs: expected {expected}, got {actual}").into());
    }
    // #710 owns metadata parity; retain both complete schemas in the observation.
    for (actual_field, expected_field) in actual_fields.iter().zip(expected_fields) {
        for property in ["name", "type", "nullable"] {
            if actual_field[property] != expected_field[property] {
                return Err(
                    format!("field {property} differs: expected {expected}, got {actual}").into(),
                );
            }
        }
    }
    Ok(())
}

async fn compare_case(session: &SessionContext, case: &Value, batch_size: usize) -> TestResult {
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
    let public_schema = recorded_schema(
        query.logical_plan.schema().fields(),
        Some(&query.column_names),
    )?;
    eprintln!(
        "integer_div_names: {}",
        json!({"id":case["id"],"input":case["input"],"ansi":case["ansi"],
            "batch_size":batch_size,"public_schema":public_schema})
    );
    compare_scoped_fields(&public_schema, &case["schema_stages"]["public"])?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    validate_physical_schema(
        query.logical_plan.schema().fields(),
        &query.column_names,
        schema.fields(),
    )?;
    let physical_schema = recorded_schema(schema.fields(), None)?;
    eprintln!(
        "integer_div_names: {}",
        json!({"id":case["id"],"input":case["input"],"ansi":case["ansi"],
            "batch_size":batch_size,"physical_schema":physical_schema})
    );
    if let Some(expected) = case["schema_stages"].get("executed") {
        compare_scoped_fields(&physical_schema, expected)?;
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    eprintln!(
        "integer_div_names: {}",
        json!({"id":case["id"],"input":case["input"],"ansi":case["ansi"],
            "batch_size":batch_size,"rows":rows})
    );
    if json!(rows) != case["expected"]["rows"] {
        return Err(format!("rows differ: expected {}, got {rows:?}", case["expected"]).into());
    }
    Ok(())
}

#[tokio::test]
async fn integer_div_names_values_declarations_and_errors_match_spark() -> TestResult {
    let records = references()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(
        records[0]["capture_sha256"],
        "21c499ea6d693604fa8e1e923f052f364f9ec321530a381d67eba21b19c9a032"
    );
    assert_eq!(records.len() - 1, 166);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("condition").is_none() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["public"]);
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
        }
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
            let actual = compare_case(&session, case, batch_size).await;
            let matched = match (case["expected"]["condition"].as_str(), &actual) {
                (None, Ok(())) => true,
                (Some(condition), Err(error))
                    if error.downcast_ref::<DataFusionError>().is_some() =>
                {
                    let message = error.to_string();
                    match condition {
                        "DIVIDE_BY_ZERO" => message.contains("Divide by zero"),
                        "ARITHMETIC_OVERFLOW" => message.contains("Overflow happened on"),
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matched {
                differences.push(format!(
                    "{} input={} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["input"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn native_snapshot(
    session: &SessionContext,
    sql: &str,
) -> TestResult<(String, Vec<RecordBatch>)> {
    let frame = session.sql(sql).await?;
    let analyzed = frame.schema().clone();
    let physical = frame.create_physical_plan().await?;
    let schema = physical.schema();
    let declaration = format!(
        "{analyzed:?}\n{schema:?}\n{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    let batches = collect(physical, session.task_ctx()).await?;
    for batch in &batches {
        validate_batch(schema.as_ref(), batch)?;
    }
    Ok((declaration, batches))
}

#[tokio::test]
async fn spark_div_naming_keeps_native_datafusion_sql_unchanged() -> TestResult {
    let records = references()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 32)?;
    }
    for sql in [
        "SELECT 7 / 2, (7 / 2) / 2, 7 / 2 AS quotient",
        "SELECT a / b, ABS(a / b), a / b AS quotient FROM inputs",
        "SELECT a / b FROM null_inputs",
        "SELECT a / b FROM empty_inputs",
        "SELECT 7 / 0",
        "SELECT CAST(-9223372036854775808 AS BIGINT) / -1",
    ] {
        let expected = native_snapshot(&native, sql)
            .await
            .map_err(|error| error.to_string());
        let before = native_snapshot(&configured, sql)
            .await
            .map_err(|error| error.to_string());
        assert_eq!(before, expected, "{sql}");
        for ansi_mode in [true, false] {
            capture_spark_query_result(
                &configured,
                &json!({"ansi":ansi_mode,
                    "sql":"SELECT id DIV 2, (id DIV 2) DIV 2, id DIV 2 AS quotient FROM range(3)"}),
            )
            .await?;
            let after = native_snapshot(&configured, sql)
                .await
                .map_err(|error| error.to_string());
            assert_eq!(after, before, "{sql}, ANSI={ansi_mode}");
        }
    }
    Ok(())
}
