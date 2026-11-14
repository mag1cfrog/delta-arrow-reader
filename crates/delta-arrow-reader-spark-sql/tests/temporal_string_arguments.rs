//! Shared STRING date/time coercion (#942) and timestamp trim/fractions (#943).

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, Float64Type, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{
    DefaultTimestampType, SparkSqlOptions, configure_spark_sql, plan_query,
};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, validate_batch};

fn references() -> TestResult<(Value, Vec<Value>)> {
    let mut cases = Vec::new();
    let mut metadata = Value::Null;
    for (source, captured, count) in [
        (
            include_str!("fixtures/temporal-string-arguments/requests.json"),
            include_str!("fixtures/temporal-string-arguments/spark.jsonl"),
            52,
        ),
        (
            include_str!("fixtures/temporal-string-arguments/addendum-requests.json"),
            include_str!("fixtures/temporal-string-arguments/addendum-spark.jsonl"),
            8,
        ),
    ] {
        let request: Value = serde_json::from_str(source)?;
        let records = captured
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(
            records[0]["spark_build"]["SPARK_REVISION"]["value"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
        let definitions = corpora
            .iter()
            .map(|corpus| corpus["cases"].as_array().ok_or("missing cases"))
            .collect::<Result<Vec<_>, _>>()?;
        let definitions = definitions.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(definitions.len(), count);
        assert_eq!(records.len() - 1, count);
        for (case, definition) in records[1..].iter().zip(definitions) {
            for key in [
                "corpus",
                "id",
                "sql",
                "ansi",
                "timezone",
                "ordered",
                "issue_scope",
                "timestamp_type",
            ] {
                assert_eq!(case[key], definition[key], "source identity {key}");
            }
            assert_eq!(case["timestamp_type_observed"], case["timestamp_type"]);
            assert_eq!(case["expected"]["status"], "ok");
            assert!(case["schema_stages"]["executed"]["fields"].is_array());
        }
        for corpus in corpora {
            if metadata.is_null() {
                metadata = corpus["metadata"].clone();
            }
            assert_eq!(metadata, corpus["metadata"], "source input definitions");
        }
        cases.extend_from_slice(&records[1..]);
    }
    assert_eq!(
        cases
            .iter()
            .filter(|case| case["issue_scope"] == 942)
            .count(),
        38
    );
    assert_eq!(
        cases
            .iter()
            .filter(|case| case["issue_scope"] == 943)
            .count(),
        22
    );
    Ok((metadata, cases))
}

fn rows(schema: &arrow::datatypes::Schema, batches: &[RecordBatch]) -> TestResult<Vec<Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        validate_batch(schema, batch)?;
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|column| -> TestResult<Value> {
                    if column.is_null(row) {
                        return Ok(Value::Null);
                    }
                    let text = match column.data_type() {
                        DataType::Date32 => {
                            column.as_primitive::<Date32Type>().value(row).to_string()
                        }
                        DataType::Timestamp(TimeUnit::Microsecond, _) => column
                            .as_primitive::<TimestampMicrosecondType>()
                            .value(row)
                            .to_string(),
                        DataType::Float64 => format!(
                            "{:016x}",
                            column.as_primitive::<Float64Type>().value(row).to_bits()
                        ),
                        DataType::Int32
                        | DataType::Int64
                        | DataType::Boolean
                        | DataType::Decimal128(_, _) => {
                            arrow::util::display::array_value_to_string(column, row)?
                        }
                        other => return Err(format!("unexpected result type {other}").into()),
                    };
                    Ok(json!(text))
                })
                .collect::<TestResult<Vec<_>>>()?;
            rows.push(json!(values));
        }
    }
    Ok(rows)
}

async fn observe(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        session_timezone: case["timezone"].as_str().ok_or("missing timezone")?.into(),
        ansi_mode: case["ansi"].as_bool().ok_or("missing ansi")?,
        default_timestamp_type: match case["timestamp_type"].as_str() {
            Some("TIMESTAMP_LTZ") => DefaultTimestampType::TimestampLtz,
            Some("TIMESTAMP_NTZ") => DefaultTimestampType::TimestampNtz,
            _ => return Err("missing timestamp type".into()),
        },
        ..Default::default()
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    for field in physical.schema().fields() {
        if let DataType::Timestamp(_, Some(zone)) = field.data_type() {
            assert_eq!(zone.as_ref(), options.session_timezone.as_ref());
        }
    }
    let schema = capture_schema(physical.schema().fields(), None)?;
    let physical_schema = physical.schema();
    let batches = collect(physical, session.task_ctx()).await?;
    Ok(json!({"rows":rows(physical_schema.as_ref(), &batches)?, "executed":schema}))
}

/// #1305: projected IN subqueries retain SQL UNKNOWN rather than Spark's existence-join
/// FALSE. Only the NULL-sensitive cells differ; the raw Spark records remain unchanged.
fn expected_rows(case: &Value) -> TestResult<(Vec<Value>, bool)> {
    let mut rows = case["expected"]["rows"]
        .as_array()
        .ok_or("missing rows")?
        .clone();
    match case["id"].as_str() {
        Some("date_in_valid" | "date_in_legacy_malformed") => {
            let null_row = rows
                .iter_mut()
                .find(|row| row[0] == "5")
                .ok_or("missing NULL input")?;
            assert_eq!(null_row[4], "false", "raw Spark existence-join result");
            null_row[4] = Value::Null;
        }
        Some("date_in_subquery_output") => {
            let matched = if case["ansi"] == true {
                "true"
            } else {
                "false"
            };
            assert_eq!(rows, vec![json!([matched, "false"])]);
            rows[0][1] = Value::Null;
            if case["ansi"] == false {
                rows[0][0] = Value::Null;
            }
        }
        _ => return Ok((rows, false)),
    }
    Ok((rows, true))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_string_arguments_match_spark() -> TestResult {
    let (metadata, cases) = references()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features())
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                )
                .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session, &metadata, batch_size, 1,
        )?;
        for case in &cases {
            let actual = match observe(&session, case).await {
                Ok(actual) => actual,
                Err(error) => json!({"error":error.to_string()}),
            };
            let (mut expected_rows, retained_subquery_nulls) = expected_rows(case)?;
            println!(
                "temporal_string_observation={}",
                json!({
                    "id":case["id"], "issue_scope":case["issue_scope"],
                    "timezone":case["timezone"], "ansi":case["ansi"], "batch":batch_size,
                    "retained_subquery_nulls":retained_subquery_nulls, "actual":actual
                })
            );
            let types = |fields: &Value| -> TestResult<Vec<Value>> {
                Ok(fields
                    .as_array()
                    .ok_or("missing executed fields")?
                    .iter()
                    .map(|field| field["type"].clone())
                    .collect())
            };
            let mut actual_rows = actual["rows"].as_array().cloned().unwrap_or_default();
            if case["ordered"] == false {
                expected_rows.sort_by_key(Value::to_string);
                actual_rows.sort_by_key(Value::to_string);
            }
            if actual.get("error").is_some()
                || actual_rows != expected_rows
                || types(&actual["executed"]["fields"])?
                    != types(&case["schema_stages"]["executed"]["fields"])?
            {
                differences.push(json!({"id":case["id"], "timezone":case["timezone"],
                    "ansi":case["ansi"], "batch":batch_size, "actual":actual,
                    "expected":case["expected"],
                    "executed":case["schema_stages"]["executed"]}));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences: {}",
        differences.len(),
        json!(differences)
    );
    Ok(())
}

#[tokio::test]
async fn native_sql_keeps_its_timestamp_cast() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let batches = session
        .sql("SELECT CAST('1970-01-01t00:00:01' AS TIMESTAMP) AS value")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let column = batches[0].column(0);
    assert_eq!(
        column.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        column
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
            .value(0),
        1_000_000_000
    );
    Ok(())
}
