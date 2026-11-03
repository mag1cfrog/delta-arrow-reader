//! Spark setup preserves native scalar-subquery names, schemas, rows and errors.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, capture_spark_query_result};

fn configured_session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
}

async fn capture_native(
    session: &SessionContext,
    sql: &str,
) -> Result<(DFSchema, SchemaRef, Vec<RecordBatch>)> {
    let frame = session.sql(sql).await?;
    let logical_schema = frame.schema().clone();
    let physical = frame.create_physical_plan().await?;
    let physical_schema = physical.schema();
    let batches = collect(physical, session.task_ctx()).await?;
    Ok((logical_schema, physical_schema, batches))
}

#[tokio::test]
async fn spark_setup_preserves_native_scalar_names_schemas_and_rows() -> TestResult {
    let native = SessionContext::new();
    let configured = configured_session();
    for (sql, name, value) in [
        ("SELECT NULL + (SELECT 1)", "NULL + Int64(1)", None),
        ("SELECT (SELECT 1) + NULL", "Int64(1) + NULL", None),
        ("SELECT NULL + (SELECT 1) AS value", "value", None),
        ("SELECT (SELECT 1) + NULL AS value", "value", None),
        ("SELECT (SELECT 1)", "Int64(1)", Some(1)),
        ("SELECT (SELECT 1) AS value", "value", Some(1)),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        let expected = (
            DFSchema::try_from(schema.as_ref().clone())?,
            schema.clone(),
            vec![RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![value]))],
            )?],
        );
        let plain = capture_native(&native, sql).await?;
        assert_eq!(plain, expected, "plain session: {sql}");
        let actual = capture_native(&configured, sql).await?;
        assert_eq!(actual, plain, "Spark-configured native session: {sql}");
    }
    Ok(())
}

#[tokio::test]
async fn spark_setup_preserves_native_scalar_required_errors() -> TestResult {
    let native = SessionContext::new();
    let configured = configured_session();
    for (sql, cause) in [
        (
            "SELECT (SELECT CAST('bad' AS BIGINT))",
            "Cannot cast string 'bad' to value of Int64 type",
        ),
        (
            "SELECT (SELECT CAST(v AS BIGINT) FROM (VALUES ('bad')) t(v)) AS value",
            "Cannot cast string 'bad' to value of Int64 type",
        ),
        ("SELECT (SELECT 1 / 0)", "Divide by zero error"),
    ] {
        let plain = capture_native(&native, sql)
            .await
            .expect_err("plain native scalar must retain its required error");
        let actual = capture_native(&configured, sql)
            .await
            .expect_err("Spark-configured native scalar must retain its required error");
        assert!(plain.to_string().contains(cause), "{sql}: {plain}");
        assert_eq!(
            actual.find_root().to_string(),
            plain.find_root().to_string(),
            "{sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn null_discard_preparation_keeps_spark_scalar_guards() -> TestResult {
    let records = include_str!(
        "fixtures/correlated-scalar-metadata-stages/null-discard/null-discard-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 16);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    let expected = json!({"schema":{"type":"struct","fields":[
        {"name":"r","type":"integer","nullable":true,"metadata":{}}
    ]},"rows":[[null]]});
    let reference = |sql: &str, ansi: bool| {
        let case = records[1..]
            .iter()
            .find(|case| case["sql"] == sql && case["ansi"] == ansi)
            .expect("missing original null-discard SQL/ANSI reference");
        assert_eq!(case["public_schema"], expected["schema"]);
        assert_eq!(case["analyzed_schema"], expected["schema"]);
        if case.get("error").is_none() {
            assert_eq!(case["expected"]["schema"], expected["schema"]);
            assert_eq!(case["expected"]["rows"], expected["rows"]);
            assert_eq!(
                case["optimized_schema"],
                case["executed_schema_before_collect"]
            );
            assert_eq!(
                case["executed_schema_before_collect"],
                case["executed_schema_after_collect"]
            );
        }
        case
    };
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
        for ansi in [true, false] {
            for sql in [
                "SELECT NULL + (SELECT 1) AS r",
                "SELECT (SELECT 1) + NULL AS r",
                "SELECT NULL + (SELECT CAST('bad' AS INT)) AS r",
                "SELECT (SELECT CAST('bad' AS INT)) + NULL AS r",
                "SELECT (SELECT CAST(NULL AS INT)) AS r",
            ] {
                let actual = capture_spark_query_result(&session, reference(sql, ansi)).await?;
                assert_eq!(actual, expected, "ANSI={ansi} batch={batch_size}: {sql}");
            }
            for sql in [
                "SELECT (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) AS r",
                "SELECT NULL + (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) AS r",
                "SELECT (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) + NULL AS r",
            ] {
                let case = reference(sql, ansi);
                if ansi {
                    assert_eq!(case["error"]["condition"], "CAST_INVALID_INPUT");
                    let query = plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            session_timezone: "UTC".into(),
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    assert_eq!(
                        capture_schema(
                            query.logical_plan.schema().fields(),
                            Some(&query.column_names),
                        )?,
                        case["public_schema"],
                        "ANSI={ansi} batch={batch_size}: {sql}"
                    );
                    let error = query
                        .create_physical_plan(&session)
                        .await
                        .expect_err("local scalar input must retain its CAST error");
                    assert!(
                        matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
                            if matches!(cause.as_ref(), ArrowError::CastError(message)
                                if message == "Cannot cast string 'bad' to value of Int32 type")),
                        "ANSI={ansi} batch={batch_size}: {sql}: {error}"
                    );
                } else {
                    let actual = capture_spark_query_result(&session, case).await?;
                    assert_eq!(actual, expected, "ANSI={ansi} batch={batch_size}: {sql}");
                }
            }
        }
    }
    Ok(())
}
