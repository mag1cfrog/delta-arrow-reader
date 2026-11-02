//! Spark setup preserves native scalar-subquery names, schemas, rows and errors.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::json;

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

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
    let session = configured_session();
    let expected = json!({"schema":{"type":"struct","fields":[
        {"name":"r","type":"integer","nullable":true,"metadata":{}}
    ]},"rows":[[null]]});
    for ansi in [true, false] {
        for sql in [
            "SELECT NULL + (SELECT 1) AS r",
            "SELECT (SELECT 1) + NULL AS r",
            "SELECT NULL + (SELECT CAST('bad' AS INT)) AS r",
            "SELECT (SELECT CAST('bad' AS INT)) + NULL AS r",
            "SELECT (SELECT CAST(NULL AS INT)) AS r",
        ] {
            let actual =
                capture_spark_query_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            assert_eq!(actual, expected, "ANSI={ansi}: {sql}");
        }
        for sql in [
            "SELECT (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) AS r",
            "SELECT NULL + (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) AS r",
            "SELECT (SELECT CAST(v AS INT) FROM VALUES ('bad') t(v) LIMIT 0) + NULL AS r",
        ] {
            let actual =
                capture_spark_query_result(&session, &json!({"sql":sql,"ansi":ansi})).await;
            if ansi {
                let error = actual.expect_err("local scalar input must retain its CAST error");
                assert!(
                    error
                        .to_string()
                        .contains("Cannot cast string 'bad' to value of Int32 type"),
                    "{sql}: {error}"
                );
            } else {
                assert_eq!(actual?, expected, "ANSI={ansi}: {sql}");
            }
        }
    }
    Ok(())
}
