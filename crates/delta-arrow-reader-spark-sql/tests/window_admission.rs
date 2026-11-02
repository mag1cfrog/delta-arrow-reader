//! Spark window definitions must satisfy their function's ordering and frame requirements.

use std::collections::BTreeSet;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

#[tokio::test]
async fn window_ordering_frames_and_distinct_match_spark() -> TestResult {
    let records = include_str!("fixtures/window-admission-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 572);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(
            identities.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                case["empty"]
                    .as_bool()
                    .ok_or("missing empty-input marker")?,
            ))
        );
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
        for case in &records[1..] {
            let expected = &case["expected"];
            if let Some(condition) = expected["condition"].as_str() {
                let planned = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                let expected_cause = match condition {
                    "_LEGACY_ERROR_TEMP_1036" => "[WINDOW_FRAME_MISMATCH]",
                    "_LEGACY_ERROR_TEMP_1035" => "[WINDOW_OFFSET_FRAME_MISMATCH]",
                    "DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER" => {
                        "range window frame requires exactly one order by expression"
                    }
                    "WINDOW_FUNCTION_FRAME_NOT_ORDERED" => "[WINDOW_FUNCTION_FRAME_NOT_ORDERED]",
                    "DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER" => {
                        "[DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER]"
                    }
                    "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON" => {
                        "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON]"
                    }
                    "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED" => {
                        "[DISTINCT_WINDOW_FUNCTION_UNSUPPORTED]"
                    }
                    _ => return Err(format!("unclassified reference error: {condition}").into()),
                };
                match planned {
                    Err(error) if error.to_string().contains(expected_cause) => {}
                    other => {
                        differences.push(format!("{} ANSI={} empty={} batch={batch_size}: expected {condition} during planning, got {other:?}",case["id"],case["ansi"],case["empty"]));
                    }
                }
            } else {
                let actual = capture_spark_query_result(&session, case).await;
                if actual.as_ref().ok() != Some(expected) {
                    differences.push(format!("{} ANSI={} empty={} batch={batch_size}: expected {expected}, got {actual:?}",case["id"],case["ansi"],case["empty"]));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_window_rejections_leave_native_sql_unchanged() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT RANK() OVER () AS r FROM (VALUES(1),(2)) inputs(v)",
        "SELECT COUNT(DISTINCT v) OVER () AS n FROM (VALUES(1),(2),(2)) inputs(v)",
    ] {
        assert!(
            capture_spark_query_result(&configured, &json!({"sql":sql,"ansi":true}))
                .await
                .is_err()
        );
        let expected = native.sql(sql).await?.collect().await?;
        let actual = configured.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn invalid_offset_frames_do_not_evaluate_volatile_functions() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_offset",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))
        }),
    ));
    for ansi in [true, false] {
        let result = plan_query(
            &session,
            "SELECT LAG(v,observed_offset()) OVER (ORDER BY v ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) FROM VALUES(1) inputs(v)",
            &SparkSqlOptions { ansi_mode: ansi, ..SparkSqlOptions::default() },
        ).await;
        assert!(
            result.is_err_and(|error| error.to_string().contains("[WINDOW_OFFSET_FRAME_MISMATCH]"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
