//! Independent Decimal integer CAST/TRY_CAST boundaries and native preservation.

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/decimal-integer-overflow/decimal-integer-overflow-spark.jsonl")
        .lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, 624);
    let mut identities = std::collections::BTreeSet::new();
    let mut errors = 0;
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            assert!(case["expected"]["rows"].is_array());
            for stage in ["analyzed", "optimized", "executed_before_collect", "executed"] {
                assert!(case["schema_stages"][stage].is_object(), "missing {stage}: {case}");
            }
        } else {
            errors += 1;
            assert_eq!(case["expected"]["condition"], "CAST_OVERFLOW");
            assert!(case["schema_stages"]["analyzed"].is_object());
            assert!(matches!(case["spark_error_phase"].as_str(), Some("optimized" | "collect")));
        }
    }
    assert_eq!(errors, 48);
    Ok(records)
}

#[tokio::test]
async fn decimal_integer_overflow_matches_pinned_spark_stages() -> TestResult {
    let records = references()?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            ),
        ).build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison = spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("DECIMAL_INTEGER_OVERFLOW_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 1248);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_decimal_integer_overflow_preserves_plain_results() -> TestResult {
    let records = references()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(&configured,
                "SELECT CAST(v AS INT) AS r FROM int_required WHERE id=4",
                &SparkSqlOptions::default(),
            ).await?;
            collect(query.create_physical_plan(&configured).await?, configured.task_ctx()).await?;
        }
        for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
            let table = target.to_ascii_lowercase();
            for sql in [
                format!("SELECT CAST(v AS {target}) AS r FROM {table}_required WHERE id=4"),
                format!("SELECT CAST(v AS {target}) AS r FROM {table}_required WHERE id=5"),
                format!("SELECT CAST(v AS {target}) AS r FROM {table}_required_empty"),
                format!("SELECT TRY_CAST(v AS {target}) AS r FROM {table}_nullable ORDER BY id"),
            ] {
                let expected = spark_results::capture_native_query_observation(&native, &sql).await?;
                let actual = spark_results::capture_native_query_observation(&configured, &sql).await?;
                eprintln!("DECIMAL_INTEGER_OVERFLOW_NATIVE_OBSERVATION {}", json!({
                    "sql":sql,"after_spark":after_spark,"plain":expected,"configured":actual,
                }));
                assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
            }
        }
    }
    Ok(())
}
