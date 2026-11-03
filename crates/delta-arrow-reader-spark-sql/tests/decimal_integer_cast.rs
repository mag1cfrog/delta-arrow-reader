//! Decimal-to-integer CAST declarations keep Spark fields and native conversion.

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

const DIRECT_REFERENCE: &str =
    include_str!("fixtures/decimal-integer-cast/decimal-integer-cast-spark.jsonl");
const INTERACTION_REFERENCE: &str =
    include_str!("fixtures/decimal-integer-cast/interactions/interactions-spark.jsonl");

fn references(fixture: &str) -> TestResult<Vec<Value>> {
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 14);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(
                    case["schema_stages"][stage].is_object(),
                    "missing {stage}: {case}"
                );
            }
            assert!(case["expected"]["rows"].is_array());
        }
    }
    Ok(records)
}

#[tokio::test]
async fn decimal_integer_cast_declarations_match_spark() -> TestResult {
    let records = references(DIRECT_REFERENCE)?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("DECIMAL_INTEGER_CAST_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 28);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn decimal_integer_cast_interactions_match_spark() -> TestResult {
    let records = references(INTERACTION_REFERENCE)?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("DECIMAL_CAST_INTERACTION_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 28);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_decimal_integer_casts_keep_plain_results() -> TestResult {
    let records = references(DIRECT_REFERENCE)?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(
                &configured,
                "SELECT CAST(v AS INT) AS r FROM decimal_nullable",
                &SparkSqlOptions::default(),
            )
            .await?;
            collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
        }
        for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
            let sql = format!("SELECT CAST(v AS {target}) AS r FROM decimal_nullable ORDER BY v");
            let expected = native.sql(&sql).await?;
            let actual = configured.sql(&sql).await?;
            assert_eq!(
                actual.schema(),
                expected.schema(),
                "{sql}, after Spark={after_spark}"
            );
            let expected = expected.create_physical_plan().await?;
            let actual = actual.create_physical_plan().await?;
            assert_eq!(
                actual.schema(),
                expected.schema(),
                "{sql}, after Spark={after_spark}"
            );
            assert_eq!(
                collect(actual, configured.task_ctx()).await?,
                collect(expected, native.task_ctx()).await?,
                "{sql}, after Spark={after_spark}"
            );
        }
    }
    Ok(())
}
