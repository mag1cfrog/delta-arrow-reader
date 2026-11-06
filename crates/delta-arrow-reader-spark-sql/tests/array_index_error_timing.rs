//! ARRAY bounds errors retain Spark's preparation phase and conditional reachability.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

fn records(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

async fn replay(records: &[Value]) -> TestResult {
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("rows").is_some() {
            assert_eq!(case["public_schema"], case["expected"]["schema"]);
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some(), "{case}");
            }
            assert!(case["rows_raw"].is_array(), "{case}");
        } else {
            assert!(case["expected"]["condition"].is_string(), "{case}");
            assert!(case["spark_error_phase"].is_string(), "{case}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        // One input partition keeps competing provider failures deterministic.
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            batch_size,
            1,
        )?;
        for case in &records[1..] {
            let actual = spark_results::compare_exact_spark_query_stages_with_optimized(
                &session, case,
            )
            .await?;
            let cause_matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                spark_results::matches_spark_int32_cast_input(case, &actual)
            } else if let Some(condition) = case["expected"]["condition"].as_str() {
                let prefix = format!("[{condition}] ");
                let (_, cause) = case["spark_error"]
                    .as_str()
                    .ok_or("missing independent cause")?
                    .split_once(&prefix)
                    .ok_or("missing independent condition body")?;
                let (body, _) = cause
                    .split_once(" SQLSTATE:")
                    .ok_or("missing independent SQLSTATE boundary")?;
                actual["error"]
                    .as_str()
                    .is_some_and(|error| error.ends_with(&format!("{prefix}{body}")))
            } else {
                true
            };
            let raw_rows_match = match case.get("rows_raw") {
                Some(expected) => {
                    let mut expected = expected.as_array().ok_or("missing raw rows")?.clone();
                    expected.sort_by_key(Value::to_string);
                    actual["rows"] == json!(expected)
                }
                None => true,
            };
            eprintln!(
                "ARRAY_INDEX_TIMING_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"cause_matches":cause_matches,
                    "raw_rows_match":raw_rows_match})
            );
            if actual["matches"] != true
                || actual["optimized_matches"] != true
                || !cause_matches
                || !raw_rows_match
            {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: cause={cause_matches} raw_rows={raw_rows_match} {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn original_array_bounds_errors_and_agreeing_controls_keep_exact_stages() -> TestResult {
    let records = records(include_str!(
        "fixtures/array-index-error-timing/original-spark.jsonl"
    ))?;
    let provenance: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/original-provenance.json"
    ))?;
    assert_eq!(records.len() - 1, 16);
    assert_eq!(provenance["selected_observations"], 16);
    assert_eq!(
        provenance["source_sha256"],
        "bca7979a35a8eca98ae3f5340c87a8c364c184bc7698388357f8958ebe922c2d"
    );
    assert_eq!(
        records[1..]
            .iter()
            .filter(|case| case["expected"]["condition"] == "INVALID_ARRAY_INDEX")
            .count(),
        3
    );
    for id in provenance["selected_ids"].as_array().ok_or("missing original IDs")? {
        for ansi in [true, false] {
            assert!(records[1..].iter().any(|case| case["id"] == *id && case["ansi"] == ansi));
        }
    }
    replay(&records).await
}

#[tokio::test]
async fn captured_array_bounds_reachability_and_first_errors_match_each_stage() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/manifest.json"
    ))?;
    // Root captures this independent oracle before executing the unchanged runtime.
    let records = records(&std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/array-index-error-timing/supplement-spark.jsonl"
    ))?)?;
    assert_eq!(records.len() - 1, 108);
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    for definition in manifest["cases"].as_array().ok_or("missing definitions")? {
        for ansi in [true, false] {
            let case = records[1..]
                .iter()
                .find(|case| case["id"] == definition["id"] && case["ansi"] == ansi)
                .ok_or("missing captured query")?;
            for (field, expected) in definition.as_object().ok_or("invalid definition")? {
                assert_eq!(case[field], *expected, "{field}: {case}");
            }
        }
    }
    replay(&records).await
}

#[test]
fn ordinary_internal_and_user_raise_error_simplification_stays_deferred() -> TestResult {
    use std::sync::Arc;

    use datafusion::common::{ScalarValue, config::ConfigOptions};
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{ScalarUDFImpl, col, lit};
    use sail_function::scalar::misc::raise_error::RaiseError;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    for spark in [false, true] {
        let mut options = ConfigOptions::default();
        if spark {
            options.extensions.insert(SparkSqlExpressionSemantics::default());
        }
        let context = SimplifyContext::builder()
            .with_config_options(Arc::new(options))
            .build();
        for function in [RaiseError::new(), RaiseError::new_user_error()] {
            for message in [
                lit("[INVALID_ARRAY_INDEX] user supplied text"),
                lit(ScalarValue::LargeUtf8(Some("internal cause".into()))),
                lit(ScalarValue::Utf8View(Some("view cause".into()))),
                lit(ScalarValue::Utf8(None)),
                col("message"),
            ] {
                let args = vec![message];
                match function.simplify(args.clone(), &context)? {
                    ExprSimplifyResult::Original(actual) => assert_eq!(actual, args),
                    other => return Err(format!("generic error policy changed: {other:?}").into()),
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_array_bounds_keep_fields_values_and_stages_after_spark_planning() -> TestResult {
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    let metadata: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/manifest.json"
    ))?;
    for batch_size in [1, 32] {
        let native = SessionContext::new_with_config(
            SessionConfig::new().with_batch_size(batch_size).with_target_partitions(1),
        );
        let configured = session(batch_size);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs_with_partition_count(
                session, &metadata, batch_size, 1,
            )?;
        }
        for after_spark in [false, true] {
            if after_spark {
                let query = plan_query(
                    &configured,
                    "SELECT ARRAY(7)[0] AS r",
                    &SparkSqlOptions::default(),
                )
                .await?;
                query.create_physical_plan(&configured).await?;
            }
            for sql in [
                "SELECT ARRAY[7][0] AS r",
                "SELECT ARRAY[7][-1] AS r",
                "SELECT ARRAY[7][2] AS r",
                "SELECT CASE WHEN false THEN ARRAY[7][2] ELSE 2 END AS r",
                "SELECT id,a[1] AS r FROM bounds_rows ORDER BY id",
                "SELECT a[2] AS r FROM bounds_empty",
            ] {
                let expected = spark_results::capture_native_query_observation(&native, sql).await?;
                let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
                assert_eq!(expected["error"], Value::Null, "{sql}: {expected}");
                assert_eq!(actual, expected, "batch={batch_size} after_spark={after_spark} {sql}");
            }
        }
    }
    Ok(())
}
