//! SQL MAP declares a nonnull container before lambda binding or scalar inlining.

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_native_query_observation, capture_spark_query_observation,
    capture_spark_query_result, compare_exact_spark_query_stages,
};

const MAP: &str = include_str!("fixtures/map-constructor-nullability-spark.jsonl");
const LAMBDA: &str = include_str!("fixtures/folded-map-lambda-816-core-spark.jsonl");

fn records(source: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, count);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"]
            .as_str()
            .ok_or("missing build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    Ok(records)
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

#[tokio::test]
async fn map_constructor_fields_and_rows_match_recorded_core_cases() -> TestResult {
    let records = records(MAP, 102)?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let mut checked = 0;
        for case in &records[1..] {
            if !matches!(
                case["id"].as_str(),
                Some(
                    "direct_nullable"
                        | "direct_required"
                        | "empty_map"
                        | "array_element"
                        | "struct_child"
                        | "map_value"
                        | "lambda_result"
                        | "from_arrays_control"
                        | "from_entries_control"
                )
            ) {
                continue;
            }
            let stages = json!({"sql":case["sql"],"ansi":case["ansi"],
                "expected":case["expected"],"schema_stages":{
                    "analyzed":case["public_schema"],
                    "executed_before_collect":case["executed_schema_before_collect"],
                    "executed":case["executed_schema_after_collect"]}});
            let actual = compare_exact_spark_query_stages(&session, &stages).await?;
            if actual["matches"] != true {
                differences.push(format!(
                    "{} input={} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["input"], case["ansi"]
                ));
            }
            checked += 1;
        }
        assert_eq!(checked, 54);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn scalar_map_container_narrows_after_the_single_row_proof() -> TestResult {
    let records = records(MAP, 102)?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        let mut checked = 0;
        for case in records[1..].iter().filter(|case| case["id"] == "scalar") {
            let actual = capture_spark_query_observation(&session, case).await?;
            assert_eq!(actual["result"]["schema"], case["public_schema"]);
            assert_eq!(actual["result"]["rows"], case["expected"]["rows"]);
            // The complete external replay retains metadata-stage differences under #818.
            let physical = &actual["physical_schema"]["fields"][0];
            let expected = &case["executed_schema_after_collect"]["fields"][0];
            assert_eq!(physical["name"], expected["name"]);
            assert_eq!(physical["type"], expected["type"]);
            assert_eq!(physical["nullable"], expected["nullable"]);
            checked += 1;
        }
        assert_eq!(checked, 6);
    }
    Ok(())
}

#[tokio::test]
async fn original_folded_map_lambda_and_null_element_match_spark() -> TestResult {
    let records = records(LAMBDA, 4)?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages(&session, case).await?;
            assert_eq!(
                actual["matches"], true,
                "batch={batch_size}: {case}: {actual}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn map_constructor_preserves_null_cast_native_and_cardinality_guards() -> TestResult {
    let native = SessionContext::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for ansi in [false, true] {
            for sql in [
                "SELECT MAP_FROM_ARRAYS(CAST(NULL AS ARRAY<STRING>),ARRAY(1)) AS r",
                "SELECT MAP_FROM_ENTRIES(CAST(NULL AS ARRAY<STRUCT<key:STRING,value:INT>>)) AS r",
                "SELECT (SELECT MAP('key',v) FROM (VALUES (1)) q(v) WHERE false) AS r",
            ] {
                let actual =
                    capture_spark_query_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                assert_eq!(actual["rows"], json!([[null]]), "{sql}");
                assert_eq!(actual["schema"]["fields"][0]["nullable"], true, "{sql}");
            }
            let cast = capture_spark_query_observation(
                &session,
                &json!({"ansi":ansi,
                "sql":"SELECT CAST(MAP('k',1) AS MAP<STRING,INT>) AS r"}),
            )
            .await?;
            assert_eq!(
                cast["result"]["schema"]["fields"][0]["type"]["valueContainsNull"],
                true
            );
            assert_eq!(cast["result"]["rows"], json!([[[["k", "1"]]]]));
            let error = capture_spark_query_observation(
                &session,
                &json!({"ansi":ansi,
                "sql":"SELECT (SELECT MAP('key',v) FROM (VALUES (1),(2)) q(v)) AS r"}),
            )
            .await
            .expect_err("multirow scalar must fail");
            assert!(
                matches!(error.downcast_ref::<DataFusionError>().map(DataFusionError::find_root),
                Some(DataFusionError::Execution(message)) if message.contains("Scalar subquery returned more than one row")),
                "{error}"
            );
            let error = capture_spark_query_observation(
                &session,
                &json!({"ansi":ansi,
                "sql":"SELECT MAP('k',1,'k',2) AS r"}),
            )
            .await
            .expect_err("duplicate keys must fail");
            assert!(
                matches!(error.downcast_ref::<DataFusionError>().map(DataFusionError::find_root),
                Some(DataFusionError::Execution(message)) if message.contains("[DUPLICATED_MAP_KEY]")),
                "{error}"
            );
        }
        for (sql, cause) in [
            (
                "SELECT MAP('a',1,2,2) AS r",
                "Cannot cast string 'a' to value of Int32 type",
            ),
            (
                "SELECT MAP(1,'invalid',2,2) AS r",
                "Cannot cast string 'invalid' to value of Int32 type",
            ),
        ] {
            let query = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode: true,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let error = query
                .create_physical_plan(&session)
                .await
                .expect_err("required numeric cast");
            let DataFusionError::ArrowError(error, _) = error.find_root() else {
                return Err(format!("unexpected numeric cast failure: {error}").into());
            };
            assert!(
                matches!(error.as_ref(), arrow::error::ArrowError::CastError(message) if message == cause)
            );
        }
        for sql in ["SELECT make_array(1,NULL) AS r", "SELECT make_array() AS r"] {
            let expected = capture_native_query_observation(&native, sql).await?;
            assert!(expected["error"].is_null(), "{expected}");
            assert_eq!(
                capture_native_query_observation(&session, sql).await?,
                expected
            );
        }
    }
    Ok(())
}
