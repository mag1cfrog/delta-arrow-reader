//! SLICE parent fields and bounded controls match independently captured Spark plans.

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::FutureExt;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SCENARIO_IDS: &[&str] = &[
    "a_nn_required",
    "a_nn_nullable_start",
    "a_nn_nullable_length",
    "a_nn_nullable_both",
    "a_ne_required",
    "a_ne_nullable_start",
    "a_ne_nullable_length",
    "a_ne_nullable_both",
    "a_pn_required",
    "a_pn_nullable_start",
    "a_pn_nullable_length",
    "a_pn_nullable_both",
    "a_pe_required",
    "a_pe_nullable_start",
    "a_pe_nullable_length",
    "a_pe_nullable_both",
    "literal_required",
    "literal_null_element",
    "literal_empty",
    "literal_all_null",
    "literal_null_array",
    "literal_null_start",
    "literal_null_length",
    "nested_struct_metadata",
    "nested_array_elements",
    "provider_zero_rows",
    "zero_start_constant",
    "negative_length_constant",
    "zero_start_runtime",
    "negative_length_runtime",
    "negative_start_past_beginning",
    "null_array_bad_start",
    "bad_array_null_start",
    "bad_array_null_start_is_null",
    "constant_bad_array_null_start",
];

async fn capture_slice_observation(
    observation: impl std::future::Future<Output = TestResult<Value>>,
) -> TestResult<Value> {
    match std::panic::AssertUnwindSafe(observation)
        .catch_unwind()
        .await
    {
        Ok(observation) => observation,
        Err(payload) => {
            let panic = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                })
                .unwrap_or_else(|| "non-string panic payload".into());
            Ok(json!({"matches":false,"panic":panic,
                "public_schema":null,"before_collect":null,"physical_schema":null,
                "rows":null,"rows_raw":null,"native_phase":null,"error":null,
                "public_matches":false,"before_matches":false,"physical_matches":false,
                "rows_match":false,"ordered_rows_match":false,"error_matches":false}))
        }
    }
}

async fn capture_slice_optimized_observation(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    let optimized = state.optimize(&query.logical_plan)?;
    let raw_schema = spark_results::capture_schema(optimized.schema().fields(), None)?;
    let names_match = optimized.schema().fields().len() == query.logical_plan.schema().fields().len()
        && optimized
            .schema()
            .fields()
            .iter()
            .zip(query.logical_plan.schema().fields())
            .all(|(actual, resolved)| actual.name() == resolved.name());
    let schema = names_match
        .then(|| {
            spark_results::capture_schema(optimized.schema().fields(), Some(&query.column_names))
        })
        .transpose()?;
    Ok(json!({"raw_schema":raw_schema,"schema":schema,"names_match":names_match}))
}

#[tokio::test]
async fn slice_parent_fields_match_spark() -> TestResult {
    let records = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["group"], "slice-parent-878-current-v1");
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    assert_eq!(records.len(), 71);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let expected_identities = [true, false]
        .into_iter()
        .flat_map(|ansi| SCENARIO_IDS.iter().map(move |id| (*id, ansi)))
        .collect::<Vec<_>>();
    assert_eq!(identities, expected_identities);
    let mut differences = Vec::new();
    let mut completed = std::collections::BTreeSet::new();
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
            let mut comparison = capture_slice_observation(async {
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages(&session, case).await?;
                let optimized = capture_slice_optimized_observation(&session, case).await;
                comparison["optimized_matches"] = json!(match &optimized {
                    Ok(observation) => observation["names_match"] == true
                        && Some(&observation["schema"]) == case["schema_stages"].get("optimized"),
                    Err(_) => case["schema_stages"].get("optimized").is_none(),
                });
                comparison["optimized_schema"] = optimized
                    .as_ref()
                    .map(|observation| observation["schema"].clone())
                    .unwrap_or(Value::Null);
                comparison["optimized_observation"] =
                    optimized.as_ref().ok().cloned().unwrap_or(Value::Null);
                comparison["optimized_error"] =
                    json!(optimized.as_ref().err().map(ToString::to_string));
                Ok(comparison)
            })
            .await?;
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            )));
            eprintln!("SLICE_PARENT_FIELDS_OBSERVATION {comparison}");
            if comparison["matches"] != true || comparison["optimized_matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(completed.len(), 140);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_successful_native_array_slice() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT ARRAY_SLICE([1, 2, 3, 4], 2, 3) AS r",
        "SELECT ARRAY_SLICE([1, NULL, 3], 1, 2) AS r",
        "SELECT ARRAY_SLICE([1, 2, 3], -2, -1) AS r",
        "SELECT ARRAY_SLICE([], 1, 2) AS r",
        "SELECT ARRAY_SLICE(CAST(NULL AS INT[]), 1, 2) AS r",
        "SELECT ARRAY_SLICE([1, 2], CAST(NULL AS BIGINT), 2) AS r",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        let observation = json!({"sql":sql,"native":expected,"configured":actual});
        eprintln!("SLICE_NATIVE_OBSERVATION {observation}");
        assert!(observation["native"]["error"].is_null(), "{observation}");
        assert_eq!(observation["native"]["phase"], "encode", "{observation}");
        for stage in ["public_schema", "before_collect", "physical_schema"] {
            assert!(observation["native"][stage].is_object(), "{observation}");
        }
        assert!(observation["native"]["rows"].is_array(), "{observation}");
        assert_eq!(
            observation["configured"], observation["native"],
            "{observation}"
        );
    }
    Ok(())
}
