//! ARRAY_COMPACT fields and consumers match independently captured Spark results.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SCENARIO_IDS: &[&str] = &[
    "declarations_direct",
    "declarations_is_null",
    "declarations_struct",
    "declarations_empty_direct",
    "declarations_empty_is_null",
    "declarations_empty_struct",
    "compacted_twice",
    "membership_consumer",
    "overlap_consumer",
    "nested_direct",
    "nested_is_null",
    "nested_struct",
    "nested_empty_direct",
    "nested_empty_is_null",
    "nested_empty_struct",
    "flatten_consumer",
    "typed_literals_direct",
    "typed_literals_is_null",
    "typed_literals_struct",
    "untyped_null_direct",
    "untyped_null_is_null",
    "untyped_null_struct",
    "nested_literal_direct",
    "nested_literal_is_null",
    "nested_literal_struct",
    "constant_bad_cast_direct",
    "constant_bad_cast_is_null",
    "constant_bad_cast_struct",
    "runtime_bad_cast_direct",
    "runtime_bad_cast_is_null",
    "runtime_bad_cast_struct",
    "runtime_bad_cast_empty_direct",
    "runtime_bad_cast_empty_is_null",
    "runtime_bad_cast_empty_struct",
    "invalid_non_array",
];

async fn capture_compact_comparison(session: &SessionContext, case: &Value) -> Value {
    let observation = async {
        let mut comparison =
            spark_results::compare_exact_spark_query_stages(session, case).await?;
        let optimized = spark_results::capture_spark_optimized_observation(session, case).await;
        comparison["optimized_matches"] = json!(match &optimized {
            Ok(observation) =>
                observation["names_match"] == true
                    && Some(&observation["schema"]) == case["schema_stages"].get("optimized"),
            Err(error) =>
                case["schema_stages"].get("optimized").is_none()
                    && comparison["error_matches"] == true
                    && matches!(comparison["native_phase"].as_str(), Some("resolve" | "create_physical_plan"))
                    && comparison["error"].as_str() == Some(error.to_string().as_str()),
        });
        comparison["optimized_observation"] = optimized.as_ref().ok().cloned().unwrap_or(Value::Null);
        comparison["optimized_error"] = json!(optimized.as_ref().err().map(ToString::to_string));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(comparison)
    };
    match std::panic::AssertUnwindSafe(observation).catch_unwind().await {
        Ok(Ok(observation)) => observation,
        Ok(Err(error)) => json!({"matches":false,"observation_error":error.to_string()}),
        Err(payload) => {
            let panic = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| (*text).to_owned()))
                .unwrap_or_else(|| "non-string panic payload".into());
            json!({"matches":false,"panic":panic})
        }
    }
}

#[tokio::test]
async fn compact_result_fields_match_spark() -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for (fixture, group, ids, record_count) in [
        (
            include_str!("fixtures/array-compact-fields-spark.jsonl"),
            "array-compact-844-current-v1",
            SCENARIO_IDS,
            71,
        ),
        (
            include_str!("fixtures/array-compact-slices-spark.jsonl"),
            "array-compact-842-independent",
            &["compact_integer", "compact_string"][..],
            13,
        ),
    ] {
        let records = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len(), record_count);
        assert_eq!(records[0]["group"], group);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
        if group == "array-compact-844-current-v1" {
            assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
        }
        let cases = records[1..]
            .iter()
            .filter(|case| case["id"].as_str().is_some_and(|id| ids.contains(&id)))
            .collect::<Vec<_>>();
        let identities = cases
            .iter()
            .map(|case| {
                Ok((case["id"].as_str().ok_or("missing ID")?, case["ansi"].as_bool().ok_or("missing ANSI")?))
            })
            .collect::<TestResult<Vec<_>>>()?;
        let expected_identities = [true, false]
            .into_iter()
            .flat_map(|ansi| ids.iter().map(move |id| (*id, ansi)))
            .collect::<Vec<_>>();
        assert_eq!(identities, expected_identities);
        for batch_size in [1, 32] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)),
                )
                .build(),
            );
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
            for case in &cases {
                let mut comparison = capture_compact_comparison(&session, case).await;
                comparison["group"] = json!(group);
                comparison["id"] = case["id"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                assert!(completed.insert((group.to_owned(), case["id"].to_string(), case["ansi"].to_string(), batch_size)));
                eprintln!("ARRAY_COMPACT_FIELDS_OBSERVATION {comparison}");
                if comparison["matches"] != true || comparison["optimized_matches"] != true {
                    differences.push(format!("{group} {} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}", case["id"], case["ansi"], case["expected"], case["schema_stages"]));
                }
            }
        }
    }
    assert_eq!(completed.len(), 148);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_compaction_preserves_successful_native_array_queries() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let sqls = [
        "SELECT ARRAY_HAS_ANY([1, NULL], [2]) AS r",
        "SELECT ARRAY_HAS_ANY([], [NULL]) AS r",
        "SELECT ARRAY_HAS_ANY(CAST(NULL AS INT[]), [1]) AS r",
        "SELECT [1, NULL, 2] AS r, [1, 2] IS NULL AS n",
        "SELECT CAST(NULL AS INT[]) IS NULL AS n, [] AS r",
    ];
    let mut expected = Vec::new();
    for sql in sqls {
        let observation = spark_results::capture_native_query_observation(&native, sql).await?;
        assert!(observation["error"].is_null(), "{sql}: {observation}");
        assert_eq!(observation["phase"], "encode", "{sql}: {observation}");
        for stage in ["public_schema", "before_collect", "physical_schema"] {
            assert!(observation[stage].is_object(), "{sql}: {observation}");
        }
        assert!(observation["rows"].is_array(), "{sql}: {observation}");
        expected.push(observation);
    }
    for after_spark in [false, true] {
        if after_spark {
            let case = json!({"sql":"SELECT ARRAY_COMPACT(ARRAY(1, NULL, 2)) AS r", "ansi":true});
            spark_results::capture_spark_query_result(&configured, &case).await?;
        }
        for (sql, expected) in sqls.into_iter().zip(&expected) {
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            eprintln!("ARRAY_COMPACT_NATIVE_OBSERVATION {}", json!({"sql":sql,"after_spark":after_spark,"native":expected,"configured":actual}));
            assert_eq!(&actual, expected, "{sql} after_spark={after_spark}");
        }
    }
    Ok(())
}
