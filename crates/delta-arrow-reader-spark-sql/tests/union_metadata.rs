//! UNION output aliases follow the left input without changing native metadata policy.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
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

#[tokio::test]
async fn union_output_metadata_matches_pinned_spark_stages() -> TestResult {
    let records = include_str!("fixtures/conditional-nested-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases = records[1..]
        .iter()
        .filter(|case| {
            matches!(
                case["id"].as_str(),
                Some(
                    "union_struct_reordered_names"
                        | "union_struct_different_names"
                        | "union_struct_same_names_numeric_control"
                )
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 6);
    replay_set_operation_metadata(&records[0], &cases, "original_nested").await
}

#[tokio::test]
async fn set_operation_metadata_controls_match_pinned_spark_stages() -> TestResult {
    let records = include_str!("fixtures/union-metadata-stages/union-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 136);
    assert_eq!(
        records[0]["manifest_sha256"],
        "1ad939e5b35de84d86ee0dbcf6d9dc24c56bd13ec88699ce990c74f3fbd980bc"
    );
    assert_eq!(
        records[0]["capture_sha256"],
        "0852fac11447760bac59f48fef08c24c53f305bcfbad7dc1c7f91120d11bc262"
    );
    let cases = records[1..].iter().collect::<Vec<_>>();
    replay_set_operation_metadata(&records[0], &cases, "captured_controls").await
}

async fn replay_set_operation_metadata(
    header: &Value,
    cases: &[&Value],
    group: &str,
) -> TestResult {
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(header["timezone"].as_str().unwrap_or("UTC"), "UTC");
    let mut identities = std::collections::BTreeSet::new();
    for case in cases {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            for stage in [
                "analyzed", "optimized", "executed_before_collect", "executed",
            ] {
                assert!(
                    case["schema_stages"][stage].is_object(),
                    "missing {stage}: {case}"
                );
            }
            assert!(case["expected"]["rows"].is_array(), "missing rows: {case}");
        }
    }
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
        spark_inputs::register_recorded_inputs(&session, header, batch_size)?;
        for case in cases {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["group"] = json!(group);
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("UNION_METADATA_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={}, stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, cases.len() * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_union_matches_unconfigured_results() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let metadata = [
        ("__autoGeneratedAlias".to_string(), "true".to_string()),
        ("source".to_string(), "right".to_string()),
    ]
    .into_iter()
    .collect();
    let field = Field::new("v", DataType::Int32, false).with_metadata(metadata);
    for session in [&native, &configured] {
        for (table, field, value) in [
            ("bare", Field::new("v", DataType::Int32, false), 1),
            ("marked", field.clone(), 2),
        ] {
            session.register_batch(
                table,
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field])),
                    vec![Arc::new(Int32Array::from(vec![value]))],
                )?,
            )?;
        }
    }
    let sql = "SELECT v AS r FROM bare UNION ALL SELECT v FROM marked";
    let expected = native.sql(sql).await?.collect().await?;
    let actual = configured.sql(sql).await?.collect().await?;
    assert_eq!(actual, expected);
    Ok(())
}
