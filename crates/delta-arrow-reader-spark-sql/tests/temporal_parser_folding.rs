//! These Spark references distinguish required parse errors from unused expressions.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::{
    STAGES, TestResult, case_identity, compare_exact_spark_query_stages_with_optimized,
    matches_inherited_query_observation, native_result_matches_spark_reference,
    validate_spark_reference_result,
};

fn load_temporal_folding_references() -> TestResult<(Value, Vec<Value>, Value)> {
    let request: Value =
        serde_json::from_str(include_str!("fixtures/temporal-fold-1123-cases.json"))?;
    let mut lines =
        include_str!("fixtures/temporal-parser-folding/temporal-fold-1123-spark.jsonl").lines();
    let header: Value = serde_json::from_str(lines.next().ok_or("missing capture header")?)?;
    let cases = lines
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let retained: Value = serde_json::from_str(include_str!(
        "fixtures/temporal-parser-folding/temporal-fold-1123-retained.json"
    ))?;
    assert_eq!(request["issue"], 1123);
    assert_eq!(request["expected_record_count"], 44);
    assert_eq!(request["sql_source_count"], 22);
    assert_eq!(request["native_batch_sizes"], json!([1, 32]));
    assert_eq!(request["native_provider_partitions"], 2);
    assert_eq!(request["native_target_partitions"], 2);
    assert_eq!(header["issue"], request["issue"]);
    assert_eq!(header["spark_version"], request["spark_version"]);
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        request["spark_revision"]
    );
    assert_eq!(header["spark_only"], true);
    assert_eq!(header["reads_expected_outputs"], false);
    assert_eq!(
        header["schema_stage_protocol"],
        json!(STAGES.map(|(stage, _)| stage))
    );
    assert_eq!(
        header["input_request"]["sha256"],
        retained["source_request_sha256"]
    );
    assert_eq!(
        header["corpora"]
            .as_array()
            .ok_or("missing captured corpora")?
            .len(),
        1
    );
    assert_eq!(
        request["corpora"]
            .as_array()
            .ok_or("missing source corpora")?
            .len(),
        1
    );
    let corpus = &request["corpora"][0];
    for key in ["corpus", "source", "metadata"] {
        assert_eq!(header["corpora"][0][key], corpus[key]);
    }
    assert_eq!(corpus["metadata"]["timezone"], "UTC");
    let inputs = corpus["metadata"]["inputs"]
        .as_array()
        .ok_or("missing providers")?;
    assert_eq!(inputs.len(), 6);
    let source_cases = corpus["cases"].as_array().ok_or("missing source cases")?;
    let expected = source_cases
        .iter()
        .map(case_identity)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(source_cases.len(), 44);
    assert_eq!(expected.len(), 44);
    assert_eq!(cases.len(), 44);
    let mut captured = BTreeSet::new();
    for case in &cases {
        validate_spark_reference_result(case)?;
        assert_eq!(case["timezone"], "UTC");
        let key = case_identity(case)?;
        assert!(captured.insert(key.clone()), "duplicate captured context");
        let source = source_cases
            .iter()
            .find(|source| case_identity(source).ok().as_ref() == Some(&key))
            .ok_or("captured identity absent from source")?;
        assert!(
            source
                .as_object()
                .ok_or("source case is not an object")?
                .iter()
                .all(|(key, value)| case.get(key) == Some(value)),
            "captured SQL or source context changed"
        );
    }
    assert_eq!(captured, expected);
    for (corpus, id, _) in &captured {
        assert!(captured.contains(&(corpus.clone(), id.clone(), true)));
        assert!(captured.contains(&(corpus.clone(), id.clone(), false)));
    }
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained controls")?;
    let mut controlled = BTreeSet::new();
    for control in controls {
        let owners = control["owner_issues"].as_array().ok_or("missing owners")?;
        assert!(!owners.is_empty());
        assert!(
            owners
                .iter()
                .all(|owner| matches!(owner.as_u64(), Some(1016 | 1057)))
        );
        if owners.iter().any(|owner| owner.as_u64() == Some(1016)) {
            assert!(matches!(
                control["reference"]["id"].as_str(),
                Some("formatted_date" | "formatted_timestamp")
            ));
            assert_eq!(control["reference"]["ansi"], true);
        }
        assert_eq!(control["reference"]["status"], "error");
        assert!(
            control["baseline_observation"].is_object(),
            "missing inheritance proof"
        );
        assert!(
            control["observation"].is_object(),
            "missing complete candidate profile"
        );
        let key = case_identity(&control["reference"])?;
        assert!(
            controlled.insert(key.clone()),
            "duplicate retained identity"
        );
        let reference = cases
            .iter()
            .find(|case| case_identity(case).ok().as_ref() == Some(&key))
            .ok_or("retained identity absent from capture")?;
        assert_eq!(&control["reference"], reference);
    }
    assert_eq!(retained["retained_contexts"], json!(controls.len() * 2));
    Ok((header, cases, retained))
}

#[tokio::test]
async fn temporal_folding_matches_source_stages_values_and_causes() -> TestResult {
    let (header, cases, retained) = load_temporal_folding_references()?;
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained controls")?;
    let mut completed = BTreeSet::new();
    let mut strict_count = 0;
    let mut retained_count = 0;
    let mut source_field_count = 0;
    let mut failures = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &header["corpora"][0]["metadata"], batch)?;
        for case in &cases {
            let key = case_identity(case)?;
            let mut actual =
                compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            actual["batch"] = json!(batch);
            let control = controls
                .iter()
                .find(|control| case_identity(&control["reference"]).ok().as_ref() == Some(&key));
            let source_fields_match =
                STAGES.iter().all(
                    |(stage, field_key)| match case["schema_stages"].get(*stage) {
                        Some(expected) => actual.get(*field_key) == Some(expected),
                        None => actual.get(*field_key).is_some_and(Value::is_null),
                    },
                );
            let retained_declaration = control.is_some_and(|control| {
                control["owner_issues"]
                    .as_array()
                    .is_some_and(|owners| owners.iter().any(|owner| owner.as_u64() == Some(1016)))
            });
            let fields_preserved = STAGES.iter().all(|(stage, field_key)| {
                // #1016 must retain the complete baseline declaration, not a new approximation.
                if retained_declaration && matches!(*stage, "public" | "analyzed") {
                    actual.get(*field_key)
                        == control.map(|control| &control["baseline_observation"]["public_schema"])
                } else {
                    match case["schema_stages"].get(*stage) {
                        Some(expected) => actual.get(*field_key) == Some(expected),
                        None => actual.get(*field_key).is_some_and(Value::is_null),
                    }
                }
            });
            if case["status"] == "error"
                && matches!(
                    case["spark_error_phase"].as_str(),
                    Some("optimized" | "optimized_schema")
                )
            {
                let error = actual["optimized_error"]
                    .as_str()
                    .ok_or("missing optimized failure")?;
                assert!(!error.is_empty());
                assert!(
                    !error.contains("only support Literal types"),
                    "lost parser cause"
                );
                assert_eq!(actual["native_phase"], "create_physical_plan");
                assert_eq!(actual["error"], actual["optimized_error"]);
            }
            let strict = native_result_matches_spark_reference(&actual, case);
            let retained_match = control.is_some_and(|control| {
                !strict && matches_inherited_query_observation(case, &actual, control)
            });
            let accepted = fields_preserved
                && match control {
                    Some(_) => retained_match,
                    None => strict,
                };
            source_field_count += usize::from(source_fields_match);
            strict_count += usize::from(strict);
            retained_count += usize::from(retained_match);
            assert!(completed.insert((key, batch)), "duplicate native context");
            let observation = json!({"reference":case,"actual":actual,"batch":batch,
                "complete_fields_match":source_fields_match,"declarations_preserved":fields_preserved,"strict":strict,
                "retained_exact_match":retained_match,
                "owner_issues":control.map(|control| &control["owner_issues"])});
            eprintln!("TEMPORAL_PARSER_FOLDING {observation}");
            if !accepted {
                failures.push(observation);
            }
        }
    }
    assert_eq!(completed.len(), 88);
    assert_eq!(json!(strict_count), retained["strict_contexts"]);
    assert_eq!(json!(retained_count), retained["retained_contexts"]);
    assert_eq!(json!(source_field_count), retained["source_field_contexts"]);
    assert!(
        failures.is_empty(),
        "unmatched or stale profiles: {failures:#?}"
    );
    Ok(())
}
