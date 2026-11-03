//! Hidden ORDER BY output projections retain their nested Spark declarations.

use std::collections::BTreeSet;

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

const PACKAGES: &str =
    include_str!("fixtures/case-hidden-sort-declarations/delivery-packages.json");
const REFERENCES: &[&str] = &[
    include_str!("fixtures/case-hidden-sort-declarations/references/original-parents.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/scalar-case-literal.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/scalar-case-folded.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/absent-scalar-original.jsonl"),
    include_str!(
        "fixtures/case-hidden-sort-declarations/references/numeric-pruning-original.jsonl"
    ),
    include_str!("fixtures/case-hidden-sort-declarations/references/new-boundaries.jsonl"),
];
const INHERITED_CASES: &[(&str, &str, &str, u64)] = &[
    (
        "numeric-pruning-original",
        "array_numeric_pruned_value_coercion",
        "case_nested_mixed",
        828,
    ),
    (
        "new-boundaries",
        "generated_output_name_no_order",
        "case_parent_mixed",
        790,
    ),
    (
        "new-boundaries",
        "generated_output_name_hidden_order",
        "case_parent_mixed",
        790,
    ),
    (
        "new-boundaries",
        "numeric_dead_wide_arm_no_order",
        "case_parent_mixed",
        867,
    ),
];

// Options distinguish an absent field from a field whose value is JSON null.
fn schema_differences(
    spark: Option<&Value>,
    dar: Option<&Value>,
    path: &str,
) -> Vec<(String, Option<Value>, Option<Value>)> {
    if spark == dar {
        return Vec::new();
    }
    match (spark, dar) {
        (Some(Value::Object(spark)), Some(Value::Object(dar))) => spark
            .keys()
            .chain(dar.keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .flat_map(|key| {
                let key_path = key.replace('~', "~0").replace('/', "~1");
                schema_differences(spark.get(key), dar.get(key), &format!("{path}/{key_path}"))
            })
            .collect(),
        (Some(Value::Array(spark)), Some(Value::Array(dar))) if spark.len() == dar.len() => spark
            .iter()
            .zip(dar)
            .enumerate()
            .flat_map(|(index, (spark, dar))| {
                schema_differences(Some(spark), Some(dar), &format!("{path}/{index}"))
            })
            .collect(),
        _ => vec![(path.to_owned(), spark.cloned(), dar.cloned())],
    }
}

#[test]
fn schema_differences_keep_missing_keys_and_array_shape() {
    assert_eq!(
        schema_differences(Some(&json!({"x": null})), Some(&json!({})), ""),
        vec![("/x".to_owned(), Some(Value::Null), None)]
    );
    assert_eq!(
        schema_differences(Some(&json!([1])), Some(&json!([1, 2])), ""),
        vec![("".to_owned(), Some(json!([1])), Some(json!([1, 2])))]
    );
    assert_eq!(
        schema_differences(Some(&json!([1, 2])), Some(&json!([2, 1])), ""),
        vec![
            ("/0".to_owned(), Some(json!(1)), Some(json!(2))),
            ("/1".to_owned(), Some(json!(2)), Some(json!(1))),
        ]
    );
}

#[tokio::test]
async fn hidden_sort_output_declarations_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(PACKAGES)?;
    let packages = manifest["packages"].as_array().ok_or("missing packages")?;
    assert_eq!(packages.len(), REFERENCES.len());
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut strict_matches = 0;
    let mut inherited_observations = BTreeSet::new();
    for (package, reference) in packages.iter().zip(REFERENCES) {
        let records = reference
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], manifest["spark_version"]);
        assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
        assert_eq!(json!(records.len() - 1), package["source_records"]);
        let cases = records[1..]
            .iter()
            .filter(|case| {
                package["select_ids"]
                    .as_array()
                    .is_none_or(|ids| ids.contains(&case["id"]))
            })
            .collect::<Vec<_>>();
        assert_eq!(json!(cases.len()), package["records"]);
        for case in &cases {
            assert!(identities.insert((package["package"].to_string(), case.to_string())));
            for (stage, alias) in [
                ("analyzed", "analyzed_schema"),
                ("executed_before_collect", "executed_schema_before_collect"),
                ("executed", "executed_schema_after_collect"),
            ] {
                assert!(
                    case["schema_stages"]
                        .get(stage)
                        .or_else(|| case.get(alias))
                        .is_some()
                );
            }
            assert!(case["expected"]["rows"].is_array());
        }
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
            for case in &cases {
                comparisons += 1;
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages(&session, case).await?;
                comparison["package"] = package["package"].clone();
                comparison["id"] = case["id"].clone();
                comparison["input"] = case["input"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                eprintln!("CASE_HIDDEN_SORT_DECLARATIONS_OBSERVATION {comparison}");
                let package_name = package["package"].as_str().ok_or("missing package")?;
                let id = case["id"].as_str().ok_or("missing ID")?;
                let input = case["input"].as_str();
                let inherited_issue = INHERITED_CASES
                    .iter()
                    .find(|(known_package, known_id, known_input, _)| {
                        package_name == *known_package
                            && id == *known_id
                            && input == Some(*known_input)
                    })
                    .map(|(_, _, _, issue)| *issue);
                for (stage, alias, actual_key, match_key) in [
                    (
                        "analyzed",
                        "analyzed_schema",
                        "public_schema",
                        "public_matches",
                    ),
                    (
                        "executed_before_collect",
                        "executed_schema_before_collect",
                        "before_collect",
                        "before_matches",
                    ),
                    (
                        "executed",
                        "executed_schema_after_collect",
                        "physical_schema",
                        "physical_matches",
                    ),
                ] {
                    let expected = case["schema_stages"].get(stage).or_else(|| case.get(alias));
                    let actual_differences =
                        schema_differences(expected, Some(&comparison[actual_key]), "");
                    let expected_differences = match (inherited_issue, stage) {
                        // The name is the sole #790 difference; nested nullability stays exact.
                        (Some(790), _) => vec![(
                            "/fields/0/name".to_owned(),
                            Some(json!(
                                "struct(CASE WHEN true THEN scalarsubquery(required) ELSE NULL END AS x)"
                            )),
                            Some(json!(
                                "struct(CASE WHEN true THEN subquery ELSE NULL END AS x)"
                            )),
                        )],
                        (Some(828), "analyzed") => vec![(
                            "/fields/0/type/containsNull".to_owned(),
                            Some(json!(true)),
                            Some(json!(false)),
                        )],
                        (Some(867), "executed_before_collect" | "executed") => vec![(
                            "/fields/0/type/fields/0/metadata/source".to_owned(),
                            None,
                            Some(json!("case-id")),
                        )],
                        _ => Vec::new(),
                    };
                    if actual_differences != expected_differences
                        || comparison[match_key] != json!(expected_differences.is_empty())
                    {
                        differences.push(format!(
                            "package={package_name}, id={id}, input={input:?}, ANSI={}, batch={batch_size}, stage={stage}, issue={inherited_issue:?}, expected differences={expected_differences:?}, actual differences={actual_differences:?}",
                            case["ansi"]
                        ));
                    }
                }
                if comparison["rows_match"] != true
                    || comparison["ordered_rows_match"] != true
                    || comparison["error_matches"] != true
                    || !comparison["error"].is_null()
                    || comparison["native_phase"] != "encode"
                    || comparison["matches"] != json!(inherited_issue.is_none())
                {
                    differences.push(format!(
                        "package={}, batch={batch_size}, expected={case}, actual={comparison}",
                        package["package"]
                    ));
                }
                if comparison["matches"] == true {
                    strict_matches += 1;
                }
                if inherited_issue.is_some() {
                    assert!(inherited_observations.insert((
                        package_name.to_owned(),
                        id.to_owned(),
                        input.ok_or("missing inherited input")?.to_owned(),
                        case["ansi"].as_bool().ok_or("missing ANSI")?,
                        batch_size,
                    )));
                }
            }
        }
    }
    assert_eq!(identities.len(), 86);
    assert_eq!(comparisons, 172);
    assert_eq!(strict_matches, 156);
    let expected_inherited = INHERITED_CASES
        .iter()
        .flat_map(|(package, id, input, _)| {
            [true, false].into_iter().flat_map(move |ansi| {
                [1, 32].into_iter().map(move |batch| {
                    (
                        package.to_string(),
                        id.to_string(),
                        input.to_string(),
                        ansi,
                        batch,
                    )
                })
            })
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(inherited_observations, expected_inherited);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
