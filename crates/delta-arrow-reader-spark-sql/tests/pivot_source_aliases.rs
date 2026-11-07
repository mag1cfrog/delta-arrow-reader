//! Source and result aliases bind on their respective sides of SQL PIVOT.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, compare_exact_spark_query_stages_with_optimized};

fn reference_with_arrow_boolean_text(case: &Value) -> Value {
    let mut reference = case.clone();
    let Some(fields) = case["expected"]["schema"]["fields"].as_array() else {
        return reference;
    };
    if let Some(rows) = reference["expected"]["rows"].as_array_mut() {
        for row in rows {
            if let Some(cells) = row.as_array_mut() {
                for (cell, field) in cells.iter_mut().zip(fields) {
                    if field["type"] == "boolean" {
                        // PySpark str(bool) uses capitals; Arrow's recorded Boolean text does not.
                        match cell.as_str() {
                            Some("True") => *cell = json!("true"),
                            Some("False") => *cell = json!("false"),
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    reference
}

#[test]
fn boolean_recording_conversion_preserves_string_values_and_nulls() -> TestResult {
    use arrow::array::{BooleanArray, StringArray};
    use spark_results::encode_value;

    let case = json!({"expected":{
        "schema":{"fields":[{"type":"boolean"},{"type":"string"}]},
        "rows":[["True","True"],["False","False"],[null,null]],
    }});
    let reference = reference_with_arrow_boolean_text(&case);
    let booleans = BooleanArray::from(vec![Some(true), Some(false), None]);
    let strings = StringArray::from(vec![Some("True"), Some("False"), None]);
    for row in 0..3 {
        assert_eq!(
            reference["expected"]["rows"][row],
            json!([encode_value(&booleans, row)?, encode_value(&strings, row)?]),
        );
    }
    Ok(())
}

fn pivot_error_matches(case: &Value, observation: &Value) -> bool {
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let parameters = &case["spark_error_details"]["message_parameters"];
    let attribute = |key: &str| {
        parameters[key].as_str().map(|name| {
            let parts = name
                .split('.')
                .map(|part| format!("Identifier({:?})", part.trim_matches('`')))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ObjectName([{parts}])")
        })
    };
    let cause_matches = match case["expected"]["condition"].as_str() {
        Some("PARSE_SYNTAX_ERROR") => {
            error.starts_with("External error: error in SQL parser: ")
        }
        Some("AMBIGUOUS_REFERENCE") => attribute("name").is_some_and(|name| {
            error == format!("External error: analysis error: ambiguous attribute: {name}")
        }),
        Some("UNRESOLVED_COLUMN.WITH_SUGGESTION") => {
            attribute("objectName").is_some_and(|name| {
                error == format!(
                    "External error: analysis error: attribute {name} is missing from the schema: cannot resolve attribute"
                )
            })
        }
        Some("INVALID_EXTRACT_BASE_FIELD_TYPE") => {
            parameters["base"] == "\"p\""
                && parameters["other"] == "\"BOOLEAN\""
                && error == "External error: analysis error: cannot resolve attribute: [INVALID_EXTRACT_BASE_FIELD_TYPE] field \"id\" cannot be extracted from Boolean"
        }
        Some("ASSIGNMENT_ARITY_MISMATCH") => {
            match (parameters["numExpr"].as_str(), parameters["numTarget"].as_str()) {
                (Some(aliases), Some(columns)) => error == format!(
                    "External error: invalid argument: number of column names ({aliases}) does not match number of columns ({columns})"
                ),
                _ => false,
            }
        }
        _ => false,
    };
    cause_matches
        && case["spark_error_phase"] == "sql"
        && observation["native_phase"] == "resolve"
        && observation["optimized_error"] == error
        && [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
            "optimized_observation",
        ]
        .into_iter()
        .all(|key| observation.get(key).is_some_and(Value::is_null))
}

const RETAINED_RELATION_FACETS: [&str; 16] = [
    "public_schema",
    "optimized_schema",
    "before_collect",
    "physical_schema",
    "rows",
    "native_phase",
    "error",
    "optimized_error",
    "public_matches",
    "before_matches",
    "physical_matches",
    "rows_match",
    "ordered_rows_match",
    "error_matches",
    "matches",
    "optimized_matches",
];

fn inherited_relation_gap_owner(
    case: &Value,
    comparison: &Value,
    controls: &[Value],
) -> Option<&'static str> {
    let owner = match case["id"].as_str()? {
        "duplicate_pivot_input" | "prior_duplicate_alias_free" => "#786",
        "unpivot_source_and_output_aliases" | "prior_unpivot_without_source_alias" => "#1076",
        _ => return None,
    };
    if comparison["reference"] != *case
        || comparison["id"] != case["id"]
        || comparison["ansi"] != case["ansi"]
        || comparison["matches"] != false
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
    {
        return None;
    }
    let control = controls.iter().find(|control| {
        control["id"] == comparison["id"]
            && control["ansi"] == comparison["ansi"]
            && control["batch"] == comparison["batch"]
            && control["owner"] == owner
            && control["reference"] == *case
    })?;
    let observation = &comparison["actual"];
    if !RETAINED_RELATION_FACETS.into_iter().all(|key| {
        control["native"]
            .get(key)
            .is_some_and(|expected| observation.get(key) == Some(expected))
    }) {
        return None;
    }
    let complete = match observation["native_phase"].as_str()? {
        "resolve" => ["rows_raw", "optimized_observation"]
            .into_iter()
            .all(|key| observation.get(key).is_some_and(Value::is_null)),
        "encode" => {
            // Unordered raw rows may move; keep their complete multiset, including duplicates.
            let mut rows = observation["rows_raw"].as_array()?.clone();
            rows.sort_by_key(Value::to_string);
            json!(rows) == observation["rows"]
                && observation["optimized_observation"]["names_match"] == true
                && observation["optimized_observation"]["schema"] == observation["optimized_schema"]
        }
        _ => false,
    };
    complete.then_some(owner)
}

#[test]
fn inherited_relation_contracts_reject_changed_context_fields_rows_and_errors() -> TestResult {
    let retained: Value = serde_json::from_str(include_str!(
        "fixtures/pivot-source-aliases-retained-baseline-facets.json"
    ))?;
    let controls = retained["controls"].as_array().ok_or("missing controls")?;
    let expected_contexts = [
        "duplicate_pivot_input",
        "prior_duplicate_alias_free",
        "unpivot_source_and_output_aliases",
        "prior_unpivot_without_source_alias",
    ]
    .into_iter()
    .flat_map(|id| {
        [true, false]
            .into_iter()
            .flat_map(move |ansi| [1_u64, 32].into_iter().map(move |batch| (id, ansi, batch)))
    })
    .collect::<BTreeSet<_>>();
    let mut contexts = BTreeSet::new();
    for control in controls {
        assert!(contexts.insert((
            control["id"].as_str().ok_or("missing ID")?,
            control["ansi"].as_bool().ok_or("missing ANSI")?,
            control["batch"].as_u64().ok_or("missing batch")?,
        )));
        let case = &control["reference"];
        let mut actual = control["native"].clone();
        actual["rows_raw"] = actual["rows"].clone();
        actual["optimized_observation"] = if actual["optimized_schema"].is_null() {
            Value::Null
        } else {
            json!({"schema":actual["optimized_schema"], "names_match":true})
        };
        let observed = json!({
            "id":control["id"], "ansi":control["ansi"], "batch":control["batch"],
            "reference":case, "matches":false, "actual":actual,
        });
        let owner = control["owner"].as_str().ok_or("missing owner")?;
        assert_eq!(
            inherited_relation_gap_owner(case, &observed, controls),
            Some(owner)
        );
        for (key, wrong) in [
            ("id", json!("ordinary_source_alias")),
            (
                "ansi",
                json!(!case["ansi"].as_bool().ok_or("missing ANSI")?),
            ),
            ("batch", json!(2)),
            ("matches", json!(true)),
        ] {
            let mut changed = observed.clone();
            changed[key] = wrong;
            assert_eq!(
                inherited_relation_gap_owner(case, &changed, controls),
                None,
                "{key}"
            );
        }
        for key in RETAINED_RELATION_FACETS {
            let mut changed = observed.clone();
            changed["actual"][key] = json!({"unexpected":"facet"});
            assert_eq!(
                inherited_relation_gap_owner(case, &changed, controls),
                None,
                "{key}"
            );
            changed["actual"]
                .as_object_mut()
                .ok_or("missing observation")?
                .remove(key);
            assert_eq!(
                inherited_relation_gap_owner(case, &changed, controls),
                None,
                "missing {key}"
            );
        }
        for key in [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
        ] {
            if actual[key].is_null() {
                continue;
            }
            for (attribute, wrong) in [
                ("name", json!("wrong")),
                ("type", json!("long")),
                (
                    "nullable",
                    json!(
                        !actual[key]["fields"][0]["nullable"]
                            .as_bool()
                            .ok_or("missing nullable")?
                    ),
                ),
                ("metadata", json!({"source":"wrong"})),
            ] {
                let mut changed = observed.clone();
                changed["actual"][key]["fields"][0][attribute] = wrong;
                assert_eq!(
                    inherited_relation_gap_owner(case, &changed, controls),
                    None,
                    "{key}:{attribute}"
                );
            }
        }
        if let Some(rows) = actual["rows"].as_array() {
            for remove in [true, false] {
                let mut changed = observed.clone();
                let changed_rows = changed["actual"]["rows"]
                    .as_array_mut()
                    .ok_or("missing rows")?;
                if remove {
                    changed_rows.pop();
                } else {
                    changed_rows.push(rows[0].clone());
                }
                assert_eq!(inherited_relation_gap_owner(case, &changed, controls), None);
            }
            let mut reordered = observed.clone();
            reordered["actual"]["rows_raw"]
                .as_array_mut()
                .ok_or("missing raw rows")?
                .reverse();
            assert_eq!(
                inherited_relation_gap_owner(case, &reordered, controls),
                Some(owner)
            );
        } else {
            for key in ["error", "optimized_error"] {
                let mut changed = observed.clone();
                changed["actual"][key] = json!(format!(
                    "{}: unrelated suffix",
                    actual[key].as_str().ok_or("missing error")?
                ));
                assert_eq!(inherited_relation_gap_owner(case, &changed, controls), None);
            }
        }
        for key in ["rows_raw", "optimized_observation"] {
            let mut changed = observed.clone();
            changed["actual"][key] = json!({"unexpected":"materialization"});
            assert_eq!(
                inherited_relation_gap_owner(case, &changed, controls),
                None,
                "{key}"
            );
        }
        for pointer in [
            "/sql",
            "/expected/rows/0/0",
            "/schema_stages/analyzed/fields/0/nullable",
        ] {
            let mut changed_case = case.clone();
            *changed_case
                .pointer_mut(pointer)
                .ok_or("missing reference facet")? = json!("changed");
            let mut changed = observed.clone();
            changed["reference"] = changed_case.clone();
            assert_eq!(
                inherited_relation_gap_owner(&changed_case, &changed, controls),
                None,
                "{pointer}"
            );
        }
        if control["baseline_control_id"] != control["id"] {
            let mut rejected = observed.clone();
            rejected["actual"] = control["baseline_observation"].clone();
            rejected["actual"]["rows_raw"] = Value::Null;
            rejected["actual"]["optimized_observation"] = Value::Null;
            assert_eq!(
                inherited_relation_gap_owner(case, &rejected, controls),
                None
            );
        }
    }
    assert_eq!(contexts, expected_contexts);
    let owned: Value = serde_json::from_str(
        include_str!("fixtures/pivot-source-aliases-spark.jsonl")
            .lines()
            .nth(1)
            .ok_or("missing owned case")?,
    )?;
    let fake_gap = json!({"id":owned["id"], "ansi":owned["ansi"], "batch":1,
        "reference":owned, "matches":false, "actual":null});
    assert_eq!(
        inherited_relation_gap_owner(&owned, &fake_gap, controls),
        None
    );
    Ok(())
}

#[tokio::test]
async fn pivot_source_and_output_aliases_check_spark_and_inherited_facets() -> TestResult {
    let mut records = Vec::new();
    for fixture in [
        include_str!("fixtures/pivot-source-aliases-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-supplement-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-boundaries-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-join-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-relation-order-spark.jsonl"),
    ] {
        let mut captured = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(captured[0]["spark_version"], "4.2.0");
        assert_eq!(captured[0]["timezone"], "UTC");
        if records.is_empty() {
            records.push(captured.remove(0));
        } else {
            assert_eq!(captured.remove(0)["inputs"], records[0]["inputs"]);
        }
        records.extend(captured);
    }
    assert_eq!(records.len() - 1, 82);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let retained: Value = serde_json::from_str(include_str!(
        "fixtures/pivot-source-aliases-retained-baseline-facets.json"
    ))?;
    let controls = retained["controls"].as_array().ok_or("missing controls")?;
    let mut differences = Vec::new();
    let mut observations = Vec::new();
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let comparison_reference = reference_with_arrow_boolean_text(case);
            let actual =
                compare_exact_spark_query_stages_with_optimized(&session, &comparison_reference)
                    .await;
            let matches = actual.as_ref().is_ok_and(|observation| {
                if case["expected"].get("condition").is_some() {
                    pivot_error_matches(case, observation)
                } else {
                    observation["matches"] == true && observation["optimized_matches"] == true
                }
            });
            let mut comparison = json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.as_ref().map_or_else(
                    |error| json!({"error":error.to_string()}), |value| value.clone()),
            });
            let gap_owner = inherited_relation_gap_owner(case, &comparison, controls);
            let regression_matches = matches || gap_owner.is_some();
            comparison["independent_gap_owner"] = json!(gap_owner);
            comparison["regression_matches"] = json!(regression_matches);
            observations.push(comparison);
            if !regression_matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_PIVOT_SOURCE_ALIASES_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&observations)?)?;
    }
    let context_key = |record: &Value| -> TestResult<(String, bool, u64)> {
        Ok((
            record["id"].as_str().ok_or("missing ID")?.to_owned(),
            record["ansi"].as_bool().ok_or("missing ANSI mode")?,
            record["batch"].as_u64().ok_or("missing batch")?,
        ))
    };
    let expected_contexts = identities
        .iter()
        .flat_map(|(id, ansi)| {
            [1_u64, 32].map(move |batch| ((*id).to_owned(), *ansi, batch))
        })
        .collect::<BTreeSet<_>>();
    let observed_contexts = observations
        .iter()
        .map(context_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(observed_contexts.len(), observations.len(), "duplicate replay context");
    assert_eq!(observed_contexts, expected_contexts, "replay context coverage changed");
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    let declared_credits = controls
        .iter()
        .map(context_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    let observed_credits = observations
        .iter()
        .filter(|comparison| comparison["independent_gap_owner"].is_string())
        .map(context_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(
        observed_credits, declared_credits,
        "retire inherited controls for contexts that now match Spark"
    );
    Ok(())
}
