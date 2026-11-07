//! REGEXP operator names retain Spark fields, rows and required pattern errors.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code, reason = "This suite selects the shared stage observers.")]
#[path = "support/spark_results.rs"]
mod spark_results;
#[allow(dead_code, reason = "This suite uses complete observations, including errors.")]
#[path = "support/string_binary.rs"]
mod string_binary;
use spark_results::{capture_native_query_observation, capture_spark_optimized_observation};
use string_binary::{TestResult, capture_observation, register_inputs};

const REFERENCE: &str = include_str!("fixtures/regexp-operator-names-spark.jsonl");

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

fn required_pattern_error_matches(expected: &Value, actual: &Value) -> bool {
    let phase = match expected["phase"].as_str() {
        Some("optimized_schema") => "physical",
        Some("collect") => "execute",
        _ => return false,
    };
    let recorded_cause = expected["java_causes"].as_array().is_some_and(|causes| {
        causes.iter().any(|cause| {
            cause["class"] == "java.util.regex.PatternSyntaxException"
                && cause["message"] == "Unclosed character class near index 0\n[\n^"
        })
    });
    recorded_cause
        && actual["error_stage"] == phase
        && actual.get("rows").is_none()
        && actual["error"].as_str().is_some_and(|error| {
            error.starts_with(&format!("{phase}: "))
                && error.contains("regex parse error:\n    [\n")
                && error.contains("error: unclosed character class")
        })
}

#[test]
fn required_pattern_errors_reject_wrong_phase_pattern_and_cause() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for case in records.iter().filter(|case| case["require_error"] == true) {
        let phase = if case["expected"]["phase"] == "optimized_schema" {
            "physical"
        } else {
            "execute"
        };
        // Synthetic diagnostics test the matcher; integration logs retain actual errors.
        let error = format!(
            "{phase}: regex parse error:\n    [\n    ^\nerror: unclosed character class"
        );
        let actual = json!({"error_stage":phase, "error":error});
        assert!(required_pattern_error_matches(&case["expected"], &actual));
        for wrong in [
            json!({"error_stage":"resolve", "error":error}),
            json!({"error_stage":phase, "error":error.replace("[\n", "(\n")}),
            json!({"error_stage":phase, "error":error.replace("unclosed character class", "unrelated error")}),
            json!({"error_stage":phase, "error":error, "rows":[]}),
        ] {
            assert!(!required_pattern_error_matches(&case["expected"], &wrong));
        }
        let mut missing_cause = case["expected"].clone();
        missing_cause["java_causes"] = json!([]);
        assert!(!required_pattern_error_matches(&missing_cause, &actual));
    }
    Ok(())
}

#[tokio::test]
async fn regexp_operator_names_match_independent_spark_stages() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 23);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?.to_owned(),
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        assert_eq!(case["timezone"], "UTC");
        assert_eq!(case["effective_spark_sql_settings"]["spark.sql.parser.escapedStringLiterals"], "false");
        assert_eq!(case["effective_spark_sql_settings"]["spark.sql.variable.substitute"], "true");
        if case["id"] == "regexp_operator_original_mixed" {
            assert_eq!(case["sql"], "SELECT 'ab' RLIKE '(a)', 'ab' REGEXP '(a)', rlike('ab','(a)'), regexp_like('ab','(a)'), regexp_extract('ab','(a)',1), regexp_replace('ab','a','x')");
        }
    }
    let mut observations = Vec::new();
    let mut differences = Vec::new();
    let mut successes = 0;
    let mut errors = 0;
    let mut original_mixed = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                let id = case["id"].as_str().ok_or("missing ID")?;
                let ansi = case["ansi"].as_bool().ok_or("missing ANSI")?;
                original_mixed += usize::from(id == "regexp_operator_original_mixed");
                let expected = &case["expected"];
                let actual = capture_observation(&session, case).await?;
                let optimized = if expected["optimized_schema"].is_object() {
                    match capture_spark_optimized_observation(&session, case).await {
                        Ok(observed) => observed,
                        Err(error) => json!({"error":error.to_string()}),
                    }
                } else {
                    Value::Null
                };
                let mut comparison = json!({
                    "public_schema":actual["analyzed_schema"] == expected["schema"],
                    "analyzed_schema":actual["analyzed_schema"] == expected["analyzed_schema"],
                    "analyzed_raw":actual["analyzed_arrow_schema"].is_string(),
                });
                if expected["optimized_schema"].is_object() {
                    comparison["optimized_names"] = json!(optimized["names_match"] == true);
                    comparison["optimized_schema"] = json!(optimized["schema"] == expected["optimized_schema"]);
                    comparison["optimized_raw"] = json!(optimized["raw_schema"].is_object());
                }
                if expected["physical_schema"].is_object() {
                    comparison["physical_schema"] = json!(actual["physical_schema"] == expected["physical_schema"]);
                }
                if expected["status"] == "ok" {
                    successes += 1;
                    comparison["status"] = json!(actual["error"].is_null());
                    comparison["rows"] = json!(actual["rows"] == expected["rows"]);
                    comparison["physical_raw"] = json!(actual["physical_arrow_schema"].is_string());
                } else {
                    assert_eq!(expected["status"], "error");
                    assert_eq!(case["require_error"], true);
                    errors += 1;
                    comparison["required_error_cause_and_stage"] = json!(required_pattern_error_matches(expected, &actual));
                    comparison["completed_physical_fields"] = json!(if actual["error_stage"] == "execute" {
                        actual["physical_schema"].is_object() && actual["physical_arrow_schema"].is_string()
                    } else {
                        actual["physical_schema"].is_null() && actual["physical_arrow_schema"].is_null()
                    });
                }
                let matches = comparison.as_object().ok_or("missing comparison")?
                    .values().all(|facet| facet == true);
                let observation = json!({
                    "id":id, "ansi":ansi, "representation":representation, "batch_size":batch_size,
                    "reference":case, "actual":actual, "optimized":optimized,
                    "comparison":comparison, "required_contract_matches":matches,
                    "raw_error_text_equality":if expected["status"] == "error" {json!(actual["error"] == expected["message"])} else {Value::Null},
                    "available_reference_stages":{
                        "public":expected["schema"].is_object(), "analyzed":expected["analyzed_schema"].is_object(),
                        "optimized":expected["optimized_schema"].is_object(), "physical":expected["physical_schema"].is_object(),
                    },
                });
                eprintln!("regexp operator observation: {observation}");
                observations.push(observation);
                if !matches {
                    differences.push(format!("{id} ANSI={ansi} representation={representation} batch={batch_size}"));
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_REGEXP_OPERATOR_NAMES_REPORT") {
        serde_json::to_writer_pretty(std::fs::File::create_new(path)?, &observations)?;
    }
    let expected_contexts = identities.iter().flat_map(|(id, ansi)| {
        (0_u64..3).flat_map(move |representation| {
            [1_u64, 32].map(move |batch_size| (id.clone(), *ansi, representation, batch_size))
        })
    }).collect::<BTreeSet<_>>();
    let observed_contexts = observations.iter().map(|observed| {
        Ok((
            observed["id"].as_str().ok_or("missing observed ID")?.to_owned(),
            observed["ansi"].as_bool().ok_or("missing observed ANSI")?,
            observed["representation"].as_u64().ok_or("missing observed representation")?,
            observed["batch_size"].as_u64().ok_or("missing observed batch size")?,
        ))
    }).collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(observed_contexts.len(), observations.len(), "duplicate observation context");
    assert_eq!(observed_contexts, expected_contexts);
    assert_eq!(observations.len(), 132);
    assert_eq!(successes, 108);
    assert_eq!(errors, 24);
    assert_eq!(original_mixed, 12);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn regexp_operator_repair_preserves_native_sql() -> TestResult {
    let metadata: Value = serde_json::from_str(REFERENCE.lines().next().ok_or("missing metadata")?)?;
    let sql_controls = [
        ("SELECT regexp_like('ab','(a)') AS matched, 'ab' LIKE 'a%' AS like_match, 'Ab' ILIKE 'a%' AS ilike_match", false),
        ("SELECT k, regexp_like(s,re) AS matched, s LIKE pattern AS like_match, s ILIKE pattern AS ilike_match FROM regexp_required ORDER BY k", false),
        ("SELECT k, regexp_like(s,re) AS matched FROM regexp_nullable ORDER BY k", false),
        ("SELECT k, regexp_like(s,re) AS matched FROM regexp_empty ORDER BY k", false),
        ("SELECT regexp_like('ab','[') AS matched", true),
    ];
    let mut observations = Vec::new();
    let mut differences = Vec::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let plain = SessionContext::new_with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            );
            let configured = session(batch_size);
            for session in [&plain, &configured] {
                register_inputs(session, &metadata, representation, batch_size)?;
            }
            for (sql, expected_error) in sql_controls {
                let expected = capture_native_query_observation(&plain, sql).await?;
                let actual = capture_native_query_observation(&configured, sql).await?;
                let required_status_matches = if expected_error {
                    actual["phase"] == "create_physical_plan"
                        && actual["rows"].is_null()
                        && actual["public_schema"].is_object()
                        && actual["before_collect"].is_null()
                        && actual["physical_schema"].is_null()
                        && actual["error"].as_str().is_some_and(|error| {
                            error.contains("regex parse error:\n    [\n")
                                && error.contains("error: unclosed character class")
                        })
                } else {
                    actual["error"].is_null()
                        && actual["public_schema"].is_object()
                        && actual["before_collect"].is_object()
                        && actual["physical_schema"].is_object()
                        && actual["rows"].is_array()
                };
                let matches = actual == expected && required_status_matches;
                let observation = json!({"sql":sql,"representation":representation,"batch_size":batch_size,
                    "expected_error":expected_error,"plain":expected,"configured":actual,
                    "required_status_matches":required_status_matches,"matches":matches});
                eprintln!("regexp operator native observation: {observation}");
                observations.push(observation);
                if !matches {
                    differences.push(format!("native representation={representation} batch={batch_size}: {sql}"));
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_REGEXP_OPERATOR_NAMES_NATIVE_REPORT") {
        serde_json::to_writer_pretty(std::fs::File::create_new(path)?, &observations)?;
    }
    let expected_contexts = sql_controls.into_iter().flat_map(|(sql, _)| {
        (0_u64..3).flat_map(move |representation| {
            [1_u64, 32].map(move |batch_size| (sql.to_owned(), representation, batch_size))
        })
    }).collect::<BTreeSet<_>>();
    let observed_contexts = observations.iter().map(|observed| {
        Ok((
            observed["sql"].as_str().ok_or("missing observed SQL")?.to_owned(),
            observed["representation"].as_u64().ok_or("missing observed representation")?,
            observed["batch_size"].as_u64().ok_or("missing observed batch size")?,
        ))
    }).collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(observed_contexts.len(), observations.len(), "duplicate native observation context");
    assert_eq!(observed_contexts, expected_contexts);
    assert_eq!(observations.len(), 30);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
