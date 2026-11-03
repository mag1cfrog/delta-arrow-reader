//! Java replacement expansion over native regex matching, with named external gaps.

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

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

fn complete_result_matches(actual: &Value, expected: &Value) -> bool {
    ["analyzed_schema", "physical_schema", "rows"]
        .into_iter()
        .all(|key| actual[key] == expected[key])
}

fn schema_comparison(actual: &Value, expected: &Value, key: &str) -> &'static str {
    match (
        actual.get(key).filter(|value| !value.is_null()),
        expected.get(key).filter(|value| !value.is_null()),
    ) {
        (Some(actual), Some(expected)) if actual == expected => "exact",
        (Some(_), Some(_)) => "difference",
        (Some(_), None) => "reference_unavailable",
        (None, Some(_)) => "actual_unavailable",
        (None, None) => "unavailable",
    }
}

fn known_utf16_empty_matches(actual: &Value, expected: &Value, id: &str) -> bool {
    if actual["analyzed_schema"] != expected["analyzed_schema"]
        || actual["physical_schema"] != expected["physical_schema"]
    {
        return false;
    }
    let (native, spark) = match id {
        "empty_supplementary" => ("XaX😀XbX", "XaX?X?XbX"),
        "optional_supplementary" => ("<a><>😀<>b<>", "<a><>?<>?<>b<>"),
        "nonboundary_supplementary" => ("a😀b", "a?X?b"),
        _ => return false,
    };
    actual["rows"] == json!([[native]]) && expected["rows"] == json!([[spark]])
}

// Match the independent Java diagnostic to its native parser category.
fn native_pattern_cause(expected: &Value) -> Option<(&str, &str)> {
    let cause = expected["java_causes"].as_array()?.last()?;
    if cause["class"] != "java.util.regex.PatternSyntaxException" {
        return None;
    }
    let mut lines = cause["message"].as_str()?.lines();
    let cause = match lines.next()? {
        "Unclosed character class near index 0" => "unclosed character class",
        "Unexpected internal error near index 1" => {
            "incomplete escape sequence, reached end of pattern prematurely"
        }
        "named capturing group is missing trailing '>' near index 6" => {
            "unclosed capture group name"
        }
        "Unknown inline modifier near index 3" => "expected flag but got end of regex",
        _ => return None,
    };
    Some((lines.next()?, cause))
}

fn required_error_matches(error: &str, expected: &Value) -> bool {
    let Some(condition) = expected["condition"].as_str() else {
        return false;
    };
    if !error.contains(&format!("[{condition}]")) {
        return false;
    }
    match condition {
        "INVALID_REGEXP_REPLACE" => expected["java_causes"]
            .as_array()
            .and_then(|causes| causes.last())
            .is_some_and(|cause| {
                error.contains(cause["class"].as_str().unwrap_or("missing class"))
                    && error.contains(cause["message"].as_str().unwrap_or("missing cause"))
            }),
        "INVALID_PARAMETER_VALUE.PATTERN" => {
            native_pattern_cause(expected).is_some_and(|(pattern, cause)| {
                error.contains(&format!(
                    "regexp_replace pattern {pattern:?}: regex parse error:"
                )) && error.contains(&format!("error: {cause}"))
            })
        }
        "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT" => {
            error.contains("position") && error.contains("foldable")
        }
        "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE" => {
            error.contains("position") && error.contains("(0, 2147483647]")
        }
        "WRONG_NUM_ARGS.WITHOUT_SUGGESTION" => {
            error.contains("regexp_replace") && error.contains("3 or 4")
        }
        _ => false,
    }
}

fn required_error_outcome_matches(
    error: &str,
    analyzed_schema: Option<&Value>,
    expected: &Value,
) -> bool {
    analyzed_schema == expected.get("analyzed_schema") && required_error_matches(error, expected)
}

#[test]
fn error_outcomes_require_available_analyzed_fields() -> TestResult {
    let records = include_str!("fixtures/regexp-replace-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let expected = &records
        .iter()
        .find(|case| case["id"] == "regexp_replace_literal_bad_group")
        .ok_or("missing failing replacement")?["expected"];
    let error = "[INVALID_REGEXP_REPLACE] java.lang.IndexOutOfBoundsException: No group 2";
    let schema = &expected["analyzed_schema"];
    assert!(required_error_outcome_matches(
        error,
        Some(schema),
        expected
    ));
    assert!(!required_error_outcome_matches(error, None, expected));
    for (key, value) in [
        ("name", json!("wrong_name")),
        ("type", json!("integer")),
        ("nullable", json!(true)),
        ("metadata", json!({"wrong":"metadata"})),
    ] {
        let mut incorrect = schema.clone();
        incorrect["fields"][0][key] = value;
        assert!(!required_error_outcome_matches(
            error,
            Some(&incorrect),
            expected,
        ));
    }
    let observed = json!({"physical_schema":schema});
    assert_eq!(
        schema_comparison(&observed, expected, "physical_schema"),
        "reference_unavailable"
    );
    assert_eq!(
        schema_comparison(&json!({}), expected, "physical_schema"),
        "unavailable"
    );
    assert_eq!(
        schema_comparison(&json!({}), &observed, "physical_schema"),
        "actual_unavailable"
    );
    Ok(())
}

#[test]
fn pattern_errors_require_the_recorded_pattern_and_cause() -> TestResult {
    let mut checked = 0;
    for capture in [
        include_str!("fixtures/regexp-replace-spark.jsonl"),
        include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
    ] {
        for record in capture.lines().skip(1) {
            let record: Value = serde_json::from_str(record)?;
            let expected = &record["expected"];
            if expected["condition"] != "INVALID_PARAMETER_VALUE.PATTERN" {
                continue;
            }
            checked += 1;
            let (pattern, cause) = native_pattern_cause(expected).ok_or("missing pattern cause")?;
            let actual = format!(
                "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern {pattern:?}: regex parse error:\n    {pattern}\nerror: {cause}"
            );
            assert!(required_error_matches(&actual, expected), "{actual}");
            assert!(!required_error_matches(
                "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace: regex parse error",
                expected,
            ));
            assert!(!required_error_matches(
                &format!(
                    "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern \"wrong pattern\": regex parse error:\nerror: {cause}"
                ),
                expected,
            ));
            assert!(!required_error_matches(
                &format!(
                    "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern {pattern:?}: regex parse error:\nerror: wrong cause"
                ),
                expected,
            ));
        }
    }
    assert_eq!(checked, 14);
    Ok(())
}

// Keep full original mixed queries visible while their independent field owners land.
fn known_shared_fields(actual: &Value, expected: &Value, id: &str) -> bool {
    if !matches!(id, "frozen_strings_regexp" | "unaliased_pattern_names")
        || actual["rows"] != expected["rows"]
    {
        return false;
    }
    for phase in ["analyzed_schema", "physical_schema"] {
        let (Some(actual), Some(expected)) = (
            actual[phase]["fields"].as_array(),
            expected[phase]["fields"].as_array(),
        ) else {
            return false;
        };
        if actual.len() != expected.len() {
            return false;
        }
        for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
            let name_matches = actual["name"] == expected["name"]
                || (id == "unaliased_pattern_names"
                    && index == 0
                    && actual["name"] == "like(a, %)"
                    && expected["name"] == "a LIKE %");
            if !name_matches
                || actual["nullable"] != expected["nullable"]
                || actual["type"] != expected["type"]
                || !(actual["metadata"] == expected["metadata"]
                    || (actual["metadata"] == json!({})
                        && expected["metadata"] == json!({"__autoGeneratedAlias":"true"})))
            {
                return false;
            }
        }
    }
    true
}

#[tokio::test]
async fn regexp_replace_matches_spark_and_retains_external_boundaries() -> TestResult {
    let mut compared = 0;
    let mut exact_case_results = 0;
    let mut required_error_fields_compared = 0;
    let mut external_error_fields_compared = 0;
    let mut admission_errors_compared = 0;
    let mut position_analysis_unavailable = 0;
    let mut error_physical_reference_unavailable = 0;
    let mut differences = Vec::new();
    let mut external = std::collections::BTreeMap::<&str, usize>::new();
    for (capture, count) in [
        (include_str!("fixtures/regexp-replace-spark.jsonl"), 94),
        (
            include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
            138,
        ),
        (
            include_str!("fixtures/regexp-replace-boundary-spark.jsonl"),
            14,
        ),
        (
            include_str!("fixtures/regexp-replace-zero-width-spark.jsonl"),
            20,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len() - 1, count);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let expected = &case["expected"];
                    let external_error = matches!(
                        id,
                        "regexp_replace_literal_bad_named_group" | "position_supplementary_3"
                    );
                    let position_coercion = matches!(
                        id,
                        "position_numeric_type" | "position_decimal_type" | "position_string_type"
                    );
                    let observed = capture_observation(&session, case).await?;
                    let error = observed["error"].as_str();
                    let analyzed_schema = observed
                        .get("analyzed_schema")
                        .filter(|value| !value.is_null());
                    let analysis_unavailable =
                        analyzed_schema.is_none() && observed["error_stage"] == "resolve";
                    let physical_comparison =
                        schema_comparison(&observed, expected, "physical_schema");
                    if expected["status"] == "error" {
                        if expected.get("analyzed_schema").is_some() {
                            required_error_fields_compared += 1;
                        } else {
                            admission_errors_compared += 1;
                        }
                        if expected.get("physical_schema").is_none() {
                            error_physical_reference_unavailable += 1;
                        }
                    }
                    if external_error {
                        external_error_fields_compared += 1;
                    }
                    if position_coercion && analysis_unavailable {
                        position_analysis_unavailable += 1;
                    }
                    let status = if error.is_none() && complete_result_matches(&observed, expected) {
                        "exact"
                    } else if physical_comparison != "difference" && error.is_some_and(|error| required_error_outcome_matches(error, analyzed_schema, expected)) {
                        if expected.get("analyzed_schema").is_some() { "required_error" } else { "required_admission_error" }
                    } else if error.is_none() && known_shared_fields(&observed, expected, id) {
                        "external_shared_fields_710_791"
                    } else if (id == "regexp_replace_literal_named_group" && error.is_none()
                        && observed["analyzed_schema"] == expected["analyzed_schema"] && observed["physical_schema"] == expected["physical_schema"]
                        && observed["rows"] == json!([["a-b"]]) && expected["rows"] == json!([["-b"]]))
                        || (id == "regexp_replace_literal_bad_named_group" && physical_comparison != "difference" && analyzed_schema == Some(&expected["analyzed_schema"]) && expected["rows"] == json!([[""]]) && error.is_some_and(|error| {
                        error.contains("[INVALID_REGEXP_REPLACE]") && error.contains("java.lang.IllegalArgumentException: No group with name {missing}")
                    })) {
                        "external_sql_variable_substitution_789"
                    } else if id == "position_supplementary_3" && physical_comparison != "difference" && analyzed_schema == Some(&expected["analyzed_schema"]) && expected["rows"] == json!([["a?XX"]]) && error.is_some_and(|error| error.contains("cannot start a native regex region inside a UTF-16 surrogate pair")) {
                        "external_java_utf16_region_798"
                    } else if error.is_none() && known_utf16_empty_matches(&observed, expected, id) {
                        "external_java_utf16_empty_match_798"
                    } else if position_coercion && analysis_unavailable
                        && expected["rows"] == json!([["a1X"]]) && error.is_some_and(|error| {
                            error.contains("regexp_replace position coercion to INT is outside the supported position contract")
                        }) {
                        "external_position_coercion_799_analysis_unavailable"
                    } else if id == "anchored_last_newline" && expected["rows"] == json!([["a\n"]]) && error.is_none()
                        && observed["analyzed_schema"] == expected["analyzed_schema"] && observed["physical_schema"] == expected["physical_schema"] && observed["rows"] == json!([["abc\n"]]) {
                        "external_java_pattern_779"
                    } else {
                        differences.push(format!("{id} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["ansi"]));
                        "difference"
                    };
                    if matches!(id, "guarded_case" | "guarded_literal") && status == "exact" {
                        exact_case_results += 1;
                    }
                    if status.starts_with("external_") {
                        *external.entry(status).or_default() += 1;
                    }
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":id,"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"status":status,
                            "schema_comparison":{"analyzed":schema_comparison(&observed,expected,"analyzed_schema"),"physical":physical_comparison},"actual":observed})
                    );
                }
            }
        }
    }
    assert_eq!(compared, (94 + 138 + 14 + 20) * 6);
    assert_eq!(required_error_fields_compared, 52 * 6);
    assert_eq!(external_error_fields_compared, 4 * 6);
    assert_eq!(admission_errors_compared, 24 * 6);
    assert_eq!(position_analysis_unavailable, 6 * 6);
    assert_eq!(error_physical_reference_unavailable, 76 * 6);
    assert_eq!(
        external.get("external_sql_variable_substitution_789"),
        Some(&24)
    );
    assert_eq!(external.get("external_java_utf16_region_798"), Some(&12));
    assert_eq!(
        external.get("external_java_utf16_empty_match_798"),
        Some(&36)
    );
    assert_eq!(
        external.get("external_position_coercion_799_analysis_unavailable"),
        Some(&36)
    );
    assert_eq!(external.get("external_java_pattern_779"), Some(&12));
    assert_eq!(exact_case_results, 24);
    eprintln!(
        "C06 replacement compared {compared}; error analyzed fields required={required_error_fields_compared} external={external_error_fields_compared}; admission errors={admission_errors_compared}; position analysis unavailable={position_analysis_unavailable}; error physical reference unavailable={error_physical_reference_unavailable}; external {external:?}; differences {}",
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn regexp_replace_preserves_native_sql() -> TestResult {
    let sql = r"SELECT regexp_replace('ab12cd34','([0-9]+)', E'X\\1Y','g') AS r";
    let plain = SessionContext::new();
    let expected = plain.sql(sql).await?.collect().await?;
    let session = session(32);
    assert_eq!(session.sql(sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        capture_result(
            &session,
            &json!({"sql":"SELECT regexp_replace('ab12cd34','([0-9]+)','$1X') AS r","ansi":ansi}),
        )
        .await?;
        assert_eq!(session.sql(sql).await?.collect().await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn regexp_replace_preserves_declared_arrow_strings() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/regexp-replace-expanded-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    for (representation, data_type) in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
        .into_iter()
        .enumerate()
    {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &metadata, representation, batch_size)?;
            let query = plan_query(
                &session,
                "SELECT regexp_replace(s,re,replacement) AS r FROM rr_valid ORDER BY k",
                &SparkSqlOptions::default(),
            )
            .await?;
            assert_eq!(query.logical_plan.schema().field(0).data_type(), &data_type);
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(physical.schema().field(0).data_type(), &data_type);
            for batch in datafusion::physical_plan::collect(physical, session.task_ctx()).await? {
                assert_eq!(batch.column(0).data_type(), &data_type);
            }
        }
    }
    Ok(())
}
