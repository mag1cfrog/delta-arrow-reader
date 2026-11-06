//! Java replacement expansion over native regex matching, with named external gaps.

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

#[path = "support/regexp_partition_layout.rs"]
mod regexp_partition_layout;

#[path = "support/regexp_capability.rs"]
mod regexp_capability;

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

#[tokio::test]
async fn regexp_replace_matches_spark_and_retains_external_boundaries() -> TestResult {
    let mut compared = 0;
    let mut reference_mismatches = 0;
    let mut exact_case_results = 0;
    let mut required_error_fields_compared = 0;
    let mut external_error_fields_compared = 0;
    let mut admission_errors_compared = 0;
    let mut error_physical_reference_unavailable = 0;
    let mut differences = Vec::new();
    let mut external = std::collections::BTreeMap::<&str, usize>::new();
    for (fixture_name, capture, count) in [
        (
            "regexp-replace-spark.jsonl",
            include_str!("fixtures/regexp-replace-spark.jsonl"),
            94,
        ),
        (
            "regexp-replace-expanded-spark.jsonl",
            include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
            138,
        ),
        (
            "regexp-replace-boundary-spark.jsonl",
            include_str!("fixtures/regexp-replace-boundary-spark.jsonl"),
            14,
        ),
        (
            "regexp-replace-zero-width-spark.jsonl",
            include_str!("fixtures/regexp-replace-zero-width-spark.jsonl"),
            20,
        ),
        (
            "regexp-replace-empty-utf16-spark.jsonl",
            include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"),
            252,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len() - 1, count);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(fixture_name);
        eprintln!(
            "c06 utf16 capture header: {}",
            json!({"run_label":"retained","fixture":fixture,"spark_capture_header":records[0],
                "native_target_partitions":2,"native_session_timezone":"UTC"})
        );
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let expected = &case["expected"];
                    let external_error =
                        matches!(id, "position_supplementary_3" | "nonempty_split_dot");
                    let observed = capture_observation(&session, case).await?;
                    let limit = regexp_capability::recorded_limit(
                        fixture_name,
                        case,
                        representation,
                        batch_size,
                        &observed,
                    )?;
                    if !regexp_capability::completed_stages_are_retained(&observed) {
                        differences.push(format!("{id}: missing completed native stage"));
                    }
                    let error = observed["error"].as_str();
                    let analyzed_schema = observed
                        .get("analyzed_schema")
                        .filter(|value| !value.is_null());
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
                    let status = if error.is_none() && complete_result_matches(&observed, expected)
                    {
                        "exact"
                    } else if !regexp_capability::has_recorded_case(fixture_name, case)?
                        && physical_comparison != "difference"
                        && error.is_some_and(|error| {
                            required_error_outcome_matches(error, analyzed_schema, expected)
                        })
                    {
                        if expected.get("analyzed_schema").is_some() {
                            "required_error"
                        } else {
                            "required_admission_error"
                        }
                    } else if limit == Some("utf16_split_region")
                        || (id == "position_supplementary_3"
                            && case["sql"] == "SELECT regexp_replace('a😀b','.','X',3) AS r"
                            && observed["analyzed_schema"] == expected["analyzed_schema"]
                            && observed["physical_schema"] == expected["physical_schema"]
                            && expected["rows"] == json!([["a?XX"]])
                            && observed["error_stage"] == "execute"
                            && error
                                == Some(
                                    "execute: Execution error: regexp_replace cannot start a native regex region inside a UTF-16 surrogate pair",
                                ))
                    {
                        "external_java_utf16_region_798"
                    } else if limit == Some("utf16_optional_advancement") {
                        "external_java_utf16_empty_match_798"
                    } else if limit == Some("accepted_pattern_line_terminator") {
                        "external_java_pattern_779"
                    } else {
                        differences.push(format!("{id} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["ansi"]));
                        "difference"
                    };
                    if matches!(
                        id,
                        "empty_supplementary"
                            | "nonboundary_supplementary"
                            | "nonempty_nonboundary"
                    ) && status != "exact"
                    {
                        differences
                            .push("the exact zero-width pattern repair must match Spark".into());
                    }
                    let matches_reference = matches!(
                        status,
                        "exact" | "required_error" | "required_admission_error"
                    );
                    reference_mismatches += usize::from(!matches_reference);
                    eprintln!(
                        "c06 utf16 observation: {}",
                        json!({"run_label":"retained","fixture":fixture,"id":case["id"],"ansi":case["ansi"],
                            "representation":representation,"batch_size":batch_size,"matches_reference":matches_reference,
                            "sql":case["sql"],"expected":expected,"actual":observed})
                    );
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
    eprintln!(
        "c06 utf16 capture summary: {}",
        json!({"run_label":"retained","references":518,"observations":compared,
            "capture_failures":0,"differences":reference_mismatches,"capture_only":false,
            "full_compatibility_pass":reference_mismatches == 0})
    );
    assert_eq!(compared, (94 + 138 + 14 + 20 + 252) * 6);
    assert_eq!(reference_mismatches, 60);
    assert_eq!(required_error_fields_compared, (52 + 42) * 6);
    assert_eq!(external_error_fields_compared, 4 * 6);
    assert_eq!(admission_errors_compared, (24 + 4) * 6);
    assert_eq!(error_physical_reference_unavailable, (76 + 46) * 6);
    assert_eq!(external.get("external_java_utf16_region_798"), Some(&24));
    assert_eq!(
        external.get("external_java_utf16_empty_match_798"),
        Some(&24)
    );
    assert_eq!(external.get("external_java_pattern_779"), Some(&12));
    assert_eq!(exact_case_results, 24);
    eprintln!(
        "C06 replacement compared {compared}; error analyzed fields required={required_error_fields_compared} external={external_error_fields_compared}; admission errors={admission_errors_compared}; error physical reference unavailable={error_physical_reference_unavailable}; external {external:?}; differences {}",
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn known_nonboundary_mixed_optional(actual: &Value, case: &Value) -> bool {
    let expected = &case["expected"];
    let Some(id) = case["id"].as_str() else {
        return false;
    };
    let sql = match id {
        "nonboundary_probe_column_mixed" => {
            "SELECT k,regexp_replace(s,re,replacement) AS r FROM rr_nonboundary ORDER BY k"
        }
        "nonboundary_probe_column_position_2" => {
            "SELECT k,regexp_replace(s,re,replacement,2) AS r FROM rr_nonboundary ORDER BY k"
        }
        _ => return false,
    };
    if case["sql"] != sql || !regexp_capability::completed_stages_are_retained(actual) {
        return false;
    }
    if actual["analyzed_schema"] != expected["analyzed_schema"]
        || actual["physical_schema"] != expected["physical_schema"]
    {
        return false;
    }
    let (native, spark) = match id {
        "nonboundary_probe_column_mixed" => ("<a><>😀<>b<>", "<a><>?<>?<>b<>"),
        "nonboundary_probe_column_position_2" => ("a<>😀<>b<>", "a<>?<>?<>b<>"),
        _ => return false,
    };
    let (Some(actual), Some(expected)) = (actual["rows"].as_array(), expected["rows"].as_array())
    else {
        return false;
    };
    let mut optional_rows = 0;
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            if expected[0] == "9" {
                optional_rows += 1;
                actual == &json!(["9", native]) && expected == &json!(["9", spark])
            } else {
                actual == expected
            }
        })
        && optional_rows == 1
}

#[tokio::test]
async fn regexp_replace_nonboundaries_match_spark_and_retain_adjacent_patterns() -> TestResult {
    let records = include_str!("fixtures/regexp-replace-nonboundary-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 80);
    let mut compared = 0;
    let mut exact_results = 0;
    let mut required_errors = 0;
    let mut external_optional = 0;
    let mut external_mixed = 0;
    let mut external_region = 0;
    let mut external_partition_layout = 0;
    let mut differences = Vec::new();
    eprintln!(
        "c06 nonboundary capture header: {}",
        json!({"spark_capture_header":records[0],"references":80,
            "exact_nonboundary_references":64,"adjacent_pattern_references":16,
            "native_target_partitions":2,"native_session_timezone":"UTC"})
    );
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                compared += 1;
                let id = case["id"].as_str().ok_or("missing ID")?;
                let expected = &case["expected"];
                let actual = capture_observation(&session, case).await?;
                let limit = regexp_capability::recorded_limit(
                    "regexp-replace-nonboundary-spark.jsonl",
                    case,
                    representation,
                    batch_size,
                    &actual,
                )?;
                if !regexp_capability::completed_stages_are_retained(&actual) {
                    differences.push(format!("{id}: missing completed native stage"));
                }
                let error = actual["error"].as_str();
                let analyzed = actual
                    .get("analyzed_schema")
                    .filter(|value| !value.is_null());
                let physical_comparison = schema_comparison(&actual, expected, "physical_schema");
                let error_fields_match = matches!(
                    physical_comparison,
                    "exact" | "reference_unavailable" | "unavailable"
                );
                let status = if error.is_none() && complete_result_matches(&actual, expected) {
                    exact_results += 1;
                    "exact"
                } else if error_fields_match
                    && error.is_some_and(|error| {
                        required_error_outcome_matches(error, analyzed, expected)
                    })
                {
                    required_errors += 1;
                    "required_error"
                } else if regexp_partition_layout::known_partition_layout_error(
                    case,
                    representation,
                    batch_size,
                    &actual,
                )? {
                    external_partition_layout += 1;
                    "external_partition_layout_error"
                } else if limit == Some("utf16_optional_advancement") {
                    external_optional += 1;
                    "external_optional_utf16_798"
                } else if error.is_none() && known_nonboundary_mixed_optional(&actual, case) {
                    external_mixed += 1;
                    "external_mixed_optional_utf16_798"
                } else if limit == Some("utf16_split_region") {
                    external_region += 1;
                    "external_split_region_utf16_798"
                } else {
                    differences.push(format!("{id} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {actual}",case["ansi"]));
                    "difference"
                };
                eprintln!(
                    "c06 nonboundary observation: {}",
                    json!({"id":id,"ansi":case["ansi"],"sql":case["sql"],
                        "representation":representation,"batch_size":batch_size,
                        "status":status,"matches_reference":matches!(status,"exact" | "required_error"),
                        "schema_comparison":{"analyzed":schema_comparison(&actual,expected,"analyzed_schema"),
                            "physical":physical_comparison},"expected":expected,"actual":actual})
                );
            }
        }
    }
    let strict_differences = compared - exact_results - required_errors;
    eprintln!(
        "c06 nonboundary capture summary: {}",
        json!({"references":80,"observations":compared,"strict_differences":strict_differences,
            "full_compatibility_pass":strict_differences == 0,"unexpected_differences":differences.len(),
            "external_partition_layout_errors":external_partition_layout})
    );
    assert_eq!(compared, 480);
    assert_eq!(exact_results, 384);
    assert_eq!(required_errors, 42);
    assert_eq!(external_partition_layout, 6);
    assert_eq!(external_optional, 12);
    assert_eq!(external_mixed, 24);
    assert_eq!(external_region, 12);
    assert_eq!(strict_differences, 54);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn regexp_native_capability_limits_retain_strict_spark_differences() -> TestResult {
    use std::collections::{BTreeMap, HashSet};

    let mut keys = HashSet::new();
    let mut exact = 0;
    let mut limits = BTreeMap::new();
    let mut differences = Vec::new();
    for (fixture, capture, references) in [
        (
            "regex-syntax-missing-spark.jsonl",
            include_str!("fixtures/regex-syntax-missing-spark.jsonl"),
            38,
        ),
        (
            "regex-line-missing-spark.jsonl",
            include_str!("fixtures/regex-line-missing-spark.jsonl"),
            88,
        ),
        (
            "regex-utf16-missing-spark.jsonl",
            include_str!("fixtures/regex-utf16-missing-spark.jsonl"),
            30,
        ),
        (
            "regex-word-categories-spark.jsonl",
            include_str!("fixtures/regex-word-categories-spark.jsonl"),
            24,
        ),
    ] {
        let records = capture
            .split('\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len() - 1, references);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        eprintln!(
            "c06 regex capability header: {}",
            json!({"fixture":fixture,"spark_capture_header":records[0],
                "native_target_partitions":2,"native_session_timezone":"UTC",
                "native_optimized_schema_available":false,"support_decision_approved":false})
        );
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    let key = json!({"fixture":fixture,"id":case["id"],"ansi":case["ansi"],
                        "representation":representation,"batch_size":batch_size});
                    assert!(keys.insert(key.to_string()), "duplicate observation {key}");
                    let actual = capture_observation(&session, case).await?;
                    let matches_reference =
                        regexp_capability::complete_match(&actual, &case["expected"]);
                    let limit = regexp_capability::recorded_limit(
                        fixture,
                        case,
                        representation,
                        batch_size,
                        &actual,
                    )?;
                    if matches_reference {
                        exact += 1;
                        assert!(
                            limit.is_none(),
                            "a positive control cannot use a gap profile"
                        );
                    } else if let Some(limit) = limit {
                        *limits.entry(limit).or_insert(0) += 1;
                    }
                    if !regexp_capability::completed_stages_are_retained(&actual)
                        || (!matches_reference && limit.is_none())
                    {
                        differences.push(format!(
                            "{key}: expected {}, observed {actual}",
                            case["expected"]
                        ));
                    }
                    if fixture == "regex-word-categories-spark.jsonl"
                        && case["id"] == "regexp_replace_nonboundary_literal"
                    {
                        assert_eq!(actual["rows"][0][1], json!("a|?|?|b"));
                        assert_eq!(actual["rows"][0][1], case["expected"]["rows"][0][1]);
                    }
                    if fixture == "regex-word-categories-spark.jsonl"
                        && case["id"] == "regexp_replace_nonboundary_column"
                    {
                        assert_eq!(actual["rows"][1], json!(["2", "a|?|?|b"]));
                        assert_eq!(actual["rows"][1], case["expected"]["rows"][1]);
                    }
                    eprintln!(
                        "c06 regex capability observation: {}",
                        json!({"key":key,"sql":case["sql"],"known_limit":limit,
                            "matches_reference":matches_reference,"full_compatibility_pass":false,
                            "native_optimized_schema_available":false,
                            "analyzed_comparison":schema_comparison(&actual,&case["expected"],"analyzed_schema"),
                            "physical_comparison":schema_comparison(&actual,&case["expected"],"physical_schema"),
                            "expected":case["expected"],"actual":actual})
                    );
                }
            }
        }
    }
    assert_eq!(keys.len(), 180 * 6);
    assert_eq!(exact, 660);
    assert_eq!(
        limits,
        BTreeMap::from([
            ("accepted_pattern_line_terminator", 192),
            ("java_only_lookbehind_syntax", 84),
            ("malformed_pattern_diagnostic", 24),
            ("native_unicode_word_classification", 96),
            ("utf16_optional_advancement", 12),
            ("utf16_split_region", 12),
        ])
    );
    eprintln!(
        "c06 regex capability summary: {}",
        json!({"references":180,"observations":keys.len(),"exact":exact,
            "strict_differences":420,"known_limits":limits,"unexpected_differences":differences.len(),
            "full_compatibility_pass":false,"final_current_staging_replay_required":true})
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn regexp_replace_preserves_native_sql() -> TestResult {
    let plain = SessionContext::new();
    let session = session(32);
    let mut native = Vec::new();
    for sql in [
        r"SELECT regexp_replace('ab12cd34','([0-9]+)', E'X\\1Y','g') AS r",
        "SELECT regexp_replace('a😀b','','X','g') AS r",
        r"SELECT regexp_replace('a😀b', E'\\B','X','g') AS r",
        "SELECT '${x}' AS literal_x,'${missing}' AS literal_missing,regexp_replace('a','(a)','${x}','g') AS r",
    ] {
        let expected = plain.sql(sql).await?.collect().await?;
        assert_eq!(session.sql(sql).await?.collect().await?, expected);
        native.push((sql, expected));
    }
    for ansi in [true, false] {
        for sql in [
            "SELECT regexp_replace('ab12cd34','([0-9]+)','$1X') AS r",
            "SELECT regexp_replace('a😀b','','X') AS r",
            r"SELECT regexp_replace('a😀b',r'\B','X') AS r",
        ] {
            capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
        }
        for (sql, expected) in &native {
            assert_eq!(session.sql(sql).await?.collect().await?, *expected);
        }
        for enabled in [true, false] {
            let actual = capture_result(&session, &json!({
                "sql":"SELECT '${x}' AS r", "ansi":ansi,
                "settings":{"spark.sql.variable.substitute":enabled.to_string(),"x":"configured"},
            })).await?;
            assert_eq!(
                actual["rows"],
                json!([[if enabled { "configured" } else { "${x}" }]])
            );
            for (sql, expected) in &native {
                assert_eq!(session.sql(sql).await?.collect().await?, *expected);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn sql_variable_substitution_keeps_read_only_single_statement_validation() -> TestResult {
    let session = session(32);
    let table = "substitution_must_not_create_this_table";
    assert!(!session.table_exist(table)?);
    for sql in [
        "SELECT 1; SELECT 2".to_owned(),
        format!("CREATE TABLE {table} AS SELECT 1 AS value"),
    ] {
        let options = SparkSqlOptions {
            variable_substitution_values: std::collections::BTreeMap::from([(
                "statement".into(),
                sql,
            )]),
            ..Default::default()
        };
        assert!(
            plan_query(&session, "${statement}", &options)
                .await
                .is_err()
        );
        assert!(!session.table_exist(table)?);
    }
    Ok(())
}

fn substitution_error_matches(case: &Value, actual: &Value) -> bool {
    let expected = &case["expected"];
    let Some(error) = actual["error"].as_str() else {
        return false;
    };
    match expected["condition"].as_str() {
        Some("INVALID_REGEXP_REPLACE") => {
            let stage = if expected["phase"] == "collect" {
                "execute"
            } else {
                "physical"
            };
            actual["error_stage"] == stage
                && required_error_outcome_matches(error, actual.get("analyzed_schema"), expected)
                && schema_comparison(actual, expected, "physical_schema") != "difference"
        }
        Some("PARSE_SYNTAX_ERROR") => {
            actual["error_stage"] == "resolve"
                && actual["analyzed_schema"].is_null()
                && actual["physical_schema"].is_null()
                && match case["id"].as_str() {
                    Some("quote_changes_sql_syntax") => {
                        expected["parameters"]["error"] == "'''"
                            && error.starts_with("resolve: error in SQL parser:")
                    }
                    Some("multiple_statement_input") => {
                        expected["parameters"]["error"] == "'SELECT'"
                            && error == "resolve: invalid argument: expected one statement"
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

#[tokio::test]
async fn sql_variable_substitution_matches_spark_configuration_references() -> TestResult {
    let records = include_str!("fixtures/sql-variable-substitution-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 116);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut complete_results = 0;
    let mut required_errors = 0;
    let mut parse_errors = 0;
    let mut differences = Vec::new();
    eprintln!("sql substitution capture header: {}", records[0]);
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                let actual = capture_observation(&session, case).await?;
                let expected = &case["expected"];
                let complete = expected["status"] == "ok"
                    && actual["error"].is_null()
                    && complete_result_matches(&actual, expected);
                let required_error = substitution_error_matches(case, &actual);
                let stages_retained = regexp_capability::completed_stages_are_retained(&actual);
                let matches_reference = stages_retained && (complete || required_error);
                complete_results += usize::from(complete);
                required_errors += usize::from(required_error);
                parse_errors +=
                    usize::from(required_error && expected["condition"] == "PARSE_SYNTAX_ERROR");
                let error_identifier_matches =
                    expected["condition"].as_str().is_none_or(|condition| {
                        actual["error"]
                            .as_str()
                            .is_some_and(|error| error.contains(&format!("[{condition}]")))
                    });
                eprintln!(
                    "sql substitution observation: {}",
                    json!({
                        "id":case["id"],"ansi":case["ansi"],"settings":case["settings"],
                        "representation":representation,"batch_size":batch_size,
                        "sql":case["sql"],"expected":expected,"actual":actual,
                        "schema_comparison":{"analyzed":schema_comparison(&actual,expected,"analyzed_schema"),
                            "physical":schema_comparison(&actual,expected,"physical_schema")},
                        "completed_stages_retained":stages_retained,"matches_reference":matches_reference,
                        "spark_error_identifier_matches":error_identifier_matches,
                        "native_optimized_schema_available":false,
                    })
                );
                if !matches_reference {
                    differences.push(format!(
                        "{} ANSI={} representation={representation} batch={batch_size}: {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    assert_eq!(complete_results, 96 * 6);
    assert_eq!(required_errors, 20 * 6);
    assert_eq!(parse_errors, 4 * 6);
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

#[tokio::test]
async fn regexp_replace_evaluates_each_volatile_argument_once() -> TestResult {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

    for (capture, table) in [
        (
            include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"),
            "rr_utf16",
        ),
        (
            include_str!("fixtures/regexp-replace-nonboundary-spark.jsonl"),
            "rr_nonboundary",
        ),
    ] {
        let metadata: Value =
            serde_json::from_str(capture.lines().next().ok_or("missing metadata")?)?;
        for (representation, data_type) in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
            .into_iter()
            .enumerate()
        {
            for batch_size in [1, 32] {
                for ansi_mode in [true, false] {
                    let session = session(batch_size);
                    register_inputs(&session, &metadata, representation, batch_size)?;
                    let evaluated_rows: [Arc<AtomicUsize>; 3] =
                        std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
                    for (name, counter) in ["count_source", "count_pattern", "count_replacement"]
                        .into_iter()
                        .zip(&evaluated_rows)
                    {
                        let counter = Arc::clone(counter);
                        session.register_udf(create_udf(
                            name,
                            vec![data_type.clone()],
                            data_type.clone(),
                            Volatility::Volatile,
                            Arc::new(move |arguments| {
                                let arrays = ColumnarValue::values_to_arrays(arguments)?;
                                counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
                                Ok(ColumnarValue::Array(Arc::clone(&arrays[0])))
                            }),
                        ));
                    }
                    let query = plan_query(
                        &session,
                        &format!("SELECT k,regexp_replace(count_source(s),count_pattern(re),count_replacement(replacement)) AS r FROM {table} ORDER BY k"),
                        &SparkSqlOptions { ansi_mode, ..Default::default() },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(
                        evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
                        [0; 3]
                    );
                    let batches =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
                    assert_eq!(
                        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        14
                    );
                    assert_eq!(
                        evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst)),
                        [14; 3]
                    );
                    for batch in batches {
                        assert_eq!(batch.column(1).data_type(), &data_type);
                    }
                }
            }
        }
    }
    Ok(())
}
