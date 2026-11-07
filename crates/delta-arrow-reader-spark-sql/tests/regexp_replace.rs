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
                    } else if limit == Some("utf16_split_region") {
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

async fn native_batches(
    session: &SessionContext,
    sql: &str,
    context: Value,
) -> TestResult<Vec<arrow::record_batch::RecordBatch>> {
    let frame = session.sql(sql).await?;
    let analyzed_schema = string_binary::capture_schema(frame.schema().fields(), None)?;
    let analyzed_arrow_schema = format!("{:?}", frame.schema());
    let physical = frame.create_physical_plan().await?;
    let before = physical.schema();
    let batches = datafusion::physical_plan::collect(physical.clone(), session.task_ctx()).await?;
    let schema = physical.schema();
    let result = string_binary::capture_batches(&batches)?;
    eprintln!(
        "c06 replacement native SQL: {}",
        json!({"sql":sql,"context":context,"actual":{
            "public_schema":analyzed_schema,"public_arrow_schema":analyzed_arrow_schema,
            "before_collect":string_binary::capture_schema(before.fields(), None)?,
            "before_collect_arrow_schema":format!("{before:?}"),
            "physical_schema":string_binary::capture_schema(schema.fields(), None)?,
            "physical_arrow_schema":format!("{schema:?}"),
            "rows":result["rows"],"error":null,"phase":"encode","result":result,
        },"spark_custom_udf_oracle":false})
    );
    Ok(batches)
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
        let expected = native_batches(&plain, sql, json!({"phase":"plain"})).await?;
        assert_eq!(
            native_batches(&session, sql, json!({"phase":"configured_before"})).await?,
            expected
        );
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
            assert_eq!(
                native_batches(&session, sql, json!({"phase":"after_spark","ansi":ansi})).await?,
                *expected
            );
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
                assert_eq!(
                    native_batches(
                        &session,
                        sql,
                        json!({"phase":"after_substitution","ansi":ansi,"enabled":enabled})
                    )
                    .await?,
                    *expected
                );
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
            let [
                Some(source),
                Some(pattern),
                Some(replacement),
                Some(position),
            ] = ["source", "pattern", "replacement", "position"]
                .map(|key| expected["parameters"][key].as_str())
            else {
                return false;
            };
            let has_required_phase = (expected["phase"] == "collect"
                && actual["error_stage"] == "execute")
                || (expected["phase"] == "optimized_schema"
                    && matches!(
                        case["id"].as_str(),
                        Some(
                            "disabled_undefined_regexp_replace_literal_bad_named_group"
                                | "disabled_defined_regexp_replace_literal_bad_named_group"
                                | "defined_invalid_replacement_group"
                        )
                    )
                    && actual["error_stage"] == "physical"
                    && actual.get("physical_schema") == Some(&Value::Null)
                    && actual.get("physical_arrow_schema") == Some(&Value::Null));
            has_required_phase
                && required_error_outcome_matches(error, actual.get("analyzed_schema"), expected)
                && error.contains(&format!(
                    "[INVALID_REGEXP_REPLACE] source={source:?}, pattern={pattern:?}, replacement={replacement:?}, position={position}:"
                ))
                && schema_comparison(actual, expected, "physical_schema") != "difference"
        }
        Some("PARSE_SYNTAX_ERROR") => {
            expected["phase"] == "sql"
                && actual["error_stage"] == "resolve"
                && actual["analyzed_schema"].is_null()
                && actual["physical_schema"].is_null()
                && match case["id"].as_str() {
                    Some("quote_changes_sql_syntax") => {
                        expected["parameters"]["error"] == "'''"
                            && expected["parameters"]["hint"] == ""
                            && error
                                == "resolve: External error: error in SQL parser: found end of input expected any, '\\', or '''"
                    }
                    Some("multiple_statement_input") => {
                        expected["parameters"]["error"] == "'SELECT'"
                            && expected["parameters"]["hint"] == ": extra input 'SELECT'"
                            && error
                                == "resolve: External error: invalid argument: expected one statement"
                    }
                    _ => false,
                }
        }
        None if matches!(
            case["id"].as_str(),
            Some("enabled_self_cycle" | "enabled_two_key_cycle")
        ) =>
        {
            let Some(causes) = expected["java_causes"].as_array() else {
                return false;
            };
            let [cause] = causes.as_slice() else {
                return false;
            };
            let Some(message) = cause["message"].as_str() else {
                return false;
            };
            expected["status"] == "error"
                && expected["condition"].is_null()
                && expected["phase"] == "sql"
                && cause["class"] == "java.lang.IllegalArgumentException"
                && cause["condition"].is_null()
                && expected["message"] == message
                && actual["error_stage"] == "resolve"
                && actual["analyzed_schema"].is_null()
                && actual["physical_schema"].is_null()
                && error
                    == format!(
                        "resolve: External error: invalid argument: java.lang.IllegalArgumentException: {message}"
                    )
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
    assert_eq!(records.len() - 1, 124);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut complete_results = 0;
    let mut required_errors = 0;
    let mut parse_errors = 0;
    let mut cycle_errors = 0;
    let mut phase_differences = 0;
    let mut identifier_differences = 0;
    let mut strict_matches = 0;
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
                let semantic_contract_matches = stages_retained && (complete || required_error);
                complete_results += usize::from(complete);
                required_errors += usize::from(required_error);
                parse_errors +=
                    usize::from(required_error && expected["condition"] == "PARSE_SYNTAX_ERROR");
                cycle_errors += usize::from(required_error && expected["condition"].is_null());
                let error_identifier_matches = expected["condition"].as_str().map(|condition| {
                    actual["error"]
                        .as_str()
                        .is_some_and(|error| error.contains(&format!("[{condition}]")))
                });
                let phase_matches = expected["phase"].as_str().map(|phase| match phase {
                    "sql" => actual["error_stage"] == "resolve",
                    "optimized_schema" => actual["error_stage"] == "physical",
                    "collect" => actual["error_stage"] == "execute",
                    _ => false,
                });
                let matches_reference = semantic_contract_matches
                    && phase_matches != Some(false)
                    && (expected["status"] == "ok" || error_identifier_matches == Some(true));
                phase_differences += usize::from(phase_matches == Some(false));
                identifier_differences += usize::from(error_identifier_matches == Some(false));
                strict_matches += usize::from(matches_reference);
                eprintln!(
                    "sql substitution observation: {}",
                    json!({
                        "id":case["id"],"ansi":case["ansi"],"settings":case["settings"],
                        "representation":representation,"batch_size":batch_size,
                        "sql":case["sql"],"expected":expected,"actual":actual,
                        "schema_comparison":{"analyzed":schema_comparison(&actual,expected,"analyzed_schema"),
                            "physical":schema_comparison(&actual,expected,"physical_schema")},
                        "completed_stages_retained":stages_retained,"matches_reference":matches_reference,
                        "semantic_contract_matches":semantic_contract_matches,
                        "spark_phase_matches":phase_matches,
                        "spark_error_identifier_matches":error_identifier_matches,
                        "native_optimized_schema_available":false,
                    })
                );
                if !semantic_contract_matches {
                    differences.push(format!(
                        "{} ANSI={} representation={representation} batch={batch_size}: {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    assert_eq!(complete_results, 100 * 6);
    assert_eq!(required_errors, 24 * 6);
    assert_eq!(parse_errors, 4 * 6);
    assert_eq!(cycle_errors, 4 * 6);
    assert_eq!(phase_differences, 0);
    assert_eq!(identifier_differences, 4 * 6);
    assert_eq!(strict_matches, 116 * 6);
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
            let schema = physical.schema();
            let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
            for batch in &batches {
                assert_eq!(batch.column(0).data_type(), &data_type);
            }
            eprintln!(
                "c06 replacement Arrow strings: {}",
                json!({"representation":representation,"batch_size":batch_size,
                    "sql":"SELECT regexp_replace(s,re,replacement) AS r FROM rr_valid ORDER BY k",
                    "analyzed_schema":string_binary::capture_schema(query.logical_plan.schema().fields(), Some(&query.column_names))?,
                    "analyzed_arrow_schema":format!("{:?}", query.logical_plan.schema()),
                    "physical_schema":string_binary::capture_schema(schema.fields(), None)?,
                    "physical_arrow_schema":format!("{schema:?}"),
                    "result":string_binary::capture_batches(&batches)?,
                })
            );
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
                    let sql = format!(
                        "SELECT k,regexp_replace(count_source(s),count_pattern(re),count_replacement(replacement)) AS r FROM {table} ORDER BY k"
                    );
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    let before_collect =
                        evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst));
                    assert_eq!(before_collect, [0; 3]);
                    let schema = physical.schema();
                    let batches =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
                    assert_eq!(
                        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        14
                    );
                    let after_collect = evaluated_rows.each_ref().map(|n| n.load(Ordering::SeqCst));
                    assert_eq!(after_collect, [14; 3]);
                    for batch in &batches {
                        assert_eq!(batch.column(1).data_type(), &data_type);
                    }
                    eprintln!(
                        "c06 replacement volatile arguments: {}",
                        json!({"table":table,"sql":sql,"representation":representation,"batch_size":batch_size,"ansi":ansi_mode,
                            "analyzed_schema":string_binary::capture_schema(query.logical_plan.schema().fields(), Some(&query.column_names))?,
                            "analyzed_arrow_schema":format!("{:?}", query.logical_plan.schema()),
                            "physical_schema":string_binary::capture_schema(schema.fields(), None)?,
                            "physical_arrow_schema":format!("{schema:?}"),
                            "before_collect":before_collect,"after_collect":after_collect,
                            "result":string_binary::capture_batches(&batches)?,"spark_custom_udf_oracle":false,
                        })
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn regexp_replace_reads_all_referenced_provider_layouts() -> TestResult {
    use datafusion::physical_plan::{ExecutionPlanProperties, collect_partitioned};

    // Bind only providers referenced by these replacement corpora.
    let fixtures: &[(&str, &str, &[&str])] = &[
        (
            "regex-line-missing-spark.jsonl",
            include_str!("fixtures/regex-line-missing-spark.jsonl"),
            &["rr_line_boundary"],
        ),
        (
            "regex-syntax-missing-spark.jsonl",
            include_str!("fixtures/regex-syntax-missing-spark.jsonl"),
            &["rr_syntax_boundary", "rr_syntax_empty"],
        ),
        (
            "regex-utf16-missing-spark.jsonl",
            include_str!("fixtures/regex-utf16-missing-spark.jsonl"),
            &["rr_utf16", "rr_utf16_empty", "rr_nonboundary"],
        ),
        (
            "regex-word-categories-spark.jsonl",
            include_str!("fixtures/regex-word-categories-spark.jsonl"),
            &["c06_word_categories"],
        ),
        (
            "regexp-replace-empty-utf16-spark.jsonl",
            include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"),
            &["rr_utf16", "rr_utf16_empty", "rr_utf16_errors"],
        ),
        (
            "regexp-replace-expanded-spark.jsonl",
            include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
            &[
                "rr_valid",
                "rr_invalid",
                "rr_nullable",
                "rr_empty_required",
                "rr_empty_nullable",
                "rr_all_null",
                "rr_guard",
            ],
        ),
        (
            "regexp-replace-nonboundary-spark.jsonl",
            include_str!("fixtures/regexp-replace-nonboundary-spark.jsonl"),
            &["rr_nonboundary", "rr_nonboundary_empty"],
        ),
        (
            "regexp-replace-position-int-spark.jsonl",
            include_str!("fixtures/regexp-replace-position-int-spark.jsonl"),
            &["rp799", "rp799_empty"],
        ),
        (
            "regexp-replace-position-type-priority-spark.jsonl",
            include_str!("fixtures/regexp-replace-position-type-priority-spark.jsonl"),
            &["type_priority", "type_priority_empty"],
        ),
        (
            "regexp-replace-spark.jsonl",
            include_str!("fixtures/regexp-replace-spark.jsonl"),
            &[
                "c06_strings_required",
                "c06_strings_nullable",
                "c06_strings_empty_required",
                "c06_strings_empty_nullable",
                "c06_regex_cases",
            ],
        ),
        (
            "sql-variable-substitution-spark.jsonl",
            include_str!("fixtures/sql-variable-substitution-spark.jsonl"),
            &["rr789_required", "rr789_nullable"],
        ),
    ];
    let mut definitions: Vec<(Value, Vec<&str>)> = Vec::new();
    let mut bindings = 0;
    for (fixture, capture, names) in fixtures {
        let header: Value = serde_json::from_str(capture.lines().next().ok_or("missing header")?)?;
        let inputs = header
            .get("registered_inputs")
            .or_else(|| header.get("inputs"))
            .and_then(Value::as_array)
            .ok_or("missing providers")?;
        for name in *names {
            let matching = inputs
                .iter()
                .filter(|input| input["name"] == *name)
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{fixture}/{name}");
            let input = matching[0];
            if let Some(index) = definitions
                .iter()
                .position(|(definition, _)| definition == input)
            {
                definitions[index].1.push(fixture);
            } else {
                definitions.push((input.clone(), vec![fixture]));
            }
            bindings += 1;
        }
    }
    assert_eq!(bindings, 30);
    assert_eq!(definitions.len(), 27);
    let mut observed = 0;
    for (definition_id, (input, fixtures)) in definitions.iter().enumerate() {
        let name = input["name"].as_str().ok_or("missing provider name")?;
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(
                    &session,
                    &json!({"registered_inputs":[input]}),
                    representation,
                    batch_size,
                )?;
                let provider = session.table_provider(name).await?;
                let schema = provider.schema();
                assert_eq!(
                    string_binary::capture_schema(schema.fields(), None)?,
                    input["schema"]
                );
                for (field, declared) in schema.fields().iter().zip(
                    input["schema"]["fields"]
                        .as_array()
                        .ok_or("missing fields")?,
                ) {
                    let expected = match declared["type"].as_str().ok_or("missing type")? {
                        "integer" => DataType::Int32,
                        "long" => DataType::Int64,
                        "string" => [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
                            [representation]
                            .clone(),
                        "binary" => [
                            DataType::Binary,
                            DataType::LargeBinary,
                            DataType::BinaryView,
                        ][representation]
                            .clone(),
                        other => return Err(format!("unexpected provider type: {other}").into()),
                    };
                    assert_eq!(field.data_type(), &expected, "{name}/{}", field.name());
                }
                let frame = session.table(name).await?;
                assert_eq!(frame.schema().inner().as_ref(), schema.as_ref());
                let state = session.state();
                let physical = provider.scan(&state, None, &[], None).await?;
                let physical_schema = physical.schema();
                assert_eq!(physical_schema, schema);
                let partition_count = physical.output_partitioning().partition_count();
                let partitions = collect_partitioned(physical.clone(), session.task_ctx()).await?;
                let executed_schema = physical.schema();
                assert_eq!(executed_schema, physical_schema);
                assert_eq!(partition_count, 2);
                assert_eq!(partitions.len(), partition_count);
                let expected_rows = input["rows"].as_array().ok_or("missing provider rows")?;
                let mut actual = Vec::new();
                for (partition, batches) in partitions.iter().enumerate() {
                    for batch in batches {
                        assert_eq!(batch.schema(), schema);
                    }
                    let result = string_binary::capture_batches(batches)?;
                    let expected_batches = expected_rows
                        .chunks(batch_size)
                        .enumerate()
                        .filter(|(chunk, _)| chunk % 2 == partition)
                        .map(|(_, rows)| rows)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        result["rows"],
                        json!(
                            expected_batches
                                .iter()
                                .flat_map(|rows| rows.iter())
                                .collect::<Vec<_>>()
                        )
                    );
                    assert_eq!(
                        batches
                            .iter()
                            .map(|batch| batch.num_rows())
                            .collect::<Vec<_>>(),
                        expected_batches
                            .iter()
                            .map(|rows| rows.len())
                            .collect::<Vec<_>>()
                    );
                    actual.push(json!({"partition":partition,"result":result}));
                }
                eprintln!(
                    "c06 replacement provider layout: {}",
                    json!({"definition_id":definition_id,"definition":input,"fixtures":fixtures,
                        "settings":{"batch_size":state.config().batch_size(),"target_partitions":state.config().target_partitions()},"representation":representation,
                        "name":name,"provider_schema":string_binary::capture_schema(schema.fields(), None)?,
                        "provider_arrow_schema":format!("{schema:?}"),
                        "public_schema":string_binary::capture_schema(frame.schema().fields(), None)?,
                        "public_arrow_schema":format!("{:?}",frame.schema()),
                        "before_collect_schema":string_binary::capture_schema(physical_schema.fields(), None)?,
                        "before_collect_arrow_schema":format!("{physical_schema:?}"),
                        "physical_schema":string_binary::capture_schema(executed_schema.fields(), None)?,
                        "physical_arrow_schema":format!("{executed_schema:?}"),
                        "actual_partition_count":partition_count,"partitions":actual,"error":null,
                    })
                );
                observed += 1;
            }
        }
    }
    assert_eq!(observed, 162);
    Ok(())
}
