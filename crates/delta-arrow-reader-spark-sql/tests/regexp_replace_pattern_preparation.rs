//! Literal pattern preparation and its NULL, row-dependent and empty-input boundaries.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[expect(
    dead_code,
    reason = "This target uses full observations rather than the result-only helper."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation};

#[path = "support/spark_results.rs"]
mod spark_results;

fn records(capture: &str) -> TestResult<Vec<Value>> {
    Ok(capture
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

fn key(case: &Value) -> TestResult<(&str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

fn complete_keys(actual: &Value, error: bool) -> bool {
    let mut expected = BTreeSet::from([
        "analyzed_schema",
        "analyzed_arrow_schema",
        "physical_schema",
        "physical_arrow_schema",
    ]);
    if error {
        expected.extend(["error", "error_stage"]);
    } else {
        expected.insert("rows");
    }
    actual.as_object().is_some_and(|object| {
        object.keys().map(String::as_str).collect::<BTreeSet<_>>() == expected
    })
}

fn pattern_error_matches(error: &str, expected: &Value) -> bool {
    let Some(cause) = expected["java_causes"]
        .as_array()
        .and_then(|causes| causes.last())
    else {
        return false;
    };
    expected["condition"] == "INVALID_PARAMETER_VALUE.PATTERN"
        && expected["parameters"]["parameter"] == "`regexp`"
        && expected["parameters"]["functionName"] == "`regexp_replace`"
        && expected["parameters"]["value"] == "'['"
        && cause["class"] == "java.util.regex.PatternSyntaxException"
        && cause["message"] == "Unclosed character class near index 0\n[\n^"
        && error.contains(
            "[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern \"[\": regex parse error:",
        )
        && error.ends_with("error: unclosed character class")
}

fn matches_reference(actual: &Value, expected: &Value) -> bool {
    if actual.get("analyzed_schema") != expected.get("analyzed_schema")
        || expected
            .get("schema")
            .is_some_and(|schema| actual.get("analyzed_schema") != Some(schema))
        || actual["analyzed_arrow_schema"]
            .as_str()
            .is_none_or(|schema| schema.is_empty())
    {
        return false;
    }
    if expected["status"] == "ok" {
        return complete_keys(actual, false)
            && actual.get("physical_schema") == expected.get("physical_schema")
            && actual["physical_arrow_schema"]
                .as_str()
                .is_some_and(|schema| !schema.is_empty())
            && actual.get("rows") == expected.get("rows");
    }
    let stage = match expected["phase"].as_str() {
        Some("optimized_schema") => "physical",
        Some("collect") => "execute",
        _ => return false,
    };
    complete_keys(actual, true)
        && actual["error_stage"] == stage
        && actual["error"].as_str().is_some_and(|error| {
            error.starts_with(&format!("{stage}: ")) && pattern_error_matches(error, expected)
        })
        && if stage == "physical" {
            expected.get("physical_schema").is_none()
                && expected.get("physical_schema_before_collect").is_none()
                && actual["physical_schema"].is_null()
                && actual["physical_arrow_schema"].is_null()
        } else {
            // This Spark collect failure records the actual pre-collect Fields.
            let fields = expected
                .get("physical_schema")
                .or_else(|| expected.get("physical_schema_before_collect"));
            fields.is_some()
                && actual.get("physical_schema") == fields
                && actual["physical_arrow_schema"]
                    .as_str()
                    .is_some_and(|schema| !schema.is_empty())
        }
}

fn marker_absent(session: &SessionContext) -> bool {
    session
        .state()
        .config_options()
        .extensions
        .get::<SparkSqlExpressionSemantics>()
        .is_none()
}

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

async fn native_controls(session: &SessionContext, context: Value) -> TestResult<Value> {
    let mut observations = Vec::new();
    for sql in [
        "SELECT regexp_replace('abc123','([0-9]+)','<$1>') AS r",
        "SELECT regexp_replace('a','[','x') AS r",
    ] {
        observations.push(spark_results::capture_native_query_observation(session, sql).await?);
    }
    let actual = json!(observations);
    eprintln!(
        "c06 pattern native preparation controls: {}",
        json!({"context":context,"actual":actual,
            "observation_scope":"normalized_fields_rows_and_stage_errors",
            "arrow_representation":"not_recorded_by_this_observer",
            "batch_layout":"not_recorded_by_this_observer",
        })
    );
    Ok(actual)
}

async fn compare_cases(
    corpus: &str,
    header: Option<&Value>,
    cases: &[&Value],
    expected_key: &str,
    differences: &mut Vec<String>,
) -> TestResult<usize> {
    let native = native_controls(
        &SessionContext::new(),
        json!({"corpus":corpus,"provider_inputs":header.is_some(),"phase":"plain"}),
    )
    .await?;
    let mut compared = BTreeSet::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            if let Some(header) = header {
                string_binary::register_inputs(&session, header, representation, batch_size)?;
                for input in header["registered_inputs"]
                    .as_array()
                    .ok_or("missing provider inputs")?
                {
                    let name = input["name"].as_str().ok_or("missing provider name")?;
                    let schema = session.table_provider(name).await?.schema();
                    assert_eq!(
                        string_binary::capture_schema(schema.fields(), None)?,
                        input["schema"]
                    );
                    let string_type = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
                        [representation]
                        .clone();
                    for (field, declared) in schema.fields().iter().zip(
                        input["schema"]["fields"]
                            .as_array()
                            .ok_or("missing provider Fields")?,
                    ) {
                        if declared["type"] == "string" {
                            assert_eq!(field.data_type(), &string_type);
                        }
                    }
                }
            }
            assert!(marker_absent(&session));
            assert_eq!(native_controls(&session, json!({"corpus":corpus,"provider_inputs":header.is_some(),"phase":"before","representation":representation,"batch_size":batch_size})).await?, native);
            for case in cases {
                assert!(marker_absent(&session));
                let actual = capture_observation(&session, case).await?;
                assert!(marker_absent(&session));
                let matches = matches_reference(&actual, &case[expected_key]);
                assert!(compared.insert((key(case)?, representation, batch_size)));
                eprintln!(
                    "c06 pattern preparation regression: {}",
                    json!({
                        "corpus":corpus,"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
                        "settings":case.get("settings"),"representation":representation,
                        "batch_size":batch_size,"provider_inputs":header.is_some(),
                        "actual":actual,"matches_reference":matches,"native_optimized_schema":"unexposed"
                    })
                );
                if !matches {
                    differences.push(format!(
                        "{corpus}/{:?}/{representation}/{batch_size}: {actual}",
                        key(case)?
                    ));
                }
            }
            assert_eq!(native_controls(&session, json!({"corpus":corpus,"provider_inputs":header.is_some(),"phase":"after","representation":representation,"batch_size":batch_size})).await?, native);
            assert!(marker_absent(&session));
        }
    }
    assert_eq!(compared.len(), cases.len() * 6);
    Ok(compared.len())
}

#[tokio::test]
async fn literal_patterns_match_all_18_independent_preparation_references() -> TestResult {
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-replace-pattern-preparation-inputs.json"
    ))?;
    let references = records(include_str!(
        "fixtures/regexp-replace-pattern-preparation-spark.jsonl"
    ))?;
    assert_eq!(inputs["providers"], json!([]));
    assert_eq!(references.len(), 14);
    assert_eq!(references[0]["kind"], "header");
    assert_eq!(references[0]["runtime_identity"]["spark_version"], "4.2.0");
    assert_eq!(
        references.last(),
        Some(
            &json!({"kind":"summary","observations":12,"provider_receipts":0,
            "collection_complete":true,"compatibility_credit":false})
        )
    );
    let cases = &references[1..references.len() - 1];
    let by_key = cases
        .iter()
        .map(|case| Ok((key(case)?, case)))
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    assert_eq!(by_key.len(), 12);
    for id in [
        "pattern_dead_case",
        "pattern_where_false",
        "pattern_outer_limit_zero",
        "pattern_values_outer_limit_zero",
        "pattern_concat_null_first",
        "pattern_concat_null_last",
    ] {
        for ansi in [true, false] {
            assert!(by_key.contains_key(&(id, ansi)));
        }
    }
    let declared = inputs["cases"].as_array().ok_or("missing input cases")?;
    assert_eq!(declared.len(), 12);
    for input in declared {
        let case = by_key.get(&key(input)?).ok_or("missing Spark reference")?;
        assert_eq!(case["kind"], "observation");
        assert_eq!(case["compatibility_credit"], false);
        for (name, value) in input.as_object().ok_or("invalid input case")? {
            assert_eq!(case.get(name), Some(value));
        }
        for (name, availability) in case["availability"]
            .as_object()
            .ok_or("missing stage availability")?
        {
            let observed = match case["actual"].get(name) {
                None => "absent",
                Some(Value::Null) => "null",
                Some(_) => "available",
            };
            assert_eq!(availability.as_str(), Some(observed));
        }
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            input["ansi"].to_string()
        );
        for (name, value) in inputs["settings"].as_object().ok_or("missing settings")? {
            let effective = input["settings"].get(name).unwrap_or(value);
            assert_eq!(case["effective_settings"].get(name), Some(effective));
        }
        assert!(
            case["absent_case_settings"]
                .as_object()
                .ok_or("missing absent settings")?
                .values()
                .all(Value::is_null)
        );
    }
    let mut differences = Vec::new();
    // These 12 scalar queries have no providers. Carrier labels repeat the
    // six-layout contract; STRING input storage cannot affect their execution.
    let mut compared = compare_cases(
        "pattern-guards",
        None,
        &cases.iter().collect::<Vec<_>>(),
        "actual",
        &mut differences,
    )
    .await?;
    for (corpus, capture, id) in [
        (
            "regexp-replace",
            include_str!("fixtures/regexp-replace-spark.jsonl"),
            "regexp_replace_literal_invalid_pattern",
        ),
        (
            "regexp-replace-expanded",
            include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
            "position_invalid_past_end",
        ),
        (
            "regexp-replace-empty-utf16",
            include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"),
            "bad_pattern_beyond",
        ),
    ] {
        let references = records(capture)?;
        let cases = references[1..]
            .iter()
            .filter(|case| case["id"] == id)
            .collect::<Vec<_>>();
        assert_eq!(cases.len(), 2);
        assert_eq!(
            cases
                .iter()
                .map(|case| key(case).map(|(_, ansi)| ansi))
                .collect::<TestResult<BTreeSet<_>>>()?,
            BTreeSet::from([false, true])
        );
        compared += compare_cases(corpus, None, &cases, "expected", &mut differences).await?;
    }
    assert_eq!(compared, 108);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn malformed_pattern_controls_keep_null_row_and_empty_boundaries() -> TestResult {
    let mut compared = 0;
    let mut differences = Vec::new();
    for (corpus, capture, ids) in [
        (
            "regexp-replace",
            include_str!("fixtures/regexp-replace-spark.jsonl"),
            vec![
                "regexp_replace_literal_no_match",
                "regexp_replace_literal_empty_source",
                "regexp_replace_literal_null_source",
                "regexp_replace_literal_null_pattern",
                "regexp_replace_literal_null_replacement",
                "admission_regexp_replace_position_null",
            ],
        ),
        (
            "regexp-replace-expanded",
            include_str!("fixtures/regexp-replace-expanded-spark.jsonl"),
            vec![
                "rr_nullable",
                "empty_bad_pattern",
                "guarded_case",
                "guarded_filter",
                "position_bad_replacement_past_end",
                "position_null_invalid",
                "null_source_bad_pattern",
                "null_replacement_bad_pattern",
            ],
        ),
        (
            "regexp-replace-empty-utf16",
            include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"),
            vec!["empty_at_start", "empty_beyond"],
        ),
        (
            "regex-syntax-missing",
            include_str!("fixtures/regex-syntax-missing-spark.jsonl"),
            vec!["regexp_replace_column_malformed"],
        ),
    ] {
        let references = records(capture)?;
        let cases = references[1..]
            .iter()
            .filter(|case| case["id"].as_str().is_some_and(|id| ids.contains(&id)))
            .collect::<Vec<_>>();
        assert_eq!(cases.len(), ids.len() * 2);
        for id in ids {
            assert_eq!(
                cases
                    .iter()
                    .filter(|case| case["id"] == id)
                    .map(|case| key(case).map(|(_, ansi)| ansi))
                    .collect::<TestResult<BTreeSet<_>>>()?,
                BTreeSet::from([false, true])
            );
        }
        compared += compare_cases(
            corpus,
            Some(&references[0]),
            &cases,
            "expected",
            &mut differences,
        )
        .await?;
    }
    assert_eq!(compared, 204);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
