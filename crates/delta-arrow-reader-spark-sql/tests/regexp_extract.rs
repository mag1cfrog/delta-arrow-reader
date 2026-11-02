//! REGEXP_EXTRACT fields, supported patterns and evaluation match Spark.

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_result, register_inputs};

fn matches_error(case: &Value, error: &str) -> bool {
    if !error.contains("regexp_extract") {
        return false;
    }
    match case["expected"]["condition"].as_str() {
        Some("INVALID_PARAMETER_VALUE.PATTERN") => {
            let cause = match case["id"].as_str() {
                Some("regexp_extract_column_case_1") => "incomplete escape sequence",
                Some("regexp_extract_column_case_2") => "unclosed capture group name",
                Some("regexp_extract_column_case_3") => "expected flag but got end of regex",
                _ => "unclosed character class",
            };
            error.contains("Invalid regex pattern") && error.contains(cause)
        }
        Some("INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX") => {
            let parameters = &case["expected"]["parameters"];
            match (
                parameters["groupCount"].as_str(),
                parameters["groupIndex"].as_str(),
            ) {
                (Some(count), Some(index)) => error.lines().any(|line| {
                    line.ends_with(&format!(
                        "Expects group index between 0 and {count}, but got {index}"
                    ))
                }),
                _ => false,
            }
        }
        _ => false,
    }
}

async fn capture_analyzed_schema(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await?;
    let fields = query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let data_type = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
                other => return Err(format!("unexpected extraction field type: {other}").into()),
            };
            Ok(json!({
                "name":query.column_names.get(index).ok_or("missing result name")?,
                "type":data_type,"nullable":field.is_nullable(),"metadata":field.metadata()
            }))
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok(json!({"type":"struct","fields":fields}))
}

#[test]
fn regexp_extract_error_matcher_requires_complete_group_numbers() -> TestResult {
    let records = include_str!("fixtures/regexp-extract-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for (id, index, wrong_index) in [
        ("regexp_extract_literal_missing_index", "2", "2147483647"),
        ("regexp_extract_literal_negative_index", "-1", "-10"),
    ] {
        let case = records
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == true)
            .ok_or("missing group error reference")?;
        let diagnostic = |range, supplied| {
            format!(
                "execute: Execution error: Spark `regexp_extract` function: Expects group index between 0 and {range}, but got {supplied}"
            )
        };
        assert!(matches_error(case, &diagnostic("1", index)));
        assert!(!matches_error(case, &diagnostic("1", wrong_index)));
        assert!(!matches_error(case, &diagnostic("11", index)));
    }
    Ok(())
}

#[tokio::test]
async fn regexp_extract_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    let mut compared = 0;
    let mut error_fields_compared = 0;
    let mut engine_boundaries = 0;
    for (capture, count) in [
        (include_str!("fixtures/regexp-extract-spark.jsonl"), 82),
        (
            include_str!("fixtures/regexp-extract-evaluation-spark.jsonl"),
            140,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        for representation in 0..3 {
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
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    let expected = &case["expected"];
                    let analyzed_schema = if expected["status"] == "error" {
                        // Public fields exist before physical planning or execution fails.
                        Some(capture_analyzed_schema(&session, case).await?)
                    } else {
                        None
                    };
                    let actual = capture_result(&session, case).await;
                    let observed = match &actual {
                        Ok(value) => value.clone(),
                        Err(error) => {
                            json!({"error":error.to_string(),"analyzed_schema":analyzed_schema})
                        }
                    };
                    // Spark accepts this pattern. Its native-engine gap belongs to #779.
                    let engine_boundary = case["id"] == "regexp_extract_literal_lookbehind";
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"classification":if engine_boundary {"engine_boundary_779"} else {"compatibility"},"actual":observed})
                    );
                    let matches = if engine_boundary {
                        engine_boundaries += 1;
                        assert_eq!(expected["status"], "ok");
                        actual.as_ref().is_err_and(|error| {
                            let error = error.to_string();
                            error.contains("regexp_extract")
                                && error.contains("look-around, including look-ahead and look-behind, is not supported")
                        })
                    } else {
                        compared += 1;
                        if expected["status"] == "error" {
                            error_fields_compared += 1;
                            analyzed_schema.as_ref() == Some(&expected["analyzed_schema"])
                                && actual
                                    .as_ref()
                                    .is_err_and(|error| matches_error(case, &error.to_string()))
                        } else {
                            actual.as_ref().is_ok_and(|value| {
                                value["analyzed_schema"] == expected["analyzed_schema"]
                                    && value["physical_schema"] == expected["physical_schema"]
                                    && value["rows"] == expected["rows"]
                            })
                        }
                    };
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, 220 * 6);
    assert_eq!(error_fields_compared, 30 * 6);
    assert_eq!(engine_boundaries, 2 * 6);
    eprintln!(
        "C06 compared {compared}, including {error_fields_compared} error-field comparisons, separate engine boundaries {engine_boundaries}, mismatches {}",
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn regexp_extract_preserves_native_regex_functions() -> TestResult {
    let native_sql = "SELECT regexp_match('abc123','([0-9]+)') AS r";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        let result = capture_result(
            &session,
            &json!({"sql":"SELECT regexp_extract('abc123','([0-9]+)',1) AS r","ansi":ansi}),
        )
        .await?;
        assert_eq!(result["rows"], json!([["123"]]));
        let schema = json!({"type":"struct","fields":[{"name":"r","type":"string","nullable":false,"metadata":{}}]});
        assert_eq!(result["analyzed_schema"], schema);
        assert_eq!(result["physical_schema"], schema);
        assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    }
    Ok(())
}
