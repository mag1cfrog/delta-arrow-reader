//! Nested paths have separate projection and enclosing-expression display names.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn records() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/nested-field-labels-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

#[tokio::test]
async fn nested_field_names_match_spark_with_recorded_open_gaps() -> TestResult {
    let records = records()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 172);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
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
            let id = case["id"].as_str().ok_or("missing ID")?;
            if let Some(condition) = case["expected"]["condition"].as_str() {
                assert!(matches!(
                    condition,
                    "FIELD_NOT_FOUND" | "UNRESOLVED_COLUMN.WITH_SUGGESTION"
                ));
                let result = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                assert!(
                    result
                        .is_err_and(|error| error.to_string().contains("cannot resolve attribute"))
                );
                continue;
            }
            let actual = capture_spark_query_result(&session, case).await;
            if id == "outer_generated_window" && actual.is_err() {
                // #727 owns the missing OVER clause and its outer consumer.
                assert!(
                    actual
                        .unwrap_err()
                        .to_string()
                        .contains("cannot resolve attribute")
                );
                continue;
            }
            let comparison = match actual {
                Ok(mut actual) => {
                    let mut expected = case["expected"].clone();
                    apply_recorded_comparison_exceptions(id, &mut expected, &mut actual)?;
                    if actual == expected {
                        None
                    } else {
                        Some(format!("expected {expected}, got {actual}"))
                    }
                }
                Err(error) => Some(error.to_string()),
            };
            if let Some(difference) = comparison {
                differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: {difference}",
                    case["input"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn apply_recorded_comparison_exceptions(
    id: &str,
    expected: &mut Value,
    actual: &mut Value,
) -> TestResult {
    exclude_recorded_generated_alias_markers(expected, actual)?;
    normalize_recorded_boolean_text(expected)?;
    let actual_field = &actual["schema"]["fields"][0];
    let expected_field = &mut expected["schema"]["fields"][0];
    let known_name_gap = match id {
        // #757 owns constructor alias text in generated labels.
        "constructed" => Some("struct(item).item"),
        "struct_alias" => Some("struct(renamed)"),
        // #727 owns the complete OVER clause.
        "window_partition" | "window_order" | "window_qualified" | "window_quoted" => {
            Some("SUM(v)")
        }
        "window_argument" => Some("SUM(nested.item)"),
        _ => None,
    };
    if let Some(name) = known_name_gap
        && actual_field["name"] != expected_field["name"]
    {
        assert_eq!(
            actual_field["name"], name,
            "unclassified name difference for {id}"
        );
        expected_field["name"] = json!(name);
    }
    if id == "struct" && actual_field["type"] != expected_field["type"] {
        // #757 owns inferred STRUCT child names, separately from expression display.
        assert_eq!(actual_field["type"]["fields"][0]["name"], "col1");
        expected_field["type"]["fields"][0]["name"] = json!("col1");
    }
    // #710 also owns inherited metadata on generated results.
    let known_metadata_gap = match id {
        "constructed" => Some(json!({"source":"plain"})),
        "cast" => Some(json!({"source":"nested"})),
        _ => None,
    };
    if let Some(metadata) = known_metadata_gap
        && actual_field["metadata"] != expected_field["metadata"]
    {
        assert_eq!(
            actual_field["metadata"], metadata,
            "unclassified metadata difference for {id}"
        );
        expected_field["metadata"] = metadata;
    }
    Ok(())
}

fn exclude_recorded_generated_alias_markers(
    expected: &mut Value,
    actual: &mut Value,
) -> TestResult {
    // #710 owns markers on generated outputs; explicit aliases keep exact metadata.
    let expected_fields = expected["schema"]["fields"]
        .as_array_mut()
        .ok_or("missing fields")?;
    let actual_fields = actual["schema"]["fields"]
        .as_array_mut()
        .ok_or("missing fields")?;
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (expected, actual) in expected_fields.iter_mut().zip(actual_fields) {
        if let Some(marker) = expected["metadata"]
            .as_object_mut()
            .ok_or("missing metadata")?
            .remove("__autoGeneratedAlias")
        {
            assert_eq!(marker, "true");
            if let Some(marker) = actual["metadata"]
                .as_object_mut()
                .ok_or("missing metadata")?
                .remove("__autoGeneratedAlias")
            {
                assert_eq!(marker, "true");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn nested_lambda_field_names_match_spark() -> TestResult {
    let lambda_records = include_str!("fixtures/nested-field-lambda-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(lambda_records[0], records()?[0]);
    assert_eq!(lambda_records.len() - 1, 12);
    let mut identities = BTreeSet::new();
    for case in &lambda_records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
        assert!(case["expected"].get("condition").is_none());
    }
    let mut differences = Vec::new();
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
        register_recorded_inputs(&session, &lambda_records[0], batch_size)?;
        for case in &lambda_records[1..] {
            let mut expected = case["expected"].clone();
            let mut actual = capture_spark_query_result(&session, case).await?;
            exclude_recorded_generated_alias_markers(&mut expected, &mut actual)?;
            if actual != expected {
                differences.push(format!(
                    "{} input={} ANSI={} batch={batch_size}: expected {expected}, got {actual}",
                    case["id"], case["input"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn normalize_recorded_boolean_text(expected: &mut Value) -> TestResult {
    // The Python capture uses str(bool), while Arrow's display helper uses lowercase.
    let boolean_columns = expected["schema"]["fields"]
        .as_array()
        .ok_or("missing fields")?
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field["type"] == "boolean").then_some(index))
        .collect::<Vec<_>>();
    for row in expected["rows"].as_array_mut().ok_or("missing rows")? {
        for &index in &boolean_columns {
            let value = &mut row[index];
            if !value.is_null() {
                *value = json!(match value.as_str().ok_or("missing Boolean text")? {
                    "True" => "true",
                    "False" => "false",
                    other => return Err(format!("unexpected captured Boolean: {other}").into()),
                });
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_field_names_and_metadata_are_unchanged() -> TestResult {
    let records = records()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    capture_spark_query_result(
        &configured,
        &json!({"ansi":true,
        "sql":"SELECT ABS(nested.item) FROM inputs"}),
    )
    .await?;
    for sql in [
        "SELECT nested.item,ABS(nested.item),CAST(nested.item AS BIGINT) FROM inputs",
        "SELECT nested.item AS chosen,CAST(nested.item AS BIGINT) FROM empty_inputs",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(actual.schema(), expected.schema(), "{sql}");
        assert_eq!(actual.collect().await?, expected.collect().await?, "{sql}");
    }
    Ok(())
}
