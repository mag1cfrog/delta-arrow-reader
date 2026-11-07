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

#[tokio::test]
async fn pivot_source_and_output_aliases_match_spark() -> TestResult {
    let mut records = Vec::new();
    for fixture in [
        include_str!("fixtures/pivot-source-aliases-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-supplement-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-boundaries-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-join-spark.jsonl"),
        include_str!("fixtures/pivot-source-aliases-resume-controls-spark.jsonl"),
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
            let actual = compare_exact_spark_query_stages_with_optimized(&session, &comparison_reference).await;
            let matches = actual.as_ref().is_ok_and(|observation| {
                if case["expected"].get("condition").is_some() {
                    pivot_error_matches(case, observation)
                } else {
                    observation["matches"] == true && observation["optimized_matches"] == true
                }
            });
            observations.push(json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.as_ref().map_or_else(
                    |error| json!({"error":error.to_string()}), |value| value.clone()),
            }));
            if !matches {
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
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
