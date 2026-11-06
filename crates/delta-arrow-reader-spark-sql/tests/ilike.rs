//! Spark ILIKE lowers original operands before ordinary case-sensitive LIKE.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_result, register_inputs};

async fn capture_error_fields(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
            if field.data_type() != &DataType::Boolean {
                return Err(format!("unexpected ILIKE field type: {}", field.data_type()).into());
            }
            Ok(
                json!({"name":query.column_names.get(index).ok_or("missing result name")?,
            "type":"boolean","nullable":field.is_nullable(),"metadata":field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok(json!({"type":"struct","fields":fields}))
}

fn matches_escape_dependency_error(case: &Value, error: &str) -> bool {
    let expected = &case["expected"];
    let parameters = &expected["parameters"];
    match expected["condition"].as_str() {
        Some(
            condition @ ("INVALID_FORMAT.ESC_IN_THE_MIDDLE" | "INVALID_FORMAT.ESC_AT_THE_END"),
        ) => {
            let Some(pattern) = parameters["format"].as_str() else {
                return false;
            };
            let pattern = pattern.trim_matches('\'').replace("\\\\", "\\");
            let cause = if condition == "INVALID_FORMAT.ESC_IN_THE_MIDDLE" {
                let Some(character) = parameters["char"].as_str() else {
                    return false;
                };
                format!(
                    "The format is invalid: '{pattern}'. The escape character is not allowed to precede {character}."
                )
            } else {
                format!(
                    "The format is invalid: '{pattern}'. The escape character is not allowed to end with."
                )
            };
            error.contains(&format!("[{condition}]"))
                && error.lines().any(|line| line.ends_with(&cause))
        }
        Some("INVALID_ESC") => {
            let Some(escape) = parameters["invalidEscape"].as_str() else {
                return false;
            };
            let legacy_cause = format!("invalid escape character: {}", escape.trim_matches('\''));
            let cause = format!(
                "[INVALID_ESC] Found an invalid escape string: {escape}. The escape string must contain only one UTF-16 code unit."
            );
            error
                .lines()
                .any(|line| line.ends_with(&legacy_cause) || line.ends_with(&cause))
        }
        _ => false,
    }
}

#[test]
fn ilike_dependency_errors_reject_unrelated_causes() -> TestResult {
    let case = include_str!("fixtures/ilike-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|case| case["id"] == "ilike_invalid_custom_bad_escape" && case["ansi"] == true)
        .ok_or("missing escape error reference")?;
    let diagnostic = "execute: [INVALID_FORMAT.ESC_IN_THE_MIDDLE] The format is invalid: 'a!b'. The escape character is not allowed to precede 'b'.";
    assert!(matches_escape_dependency_error(&case, diagnostic));
    assert!(!matches_escape_dependency_error(
        &case,
        &diagnostic.replace("'b'.", "'c'.")
    ));
    assert!(!matches_escape_dependency_error(
        &case,
        "execute: internal error: could not downcast ILIKE operands"
    ));
    Ok(())
}

#[tokio::test]
async fn ilike_matches_spark_unicode_lowering() -> TestResult {
    let mut differences = Vec::new();
    let mut compared = 0;
    let mut escape_dependencies = 0;
    for (capture, count) in [
        (include_str!("fixtures/ilike-spark.jsonl"), 90),
        (include_str!("fixtures/ilike-evaluation-spark.jsonl"), 144),
        (include_str!("fixtures/ilike-unicode-spark.jsonl"), 40),
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
                    let escape_dependency = expected["status"] == "error";
                    let error_fields =
                        if escape_dependency && expected.get("analyzed_schema").is_some() {
                            Some(capture_error_fields(&session, case).await?)
                        } else {
                            None
                        };
                    let actual = capture_result(&session, case).await;
                    let observed = match &actual {
                        Ok(value) => value.clone(),
                        Err(error) => {
                            json!({"error":error.to_string(),"analyzed_schema":error_fields})
                        }
                    };
                    let classification = if escape_dependency {
                        if actual.is_ok() {
                            "missing_required_error_771"
                        } else if actual.as_ref().is_err_and(|error| {
                            matches_escape_dependency_error(case, &error.to_string())
                        }) {
                            "required_escape_error_771"
                        } else {
                            "unexpected_dependency_error"
                        }
                    } else {
                        "compatibility"
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"classification":classification,"actual":observed})
                    );
                    if escape_dependency {
                        // Preserve these observations without crediting #771's errors to #772.
                        assert!(matches!(
                            expected["condition"].as_str(),
                            Some(
                                "INVALID_FORMAT.ESC_IN_THE_MIDDLE"
                                    | "INVALID_FORMAT.ESC_AT_THE_END"
                                    | "INVALID_ESC"
                            )
                        ));
                        escape_dependencies += 1;
                        let fields_match = error_fields
                            .as_ref()
                            .is_none_or(|actual| actual == &expected["analyzed_schema"]);
                        if !fields_match || classification == "unexpected_dependency_error" {
                            differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: #771 dependency has an unexpected field or error cause: expected {expected}, observed {observed}", case["id"],case["ansi"]));
                        }
                        continue;
                    }
                    compared += 1;
                    if !actual.as_ref().is_ok_and(|value| {
                        value["analyzed_schema"] == expected["analyzed_schema"]
                            && value["physical_schema"] == expected["physical_schema"]
                            && value["rows"] == expected["rows"]
                    }) {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, 234 * 6);
    assert_eq!(escape_dependencies, 40 * 6);
    eprintln!(
        "C06 ILIKE compared {compared}, separate #771 observations {escape_dependencies}, mismatches {}",
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn ilike_preserves_native_lower_and_like() -> TestResult {
    let native_sql = "SELECT 'Σ' ILIKE 'ς' AS sigma,'ſ' ILIKE 's' AS long_s,lower('İ') AS dotted_i,'x' LIKE '_' AS ordinary,'x' LIKE '_'";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        let result = capture_result(
            &session,
            &json!({"sql":"SELECT 'Σ' ILIKE 'ς' AS sigma,'ſ' ILIKE 's' AS long_s,'İ' ILIKE '__' AS dotted_i","ansi":ansi}),
        )
        .await?;
        assert_eq!(result["rows"], json!([["false", "false", "true"]]));
        let schema = json!({"type":"struct","fields":[
            {"name":"sigma","type":"boolean","nullable":false,"metadata":{}},
            {"name":"long_s","type":"boolean","nullable":false,"metadata":{}},
            {"name":"dotted_i","type":"boolean","nullable":false,"metadata":{}}
        ]});
        assert_eq!(result["analyzed_schema"], schema);
        assert_eq!(result["physical_schema"], schema);
        assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn ilike_evaluates_each_volatile_operand_once() -> TestResult {
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        session.register_batch(
            "unicode_operands",
            RecordBatch::try_from_iter(vec![
                (
                    "s",
                    Arc::new(StringArray::from(vec!["Σ", "ſ", "İ"])) as ArrayRef,
                ),
                (
                    "p",
                    Arc::new(StringArray::from(vec!["ς", "s", "__"])) as ArrayRef,
                ),
            ])?,
        )?;
        let source_rows = Arc::new(AtomicUsize::new(0));
        let pattern_rows = Arc::new(AtomicUsize::new(0));
        for (name, rows) in [
            ("count_ilike_source", &source_rows),
            ("count_ilike_pattern", &pattern_rows),
        ] {
            let rows = Arc::clone(rows);
            session.register_udf(create_udf(
                name,
                vec![DataType::Utf8],
                DataType::Utf8,
                Volatility::Volatile,
                Arc::new(move |args| {
                    let [value] =
                        datafusion::common::utils::take_function_args("count_ilike_operand", args)?;
                    let len = match value {
                        ColumnarValue::Array(array) => array.len(),
                        ColumnarValue::Scalar(_) => 1,
                    };
                    rows.fetch_add(len, Ordering::SeqCst);
                    Ok(value.clone())
                }),
            ));
        }
        for ansi in [true, false] {
            for expression in [
                "count_ilike_source(s) ILIKE count_ilike_pattern(p)",
                "ilike(count_ilike_source(s),count_ilike_pattern(p))",
                "ilike(count_ilike_source(s),count_ilike_pattern(p),'!')",
            ] {
                source_rows.store(0, Ordering::SeqCst);
                pattern_rows.store(0, Ordering::SeqCst);
                let query = plan_query(
                    &session,
                    &format!("SELECT {expression} AS r FROM unicode_operands"),
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                assert_eq!(source_rows.load(Ordering::SeqCst), 0);
                assert_eq!(pattern_rows.load(Ordering::SeqCst), 0);
                let physical = query.create_physical_plan(&session).await?;
                assert_eq!(source_rows.load(Ordering::SeqCst), 0);
                assert_eq!(pattern_rows.load(Ordering::SeqCst), 0);
                let expected_field = Field::new("r", DataType::Boolean, true);
                assert_eq!(query.column_names, vec!["r"]);
                let analyzed_field = query
                    .logical_plan
                    .schema()
                    .field(0)
                    .as_ref()
                    .clone()
                    .with_name(&query.column_names[0]);
                assert_eq!(analyzed_field, expected_field);
                assert_eq!(physical.schema().field(0), &expected_field);
                let batches = collect(physical, session.task_ctx()).await?;
                let values = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .into_iter()
                            .flat_map(|array| array.iter())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(values, vec![Some(false), Some(false), Some(true)]);
                assert_eq!(source_rows.load(Ordering::SeqCst), 3, "{expression}");
                assert_eq!(pattern_rows.load(Ordering::SeqCst), 3, "{expression}");
            }
        }
    }
    Ok(())
}
