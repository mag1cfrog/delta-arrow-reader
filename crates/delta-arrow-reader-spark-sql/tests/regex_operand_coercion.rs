//! Shared regex coercion preserves aliased Spark results and native boundaries.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use arrow::array::{AsArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, Like, Operator, col, lit};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, col as physical_col};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[allow(dead_code, reason = "This suite selects complete stage observers.")]
#[path = "support/spark_results.rs"]
mod spark_results;
#[allow(dead_code, reason = "This suite selects complete stage observers.")]
#[path = "support/string_binary.rs"]
mod string_binary;
use spark_results::{capture_native_query_observation, capture_spark_optimized_observation};
use string_binary::{TestResult, capture_observation, register_inputs};

const REFERENCE: &str = include_str!("fixtures/regex-operand-coercion-spark.jsonl");

fn session(batch_size: usize, configured: bool) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
    SessionContext::new_with_state(if configured {
        configure_spark_sql(builder).build()
    } else {
        builder.build()
    })
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|message| (*message).to_owned()))
        .unwrap_or_else(|| "non-string panic payload".into())
}

fn required_pattern_error_matches(expected: &Value, actual: &Value) -> bool {
    let phase = match expected["phase"].as_str() {
        Some("optimized_schema") => "physical",
        Some("collect") => "execute",
        _ => return false,
    };
    expected["java_causes"].as_array().is_some_and(|causes| {
        causes.iter().any(|cause| {
            cause["class"] == "java.util.regex.PatternSyntaxException"
                && cause["message"] == "Unclosed character class near index 0\n[\n^"
        })
    }) && actual["error_stage"] == phase
        && actual.get("rows").is_none()
        && actual["error"].as_str().is_some_and(|error| {
            error.starts_with(&format!("{phase}: "))
                && error.contains("regex parse error:\n    [\n")
                && error.contains("error: unclosed character class")
        })
}

#[tokio::test]
async fn aliased_regex_operands_match_independent_spark_stages() -> TestResult {
    let records = REFERENCE.lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 17);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let identities = records[1..].iter().map(|case| {
        Ok((case["id"].as_str().ok_or("missing reference ID")?.to_owned(),
            case["ansi"].as_bool().ok_or("missing reference ANSI")?))
    }).collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(identities.len(), records.len() - 1, "duplicate reference identity");
    let mut observations = Vec::new();
    let mut failures = Vec::new();
    let mut successes = 0;
    let mut required_errors = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size, true);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                let id = case["id"].as_str().ok_or("missing ID")?;
                let ansi = case["ansi"].as_bool().ok_or("missing ANSI")?;
                let expected = &case["expected"];
                let actual = match AssertUnwindSafe(capture_observation(&session, case))
                    .catch_unwind().await
                {
                    Ok(Ok(actual)) => actual,
                    Ok(Err(error)) => json!({"capture_error":error.to_string()}),
                    Err(payload) => json!({"capture_panic":panic_message(payload)}),
                };
                let optimized = if expected["optimized_schema"].is_object() {
                    match AssertUnwindSafe(capture_spark_optimized_observation(&session, case))
                        .catch_unwind().await
                    {
                        Ok(Ok(optimized)) => optimized,
                        Ok(Err(error)) => json!({"capture_error":error.to_string()}),
                        Err(payload) => json!({"capture_panic":panic_message(payload)}),
                    }
                } else {
                    Value::Null
                };
                let mut comparison = json!({
                    "capture_completed":actual["capture_error"].is_null() && actual["capture_panic"].is_null()
                        && optimized["capture_error"].is_null() && optimized["capture_panic"].is_null(),
                    "public_schema":actual["analyzed_schema"] == expected["schema"],
                    "analyzed_schema":actual["analyzed_schema"] == expected["analyzed_schema"],
                    "analyzed_raw":actual["analyzed_arrow_schema"].is_string(),
                });
                if expected["physical_schema"].is_object() {
                    comparison["physical_schema"] = json!(actual["physical_schema"] == expected["physical_schema"]);
                }
                if expected["optimized_schema"].is_object() {
                    comparison["optimized_schema"] = json!(
                        optimized["names_match"] == true
                            && optimized["schema"] == expected["optimized_schema"]
                            && optimized["raw_schema"].is_object()
                    );
                }
                if expected["status"] == "ok" {
                    successes += 1;
                    comparison["status"] = json!(actual["error"].is_null()
                        && actual["capture_error"].is_null() && actual["capture_panic"].is_null());
                    comparison["rows"] = json!(actual["rows"] == expected["rows"]);
                    comparison["physical_raw"] = json!(actual["physical_arrow_schema"].is_string());
                } else {
                    required_errors += 1;
                    assert_eq!(expected["status"], "error");
                    assert_eq!(case["require_error"], true);
                    comparison["required_error"] = json!(required_pattern_error_matches(expected, &actual));
                    comparison["completed_physical_fields"] = json!(if actual["error_stage"] == "execute" {
                        actual["physical_schema"].is_object() && actual["physical_arrow_schema"].is_string()
                    } else {
                        actual["physical_schema"].is_null() && actual["physical_arrow_schema"].is_null()
                    });
                }
                let matches = comparison.as_object().ok_or("missing comparison")?
                    .values().all(|value| value == true);
                let observation = json!({"id":id, "ansi":ansi, "representation":representation,
                    "batch_size":batch_size, "reference":case, "actual":actual,
                    "optimized":optimized, "comparison":comparison, "matches":matches,
                    "reference_stage_availability":case["stage_availability"]});
                eprintln!("regex operand observation: {observation}");
                observations.push(observation);
                if !matches {
                    failures.push(format!("{id} ANSI={ansi} representation={representation} batch={batch_size}"));
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_REGEX_OPERAND_REPORT") {
        serde_json::to_writer_pretty(std::fs::File::create_new(path)?, &observations)?;
    }
    let expected_contexts = identities.iter().flat_map(|(id, ansi)| {
        (0_u64..3).flat_map(move |representation| {
            [1_u64, 32].map(move |batch_size| (id.clone(), *ansi, representation, batch_size))
        })
    }).collect::<BTreeSet<_>>();
    let observed_contexts = observations.iter().map(|observed| {
        Ok((observed["id"].as_str().ok_or("missing observed ID")?.to_owned(),
            observed["ansi"].as_bool().ok_or("missing observed ANSI")?,
            observed["representation"].as_u64().ok_or("missing observed representation")?,
            observed["batch_size"].as_u64().ok_or("missing observed batch size")?))
    }).collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(observed_contexts.len(), observations.len(), "duplicate Spark observation context");
    assert_eq!(observed_contexts, expected_contexts);
    assert_eq!(observations.len(), 96);
    assert_eq!(successes, 48);
    assert_eq!(required_errors, 48);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn regex_expression_coercion_preserves_string_layouts_nulls_and_flags() -> TestResult {
    use DataType::{LargeUtf8, Utf8, Utf8View};
    let session = SessionContext::new();
    let pairs = [
        (Utf8, Utf8, Utf8), (Utf8, LargeUtf8, LargeUtf8), (Utf8, Utf8View, Utf8View),
        (LargeUtf8, Utf8, LargeUtf8), (LargeUtf8, LargeUtf8, LargeUtf8), (LargeUtf8, Utf8View, Utf8View),
        (Utf8View, Utf8, Utf8View), (Utf8View, LargeUtf8, Utf8View), (Utf8View, Utf8View, Utf8View),
    ];
    for (left_type, right_type, common) in pairs {
        let left = cast(&StringArray::from(vec![Some("Ab"), Some("ab"), None, Some("zz")]), &left_type)?;
        let right = cast(&StringArray::from(vec![Some("(a)"), Some("(a)"), None, Some("(a)")]), &right_type)?;
        let batch = RecordBatch::try_from_iter([("s", left.clone()), ("re", right.clone())])?;
        let schema = DFSchema::try_from(batch.schema())?;
        for (left, right, sensitive, insensitive) in [
            (col("s"), col("re"), [Some(false), Some(true), None, Some(false)], [Some(true), Some(true), None, Some(false)]),
            (lit(ScalarValue::try_from_array(&left, 0)?), col("re"), [Some(false), Some(false), None, Some(false)], [Some(true), Some(true), None, Some(true)]),
            (col("s"), lit(ScalarValue::try_from_array(&right, 0)?), [Some(false), Some(true), None, Some(false)], [Some(true), Some(true), None, Some(false)]),
        ] {
            for negated in [false, true] {
                for case_insensitive in [false, true] {
                    let expression = Expr::SimilarTo(Like::new(negated,
                        Box::new(left.clone()), Box::new(right.clone()), None, case_insensitive));
                    let physical = session.create_physical_expr(expression, &schema)?;
                    let children = physical.children();
                    assert_eq!(children.len(), 2);
                    assert_eq!(children[0].data_type(batch.schema().as_ref())?, common);
                    assert_eq!(children[1].data_type(batch.schema().as_ref())?, common);
                    let result = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
                    let expected = (if case_insensitive { insensitive } else { sensitive })
                        .map(|value| value.map(|value| value != negated));
                    assert_eq!(result.as_boolean().iter().collect::<Vec<_>>(), expected,
                        "{left_type}/{right_type}: {physical}");
                }
            }
        }
        for (left, right) in [
            (col("s"), lit(ScalarValue::Null)),
            (lit(ScalarValue::Null), col("re")),
            (lit(ScalarValue::Null), lit(ScalarValue::Null)),
        ] {
            let physical = session.create_physical_expr(Expr::SimilarTo(Like::new(
                false, Box::new(left), Box::new(right), None, false)), &schema)?;
            let result = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(result.data_type(), &DataType::Boolean);
            assert_eq!(result.null_count(), batch.num_rows());
        }
    }
    Ok(())
}

#[test]
fn regex_coerces_dictionary_values_without_changing_like() -> TestResult {
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let values = StringArray::from(vec![Some("Ab"), Some("ab"), None, Some("zz")]);
    let batch = RecordBatch::try_from_iter([("s", cast(&values, &dictionary)?)])?;
    let schema = DFSchema::try_from(batch.schema())?;
    let session = SessionContext::new();
    for case_insensitive in [false, true] {
        for regex in [false, true] {
            let expression = Like::new(false, Box::new(col("s")),
                Box::new(lit(if regex { "a" } else { "a%" })), None, case_insensitive);
            let physical = session.create_physical_expr(
                if regex { Expr::SimilarTo(expression) } else { Expr::Like(expression) }, &schema)?;
            assert_eq!(physical.children()[0].data_type(batch.schema().as_ref())?,
                if regex { DataType::Utf8 } else { dictionary.clone() });
            let result = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(result.as_boolean().iter().collect::<Vec<_>>(),
                [Some(case_insensitive), Some(true), None, Some(false)]);
        }
    }
    Ok(())
}

#[test]
fn regex_incompatible_operands_keep_planning_errors() -> TestResult {
    let session = SessionContext::new();
    let schema = DFSchema::try_from(Schema::empty())?;
    for (left, right, types) in [
        (lit(1_i64), lit("a"), "Int64 and Utf8"),
        (lit("a"), lit(1_i64), "Utf8 and Int64"),
        (lit(ScalarValue::Binary(Some(b"a".to_vec()))), lit("a"), "Binary and Utf8"),
        (lit("a"), lit(ScalarValue::Binary(Some(b"a".to_vec()))), "Utf8 and Binary"),
    ] {
        let error = session.create_physical_expr(Expr::SimilarTo(Like::new(
            false, Box::new(left), Box::new(right), None, false)), &schema).err().ok_or("expected incompatible regex operands to fail")?;
        assert!(error.to_string().contains(&format!(
            "There isn't a common type to coerce {types} in SIMILAR TO expression")));
    }
    Ok(())
}

#[test]
fn directly_mismatched_regex_arrays_return_checked_errors() -> TestResult {
    for left_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let right_type = if left_type == DataType::Utf8 { DataType::LargeUtf8 } else { DataType::Utf8 };
        let left = cast(&StringArray::from(vec!["ab"]), &left_type)?;
        let right = cast(&StringArray::from(vec!["(a)"]), &right_type)?;
        let batch = RecordBatch::try_from_iter([("s", left), ("re", right)])?;
        for operator in [Operator::RegexMatch, Operator::RegexIMatch,
            Operator::RegexNotMatch, Operator::RegexNotIMatch] {
            let expression = BinaryExpr::new(physical_col("s", batch.schema().as_ref())?,
                operator, physical_col("re", batch.schema().as_ref())?);
            let error = expression.evaluate(&batch).err().ok_or("expected mismatched regex arrays to fail")?;
            assert!(error.to_string().contains("failed to downcast array to "));
            assert!(error.to_string().contains("for operation 'regex_match_dyn'"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn shared_regex_repair_preserves_native_sql_boundaries() -> TestResult {
    let metadata: Value = serde_json::from_str(
        REFERENCE.lines().next().ok_or("missing metadata")?)?;
    let required_rows = json!([["0","true"],["1","true"],["2","false"],["3","true"]]);
    let null_rows = json!([["0",null],["1",null],["2",null]]);
    let controls = [
        ("required_tilde", "SELECT k, s ~ re AS matched FROM regexp_required ORDER BY k", false, false, true, required_rows.clone()),
        ("required_function", "SELECT k, regexp_like(s,re) AS matched FROM regexp_required ORDER BY k", true, false, true, required_rows.clone()),
        ("nullable_tilde", "SELECT k, s ~ re AS matched FROM regexp_nullable ORDER BY k", true, true, true, null_rows.clone()),
        ("nullable_function", "SELECT k, regexp_like(s,re) AS matched FROM regexp_nullable ORDER BY k", true, true, true, null_rows),
        ("empty_tilde", "SELECT k, s ~ re AS matched FROM regexp_empty ORDER BY k", true, true, true, json!([])),
        ("null_pattern", "SELECT 'ab' SIMILAR TO NULL AS matched", true, true, false, json!([[null]])),
        ("similar_pattern_admission", "SELECT k, s SIMILAR TO re AS matched FROM regexp_required ORDER BY k", false, false, true, required_rows),
        ("incompatible_pattern", "SELECT 'ab' SIMILAR TO 1 AS matched", false, false, false, Value::Null),
        ("folded_invalid_pattern", "SELECT regexp_like('ab','[') AS matched", true, true, false, Value::Null),
        ("runtime_invalid_pattern", "SELECT k, s ~ re AS matched FROM regexp_invalid ORDER BY k", false, false, true, Value::Null),
    ];
    let mut observations = Vec::new();
    let mut failures = Vec::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let plain = session(batch_size, false);
            let configured = session(batch_size, true);
            for session in [&plain, &configured] {
                register_inputs(session, &metadata, representation, batch_size)?;
            }
            for (id, sql, public_nullable, physical_nullable, has_key, rows) in &controls {
                let mut paired = Vec::new();
                for session in [&plain, &configured] {
                    let captured = AssertUnwindSafe(
                        capture_native_query_observation(session, sql)
                    ).catch_unwind().await;
                    paired.push(match captured {
                        Ok(Ok(actual)) => actual,
                        Ok(Err(error)) => json!({"capture_error":error.to_string()}),
                        Err(payload) => json!({"capture_panic":panic_message(payload)}),
                    });
                }
                let actual = &paired[1];
                let mut fields = Vec::new();
                if *has_key {
                    fields.push(json!({"name":"k", "type":"integer", "nullable":false,
                        "metadata":{"source":"regexp_operator", "column":"k"}}));
                }
                fields.push(json!({"name":"matched", "type":"boolean", "nullable":public_nullable, "metadata":{}}));
                let public_schema = json!({"type":"struct", "fields":fields});
                let mut physical_schema = public_schema.clone();
                physical_schema["fields"][usize::from(*has_key)]["nullable"] = json!(physical_nullable);
                let admission_error = *id == "incompatible_pattern"
                    || (*id == "similar_pattern_admission" && representation != 0);
                let pattern_error = matches!(*id, "folded_invalid_pattern" | "runtime_invalid_pattern");
                let contract_matches = if admission_error {
                    actual["phase"] == "sql"
                        && actual["public_schema"].is_null()
                        && actual["before_collect"].is_null()
                        && actual["physical_schema"].is_null()
                        && actual["rows"].is_null()
                        && actual["error"].as_str().is_some_and(|error| error.contains("Invalid pattern in SIMILAR TO expression"))
                } else if pattern_error {
                    let runtime = *id == "runtime_invalid_pattern";
                    actual["phase"] == (if runtime { "collect" } else { "create_physical_plan" })
                        && actual["public_schema"] == public_schema
                        && actual["before_collect"] == (if runtime { physical_schema.clone() } else { Value::Null })
                        && actual["physical_schema"] == (if runtime { physical_schema.clone() } else { Value::Null })
                        && actual["rows"].is_null()
                        && actual["error"].as_str().is_some_and(|error| {
                            error.contains("regex parse error:\n    [\n")
                                && error.contains("error: unclosed character class")
                        })
                } else {
                    actual["phase"] == "encode" && actual["error"].is_null()
                        && actual["public_schema"] == public_schema
                        && actual["before_collect"] == physical_schema
                        && actual["physical_schema"] == physical_schema
                        && actual["rows"] == *rows
                };
                let capture_completed = paired.iter().all(|actual| {
                    actual["capture_error"].is_null() && actual["capture_panic"].is_null()
                });
                let matches = capture_completed && paired[0] == paired[1] && contract_matches;
                let observation = json!({"id":id, "sql":sql, "representation":representation,
                    "batch_size":batch_size, "plain":paired[0], "configured":paired[1],
                    "capture_completed":capture_completed,
                    "contract_matches":contract_matches, "matches":matches});
                eprintln!("regex native observation: {observation}");
                observations.push(observation);
                if !matches {
                    failures.push(format!("{id} representation={representation} batch={batch_size}"));
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("DAR_REGEX_OPERAND_NATIVE_REPORT") {
        serde_json::to_writer_pretty(std::fs::File::create_new(path)?, &observations)?;
    }
    let sql_inventory = controls.iter().map(|(_, sql, _, _, _, _)| *sql)
        .collect::<BTreeSet<_>>();
    assert_eq!(sql_inventory.len(), controls.len(), "duplicate native SQL control");
    let expected_contexts = controls.iter().flat_map(|(id, sql, _, _, _, _)| {
        (0_u64..3).flat_map(move |representation| {
            [1_u64, 32].map(move |batch_size| (id.to_string(), sql.to_string(), representation, batch_size))
        })
    }).collect::<BTreeSet<_>>();
    let observed_contexts = observations.iter().map(|observed| {
        Ok((observed["id"].as_str().ok_or("missing native ID")?.to_owned(),
            observed["sql"].as_str().ok_or("missing native SQL")?.to_owned(),
            observed["representation"].as_u64().ok_or("missing native representation")?,
            observed["batch_size"].as_u64().ok_or("missing native batch size")?))
    }).collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(observed_contexts.len(), observations.len(), "duplicate native observation context");
    assert_eq!(observed_contexts, expected_contexts);
    assert_eq!(observations.len(), 60);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn required_regex_error_matcher_rejects_wrong_causes_and_phases() -> TestResult {
    let records = REFERENCE.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for case in records.iter().filter(|case| case["require_error"] == true) {
        let phase = if case["expected"]["phase"] == "optimized_schema" { "physical" } else { "execute" };
        // Synthetic diagnostics exercise the matcher; integration records real errors.
        let error = format!("{phase}: regex parse error:\n    [\n    ^\nerror: unclosed character class");
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
        let mut no_cause = case["expected"].clone();
        no_cause["java_causes"] = json!([]);
        assert!(!required_pattern_error_matches(&no_cause, &actual));
    }
    Ok(())
}

#[test]
fn panic_messages_retain_string_and_literal_payloads() {
    assert_eq!(panic_message(Box::new(String::from("owned failure"))), "owned failure");
    assert_eq!(panic_message(Box::new("literal failure")), "literal failure");
    assert_eq!(panic_message(Box::new(17_u32)), "non-string panic payload");
}
