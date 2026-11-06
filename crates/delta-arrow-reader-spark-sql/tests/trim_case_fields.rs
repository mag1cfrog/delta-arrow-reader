//! Spark trim/case fields preserve native values, types and evaluation.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
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

fn arity_error_matches(case: &Value, error: &str) -> bool {
    if case["expected"]["condition"] != "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"
        || !error.starts_with("resolve:")
    {
        return false;
    }
    let id = case["id"].as_str().unwrap_or_default();
    if ["lower", "upper", "lcase", "ucase"]
        .iter()
        .any(|name| id.starts_with(&format!("admission_arity_{name}_")))
    {
        return if id.ends_with("_zero") {
            error.contains("one value expected: []")
        } else {
            id.ends_with("_extra")
                && error.contains(
                    "one value expected: [Literal(Utf8(\"a\"), None), Literal(Utf8(\"b\"), None)]",
                )
        };
    }
    let native_name = if id.contains("_ltrim_") {
        "ltrim"
    } else if id.contains("_rtrim_") {
        "rtrim"
    } else if id.contains("_trim_") {
        "btrim"
    } else {
        return false;
    };
    if id.ends_with("_zero") {
        error.contains(&format!("'{native_name}' does not support zero arguments"))
    } else {
        id.ends_with("_extra")
            && error.contains(&format!(
                "Function '{native_name}' expects 2 arguments but received 3"
            ))
            && error.contains(&format!(
                "Function '{native_name}' expects 1 arguments but received 3"
            ))
    }
}

// #795 fails while naming the expression, before native arity validation.
// Record that precise incompatibility separately from argument-count checks.
fn has_zero_trim_formatter_gap(case: &Value) -> bool {
    ["trim", "ltrim", "rtrim"].iter().any(|function| {
        case["id"] == format!("admission_arity_{function}_zero")
            && case["sql"] == format!("SELECT {function}() AS r")
            && case["expected"]["condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"
            && case["expected"]["parameters"]["functionName"] == format!("`{function}`")
            && case["expected"]["parameters"]["actualNum"] == "0"
    })
}

fn zero_trim_formatter_gap_matches(case: &Value, error: &str) -> bool {
    has_zero_trim_formatter_gap(case)
        && error
            == "resolve: External error: error in DataFusion: Error during planning: expected at least one value, but got an empty vector"
}

fn is_numeric_cast_regression_control(id: &str) -> bool {
    matches!(
        id,
        "trim_cast_ascii_0_32_projected_coalesce" | "trim_cast_ascii_0_32_del_projected_coalesce"
    )
}

#[test]
fn trim_zero_arity_formatter_gap_rejects_other_failures() {
    for function in ["trim", "ltrim", "rtrim"] {
        let mut case = json!({
            "id":format!("admission_arity_{function}_zero"),
            "sql":format!("SELECT {function}() AS r"),
            "expected":{"condition":"WRONG_NUM_ARGS.WITHOUT_SUGGESTION","parameters":{"functionName":format!("`{function}`"),"actualNum":"0"}}
        });
        let error = "resolve: External error: error in DataFusion: Error during planning: expected at least one value, but got an empty vector";
        assert!(zero_trim_formatter_gap_matches(&case, error));
        assert!(!arity_error_matches(&case, error));
        for other in [
            error.replace("resolve:", "execute:"),
            error.replace("empty vector", "wrong type"),
            "resolve: CAST_INVALID_INPUT".into(),
        ] {
            assert!(!zero_trim_formatter_gap_matches(&case, &other));
        }
        case["sql"] = json!("SELECT lower() AS r");
        assert!(!zero_trim_formatter_gap_matches(&case, error));
    }
}

fn native_dependency_profiles() -> TestResult<Vec<Value>> {
    let mut records = include_str!("fixtures/trim-case-native-dependency-profiles.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 49);
    let header = records.remove(0);
    assert_eq!(header["format_version"], 1);
    assert_eq!(header["native_profile_count"], 48);
    Ok(records)
}

#[test]
fn trim_case_numeric_profiles_are_historical_only() -> TestResult {
    let profiles = native_dependency_profiles()?;
    let references = include_str!("fixtures/trim-case-connected-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut retired = 0;
    for profile in profiles.iter().filter(|profile| {
        profile["id"].as_str().is_some_and(is_numeric_cast_regression_control)
    }) {
        let case = references[1..].iter().find(|case| {
            case["id"] == profile["id"] && case["ansi"] == profile["ansi"]
                && case["sql"] == profile["sql"]
        }).ok_or("missing historical numeric CAST reference")?;
        assert_eq!(profile["actual"]["physical_schema"], case["expected"]["physical_schema"]);
        assert_eq!(profile["actual"]["rows"], case["expected"]["rows"]);
        assert_ne!(profile["actual"]["analyzed_schema"], case["expected"]["analyzed_schema"]);
        let old = &profile["actual"]["analyzed_schema"]["fields"][0];
        let expected = &case["expected"]["analyzed_schema"]["fields"][0];
        for component in ["name", "type", "metadata"] {
            assert_eq!(old[component], expected[component]);
        }
        assert_eq!(old["nullable"], false);
        assert_eq!(expected["nullable"], true);
        retired += 1;
    }
    assert_eq!(retired, 24);
    Ok(())
}

#[tokio::test]
async fn trim_case_fields_match_recorded_references() -> TestResult {
    let mut differences = Vec::new();
    let mut strict_matches = 0;
    let mut compared = 0;
    let mut arity_observations = 0;
    let mut formatter_gap_observations = 0;
    let mut numeric_cast_observations = 0;
    let mut nested_case_observations = 0;
    for (capture, count) in [
        (include_str!("fixtures/trim-case-fields-spark.jsonl"), 208),
        (include_str!("fixtures/trim-case-connected-spark.jsonl"), 76),
        (include_str!("fixtures/trim-case-controls-spark.jsonl"), 66),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let actual = capture_result(&session, case).await;
                    let observed = match &actual {
                        Ok(value) => value.clone(),
                        Err(error) => json!({"error":error.to_string()}),
                    };
                    let expected = &case["expected"];
                    let formatter_gap = has_zero_trim_formatter_gap(case);
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let numeric_cast_control = is_numeric_cast_regression_control(id);
                    let nested_case = matches!(
                        id,
                        "nested_trim_case_nullable" | "nested_trim_case_required"
                    );
                    let classification = if formatter_gap {
                        formatter_gap_observations += 1;
                        "open-795-zero-arity-formatter-error"
                    } else if numeric_cast_control {
                        numeric_cast_observations += 1;
                        "strict-796-numeric-cast-declarations"
                    } else if nested_case {
                        nested_case_observations += 1;
                        "strict-792-nested-case-conversion"
                    } else if expected["status"] == "error" {
                        arity_observations += 1;
                        "arity-cause-without-spark-error-class"
                    } else {
                        "strict"
                    };
                    let strict_match = expected["status"] == "ok"
                        && actual.as_ref().is_ok_and(|value| {
                            value["analyzed_schema"] == expected["analyzed_schema"]
                                && value["physical_schema"] == expected["physical_schema"]
                                && value["rows"] == expected["rows"]
                        });
                    strict_matches += usize::from(strict_match);
                    let native_profile_match = if formatter_gap {
                        actual.as_ref().is_err_and(|error| {
                            zero_trim_formatter_gap_matches(case, &error.to_string())
                        })
                    } else {
                        false
                    };
                    let meaningful_arity_cause_match = !formatter_gap
                        && expected["status"] == "error"
                        && actual
                            .as_ref()
                            .is_err_and(|error| arity_error_matches(case, &error.to_string()));
                    let matches = match expected["status"].as_str() {
                        Some("error") => native_profile_match || meaningful_arity_cause_match,
                        Some("ok") => strict_match,
                        _ => return Err("unknown Spark reference status".into()),
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],
                            "representation":representation,"batch_size":batch_size,
                            "classification":classification,"actual":observed,
                            "strict_match":strict_match,
                            "full_compatibility_pass":strict_match,
                            "native_profile_match":native_profile_match,
                            "meaningful_arity_cause_match":meaningful_arity_cause_match,
                            "regression_gate_pass":matches})
                    );
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, (208 + 76 + 66) * 6);
    assert_eq!(arity_observations, (16 + 12 - 6) * 6);
    assert_eq!(formatter_gap_observations, 6 * 6);
    assert_eq!(numeric_cast_observations, 4 * 6);
    assert_eq!(nested_case_observations, 4 * 6);
    eprintln!(
        "C06 compared {compared}, strict Spark matches {strict_matches}, regression mismatches {}",
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn trim_case_preserves_native_sql() -> TestResult {
    let references = include_str!("fixtures/trim-case-connected-spark.jsonl")
        .lines()
        .next()
        .ok_or("missing input metadata")?;
    let metadata: Value = serde_json::from_str(references)?;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let plain = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_target_partitions(2),
            );
            let configured = session(batch_size);
            register_inputs(&plain, &metadata, representation, batch_size)?;
            register_inputs(&configured, &metadata, representation, batch_size)?;
            let sql = "SELECT s AS original,lower(s) AS lo,upper(s) AS up,btrim(s) AS b,ltrim(s) AS l,rtrim(s) AS r FROM c06_775_case_required ORDER BY k";
            let expected_logical = plain.sql(sql).await?;
            let expected_schema = expected_logical.schema().clone();
            let expected = expected_logical.collect().await?;
            for ansi in [true, false] {
                let native = configured.sql(sql).await?;
                assert_eq!(native.schema(), &expected_schema);
                assert_eq!(native.collect().await?, expected);
                let result = capture_result(
                &configured,
                &json!({"sql":"SELECT lower(s) AS lo,upper(s) AS up,trim(s) AS b,ltrim(s) AS l,rtrim(s) AS r FROM c06_775_case_required ORDER BY k","ansi":ansi}),
            )
            .await?;
                assert!(
                    result["analyzed_schema"]["fields"]
                        .as_array()
                        .ok_or("missing fields")?
                        .iter()
                        .all(|f| f["nullable"] == false)
                );
                // BTRIM is separately registered and keeps its existing native policy.
                let native_btrim = plain
                    .sql("SELECT btrim(s) AS r FROM c06_775_case_required ORDER BY k")
                    .await?
                    .collect()
                    .await?;
                let spark_btrim = plan_query(
                    &configured,
                    "SELECT btrim(s) AS r FROM c06_775_case_required ORDER BY k",
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                assert!(spark_btrim.logical_plan.schema().field(0).is_nullable());
                assert_eq!(
                    collect(
                        spark_btrim.create_physical_plan(&configured).await?,
                        configured.task_ctx()
                    )
                    .await?,
                    native_btrim
                );
                for table in [
                    "c06_775_case_required",
                    "c06_775_case_nullable",
                    "c06_775_case_empty",
                ] {
                    let native_sql = format!(
                        "SELECT lower(s) AS a,upper(s) AS b,lower(s) AS c,upper(s) AS d,btrim(s) AS e,ltrim(s) AS f,rtrim(s) AS g FROM {table} ORDER BY k"
                    );
                    let native = plain.sql(&native_sql).await?;
                    let native_schema = native.schema().clone();
                    let native_physical = native.create_physical_plan().await?;
                    let native_arrow = native_physical.schema();
                    let native_batches = collect(native_physical, plain.task_ctx()).await?;
                    let spark_sql = format!(
                        "SELECT lower(s) AS a,upper(s) AS b,lcase(s) AS c,ucase(s) AS d,trim(s) AS e,ltrim(s) AS f,rtrim(s) AS g FROM {table} ORDER BY k"
                    );
                    let spark = plan_query(
                        &configured,
                        &spark_sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let physical = spark.create_physical_plan(&configured).await?;
                    let arrow = physical.schema();
                    for index in 0..7 {
                        assert_eq!(
                            spark.logical_plan.schema().field(index).data_type(),
                            native_schema.field(index).data_type(),
                            "analyzed storage: {table}, column {index}, representation {representation}, batch {batch_size}, ANSI {ansi}"
                        );
                        assert_eq!(
                            arrow.field(index).data_type(),
                            native_arrow.field(index).data_type(),
                            "physical storage: {table}, column {index}, representation {representation}, batch {batch_size}, ANSI {ansi}"
                        );
                    }
                    // Nullability intentionally differs; compare real Arrow storage and values.
                    assert_eq!(
                        spark.logical_plan.schema().field(4).metadata(),
                        native_schema.field(4).metadata()
                    );
                    let batches = collect(physical, configured.task_ctx()).await?;
                    let actual = arrow::compute::concat_batches(&arrow, &batches)?;
                    let expected = arrow::compute::concat_batches(&native_arrow, &native_batches)?;
                    assert_eq!(
                        actual.columns(),
                        expected.columns(),
                        "Arrow columns: {table}, representation {representation}, batch {batch_size}, ANSI {ansi}"
                    );
                }
                assert!(
                    capture_result(
                        &configured,
                        &json!({"sql":"SELECT trim() AS r","ansi":ansi})
                    )
                    .await
                    .is_err()
                );
                let native = configured.sql(sql).await?;
                assert_eq!(native.schema(), &expected_schema);
                assert_eq!(native.collect().await?, expected);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn trim_case_evaluates_each_supplied_argument_once() -> TestResult {
    let session = session(1);
    let inputs = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("chars", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some(" abAba "), None, Some("")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(" ab"), Some("a"), None])),
        ],
    )?;
    session.register_batch("counted_strings", inputs)?;
    let source_rows = Arc::new(AtomicUsize::new(0));
    let set_rows = Arc::new(AtomicUsize::new(0));
    for (name, counter) in [
        ("count_source", source_rows.clone()),
        ("count_set", set_rows.clone()),
    ] {
        session.register_udf(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let arrays = ColumnarValue::values_to_arrays(arguments)?;
                counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
                Ok(ColumnarValue::Array(arrays[0].clone()))
            }),
        ));
    }
    for ansi in [true, false] {
        for function in ["trim", "ltrim", "rtrim", "lower", "upper", "lcase", "ucase"] {
            source_rows.store(0, Ordering::SeqCst);
            set_rows.store(0, Ordering::SeqCst);
            let trim = matches!(function, "trim" | "ltrim" | "rtrim");
            let arguments = if trim {
                "count_set(chars),count_source(s)"
            } else {
                "count_source(s)"
            };
            let sql = format!("SELECT {function}({arguments}) AS r FROM counted_strings");
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await?;
            let plan = query.create_physical_plan(&session).await?;
            assert_eq!(source_rows.load(Ordering::SeqCst), 0);
            assert_eq!(set_rows.load(Ordering::SeqCst), 0);
            let result = collect(plan, session.task_ctx()).await?;
            assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
            assert_eq!(source_rows.load(Ordering::SeqCst), 3, "{function}");
            assert_eq!(
                set_rows.load(Ordering::SeqCst),
                if trim { 3 } else { 0 },
                "{function}"
            );
        }
    }
    Ok(())
}
