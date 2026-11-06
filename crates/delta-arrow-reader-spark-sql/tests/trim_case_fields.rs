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

#[allow(
    dead_code,
    reason = "This suite uses the shared native query observer."
)]
#[path = "support/spark_results.rs"]
mod spark_results;
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
    trim_arity_error_matches(case, error)
}

fn is_trim_arity_case(case: &Value) -> bool {
    ["trim", "ltrim", "rtrim"].iter().any(|function| {
        case["id"] == format!("admission_arity_{function}_zero")
            || case["id"] == format!("admission_arity_{function}_extra")
    })
}

fn trim_arity_error_matches(case: &Value, error: &str) -> bool {
    let expected = &case["expected"];
    is_trim_arity_case(case)
        && expected["status"] == "error"
        && expected["phase"] == "sql"
        && expected["condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"
        && expected["message"]
            .as_str()
            .and_then(|message| message.split_once(" Please,"))
            .is_some_and(|(diagnostic, _)| {
                error == format!("resolve: External error: invalid argument: {diagnostic}")
            })
}

fn is_numeric_cast_regression_control(id: &str) -> bool {
    matches!(
        id,
        "trim_cast_ascii_0_32_projected_coalesce" | "trim_cast_ascii_0_32_del_projected_coalesce"
    )
}

#[test]
fn trim_arity_diagnostics_reject_wrong_counts_and_causes() -> TestResult {
    let mut checked = 0;
    for capture in [
        include_str!("fixtures/trim-case-fields-spark.jsonl"),
        include_str!("fixtures/trim-case-controls-spark.jsonl"),
    ] {
        for line in capture.lines().skip(1) {
            let case: Value = serde_json::from_str(line)?;
            if !is_trim_arity_case(&case) {
                continue;
            }
            let expected = &case["expected"];
            let (diagnostic, _) = expected["message"]
                .as_str()
                .and_then(|message| message.split_once(" Please,"))
                .ok_or("missing captured trim arity diagnostic")?;
            let error = format!("resolve: External error: invalid argument: {diagnostic}");
            assert!(trim_arity_error_matches(&case, &error));
            let function = expected["parameters"]["functionName"]
                .as_str()
                .ok_or("missing function name")?;
            let count = expected["parameters"]["actualNum"]
                .as_str()
                .ok_or("missing argument count")?;
            for other in [
                error.replace("resolve:", "execute:"),
                error.replace(function, "`btrim_other`"),
                error.replace("[1, 2]", "1"),
                error.replace(&format!("actual number is {count}"), "actual number is 4"),
                error.replace("WRONG_NUM_ARGS.WITHOUT_SUGGESTION", "UNRESOLVED_ROUTINE"),
                "resolve: External error: error in DataFusion: Error during planning: expected at least one value, but got an empty vector".into(),
                "resolve: CAST_INVALID_INPUT".into(),
            ] {
                assert!(!trim_arity_error_matches(&case, &other));
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 12);
    Ok(())
}

#[tokio::test]
async fn trim_arity_matches_spark_and_preserves_valid_calls() -> TestResult {
    let mut differences = Vec::new();
    let mut arity_observations = 0;
    let mut valid_observations = 0;
    for capture in [
        include_str!("fixtures/trim-case-fields-spark.jsonl"),
        include_str!("fixtures/trim-case-controls-spark.jsonl"),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        for batch_size in [1, 32] {
            let session = session(batch_size);
            for case in &records[1..] {
                let arity = is_trim_arity_case(case);
                let id = case["id"].as_str().ok_or("missing case ID")?;
                if !arity
                    && ![
                        "trim_literal_spaces",
                        "ltrim_literal_spaces",
                        "rtrim_literal_spaces",
                        "trim_custom_set",
                        "ltrim_custom_set",
                        "rtrim_custom_set",
                        "trim_both_sql_syntax",
                        "trim_leading_sql_syntax",
                        "trim_trailing_sql_syntax",
                    ]
                    .contains(&id)
                {
                    continue;
                }
                let expected = &case["expected"];
                let sql = case["sql"].as_str().ok_or("missing SQL")?;
                let mut queries = vec![sql];
                if arity {
                    assert_eq!(expected["parameters"]["expectedNum"], "[1, 2]");
                    queries.push(sql.strip_suffix(" AS r").ok_or("missing explicit alias")?);
                }
                for sql in queries {
                    let mut query_case = case.clone();
                    query_case["sql"] = json!(sql);
                    let actual = capture_observation(&session, &query_case).await?;
                    let matches = if arity {
                        arity_observations += 1;
                        let error = actual["error"].as_str().unwrap_or_default();
                        trim_arity_error_matches(case, error)
                            && actual
                                == json!({
                                    "error":error,"error_stage":"resolve",
                                    "analyzed_schema":null,"physical_schema":null,
                                    "analyzed_arrow_schema":null,"physical_arrow_schema":null,
                                })
                    } else {
                        valid_observations += 1;
                        expected["status"] == "ok"
                            && actual.get("error").is_none()
                            && actual.get("error_stage").is_none()
                            && actual["analyzed_schema"] == expected["analyzed_schema"]
                            && actual["physical_schema"] == expected["physical_schema"]
                            && actual["analyzed_arrow_schema"].is_string()
                            && actual["physical_arrow_schema"].is_string()
                            && actual["rows"] == expected["rows"]
                    };
                    eprintln!(
                        "trim arity observation: {}",
                        json!({"id":id,"ansi":case["ansi"],"sql":sql,
                            "batch_size":batch_size,"strict_match":matches,"actual":actual})
                    );
                    if !matches {
                        differences.push(format!(
                            "{id} ANSI={} batch={batch_size} SQL={sql}: expected {expected}, observed {actual}",
                            case["ansi"],
                        ));
                    }
                }
            }
        }
    }
    assert_eq!(arity_observations, 48);
    assert_eq!(valid_observations, 36);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
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
        profile["id"]
            .as_str()
            .is_some_and(is_numeric_cast_regression_control)
    }) {
        let case = references[1..]
            .iter()
            .find(|case| {
                case["id"] == profile["id"]
                    && case["ansi"] == profile["ansi"]
                    && case["sql"] == profile["sql"]
            })
            .ok_or("missing historical numeric CAST reference")?;
        assert_eq!(
            profile["actual"]["physical_schema"],
            case["expected"]["physical_schema"]
        );
        assert_eq!(profile["actual"]["rows"], case["expected"]["rows"]);
        assert_ne!(
            profile["actual"]["analyzed_schema"],
            case["expected"]["analyzed_schema"]
        );
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
    let mut trim_arity_observations = 0;
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
                    let trim_arity = is_trim_arity_case(case);
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let numeric_cast_control = is_numeric_cast_regression_control(id);
                    let nested_case = matches!(
                        id,
                        "nested_trim_case_nullable" | "nested_trim_case_required"
                    );
                    let classification = if trim_arity {
                        trim_arity_observations += 1;
                        "strict-795-trim-arity"
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
                    let strict_match = if trim_arity {
                        actual
                            .as_ref()
                            .is_err_and(|error| trim_arity_error_matches(case, &error.to_string()))
                    } else {
                        expected["status"] == "ok"
                            && actual.as_ref().is_ok_and(|value| {
                                value["analyzed_schema"] == expected["analyzed_schema"]
                                    && value["physical_schema"] == expected["physical_schema"]
                                    && value["rows"] == expected["rows"]
                            })
                    };
                    strict_matches += usize::from(strict_match);
                    let meaningful_arity_cause_match = !trim_arity
                        && expected["status"] == "error"
                        && actual
                            .as_ref()
                            .is_err_and(|error| arity_error_matches(case, &error.to_string()));
                    let matches = match expected["status"].as_str() {
                        Some("error") => strict_match || meaningful_arity_cause_match,
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
                            "native_profile_match":false,
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
    assert_eq!(arity_observations, 16 * 6);
    assert_eq!(trim_arity_observations, 12 * 6);
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
async fn trim_arity_preserves_native_sql() -> TestResult {
    for batch_size in [1, 32] {
        let plain = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
        let configured = session(batch_size);
        for ansi in [true, false] {
            for function in ["trim", "ltrim", "rtrim"] {
                for arguments in ["", "'a','a','a'"] {
                    let sql = format!("SELECT {function}({arguments}) AS r");
                    let plain_before =
                        spark_results::capture_native_query_observation(&plain, &sql).await?;
                    let configured_before =
                        spark_results::capture_native_query_observation(&configured, &sql).await?;
                    assert_eq!(configured_before, plain_before, "{sql}, before Spark");
                    assert!(
                        plan_query(
                            &configured,
                            &sql,
                            &SparkSqlOptions {
                                ansi_mode: ansi,
                                ..Default::default()
                            },
                        )
                        .await
                        .is_err()
                    );
                    let plain_after =
                        spark_results::capture_native_query_observation(&plain, &sql).await?;
                    let configured_after =
                        spark_results::capture_native_query_observation(&configured, &sql).await?;
                    eprintln!(
                        "trim arity native observation: {}",
                        json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,
                            "plain_before":plain_before,"configured_before":configured_before,
                            "plain_after":plain_after,"configured_after":configured_after})
                    );
                    assert_eq!(plain_after, plain_before, "{sql}, plain after Spark");
                    assert_eq!(
                        configured_after, configured_before,
                        "{sql}, configured after Spark"
                    );
                    assert_eq!(configured_after, plain_after, "{sql}, after Spark");
                }
            }
            for sql in [
                "SELECT trim('  AbC  '),ltrim('  AbC  '),rtrim('  AbC  ')",
                "SELECT trim('abbaXba','ab') AS t,ltrim('abbaXba','ab') AS l,rtrim('abbaXba','ab') AS r",
            ] {
                let expected = plain.sql(sql).await?;
                let expected_schema = expected.schema().clone();
                let expected_rows = expected.collect().await?;
                let actual = configured.sql(sql).await?;
                assert_eq!(actual.schema(), &expected_schema, "{sql}");
                assert_eq!(actual.collect().await?, expected_rows, "{sql}");
            }
        }
    }
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
