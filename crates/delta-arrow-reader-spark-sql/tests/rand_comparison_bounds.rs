//! Spark RAND ordering predicates use the half-open interval [0, 1).

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const BOUNDS: &str = include_str!("fixtures/rand-comparison-bounds-spark.jsonl");
const OPERANDS: &str = include_str!("fixtures/rand-comparison-bounds-operands-spark.jsonl");
const ADMISSION: &str = include_str!("fixtures/rand-comparison-bounds-admission-spark.jsonl");

fn references(source: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, count);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing provider")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), count);
    Ok(records)
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

// Only these measured missing condition IDs belong to #865/#1067; raw matches stay false.
fn independent_rand_diagnostic_gap(case: &Value, comparison: &Value) -> Option<&'static str> {
    let fixture = comparison["fixture"].as_str()?;
    let position: usize = comparison["record_position"].as_u64()?.try_into().ok()?;
    let source = match fixture {
        "rand-comparison-bounds-spark.jsonl" => BOUNDS,
        "rand-comparison-bounds-operands-spark.jsonl" => OPERANDS,
        "rand-comparison-bounds-admission-spark.jsonl" => ADMISSION,
        _ => return None,
    };
    let recorded: Value = serde_json::from_str(source.lines().nth(position)?).ok()?;
    if case != &recorded
        || ["id", "input", "ansi"]
            .iter()
            .any(|key| comparison[*key] != case[*key])
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
    {
        return None;
    }
    let (owner, condition, spark_phase, native_phase, cause) =
        match (fixture, position, case["id"].as_str()?) {
            ("rand-comparison-bounds-spark.jsonl", 62, "required_prefix_error")
            | ("rand-comparison-bounds-spark.jsonl", 63, "required_result_error")
            | ("rand-comparison-bounds-operands-spark.jsonl", 31, "required_prefix_error")
            | ("rand-comparison-bounds-operands-spark.jsonl", 32, "required_result_error") => (
                "#865", "DIVIDE_BY_ZERO", "collect", "collect",
                "Arrow error: Divide by zero error",
            ),
            ("rand-comparison-bounds-operands-spark.jsonl", 17, "failing_bound_normal")
            | ("rand-comparison-bounds-operands-spark.jsonl", 18, "failing_bound_swapped")
            | ("rand-comparison-bounds-admission-spark.jsonl", 19, "live_seed_error_normal")
            | ("rand-comparison-bounds-admission-spark.jsonl", 20, "live_seed_error_swapped")
            | ("rand-comparison-bounds-admission-spark.jsonl", 21, "subquery_bound_error_normal")
            | ("rand-comparison-bounds-admission-spark.jsonl", 22, "subquery_bound_error_swapped")
            | ("rand-comparison-bounds-admission-spark.jsonl", 25, "subquery_bound_error_control") => (
                "#865", "DIVIDE_BY_ZERO", "optimized_schema", "create_physical_plan",
                "preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error",
            ),
            ("rand-comparison-bounds-admission-spark.jsonl", 3 | 28, "invalid_arity_control")
            | ("rand-comparison-bounds-admission-spark.jsonl", 4 | 29, "invalid_arity_normal")
            | ("rand-comparison-bounds-admission-spark.jsonl", 5 | 30, "invalid_arity_swapped") => (
                "#1067", "WRONG_NUM_ARGS.WITHOUT_SUGGESTION", "sql", "resolve",
                "External error: error in DataFusion: Execution error: Spark `random` function requires 0 to 1 arguments, got 2",
            ),
            ("rand-comparison-bounds-admission-spark.jsonl", 6 | 31, "invalid_seed_type_control")
            | ("rand-comparison-bounds-admission-spark.jsonl", 7 | 32, "invalid_seed_type_normal")
            | ("rand-comparison-bounds-admission-spark.jsonl", 8 | 33, "invalid_seed_type_swapped") => (
                "#1067", "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE", "sql", "resolve",
                "External error: error in DataFusion: Execution error: Unsupported Data Type: Spark `random` function expects Integer Type for seed, got Utf8",
            ),
            _ => return None,
        };
    let fields_match = [
        ("analyzed", "public_schema"),
        ("optimized", "optimized_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ]
    .into_iter()
    .all(|(stage, actual)| {
        let expected = case["schema_stages"].get(stage).or_else(|| {
            (stage == "executed" && native_phase == "collect")
                .then(|| case["schema_stages"].get("executed_before_collect"))
                .flatten()
        });
        match (expected, comparison.get(actual)) {
            (Some(expected), Some(actual)) => actual == expected,
            (None, Some(Value::Null)) => true,
            _ => false,
        }
    });
    let optimizer_matches = if native_phase == "collect" {
        comparison.get("optimized_error") == Some(&Value::Null)
            && comparison["optimized_observation"]["names_match"] == true
            && comparison["optimized_observation"]["schema"] == comparison["optimized_schema"]
    } else {
        comparison["optimized_error"] == cause
            && comparison.get("optimized_observation") == Some(&Value::Null)
    };
    (case["expected"]["condition"] == condition
        && case["expected"].get("rows").is_none()
        && case["spark_error_phase"] == spark_phase
        && comparison["native_phase"] == native_phase
        && comparison["error"] == cause
        && comparison.get("rows") == Some(&Value::Null)
        && comparison.get("rows_raw") == Some(&Value::Null)
        && comparison["contract"] == "exact_spark_stages_rows_and_errors"
        && ["public_matches", "before_matches", "physical_matches", "rows_match", "ordered_rows_match"]
            .iter().all(|key| comparison[*key] == true)
        && comparison["error_matches"] == false
        && comparison["matches"] == false
        && comparison["contract_matches"] == false
        && comparison["optimized_matches"] == (native_phase == "collect")
        && fields_match
        && optimizer_matches)
    .then_some(owner)
}

async fn compare_references(fixture: &str, source: &str, count: usize) -> TestResult {
    let records = references(source, count)?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
    let mut null_seed_contracts = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for (record_position, case) in records.iter().enumerate().skip(1) {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            let null_seed_sample = case["ansi"] == false
                && matches!(
                    case["id"].as_str(),
                    Some("live_seed_error_normal" | "live_seed_error_swapped")
                );
            // NULL seeds use the existing unseeded path; recorded samples are not a stream contract.
            let matches = if null_seed_sample {
                null_seed_contracts += 1;
                comparison["public_matches"] == true
                    && comparison["before_matches"] == true
                    && comparison["physical_matches"] == true
                    && comparison["optimized_matches"] == true
                    && comparison["error_matches"] == true
                    && comparison["rows"].as_array().is_some_and(|rows| {
                        rows.len() == 3
                            && rows.iter().all(|row| {
                                row.as_array().is_some_and(|row| {
                                    row.len() == 1
                                        && matches!(row[0].as_str(), Some("true" | "false"))
                                })
                            })
                    })
            } else {
                comparison["matches"] == true && comparison["optimized_matches"] == true
            };
            comparison["fixture"] = json!(fixture);
            comparison["record_position"] = json!(record_position);
            comparison["id"] = case["id"].clone();
            comparison["input"] = case["input"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            comparison["contract"] = json!(if null_seed_sample {
                "null_seed_boolean_domain_and_fields"
            } else {
                "exact_spark_stages_rows_and_errors"
            });
            comparison["contract_matches"] = json!(matches);
            let gap_owner = (!matches)
                .then(|| independent_rand_diagnostic_gap(case, &comparison))
                .flatten();
            comparison["independent_gap_owner"] = json!(gap_owner);
            comparison["regression_matches"] = json!(matches || gap_owner.is_some());
            eprintln!("RAND_838_OBSERVATION {comparison}");
            if comparison["regression_matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert_eq!(null_seed_contracts, if source == ADMISSION { 4 } else { 0 });
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn rand_comparison_bounds_match_spark() -> TestResult {
    compare_references("rand-comparison-bounds-spark.jsonl", BOUNDS, 130).await
}

#[tokio::test]
async fn rand_comparison_operands_and_guard_orders_match_spark() -> TestResult {
    compare_references(
        "rand-comparison-bounds-operands-spark.jsonl",
        OPERANDS,
        70,
    )
    .await
}

#[tokio::test]
async fn rand_comparison_admission_and_errors_match_spark() -> TestResult {
    compare_references(
        "rand-comparison-bounds-admission-spark.jsonl",
        ADMISSION,
        50,
    )
    .await
}

#[tokio::test]
async fn rand_rejects_nonfoldable_seeds_before_pruning() -> TestResult {
    let records = references(BOUNDS, 130)?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for ansi_mode in [true, false] {
            let options = SparkSqlOptions {
                ansi_mode,
                ..SparkSqlOptions::default()
            };
            for name in ["rand", "random"] {
                for seed in [
                    "q.id",
                    "CAST(rand(1) AS BIGINT)",
                    "(SELECT 1 DIV 0)",
                    "(SELECT 1)",
                ] {
                    let call = format!("{name}({seed})");
                    for expression in [
                        call.clone(),
                        format!("{call} IS NULL"),
                        format!("{call}>2D"),
                        format!("2D<{call}"),
                        format!("CASE WHEN true THEN false ELSE {call}>2D END"),
                    ] {
                        let sql = format!("SELECT {expression} AS r FROM case_parent_mixed q");
                        let result = plan_query(&session, &sql, &options).await;
                        assert!(result.is_err(), "invalid seed admitted: {sql}");
                        let error = result.err().ok_or("missing admission error")?;
                        assert!(
                            error
                                .to_string()
                                .contains("[SEED_EXPRESSION_IS_UNFOLDABLE]"),
                            "{sql} ANSI={ansi_mode} batch={batch_size}: {error}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn rand_validates_seed_type_and_arity_before_pruning() -> TestResult {
    let session = session(1);
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        for name in ["rand", "random"] {
            for (arguments, cause) in [
                ("1, 2", "requires 0 to 1 arguments, got 2"),
                ("'bad'", "expects Integer Type for seed"),
            ] {
                for expression in [
                    format!("{name}({arguments}) IS NULL"),
                    format!("{name}({arguments})>2D"),
                    format!("2D<{name}({arguments})"),
                    format!("CASE WHEN true THEN false ELSE {name}({arguments})>2D END"),
                ] {
                    let sql = format!("SELECT {expression} AS r");
                    let result = plan_query(&session, &sql, &options).await;
                    assert!(result.is_err(), "invalid call admitted: {sql}");
                    let error = result.err().ok_or("missing admission error")?;
                    assert!(
                        error.to_string().contains(cause),
                        "{sql} ANSI={ansi_mode}: {error}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_rand_bounds_preserve_native_random() -> TestResult {
    let native = SessionContext::new();
    let configured = session(1);
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(
                &configured,
                "SELECT rand(0)>2 AS r",
                &SparkSqlOptions::default(),
            )
            .await?;
            collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
        }
        for sql in [
            "SELECT random()>2 AS r",
            "SELECT 2<random() AS r",
            "SELECT random()>0.5 AS r",
            "SELECT 0.5<random() AS r",
        ] {
            let expected = native.sql(sql).await?.into_optimized_plan()?;
            let actual = configured.sql(sql).await?.into_optimized_plan()?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
            let mut expected =
                spark_results::capture_native_query_observation(&native, sql).await?;
            let mut actual =
                spark_results::capture_native_query_observation(&configured, sql).await?;
            for observation in [&mut expected, &mut actual] {
                let rows = observation["rows"].take();
                let rows = rows.as_array().ok_or("missing native RAND rows")?;
                assert_eq!(rows.len(), 1, "{sql}");
                assert!(
                    rows[0].as_array().is_some_and(|row| {
                        row.len() == 1 && matches!(row[0].as_str(), Some("true" | "false"))
                    }),
                    "invalid native RAND predicate domain: {sql}, {rows:?}"
                );
            }
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
        let sql = "SELECT random(1) AS r";
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        assert!(
            expected["error"].is_string(),
            "native invalid call admitted"
        );
        assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
    }
    Ok(())
}

#[tokio::test]
async fn rand_declaration_preserves_tablesample_output() -> TestResult {
    let records = references(BOUNDS, 130)?;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for ansi in [true, false] {
            let expected = spark_results::capture_spark_query_observation(
                &session,
                &json!({"sql":"SELECT * FROM case_parent_mixed", "ansi":ansi}),
            )
            .await?;
            let actual = spark_results::capture_spark_query_observation(
                &session,
                &json!({"sql":"SELECT * FROM (SELECT * FROM case_parent_mixed) TABLESAMPLE (100 PERCENT) q",
                    "ansi":ansi}),
            )
            .await?;
            assert_eq!(actual, expected, "ANSI={ansi} batch={batch_size}");
        }
    }
    Ok(())
}

#[test]
fn rand_diagnostic_gates_reject_changed_owned_facets() -> TestResult {
    for (fixture, source, count) in [
        ("rand-comparison-bounds-spark.jsonl", BOUNDS, 130),
        ("rand-comparison-bounds-operands-spark.jsonl", OPERANDS, 70),
        ("rand-comparison-bounds-admission-spark.jsonl", ADMISSION, 50),
    ] {
        let records = references(source, count)?;
        for (position, case) in records.iter().enumerate().skip(1) {
            let (owner, cause, phase) = match case["expected"]["condition"].as_str() {
                Some("DIVIDE_BY_ZERO") => (
                    "#865",
                    if case["spark_error_phase"] == "collect" {
                        "Arrow error: Divide by zero error"
                    } else {
                        "preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error"
                    },
                    if case["spark_error_phase"] == "collect" { "collect" } else { "create_physical_plan" },
                ),
                Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => (
                    "#1067",
                    "External error: error in DataFusion: Execution error: Spark `random` function requires 0 to 1 arguments, got 2",
                    "resolve",
                ),
                Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => (
                    "#1067",
                    "External error: error in DataFusion: Execution error: Unsupported Data Type: Spark `random` function expects Integer Type for seed, got Utf8",
                    "resolve",
                ),
                _ => ("", "Arrow error: Divide by zero error", "collect"),
            };
            for batch in [1, 32] {
                let stages = &case["schema_stages"];
                let physical = stages.get("executed").unwrap_or(&stages["executed_before_collect"]);
                // Synthetic observations exercise the fallback; real observer flags are never rewritten.
                let observed = json!({
                    "fixture":fixture, "record_position":position,
                    "id":case["id"], "input":case["input"], "ansi":case["ansi"], "batch":batch,
                    "public_schema":stages["analyzed"], "optimized_schema":stages["optimized"],
                    "before_collect":stages["executed_before_collect"], "physical_schema":physical,
                    "rows":null, "rows_raw":null, "native_phase":phase, "error":cause,
                    "optimized_error":if phase == "collect" { Value::Null } else { json!(cause) },
                    "optimized_observation":if stages["optimized"].is_null() { Value::Null } else {
                        json!({"schema":stages["optimized"], "names_match":true})
                    },
                    "public_matches":true, "before_matches":true, "physical_matches":true,
                    "rows_match":true, "ordered_rows_match":true, "error_matches":false,
                    "matches":false, "optimized_matches":phase == "collect",
                    "contract":"exact_spark_stages_rows_and_errors", "contract_matches":false,
                });
                if owner.is_empty() {
                    // Original guard orders, implicit NULLs and sampled successes have no diagnostic fallback.
                    assert_eq!(independent_rand_diagnostic_gap(case, &observed), None);
                    continue;
                }
                assert_eq!(independent_rand_diagnostic_gap(case, &observed), Some(owner));
                for (key, wrong) in [
                    ("fixture", json!("other.jsonl")), ("record_position", json!(0)),
                    ("id", json!("unrelated")), ("input", json!("other")),
                    ("ansi", json!(!case["ansi"].as_bool().ok_or("missing ANSI")?)),
                    ("batch", json!(2)), ("native_phase", json!("encode")),
                    ("error", Value::Null), ("error", json!("Arrow error: different failure")),
                    ("error", json!(format!("{cause}: unrelated suffix"))),
                    ("rows", json!([])), ("rows_raw", json!([])),
                    ("optimized_error", json!("different optimizer failure")),
                    ("optimized_observation", json!({})),
                    ("public_matches", json!(false)), ("before_matches", json!(false)),
                    ("physical_matches", json!(false)), ("rows_match", json!(false)),
                    ("ordered_rows_match", json!(false)), ("error_matches", json!(true)),
                    ("matches", json!(true)), ("optimized_matches", json!(phase != "collect")),
                    ("contract", json!("null_seed_boolean_domain_and_fields")),
                    ("contract_matches", json!(true)),
                ] {
                    let mut wrong_observation = observed.clone();
                    wrong_observation[key] = wrong;
                    assert_eq!(independent_rand_diagnostic_gap(case, &wrong_observation), None, "{fixture}:{position}:{key}");
                }
                for key in ["public_schema", "optimized_schema", "before_collect", "physical_schema"] {
                    if observed[key].is_null() {
                        let mut unexpected = observed.clone();
                        unexpected[key] = json!({"type":"struct", "fields":[]});
                        assert_eq!(independent_rand_diagnostic_gap(case, &unexpected), None);
                    } else {
                        for wrong in [Value::Null, json!({"type":"struct", "fields":[]})] {
                            let mut incomplete = observed.clone();
                            incomplete[key] = wrong;
                            assert_eq!(independent_rand_diagnostic_gap(case, &incomplete), None);
                        }
                        if let Some(nullable) = observed[key]["fields"][0]["type"]["fields"][0]["nullable"].as_bool() {
                            let mut nested = observed.clone();
                            nested[key]["fields"][0]["type"]["fields"][0]["nullable"] = json!(!nullable);
                            assert_eq!(independent_rand_diagnostic_gap(case, &nested), None);
                            let mut nested = observed.clone();
                            nested[key]["fields"][0]["type"]["fields"][0]["metadata"] = json!({"source":"wrong child"});
                            assert_eq!(independent_rand_diagnostic_gap(case, &nested), None);
                        }
                        for (attribute, wrong) in [
                            ("name", json!("wrong")), ("type", json!("wrong")),
                            ("nullable", json!(!observed[key]["fields"][0]["nullable"].as_bool().ok_or("missing nullable")?)),
                            ("metadata", json!({"source":"wrong"})),
                        ] {
                            let mut wrong_field = observed.clone();
                            wrong_field[key]["fields"][0][attribute] = wrong;
                            assert_eq!(independent_rand_diagnostic_gap(case, &wrong_field), None, "{key}:{attribute}");
                        }
                    }
                }
                for key in observed.as_object().ok_or("missing observation")?.keys() {
                    let mut missing = observed.clone();
                    missing.as_object_mut().ok_or("missing object")?.remove(key);
                    assert_eq!(independent_rand_diagnostic_gap(case, &missing), None, "missing {key}");
                }
                for pointer in ["/sql", "/expected/condition", "/spark_error_phase"] {
                    let mut wrong_reference = case.clone();
                    *wrong_reference.pointer_mut(pointer).ok_or("missing reference mutation target")? = json!("changed");
                    assert_eq!(independent_rand_diagnostic_gap(&wrong_reference, &observed), None);
                }
                let mut corrected = observed.clone();
                corrected["error"] = json!(format!("[{}] {cause}", case["expected"]["condition"].as_str().ok_or("missing condition")?));
                if phase != "collect" { corrected["optimized_error"] = corrected["error"].clone(); }
                corrected["matches"] = json!(true);
                corrected["error_matches"] = json!(true);
                corrected["optimized_matches"] = json!(true);
                corrected["contract_matches"] = json!(true);
                assert_eq!(independent_rand_diagnostic_gap(case, &corrected), None);
                assert!(corrected["matches"] == true && corrected["optimized_matches"] == true);
            }
        }
    }
    Ok(())
}
