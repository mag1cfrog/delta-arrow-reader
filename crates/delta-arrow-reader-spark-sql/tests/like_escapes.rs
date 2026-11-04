//! Spark LIKE validates escapes without evaluating dead dynamic arguments.

use std::collections::{BTreeMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DataFusionResult, tree_node::Transformed};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, Volatility, create_udf};
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/like_profiles.rs"]
mod like_profiles;
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{
    TestResult, capture_observation, capture_observation_with_state, capture_result,
    register_inputs,
};

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

fn required_error_matches(case: &Value, error: &str) -> bool {
    let expected = &case["expected"];
    match expected["condition"].as_str() {
        Some("INVALID_FORMAT.ESC_IN_THE_MIDDLE") | Some("INVALID_FORMAT.ESC_AT_THE_END") => {
            let condition = expected["condition"].as_str().unwrap_or_default();
            let format = expected["parameters"]["format"]
                .as_str()
                .unwrap_or_default();
            // Spark quotes its format parameter by escaping each backslash.
            let pattern = format
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(format)
                .replace("\\\\", "\\");
            let phase = if case["id"].as_str().is_some_and(|id| {
                id.contains("invalid_column_")
                    || id.contains("_dynamic_plain")
                    || id.contains("_dynamic_row_")
            }) {
                "execute:"
            } else {
                "physical:"
            };
            error.starts_with(phase)
                && error.contains(&format!("[{condition}]"))
                && error.contains(&format!("The format is invalid: '{pattern}'."))
                && match expected["parameters"]["char"].as_str() {
                    Some(character) => {
                        error.contains(&format!("not allowed to precede {character}"))
                    }
                    None => error.contains("not allowed to end with"),
                }
        }
        Some("INVALID_ESC") => {
            error.starts_with("resolve:")
                && error.contains("[INVALID_ESC]")
                && error.contains(
                    expected["parameters"]["invalidEscape"]
                        .as_str()
                        .unwrap_or("missing escape"),
                )
                && error.contains("only one UTF-16 code unit")
        }
        Some("INVALID_ESCAPE_CHAR") => {
            let expression = expected["parameters"]["sqlExpr"]
                .as_str()
                .unwrap_or("missing expression");
            error.starts_with("resolve:")
                && error.contains(&format!(
                    "[INVALID_ESCAPE_CHAR] `EscapeChar` should be a string literal of length one, but got {expression}."
                ))
        }
        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
            let count = expected["parameters"]["actualNum"]
                .as_str()
                .unwrap_or("missing count");
            error.starts_with("resolve:")
                && error.contains(&format!("like/ilike expects 2 or 3 arguments, got {count}"))
        }
        _ => false,
    }
}

fn nullif_error_matches(case: &Value, error: &str) -> bool {
    case["id"]
        .as_str()
        .is_some_and(|id| id.starts_with("like_limit_guard_nullif_"))
        && case["ansi"] == true
        && case["expected"]["condition"] == "CAST_INVALID_INPUT"
        && case["expected"]["parameters"]["expression"] == "'bad'"
        && case["expected"]["parameters"]["sourceType"] == "\"STRING\""
        && case["expected"]["parameters"]["targetType"] == "\"BIGINT\""
        && error.starts_with("physical:")
        && error.contains("[CAST_INVALID_INPUT]")
        && error
            .contains("Arrow error: Cast error: Cannot cast string 'bad' to value of Int64 type")
}

#[tokio::test]
async fn like_escapes_preserve_complete_observations() -> TestResult {
    let profiles = like_profiles::load()?;
    let mut retained_keys = HashSet::new();
    let mut strict_incompatible = 0;
    let mut full_incompatible = 0;
    let mut differences = Vec::new();
    let mut compared = 0;
    let mut generated_name_controls = 0;
    let mut arity_causes = 0;
    let mut nullif_causes = 0;
    let mut available_error_physical_fields = 0;
    for (capture, count) in [
        (include_str!("fixtures/like-escapes-spark.jsonl"), 196),
        (include_str!("fixtures/like-evaluation-spark.jsonl"), 288),
        (include_str!("fixtures/like-function-spark.jsonl"), 60),
        (include_str!("fixtures/like-clock-spark.jsonl"), 16),
        (include_str!("fixtures/like-local-values-spark.jsonl"), 24),
        (include_str!("fixtures/like-local-clock-spark.jsonl"), 24),
        (
            include_str!("fixtures/like-positive-offset-spark.jsonl"),
            48,
        ),
        (
            include_str!("fixtures/like-unevaluable-sibling-spark.jsonl"),
            64,
        ),
        (
            include_str!("fixtures/like-limit-guard-nullif-spark.jsonl"),
            40,
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
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let expected = &case["expected"];
                    let observed = capture_observation(&session, case).await?;
                    available_error_physical_fields +=
                        usize::from(observed["error_stage"] == "execute");
                    let generated_name_case = case["id"] == "unaliased_pattern_names";
                    let arity = expected["condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION";
                    let nullif = expected["condition"] == "CAST_INVALID_INPUT";
                    nullif_causes += usize::from(nullif);
                    generated_name_controls += usize::from(generated_name_case);
                    arity_causes += usize::from(arity);
                    let comparison = like_profiles::compare_spark(case, &observed);
                    strict_incompatible += usize::from(comparison["strict_compatibility"] == false);
                    full_incompatible += usize::from(comparison["full_compatibility"] == false);
                    let key = like_profiles::key(case, representation, batch_size)?;
                    let profile = profiles.get(&key);
                    let retained = profile.is_some_and(|profile| {
                        assert!(retained_keys.insert(key.clone()));
                        profile["sql"] == case["sql"]
                            && like_profiles::retained_matches(profile, &observed, &comparison)
                    });
                    let matches = if profile.is_some() {
                        retained
                    } else {
                        comparison["full_compatibility"] == true
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,
                            "classification":if profile.is_some() {"retained-complete-native-negative-observation"} else {"strict"},
                            "owner":profile.map(|profile| &profile["owner"]),
                            "schema_equivalence":{
                                "analyzed":if expected["analyzed_schema"].is_null() {"unavailable-spark-reference"} else {"compared"},
                                "physical":if expected["physical_schema"].is_null() {"unavailable-spark-reference"} else {"compared"},
                            },"actual":observed,"comparison":comparison,
                            "strict_compatibility":comparison["strict_compatibility"],
                            "full_compatibility":comparison["full_compatibility"],
                            "retained_native_actual_matches":retained,
                            "accepted_as_compatibility":comparison["full_compatibility"],
                        })
                    );
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, 760 * 6);
    assert_eq!(generated_name_controls, 2 * 6);
    assert_eq!(arity_causes, 12 * 6);
    assert_eq!(nullif_causes, 10 * 6);
    assert_eq!(available_error_physical_fields, 24 * 6);
    assert_eq!(retained_keys.len(), 180);
    assert_eq!(strict_incompatible, 180);
    assert_eq!(full_incompatible, 180);
    eprintln!(
        "C06 complete observations {compared}, strict incompatible {strict_incompatible}, full incompatible {full_incompatible}, retained {}, unexpected differences {}",
        retained_keys.len(),
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn compare_clock_controls(
    fixture_name: &str,
    capture: &str,
    sha256: &str,
    case_count: usize,
) -> TestResult {
    let records = capture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, case_count);
    let mut observation_ids = HashSet::new();
    let mut differences = Vec::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for (clock, before_cutoff) in [
                ("1999-01-01T00:00:00Z", true),
                ("2026-01-01T00:00:00Z", false),
            ] {
                for case in &records[1..] {
                    let (projection, control) = case["sql"]
                        .as_str()
                        .and_then(|sql| sql.rsplit_once(" WHERE "))
                        .ok_or("missing captured WHERE control")?;
                    let (control, suffix) = if let Some(suffix) = control.strip_prefix("true") {
                        (true, suffix)
                    } else if let Some(suffix) = control.strip_prefix("false") {
                        (false, suffix)
                    } else {
                        return Err("unexpected captured WHERE control".into());
                    };
                    let comparison = if control == before_cutoff { "<" } else { ">=" };
                    let sql = format!(
                        "{projection} WHERE current_timestamp() {comparison} TIMESTAMP '2000-01-01 00:00:00'{suffix}"
                    );
                    let mut state = session.state();
                    state.execution_props_mut().query_execution_start_time = Some(clock.parse()?);
                    state
                        .config_mut()
                        .options_mut()
                        .extensions
                        .insert(SparkSqlExpressionSemantics::default());
                    let actual_clock = state
                        .execution_props()
                        .query_execution_start_time
                        .map(|time| time.to_rfc3339());
                    let dar_config = state
                        .config_options()
                        .entries()
                        .into_iter()
                        .map(|entry| (entry.key, entry.value))
                        .collect::<BTreeMap<_, _>>();
                    let dar_case = json!({"sql":sql,"ansi":case["ansi"]});
                    let observed =
                        capture_observation_with_state(&session, &dar_case, Some(state)).await?;
                    let expected = &case["expected"];
                    let matches = observed["analyzed_schema"] == expected["analyzed_schema"]
                        && if expected["status"] == "error" {
                            observed["error_stage"] == "physical"
                                && observed["physical_schema"].is_null()
                                && observed["physical_arrow_schema"].is_null()
                                && observed["error"]
                                    .as_str()
                                    .is_some_and(|error| required_error_matches(case, error))
                        } else {
                            observed["error"].is_null()
                                && observed["physical_schema"] == expected["physical_schema"]
                                && observed["rows"] == expected["rows"]
                        };
                    let spark_comparison = like_profiles::compare_spark(case, &observed);
                    assert_eq!(spark_comparison["full_compatibility"], matches);
                    let observation_id = format!(
                        "{}:ansi={}:representation={representation}:batch={batch_size}:clock={clock}",
                        case["id"].as_str().ok_or("missing control ID")?,
                        case["ansi"],
                    );
                    assert!(observation_ids.insert(observation_id.clone()));
                    eprintln!(
                        "c06 clock observation: {}",
                        json!({
                            "observation_kind":"metamorphic-query-clock",
                            "observation_id":observation_id,
                            "source_reference":{
                                "fixture":fixture_name,
                                "sha256":sha256,
                                "header":records[0],
                                "spark_control":case,
                            },
                            "dar_sql":sql,
                            "query_execution_start_time":actual_clock,
                            "predicate_operator":comparison,
                            "source_boolean_predicate":control,
                            "ansi":case["ansi"],
                            "representation":representation,
                            "batch_size":batch_size,
                            "dar_settings":{
                                "session_config":dar_config,
                                "spark_sql_options":{
                                    "session_timezone":"UTC",
                                    "ansi_mode":case["ansi"],
                                },
                            },
                            "schema_equivalence":{
                                "analyzed":"metamorphic-compared",
                                "physical":if expected["physical_schema"].is_null() {
                                    "unavailable-spark-reference"
                                } else {
                                    "metamorphic-compared"
                                },
                            },
                            "actual":observed,
                            "comparison":spark_comparison,
                            "strict_compatibility":spark_comparison["strict_compatibility"],
                            "full_compatibility":spark_comparison["full_compatibility"],
                            "result":{
                                "actual_status":if observed["error"].is_null() {"ok"} else {"error"},
                                "matches_spark_control":matches,
                            },
                        })
                    );
                    if !matches {
                        differences.push(format!("{observation_id}: {sql}: {observed}"));
                    }
                }
            }
        }
    }
    assert_eq!(observation_ids.len(), case_count * 2 * 6);
    eprintln!(
        "C06 clock compared {}, mismatches {}",
        observation_ids.len(),
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn literal_pattern_reachability_uses_the_recorded_query_clock() -> TestResult {
    compare_clock_controls(
        "like-clock-spark.jsonl",
        include_str!("fixtures/like-clock-spark.jsonl"),
        "b879b82d09b1bd8f87d0556e2cdfb177940ccb716c4c65f89cdd4ef19f244b5a",
        16,
    )
    .await
}

#[tokio::test]
async fn local_values_pattern_ordering_uses_the_recorded_query_clock() -> TestResult {
    compare_clock_controls(
        "like-local-clock-spark.jsonl",
        include_str!("fixtures/like-local-clock-spark.jsonl"),
        "82ceb032ba44cc403f367a93701f206b32b0b0ec61fccb11b9f5440814cc6c11",
        24,
    )
    .await
}

#[tokio::test]
async fn dynamic_pattern_error_retains_actual_physical_fields() -> TestResult {
    let records = include_str!("fixtures/like-evaluation-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let case = records
        .iter()
        .find(|case| case["id"] == "like_dynamic_row_2" && case["ansi"] == true)
        .ok_or("missing dynamic error reference")?;
    assert!(case["expected"]["physical_schema"].is_null());
    let session = session(1);
    register_inputs(&session, &records[0], 2, 1)?;
    let observed = capture_observation(&session, case).await?;
    assert_eq!(observed["error_stage"], "execute");
    assert!(required_error_matches(
        case,
        observed["error"]
            .as_str()
            .ok_or("missing execution error")?,
    ));
    let actual_field = json!({"type":"struct","fields":[{
        "name":"r","type":"boolean","nullable":true,"metadata":{},
    }]});
    assert_eq!(observed["analyzed_schema"], actual_field);
    assert_eq!(observed["physical_schema"], actual_field);
    assert!(observed["analyzed_arrow_schema"].is_string());
    assert!(observed["physical_arrow_schema"].is_string());
    assert!(observed["rows"].is_null());
    Ok(())
}

#[test]
fn required_error_matcher_rejects_other_failures() {
    let case = json!({"id":"like_invalid_default_bad_escape","expected":{"condition":"INVALID_FORMAT.ESC_IN_THE_MIDDLE","parameters":{"format":"'a\\\\q'","char":"'q'"}}});
    let valid = "physical: [INVALID_FORMAT.ESC_IN_THE_MIDDLE] The format is invalid: 'a\\q'. The escape character is not allowed to precede 'q'.";
    assert!(required_error_matches(&case, valid));
    for error in [
        valid.replace("physical:", "execute:"),
        valid.replace("ESC_IN_THE_MIDDLE", "ESC_AT_THE_END"),
        valid.replace("a\\q", "other\\q"),
        valid.replace("precede 'q'", "precede 'x'"),
        "physical: CAST_INVALID_INPUT".into(),
    ] {
        assert!(!required_error_matches(&case, &error), "{error}");
    }
}

#[test]
fn function_escape_error_matcher_requires_the_recorded_admission_cause() {
    for expression in [
        "\"\"",
        "\"ab\"",
        "\"😀\"",
        "\"NULL\"",
        "\"CAST(NULL AS STRING)\"",
    ] {
        let case = json!({"expected":{"condition":"INVALID_ESCAPE_CHAR","parameters":{"sqlExpr":expression}}});
        let error = format!(
            "resolve: [INVALID_ESCAPE_CHAR] `EscapeChar` should be a string literal of length one, but got {expression}."
        );
        assert!(required_error_matches(&case, &error));
        for other in [
            error.replace("resolve:", "execute:"),
            error.replace("INVALID_ESCAPE_CHAR", "INVALID_ESC"),
            error.replace(expression, "\"other\""),
            "resolve: CAST_INVALID_INPUT".into(),
        ] {
            assert!(!required_error_matches(&case, &other));
        }
    }
}

#[test]
fn nullif_cause_matcher_rejects_unrelated_errors() {
    let case = json!({"id":"like_limit_guard_nullif_provider_control_no_limit","ansi":true,
        "expected":{"condition":"CAST_INVALID_INPUT","parameters":{
            "expression":"'bad'","sourceType":"\"STRING\"","targetType":"\"BIGINT\""}}});
    let error = "physical: preserve_spark_expression_errors\ncaused by\n[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad' to value of Int64 type";
    assert!(nullif_error_matches(&case, error));
    for other in [
        error.replace("physical:", "execute:"),
        error.replace("'bad'", "'other'"),
        error.replace("Int64", "Int32"),
        error.replace("[CAST_INVALID_INPUT]", "[DIVIDE_BY_ZERO]"),
        "physical: division by zero".into(),
    ] {
        assert!(!nullif_error_matches(&case, &other));
        assert!(!required_error_matches(&case, &other));
    }
}

#[tokio::test]
async fn like_escapes_preserve_native_sql_and_single_evaluation() -> TestResult {
    let native_sql =
        "SELECT 'Σ' ILIKE 'ς' AS sigma,'ſ' ILIKE 's' AS long_s,'a%b' LIKE 'a\\%b' AS escaped";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = session(1);
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    session.register_batch(
        "counted_like",
        RecordBatch::try_from_iter([
            (
                "s",
                Arc::new(StringArray::from(vec![Some("a%b"), None, Some("a!b")])) as ArrayRef,
            ),
            (
                "p",
                Arc::new(StringArray::from(vec![
                    Some("a!%b"),
                    Some("bad!q"),
                    Some("a!!b"),
                ])) as ArrayRef,
            ),
        ])?,
    )?;
    let source_rows = Arc::new(AtomicUsize::new(0));
    let pattern_rows = Arc::new(AtomicUsize::new(0));
    for (name, counter) in [
        ("count_source", source_rows.clone()),
        ("count_pattern", pattern_rows.clone()),
    ] {
        session.register_udf(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(move |args| {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
                Ok(ColumnarValue::Array(arrays[0].clone()))
            }),
        ));
    }
    for ansi in [true, false] {
        for function in ["LIKE", "ILIKE"] {
            for source in [
                "counted_like",
                "(VALUES ('a%b','a!%b'),(CAST(NULL AS STRING),'bad!q'),('a!b','a!!b')) v(s,p)",
            ] {
                source_rows.store(0, Ordering::SeqCst);
                pattern_rows.store(0, Ordering::SeqCst);
                let sql = format!(
                    "SELECT count_source(s) {function} count_pattern(p) ESCAPE '!' AS r FROM {source}"
                );
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
                assert_eq!(pattern_rows.load(Ordering::SeqCst), 0);
                let batches = collect(plan, session.task_ctx()).await?;
                let mut values = Vec::new();
                for batch in &batches {
                    let result = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or("expected Boolean LIKE result")?;
                    values.extend(result.iter());
                }
                eprintln!(
                    "c06 counted like observation: {}",
                    json!({
                        "sql":sql,
                        "ansi":ansi,
                        "actual_values":values,
                        "source_rows":source_rows.load(Ordering::SeqCst),
                        "pattern_rows":pattern_rows.load(Ordering::SeqCst),
                    })
                );
                assert_eq!(values, [Some(true), None, Some(true)]);
                assert_eq!(source_rows.load(Ordering::SeqCst), 3);
                assert_eq!(pattern_rows.load(Ordering::SeqCst), 2);
                let error = capture_result(
                    &session,
                    &json!({
                        "sql":format!("SELECT 'abc' {function} 'a!b' ESCAPE '!' AS r"),
                        "ansi":ansi,
                    }),
                )
                .await
                .err()
                .ok_or("expected invalid LIKE pattern error")?;
                assert!(
                    error
                        .to_string()
                        .contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]")
                );
                assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
            }
        }
    }
    for ansi in [true, false] {
        for function in ["LIKE", "ILIKE"] {
            for source in ["'abc'", "CAST(NULL AS STRING)"] {
                source_rows.store(0, Ordering::SeqCst);
                let sql = format!(
                    "SELECT s {function} 'a!b' ESCAPE '!' AS r,count_source('sibling') AS n FROM (VALUES ({source})) v(s) LIMIT 0"
                );
                let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                eprintln!(
                    "c06 volatile sibling observation: {}",
                    json!({"observation_kind":"volatile-sibling-counter-control",
                        "sql":sql,"ansi":ansi,"actual":actual,
                        "sibling_callbacks":source_rows.load(Ordering::SeqCst)})
                );
                assert_eq!(source_rows.load(Ordering::SeqCst), 0);
                if source == "'abc'" {
                    let case = json!({"id":"volatile_sibling_literal","expected":{
                        "condition":"INVALID_FORMAT.ESC_IN_THE_MIDDLE",
                        "parameters":{"format":"'a!b'","char":"'b'"}}});
                    assert!(
                        actual["error"]
                            .as_str()
                            .is_some_and(|e| required_error_matches(&case, e))
                    );
                } else {
                    assert!(actual["error"].is_null());
                    assert_eq!(actual["rows"], json!([]));
                }
            }
        }
    }
    Ok(())
}

// The rule never changes a plan. Counters observe actual callback contexts.
#[derive(Debug)]
struct HostRule {
    label: &'static str,
    native_calls: AtomicUsize,
    spark_calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct HostAnalyzer {
    native_calls: AtomicUsize,
    spark_calls: AtomicUsize,
}

impl AnalyzerRule for HostAnalyzer {
    fn name(&self) -> &str {
        "c06_host_analyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        options: &datafusion::common::config::ConfigOptions,
    ) -> DataFusionResult<LogicalPlan> {
        let spark = options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        let counter = if spark {
            &self.spark_calls
        } else {
            &self.native_calls
        };
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(plan)
    }
}

impl HostRule {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            native_calls: AtomicUsize::new(0),
            spark_calls: AtomicUsize::new(0),
        }
    }
}

impl OptimizerRule for HostRule {
    fn name(&self) -> &str {
        self.label
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let spark = config
            .options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        let counter = if spark {
            &self.spark_calls
        } else {
            &self.native_calls
        };
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(Transformed::no(plan))
    }
}

fn register_nulls(session: &SessionContext) -> DataFusionResult<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    session.register_batch(
        "isolation_values",
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![None::<&str>, None])) as ArrayRef],
        )?,
    )?;
    Ok(())
}

async fn native_error(session: &SessionContext, sql: &str) -> TestResult<String> {
    let error = match session.sql(sql).await {
        Err(error) => error,
        Ok(frame) => frame
            .collect()
            .await
            .err()
            .ok_or("native invalid CAST must fail")?,
    };
    Ok(error.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_host_rules_and_concurrent_native_spark_queries_stay_isolated() -> TestResult {
    let first = Arc::new(HostRule::new("c06_host_first"));
    let last = Arc::new(HostRule::new("c06_host_last"));
    let host_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        first.clone(),
        Arc::new(SimplifyExpressions::new()),
        last.clone(),
    ];
    let host_analyzer = Arc::new(HostAnalyzer::default());
    let state = configure_spark_sql(
        SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rules(host_rules.clone())
            .with_analyzer_rule(host_analyzer.clone()),
    )
    .build();
    let installed = &state.optimizer().rules;
    assert_eq!(installed.len(), host_rules.len());
    for (actual, expected) in installed.iter().zip(&host_rules) {
        assert!(
            Arc::ptr_eq(actual, expected),
            "host rule {} was replaced or reordered",
            expected.name()
        );
    }
    let session = Arc::new(SessionContext::new_with_state(state));
    register_nulls(&session)?;
    let plain = SessionContext::new();
    register_nulls(&plain)?;
    let native_sql = r"SELECT s LIKE 'a\q' AS r FROM isolation_values";
    let native_expected = plain.sql(native_sql).await?.collect().await?;
    let invalid_cast = "SELECT CAST('bad' AS INT) AS r";
    let native_error_expected = native_error(&plain, invalid_cast).await?;
    assert!(
        session
            .state()
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
    );

    let native_session = Arc::clone(&session);
    let native_task = tokio::spawn(async move {
        for _ in 0..12 {
            tokio::task::yield_now().await;
            let actual = native_session.sql(native_sql).await?.collect().await?;
            assert_eq!(actual, native_expected);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let error_session = Arc::clone(&session);
    let native_error_task = tokio::spawn(async move {
        for _ in 0..12 {
            tokio::task::yield_now().await;
            assert_eq!(
                native_error(&error_session, invalid_cast).await?,
                native_error_expected
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let spark_session = Arc::clone(&session);
    let spark_task = tokio::spawn(async move {
        for _ in 0..12 {
            tokio::task::yield_now().await;
            let query = plan_query(
                &spark_session,
                "SELECT 'a%b' LIKE 'a!%b' ESCAPE '!' AS r",
                &SparkSqlOptions::default(),
            )
            .await?;
            let physical = query.create_physical_plan(&spark_session).await?;
            let field = physical.schema().field(0).clone();
            assert_eq!(field.name(), "r");
            assert_eq!(field.data_type(), &DataType::Boolean);
            assert!(!field.is_nullable());
            assert!(field.metadata().is_empty());
            let batches = collect(physical, spark_session.task_ctx()).await?;
            assert_eq!(batches.len(), 1);
            let values = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean LIKE output");
            assert_eq!(values.iter().collect::<Vec<_>>(), [Some(true)]);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let spark_error_session = Arc::clone(&session);
    let spark_error_task = tokio::spawn(async move {
        for _ in 0..12 {
            tokio::task::yield_now().await;
            let query = plan_query(
                &spark_error_session,
                "SELECT s LIKE 'a!b' ESCAPE '!' AS r FROM isolation_values",
                &SparkSqlOptions::default(),
            )
            .await?;
            let error = query
                .create_physical_plan(&spark_error_session)
                .await
                .expect_err("Spark literal initializes on all-NULL provider")
                .to_string();
            assert!(
                error.contains("[INVALID_FORMAT.ESC_IN_THE_MIDDLE]"),
                "{error}"
            );
            assert!(error.contains("The format is invalid: 'a!b'."), "{error}");
            assert!(error.contains("not allowed to precede 'b'"), "{error}");
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let (native, native_error, spark, spark_error) =
        tokio::join!(native_task, native_error_task, spark_task, spark_error_task);
    native??;
    native_error??;
    spark??;
    spark_error??;
    assert!(
        session
            .state()
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
    );
    for rule in [first, last] {
        let native = rule.native_calls.load(Ordering::SeqCst);
        let spark = rule.spark_calls.load(Ordering::SeqCst);
        eprintln!(
            "c06 host isolation observation: rule={} native_callbacks={} spark_callbacks={}",
            rule.label, native, spark
        );
        assert_eq!(
            native,
            if rule.label == "c06_host_first" {
                24
            } else {
                12
            }
        );
        assert_eq!(spark, 24);
    }
    assert_eq!(host_analyzer.native_calls.load(Ordering::SeqCst), 24);
    assert_eq!(host_analyzer.spark_calls.load(Ordering::SeqCst), 24);
    Ok(())
}

#[tokio::test]
async fn like_initialization_and_pruning_survive_host_optimizer_options() -> TestResult {
    let limit_references = include_str!("fixtures/like-host-limit-boundary-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(limit_references.len() - 1, 8);
    for (label, config) in [
        ("default", SessionConfig::new()),
        (
            "skip_failed_rules",
            SessionConfig::new().set_bool("datafusion.optimizer.skip_failed_rules", true),
        ),
        (
            "zero_optimizer_passes",
            SessionConfig::new().set_usize("datafusion.optimizer.max_passes", 0),
        ),
        (
            "both",
            SessionConfig::new()
                .set_bool("datafusion.optimizer.skip_failed_rules", true)
                .set_usize("datafusion.optimizer.max_passes", 0),
        ),
    ] {
        let expected_options = config.options().clone();
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        register_nulls(&session)?;
        for ansi in [true, false] {
            for function in ["LIKE", "ILIKE"] {
                for sql in [
                    format!("SELECT s {function} 'a!b' ESCAPE '!' AS r FROM isolation_values"),
                    format!(
                        "SELECT s {function} 'a!b' ESCAPE '!' AS r FROM (VALUES ('abc')) v(s) LIMIT 0"
                    ),
                ] {
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let error = query
                        .create_physical_plan(&session)
                        .await
                        .expect_err("mandatory Spark initialization")
                        .to_string();
                    let case = json!({"expected":{"condition":"INVALID_FORMAT.ESC_IN_THE_MIDDLE","parameters":{"format":"'a!b'","char":"'b'"}}});
                    assert!(
                        required_error_matches(&case, &format!("physical: {error}")),
                        "option={label} ANSI={ansi} sql={sql}: {error}"
                    );
                }
                for (sql, expected) in [
                    (
                        format!("SELECT CAST(NULL AS STRING) {function} 'a!b' ESCAPE '!' AS r"),
                        vec![None],
                    ),
                    (
                        format!(
                            "SELECT CASE WHEN false THEN s {function} 'a!b' ESCAPE '!' ELSE true END AS r FROM isolation_values"
                        ),
                        vec![Some(true), Some(true)],
                    ),
                    (
                        format!(
                            "SELECT s {function} 'a!b' ESCAPE '!' AS r FROM isolation_values WHERE false"
                        ),
                        vec![],
                    ),
                    (
                        format!(
                            "SELECT s {function} 'a!b' ESCAPE '!' AS r FROM isolation_values LIMIT 0"
                        ),
                        vec![],
                    ),
                ] {
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    let batches = collect(physical, session.task_ctx()).await?;
                    let mut actual = Vec::new();
                    for batch in &batches {
                        actual.extend(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or("Boolean LIKE result")?
                                .iter(),
                        );
                    }
                    assert_eq!(actual, expected, "option={label} ANSI={ansi} sql={sql}");
                }
            }
        }
        for case in &limit_references[1..] {
            let actual = capture_observation(&session, case).await?;
            let expected = &case["expected"];
            let comparison = like_profiles::compare_spark(case, &actual);
            eprintln!(
                "c06 host limit reference observation: {}",
                json!({"option":label,"id":case["id"],"ansi":case["ansi"],"actual":actual,
                    "comparison":comparison,"strict_compatibility":comparison["strict_compatibility"],
                    "full_compatibility":comparison["full_compatibility"]})
            );
            assert_eq!(comparison["full_compatibility"], true);
            assert_eq!(actual["analyzed_schema"], expected["analyzed_schema"]);
            if expected["status"] == "ok" {
                assert!(actual["error"].is_null(), "option={label}: {actual}");
                assert_eq!(actual["physical_schema"], expected["physical_schema"]);
                assert_eq!(actual["rows"], expected["rows"]);
            } else {
                assert_eq!(actual["error_stage"], "physical");
                assert!(actual["physical_schema"].is_null());
                assert!(actual["physical_arrow_schema"].is_null());
                assert!(
                    actual["error"]
                        .as_str()
                        .is_some_and(|error| required_error_matches(case, error)),
                    "option={label}: {actual}"
                );
            }
        }
        for ansi in [true, false] {
            for sql in [
                "SELECT s AS r FROM (VALUES (CAST(NULL AS STRING))) v(s) LIMIT 0",
                "SELECT s AS r FROM (VALUES ('abc')) v(s) LIMIT 0 OFFSET 1",
            ] {
                let actual = capture_observation(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                assert!(actual["error"].is_null(), "option={label}: {actual}");
                assert_eq!(actual["rows"], json!([]));
                assert_eq!(actual["analyzed_schema"], actual["physical_schema"]);
            }
        }
        let no_like =
            capture_observation(&session, &json!({"sql":"SELECT 1 AS i","ansi":true})).await?;
        assert!(no_like["error"].is_null());
        assert_eq!(no_like["rows"], json!([["1"]]));
        assert_eq!(no_like["analyzed_schema"], no_like["physical_schema"]);
        let state = session.state();
        let actual_options = state.config_options();
        assert_eq!(actual_options.entries(), expected_options.entries());
        assert!(
            actual_options
                .extensions
                .get::<SparkSqlExpressionSemantics>()
                .is_none()
        );
        assert!(
            actual_options
                .extensions
                .iter()
                .all(|(prefix, _)| prefix != "dar_spark_like_query_clock")
        );
        eprintln!(
            "c06 mandatory LIKE host options: {label} passed initialization and pruning controls"
        );
    }
    Ok(())
}
