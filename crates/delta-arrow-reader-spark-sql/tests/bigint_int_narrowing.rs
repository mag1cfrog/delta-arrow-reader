//! Spark's BIGINT-to-INT contract, complete fields, and native SQL controls.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const MANIFEST: &str = include_str!("fixtures/bigint-int-narrowing/manifest.json");
const ORIGINAL: &str =
    include_str!("fixtures/bigint-int-narrowing/original-cast-controls-spark.jsonl");

fn records(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn references() -> TestResult<(Value, Vec<Value>)> {
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/bigint-int-narrowing/bigint-int-narrowing-spark.jsonl");
    let captured = records(&std::fs::read_to_string(path)?)?;
    assert_eq!(captured.len() - 1, 182);
    assert_eq!(captured[0]["spark_version"], "4.2.0");
    assert_eq!(
        captured[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(captured[0]["inputs"], manifest["inputs"]);
    let mut expected_ids = BTreeSet::new();
    for case in manifest["cases"].as_array().ok_or("missing cases")? {
        for ansi in case["ansi_modes"].as_array().ok_or("missing modes")? {
            expected_ids.insert((case["id"].to_string(), ansi.to_string()));
        }
    }
    let mut actual_ids = BTreeSet::new();
    for case in &captured[1..] {
        assert!(actual_ids.insert((case["id"].to_string(), case["ansi"].to_string())));
        let source = manifest["cases"]
            .as_array()
            .ok_or("missing cases")?
            .iter()
            .find(|source| source["id"] == case["id"])
            .ok_or("unexpected captured ID")?;
        for key in ["sql", "domain", "ordered"] {
            assert_eq!(case[key], source[key], "{key}: {case}");
        }
        if case["expected"]["condition"].is_null() {
            assert!(case["expected"]["rows"].is_array());
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        } else {
            assert!(case["spark_error_details"]["parameters"].is_object());
            assert!(case["spark_error_details"]["message"].is_string());
        }
    }
    assert_eq!(actual_ids, expected_ids);
    let original = records(ORIGINAL)?;
    assert_eq!(original.len() - 1, 10);
    assert_eq!(original[0]["spark_version"], "4.2.0");
    assert_eq!(
        original[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut cases = captured[1..].to_vec();
    for raw in &original[1..] {
        let mut case = raw.clone();
        case["domain"] = json!("original");
        case["schema_stages"] = json!({});
        for (recorded, stage) in [
            ("analyzed_schema", "analyzed"),
            ("optimized_schema", "optimized"),
            ("physical_schema", "executed"),
        ] {
            if let Some(schema) = raw["expected"].get(recorded) {
                case["schema_stages"][stage] = schema.clone();
            }
        }
        case["spark_error_phase"] = raw["expected"]["phase"].clone();
        case["spark_error_details"] = raw["expected"].clone();
        // The original capture has no before-collect observation to compare.
        case["before_collect_reference_unobserved"] = json!(true);
        cases.push(case);
    }
    Ok((manifest, cases))
}

fn session(batch_size: usize, spark: bool) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
    SessionContext::new_with_state(if spark {
        configure_spark_sql(builder).build()
    } else {
        builder.build()
    })
}

fn register_inputs(session: &SessionContext, manifest: &Value, batch_size: usize) -> TestResult {
    for input in manifest["inputs"].as_array().ok_or("missing inputs")? {
        let mut storage = input.clone();
        let visible_rows = input["rows"].as_array().ok_or("missing rows")?.len();
        if input["native_slice"] == true {
            let rows = storage["rows"].as_array_mut().ok_or("missing rows")?;
            rows.insert(0, manifest["native"]["sliced_hidden_rows"][0].clone());
            rows.push(manifest["native"]["sliced_hidden_rows"][1].clone());
        }
        let storage = spark_inputs::decode_recorded_batch(&storage)?;
        let batch = storage.slice(usize::from(input["native_slice"] == true), visible_rows);
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (chunk, offset) in (0..visible_rows).step_by(batch_size).enumerate() {
            partitions[chunk % 2].push(batch.slice(offset, batch_size.min(visible_rows - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

fn exact_conversion_cause(case: &Value, error: &str) -> bool {
    let parameters = &case["spark_error_details"]["parameters"];
    let Some(target) = parameters["targetType"].as_str() else {
        return false;
    };
    let target = match target {
        "\"INT\"" => "Int32",
        "\"BIGINT\"" => "Int64",
        _ => return false,
    };
    let cause = match case["expected"]["condition"].as_str() {
        Some("CAST_OVERFLOW") if parameters["sourceType"] == "\"BIGINT\"" => {
            let Some(value) = parameters["value"].as_str() else {
                return false;
            };
            let Some(value) = value.strip_suffix('L') else {
                return false;
            };
            let Ok(value) = value.parse::<i64>() else {
                return false;
            };
            format!("Can't cast value {value} to type {target}")
        }
        Some("CAST_INVALID_INPUT") if parameters["sourceType"] == "\"STRING\"" => {
            let Some(value) = parameters["expression"].as_str() else {
                return false;
            };
            format!("Cannot cast string {value} to value of {target} type")
        }
        _ => return false,
    };
    error
        .lines()
        .any(|line| line == format!("Arrow error: Cast error: {cause}"))
}

fn required_contract_matches(case: &Value, observation: &Value) -> bool {
    let fields_match = [
        ("analyzed", "public_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ]
    .into_iter()
    .all(|(recorded, actual)| {
        (recorded == "executed_before_collect"
            && case["before_collect_reference_unobserved"] == true)
            || observation[actual] == case["schema_stages"][recorded]
    });
    if case["expected"]["condition"].is_null() {
        return fields_match
            && observation["matches"] == true
            && observation["optimized_matches"] == true;
    }
    let expected_phase = match case["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some("optimized" | "optimized_schema") => "create_physical_plan",
        Some("collect") => "collect",
        _ => return false,
    };
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let optimized_matches = if case["schema_stages"].get("optimized").is_some() {
        observation["optimized_matches"] == true
    } else {
        observation["optimized_schema"].is_null()
            && observation["optimized_error"] == observation["error"]
    };
    fields_match
        && optimized_matches
        && observation["native_phase"] == expected_phase
        && exact_conversion_cause(case, error)
}

async fn compare_domain(adapter: bool) -> TestResult {
    let (manifest, cases) = references()?;
    let mut failures = Vec::new();
    let mut count = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        register_inputs(&session, &manifest, batch_size)?;
        for case in cases
            .iter()
            .filter(|case| (case["domain"] == "adapter") == adapter)
        {
            count += 1;
            let observation = match spark_results::compare_exact_spark_query_stages_with_optimized(
                &session, case,
            )
            .await
            {
                Ok(observation) => observation,
                Err(error) => {
                    eprintln!(
                        "BIGINT_INT_OBSERVATION {}",
                        json!({
                            "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                            "domain":case["domain"],"reference":case,"capture_error":error.to_string(),
                            "required_contract_matches":false,"full_diagnostic_parity":false,
                        })
                    );
                    failures.push(format!("capture failed for {case}: {error}"));
                    continue;
                }
            };
            let contract_matches = required_contract_matches(case, &observation);
            eprintln!(
                "BIGINT_INT_OBSERVATION {}",
                json!({
                    "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                    "domain":case["domain"], "reference":case, "observation":observation,
                    "required_contract_matches":contract_matches,
                    "full_diagnostic_parity":false,
                })
            );
            if !contract_matches {
                failures.push(format!("{case}: {observation}"));
            }
        }
    }
    assert_eq!(count, if adapter { 40 } else { 344 });
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn bigint_int_narrowing_matches_spark_contract() -> TestResult {
    compare_domain(false).await
}

#[tokio::test]
async fn bigint_int_argument_adapters_match_spark_contract() -> TestResult {
    compare_domain(true).await
}

#[tokio::test]
async fn configured_native_bigint_int_casts_keep_plain_results() -> TestResult {
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    let sqls = [
        "SELECT CAST(4294967296 AS INT) AS r",
        "SELECT TRY_CAST(4294967296 AS INT) AS r",
        "SELECT CAST(v AS INT) AS r FROM narrow_required WHERE id=11",
        "SELECT TRY_CAST(v AS INT) AS r FROM narrow_nullable ORDER BY id",
        "SELECT CAST(v AS INT) AS r FROM narrow_required WHERE id=6",
        "SELECT CAST(v AS INT) AS r FROM narrow_nullable_sliced WHERE id=11",
        "SELECT CAST(v AS INT) AS r FROM narrow_empty_required",
        "SELECT TRY_CAST(v AS INT) AS r FROM narrow_nulls ORDER BY id",
    ];
    let mut count = 0;
    for batch_size in [1, 32] {
        let plain = session(batch_size, false);
        let configured = session(batch_size, true);
        for context in [&plain, &configured] {
            register_inputs(context, &manifest, batch_size)?;
        }
        for after_spark in [false, true] {
            if after_spark {
                let query = plan_query(
                    &configured,
                    "SELECT CAST(1L AS INT) AS r",
                    &SparkSqlOptions::default(),
                )
                .await?;
                collect(
                    query.create_physical_plan(&configured).await?,
                    configured.task_ctx(),
                )
                .await?;
            }
            for (index, sql) in sqls.into_iter().enumerate() {
                let expected = spark_results::capture_native_query_observation(&plain, sql).await?;
                let actual =
                    spark_results::capture_native_query_observation(&configured, sql).await?;
                eprintln!(
                    "BIGINT_INT_NATIVE_CONTROL {}",
                    json!({
                        "sql":sql,"batch":batch_size,"after_spark":after_spark,
                        "plain":expected,"configured":actual,
                    })
                );
                assert_eq!(
                    actual, expected,
                    "{sql}, batch={batch_size}, after_spark={after_spark}"
                );
                if index == 0 {
                    assert!(expected["error"].as_str().is_some_and(|error| error.lines().any(|line|
                        line == "Arrow error: Cast error: Can't cast value 4294967296 to type Int32")));
                } else if index == 1 {
                    assert!(expected["error"].is_null());
                    assert_eq!(expected["rows"], json!([[null]]));
                }
                count += 1;
            }
        }
    }
    assert_eq!(count, 32);
    Ok(())
}

#[tokio::test]
async fn bigint_int_conversion_evaluates_its_input_once() -> TestResult {
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    let mut observations = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        register_inputs(&session, &manifest, batch_size)?;
        let visited = Arc::new(Mutex::new(Vec::<i32>::new()));
        let recorder = Arc::clone(&visited);
        let invocations = Arc::new(AtomicUsize::new(0));
        let callback_invocations = Arc::clone(&invocations);
        session.register_udf(create_udf(
            "count_bigint_input",
            vec![DataType::Int32, DataType::Int64],
            DataType::Int64,
            Volatility::Volatile,
            Arc::new(move |args| {
                callback_invocations.fetch_add(1, Ordering::SeqCst);
                let [ColumnarValue::Array(ids), value] = args else {
                    return Err(datafusion::common::DataFusionError::Execution(
                        "counted BIGINT expects ID array and value".into(),
                    ));
                };
                let ids = ids.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "counted BIGINT expects INT IDs".into(),
                    )
                })?;
                recorder
                    .lock()
                    .map_err(|_| {
                        datafusion::common::DataFusionError::Execution(
                            "poisoned BIGINT recorder".into(),
                        )
                    })?
                    .extend(ids.values().iter().copied());
                Ok(value.clone())
            }),
        ));
        for (ansi, expression, table, predicate, expected) in [
            (
                false,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect::<Vec<_>>(),
            ),
            (
                false,
                "TRY_CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect(),
            ),
            (
                true,
                "TRY_CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect(),
            ),
            (
                true,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "WHERE id=6",
                vec![6],
            ),
            (
                false,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_empty_required",
                "",
                vec![],
            ),
            (
                false,
                "CASE WHEN false THEN CAST(count_bigint_input(id,v) AS INT) ELSE 7 END",
                "narrow_required",
                "",
                vec![],
            ),
        ] {
            invocations.store(0, Ordering::SeqCst);
            let sql = format!("SELECT {expression} AS r FROM {table} {predicate}");
            let mut phase = "reset_recorder";
            let mut after_resolve = None;
            let mut after_physical = None;
            let mut resolve_invocations = None;
            let mut physical_invocations = None;
            let result: TestResult<_> = async {
                visited
                    .lock()
                    .map_err(|_| "poisoned BIGINT recorder")?
                    .clear();
                phase = "resolve";
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await;
                resolve_invocations = Some(invocations.load(Ordering::SeqCst));
                after_resolve = Some(
                    visited
                        .lock()
                        .map_err(|_| "poisoned BIGINT recorder")?
                        .clone(),
                );
                let query = query?;
                phase = "create_physical_plan";
                let physical = query.create_physical_plan(&session).await;
                physical_invocations = Some(invocations.load(Ordering::SeqCst));
                after_physical = Some(
                    visited
                        .lock()
                        .map_err(|_| "poisoned BIGINT recorder")?
                        .clone(),
                );
                let physical = physical?;
                phase = "collect";
                let batches = collect(physical, session.task_ctx()).await?;
                phase = "complete";
                Ok(batches)
            }
            .await;
            let visited_ids_raw = visited
                .lock()
                .map(|rows| rows.clone())
                .map_err(|_| "poisoned BIGINT recorder");
            let mut actual = visited_ids_raw.as_ref().ok().cloned();
            if let Some(actual) = &mut actual {
                actual.sort_unstable();
            }
            eprintln!(
                "BIGINT_INT_INPUT_EVALUATION {}",
                json!({
                    "sql":sql,"ansi":ansi,"batch":batch_size,"visited_ids":actual,
                    "visited_ids_raw":visited_ids_raw.as_ref().ok(),
                    "recorder_error":visited_ids_raw.as_ref().err(),
                    "native_phase":phase,"after_resolve":after_resolve,"after_physical":after_physical,
                    "callback_counts":{"after_resolve":resolve_invocations,
                        "after_physical":physical_invocations,"final":invocations.load(Ordering::SeqCst)},
                    "error":result.as_ref().err().map(ToString::to_string),
                    "error_debug":result.as_ref().err().map(|error| format!("{error:?}")),
                    "rows":result.as_ref().ok().map(|batches| batches.iter().map(|batch| batch.num_rows()).sum::<usize>()),
                    "expected_visited_ids":expected,"spark_oracle_credit":false,
                })
            );
            observations.push((
                sql,
                expected,
                after_resolve,
                after_physical,
                actual,
                result.map(|_| ()),
            ));
        }
    }
    assert_eq!(observations.len(), 12);
    for (sql, expected, after_resolve, after_physical, actual, result) in observations {
        assert!(
            after_resolve.is_some_and(|rows| rows.is_empty()),
            "resolve: {sql}"
        );
        assert!(
            after_physical.is_some_and(|rows| rows.is_empty()),
            "physical planning: {sql}"
        );
        result?;
        assert_eq!(actual.ok_or("poisoned BIGINT recorder")?, expected, "{sql}");
    }
    Ok(())
}

#[test]
fn overflow_comparison_requires_the_exact_input_and_target() -> TestResult {
    let original = records(ORIGINAL)?;
    let mut case = original[1].clone();
    case["spark_error_details"] = case["expected"].clone();
    let exact = "Arrow error: Cast error: Can't cast value 4294967296 to type Int32";
    assert!(exact_conversion_cause(&case, exact));
    for wrong in [
        "Arrow error: Cast error: Can't cast value 2147483648 to type Int32",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int64",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int32 (different cause)",
        "Execution error: Invalid regular expression group index 4294967296",
    ] {
        assert!(!exact_conversion_cause(&case, wrong));
    }
    Ok(())
}
