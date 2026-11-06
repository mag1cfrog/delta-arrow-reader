//! BIGINT-to-INT conversions, complete fields, and exactly tracked adapter followups.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
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
const TRACKED_FACETS: &str =
    include_str!("fixtures/bigint-int-narrowing/tracked-adapter-facets.json");
const INSPECTION_MANIFEST: &str =
    include_str!("fixtures/bigint-int-narrowing/inspection-manifest.json");
const INSPECTION: &str = include_str!("fixtures/bigint-int-narrowing/inspection-spark.jsonl");

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
    cases.extend(inspection_references(&manifest)?);
    Ok((manifest, cases))
}

fn inspection_references(inputs: &Value) -> TestResult<Vec<Value>> {
    let manifest: Value = serde_json::from_str(INSPECTION_MANIFEST)?;
    let captured = records(INSPECTION)?;
    assert_eq!(captured.len() - 1, 8);
    let header = &captured[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    for key in [
        "scope",
        "spark_version",
        "spark_revision",
        "timezone",
        "observations",
    ] {
        assert_eq!(header[key], manifest[key], "{key}");
    }
    for key in ["inputs", "native"] {
        assert_eq!(header[key], inputs[key], "{key}");
    }
    let registered = inputs["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .map(|input| json!({"name":input["name"],"schema":input["schema"],"rows":input["rows"]}))
        .collect::<Vec<_>>();
    assert_eq!(header["registered_inputs"], json!(registered));
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(definitions.len(), 4);
    let mut expected_ids = BTreeSet::new();
    for source in definitions {
        assert_eq!(source["domain"], "inspection");
        assert_eq!(source["ansi_modes"], json!([true, false]));
        for ansi in source["ansi_modes"].as_array().ok_or("missing modes")? {
            assert!(expected_ids.insert((source["id"].to_string(), ansi.to_string())));
        }
    }
    let mut actual_ids = BTreeSet::new();
    for case in &captured[1..] {
        assert!(actual_ids.insert((case["id"].to_string(), case["ansi"].to_string())));
        let source = definitions
            .iter()
            .find(|source| source["id"] == case["id"])
            .ok_or("unexpected inspection ID")?;
        for key in ["sql", "purpose", "domain", "ordered", "input_tables"] {
            assert_eq!(case[key], source[key], "{key}: {case}");
        }
        for (key, value) in manifest["settings"].as_object().ok_or("missing settings")? {
            assert_eq!(&case["settings"][key], value, "{key}: {case}");
        }
        assert_eq!(
            case["settings"]["spark.sql.ansi.enabled"],
            case["ansi"].to_string()
        );
        assert!(case["schema_stages"]["analyzed"].is_object());
        if case["expected"]["condition"].is_null() {
            assert!(case["expected"]["rows"].is_array());
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in ["optimized", "executed_before_collect", "executed"] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        } else {
            assert!(case["spark_error_details"]["parameters"].is_object());
            assert!(case["spark_error_details"]["message"].is_string());
            assert_eq!(
                case["expected"]["condition"],
                case["spark_error_details"]["condition"]
            );
        }
    }
    assert_eq!(actual_ids, expected_ids);
    Ok(captured[1..].to_vec())
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

fn matches_expected_error_cause(case: &Value, error: &str) -> bool {
    let parameters = &case["spark_error_details"]["parameters"];
    if case["expected"]["condition"] == "DATETIME_OVERFLOW" {
        let Some(operation) = parameters["operation"].as_str() else {
            return false;
        };
        let cause = format!(
            "[DATETIME_OVERFLOW] Datetime operation overflow: {operation}. SQLSTATE: 22008"
        );
        return error
            .lines()
            .any(|line| line.strip_prefix("Execution error: ").unwrap_or(line) == cause);
    }
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
            if !error.contains("[CAST_OVERFLOW]") {
                return false;
            }
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
    error.lines().any(|line| {
        line.strip_prefix("External error: error in DataFusion: ")
            .unwrap_or(line)
            == format!("Arrow error: Cast error: {cause}")
    })
}

fn required_contract_matches(case: &Value, observation: &Value) -> bool {
    observation["public_schema"] == case["schema_stages"]["analyzed"]
        && values_and_other_stages_match(case, observation)
}

fn values_and_other_stages_match(case: &Value, observation: &Value) -> bool {
    let fields_match = [
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
            && observation["error"].is_null()
            && observation["rows_match"] == true
            && observation["ordered_rows_match"] == true
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
        && matches_expected_error_cause(case, error)
}

fn tracked_facets() -> TestResult<Value> {
    let baseline: Value = serde_json::from_str(TRACKED_FACETS)?;
    assert_eq!(
        baseline["source"],
        "3853bd37067e7f194f99615373f51130742f56e7"
    );
    assert_eq!(
        baseline["source_observations_sha256"],
        "2e14c519637733f83186a1b6ec2db0d4aacf4eb9adff3dfa1f15100713bb39d8"
    );
    assert_eq!(
        baseline["source_test_sha256"],
        "e1547e455803fe6a4337208cb5469a66fcf7699130822f08bae59d409625979d"
    );
    let facets = baseline["facets"]
        .as_array()
        .ok_or("missing tracked facets")?;
    assert_eq!(facets.len(), 24);
    let mut identities = BTreeSet::new();
    for facet in facets {
        let id = facet["id"].as_str().ok_or("missing tracked ID")?;
        let ansi = facet["ansi"].as_bool().ok_or("missing tracked ANSI")?;
        let batch = facet["batch"].as_u64().ok_or("missing tracked batch")?;
        let (kind, owners) = match (id, ansi) {
            ("c06_substring_implicit" | "c06_substring_explicit", true) => {
                ("observation", json!([778]))
            }
            ("c06_extract_implicit", _) => ("observation", json!([788])),
            ("c07_timestampadd_implicit", _) => ("observation", json!([981, 982])),
            (
                "c07_timestampadd_explicit" | "c07_yearmonth_implicit" | "c07_yearmonth_explicit",
                _,
            ) => ("public_schema", json!([982])),
            _ => return Err(format!("unexpected tracked facet: {facet}").into()),
        };
        assert!(matches!(batch, 1 | 32));
        assert!(identities.insert((id, ansi, batch)));
        assert_eq!(facet["kind"], kind);
        assert_eq!(facet["owners"], owners);
        assert_eq!(facet["source_required_contract_matches"], false);
        assert_eq!(facet["source_full_diagnostic_parity"], false);
    }
    Ok(baseline)
}

fn contract_and_tracked_facets_match(
    case: &Value,
    observation: &Value,
    batch_size: usize,
    baseline: &Value,
) -> TestResult<(bool, Value)> {
    let facet = baseline["facets"]
        .as_array()
        .ok_or("missing tracked facets")?
        .iter()
        .find(|facet| {
            facet["id"] == case["id"]
                && facet["ansi"] == case["ansi"]
                && facet["batch"] == batch_size
        });
    let Some(facet) = facet else {
        return Ok((required_contract_matches(case, observation), Value::Null));
    };
    assert_eq!(facet["reference"], *case);
    let spark_matches = required_contract_matches(case, observation);
    let public_matches = observation["public_schema"] == case["schema_stages"]["analyzed"];
    let public_preserved = observation["public_schema"] == facet["observation"]["public_schema"];
    let public_allowed = public_matches
        || (facet["owners"]
            .as_array()
            .is_some_and(|owners| owners.contains(&json!(982)))
            && public_preserved);
    // Accept a real Spark improvement, or only these exact inherited stage/error facets.
    let tracked_matches = if facet["kind"] == "observation" {
        let stages_preserved = [
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
            "native_phase",
            "optimized_schema",
            "optimized_observation",
            "optimized_error",
        ]
        .iter()
        .all(|key| observation[*key] == facet["observation"][*key]);
        let error_allowed = observation["error"] == facet["observation"]["error"]
            || observation["error"]
                .as_str()
                .is_some_and(|error| matches_expected_error_cause(case, error));
        public_allowed
            && (values_and_other_stages_match(case, observation)
                || (stages_preserved && error_allowed))
    } else {
        public_allowed && values_and_other_stages_match(case, observation)
    };
    Ok((
        spark_matches || tracked_matches,
        json!({
            "owners":facet["owners"],"kind":facet["kind"],
            "source":baseline["source"],
            "source_observations_sha256":baseline["source_observations_sha256"],
            "source_log_line":facet["source_log_line"],
            "source_record_sha256":facet["source_record_sha256"],
            "public_schema_matches_spark":public_matches,
            "public_schema_matches_baseline":public_preserved,
            "exact_observation_preserved":observation == &facet["observation"],
            "bounded_spark_contract_matches":spark_matches,
            "tracked_facets_satisfied":tracked_matches,
            "source_profile_is_spark_compatibility_credit":false,
        }),
    ))
}

async fn compare_domain(adapter: bool) -> TestResult {
    let (manifest, cases) = references()?;
    let baseline = tracked_facets()?;
    let mut failures = Vec::new();
    let mut count = 0;
    let mut profiled_count = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size, true);
        spark_inputs::register_recorded_inputs_with_slices(&session, &manifest, batch_size)?;
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
                            "owned_contract_and_tracked_facets_match":false,
                        })
                    );
                    failures.push(format!("capture failed for {case}: {error}"));
                    continue;
                }
            };
            let contract_matches = required_contract_matches(case, &observation);
            let (gate_matches, preservation) =
                contract_and_tracked_facets_match(case, &observation, batch_size, &baseline)?;
            profiled_count += usize::from(!preservation.is_null());
            eprintln!(
                "BIGINT_INT_OBSERVATION {}",
                json!({
                    "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                    "domain":case["domain"], "reference":case, "observation":observation,
                    "required_contract_matches":contract_matches,
                    "owned_contract_and_tracked_facets_match":gate_matches,
                    "tracked_facets":preservation,
                    "full_diagnostic_parity":false,
                })
            );
            if !gate_matches {
                failures.push(format!("{case}: {observation}"));
            }
        }
    }
    assert_eq!(count, if adapter { 40 } else { 360 });
    assert_eq!(profiled_count, if adapter { 24 } else { 0 });
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn bigint_int_narrowing_matches_spark_contract() -> TestResult {
    compare_domain(false).await
}

#[tokio::test]
async fn bigint_int_argument_conversions_and_tracked_followups_are_preserved() -> TestResult {
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
            spark_inputs::register_recorded_inputs_with_slices(context, &manifest, batch_size)?;
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
        spark_inputs::register_recorded_inputs_with_slices(&session, &manifest, batch_size)?;
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
        for (ansi, expression, table, predicate, expected, expected_error) in [
            (
                false,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect::<Vec<_>>(),
                None,
            ),
            (
                false,
                "TRY_CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect(),
                None,
            ),
            (
                true,
                "TRY_CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "",
                (0..14).collect(),
                None,
            ),
            (
                true,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "WHERE id=6",
                vec![6],
                None,
            ),
            (
                true,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_required",
                "WHERE id=11",
                vec![11],
                Some("Arrow error: Cast error: Can't cast value 4294967296 to type Int32"),
            ),
            (
                false,
                "CAST(count_bigint_input(id,v) AS INT)",
                "narrow_empty_required",
                "",
                vec![],
                None,
            ),
            (
                false,
                "CASE WHEN false THEN CAST(count_bigint_input(id,v) AS INT) ELSE 7 END",
                "narrow_required",
                "",
                vec![],
                None,
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
            let final_invocations = invocations.load(Ordering::SeqCst);
            eprintln!(
                "BIGINT_INT_INPUT_EVALUATION {}",
                json!({
                    "sql":sql,"ansi":ansi,"batch":batch_size,"visited_ids":actual,
                    "visited_ids_raw":visited_ids_raw.as_ref().ok(),
                    "recorder_error":visited_ids_raw.as_ref().err(),
                    "native_phase":phase,"after_resolve":after_resolve,"after_physical":after_physical,
                    "callback_counts":{"after_resolve":resolve_invocations,
                        "after_physical":physical_invocations,"final":final_invocations},
                    "error":result.as_ref().err().map(ToString::to_string),
                    "error_debug":result.as_ref().err().map(|error| format!("{error:?}")),
                    "rows":result.as_ref().ok().map(|batches| batches.iter().map(|batch| batch.num_rows()).sum::<usize>()),
                    "expected_visited_ids":expected,"expected_error":expected_error,
                    "spark_oracle_credit":false,
                })
            );
            observations.push((
                sql,
                expected,
                after_resolve,
                after_physical,
                actual,
                result.map(|_| ()),
                phase,
                expected_error,
                resolve_invocations,
                physical_invocations,
                final_invocations,
            ));
        }
    }
    assert_eq!(observations.len(), 14);
    for (
        sql,
        expected,
        after_resolve,
        after_physical,
        actual,
        result,
        phase,
        expected_error,
        resolve_invocations,
        physical_invocations,
        final_invocations,
    ) in observations
    {
        assert!(
            after_resolve.is_some_and(|rows| rows.is_empty()),
            "resolve: {sql}"
        );
        assert!(
            after_physical.is_some_and(|rows| rows.is_empty()),
            "physical planning: {sql}"
        );
        assert_eq!(resolve_invocations, Some(0), "resolve: {sql}");
        assert_eq!(physical_invocations, Some(0), "physical planning: {sql}");
        if let Some(expected_error) = expected_error {
            assert_eq!(phase, "collect", "{sql}");
            let error = result.err().ok_or("ANSI singleton must overflow")?;
            assert!(
                error.to_string().lines().any(|line| line == expected_error),
                "{sql}: {error}"
            );
            assert!(
                error.to_string().contains("[CAST_OVERFLOW]"),
                "{sql}: {error}"
            );
            assert_eq!(final_invocations, 1, "{sql}");
        } else {
            result?;
        }
        if expected.is_empty() {
            assert_eq!(final_invocations, 0, "unreachable input: {sql}");
        }
        assert_eq!(actual.ok_or("poisoned BIGINT recorder")?, expected, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn try_cast_preserves_volatile_bigint_producer_errors() -> TestResult {
    let expected_error =
        datafusion::common::DataFusionError::Execution("synthetic_bigint_producer_failure".into())
            .to_string();
    let sql = "SELECT TRY_CAST(failing_bigint_input() AS INT) AS r";
    let mut observations = Vec::new();
    for batch_size in [1, 32] {
        for ansi in [false, true] {
            let session = session(batch_size, true);
            let invocations = Arc::new(AtomicUsize::new(0));
            let callback_invocations = Arc::clone(&invocations);
            session.register_udf(create_udf(
                "failing_bigint_input",
                vec![],
                DataType::Int64,
                Volatility::Volatile,
                Arc::new(move |_| {
                    callback_invocations.fetch_add(1, Ordering::SeqCst);
                    Err(datafusion::common::DataFusionError::Execution(
                        "synthetic_bigint_producer_failure".into(),
                    ))
                }),
            ));
            let mut phase = "resolve";
            let mut after_resolve = None;
            let mut after_physical = None;
            let result: TestResult = async {
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await;
                after_resolve = Some(invocations.load(Ordering::SeqCst));
                let query = query?;
                phase = "create_physical_plan";
                let physical = query.create_physical_plan(&session).await;
                after_physical = Some(invocations.load(Ordering::SeqCst));
                let physical = physical?;
                phase = "collect";
                collect(physical, session.task_ctx()).await?;
                phase = "complete";
                Ok(())
            }
            .await;
            let final_invocations = invocations.load(Ordering::SeqCst);
            let error = result.err().map(|error| error.to_string());
            eprintln!(
                "BIGINT_INT_PRODUCER_FAILURE {}",
                json!({
                    "sql":sql,"ansi":ansi,"batch":batch_size,"native_phase":phase,
                    "callback_counts":{"after_resolve":after_resolve,
                        "after_physical":after_physical,"final":final_invocations},
                    "error":error,"expected_error":expected_error,"spark_oracle_credit":false,
                })
            );
            observations.push((
                ansi,
                batch_size,
                phase,
                after_resolve,
                after_physical,
                final_invocations,
                error,
            ));
        }
    }
    assert_eq!(observations.len(), 4);
    for (ansi, batch_size, phase, after_resolve, after_physical, final_invocations, error) in
        observations
    {
        assert_eq!(after_resolve, Some(0), "ansi={ansi}, batch={batch_size}");
        assert_eq!(after_physical, Some(0), "ansi={ansi}, batch={batch_size}");
        assert_eq!(phase, "collect", "ansi={ansi}, batch={batch_size}");
        assert_eq!(
            error.ok_or("TRY_CAST must propagate the BIGINT producer error")?,
            expected_error,
            "ansi={ansi}, batch={batch_size}"
        );
        assert_eq!(final_invocations, 1, "ansi={ansi}, batch={batch_size}");
    }
    Ok(())
}

#[test]
fn overflow_comparison_requires_the_exact_input_and_target() -> TestResult {
    let original = records(ORIGINAL)?;
    let mut case = original[1].clone();
    case["spark_error_details"] = case["expected"].clone();
    let exact = "Arrow error: Cast error: Can't cast value 4294967296 to type Int32";
    assert!(!matches_expected_error_cause(&case, exact));
    let annotated = format!("[CAST_OVERFLOW]\ncaused by\n{exact}");
    assert!(matches_expected_error_cause(&case, &annotated));
    assert!(matches_expected_error_cause(
        &case,
        &format!("External error: error in DataFusion: {annotated}")
    ));
    for wrong in [
        "Arrow error: Cast error: Can't cast value 2147483648 to type Int32",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int64",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int32 (different cause)",
        "Execution error: Invalid regular expression group index 4294967296",
        "External error: error in DataFusion: Arrow error: Cast error: Can't cast value 2147483648 to type Int32",
        "External error: error in DataFusion: Arrow error: Cast error: Can't cast value 4294967296 to type Int64",
        "External error: error in DataFusion: Arrow error: Cast error: Can't cast value 4294967296 to type Int32 (different cause)",
        "Other wrapper: Arrow error: Cast error: Can't cast value 4294967296 to type Int32",
    ] {
        assert!(!matches_expected_error_cause(
            &case,
            &format!("[CAST_OVERFLOW]\ncaused by\n{wrong}")
        ));
    }
    Ok(())
}

#[test]
fn datetime_overflow_comparison_requires_the_exact_operation() {
    let operation = "add 4294967296L MONTH to TIMESTAMP '2020-01-01 00:00:00'";
    let case = json!({
        "expected":{"condition":"DATETIME_OVERFLOW"},
        "spark_error_details":{"parameters":{"operation":operation}},
    });
    let exact =
        format!("[DATETIME_OVERFLOW] Datetime operation overflow: {operation}. SQLSTATE: 22008");
    assert!(matches_expected_error_cause(&case, &exact));
    assert!(matches_expected_error_cause(
        &case,
        &format!("Execution error: {exact}")
    ));
    for wrong in [
        exact.replace("4294967296L", "2147483648L"),
        exact.replace("MONTH", "DAY"),
        exact.replace("2020-01-01", "2021-01-01"),
        format!("{exact} (different cause)"),
        "Execution error: [DATETIME_OVERFLOW] unrelated temporal failure".into(),
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int32".into(),
    ] {
        assert!(!matches_expected_error_cause(&case, &wrong));
    }
}

#[test]
fn tracked_facets_keep_conversion_and_unlisted_differences_required() -> TestResult {
    let baseline = tracked_facets()?;
    let facets = baseline["facets"]
        .as_array()
        .ok_or("missing tracked facets")?;
    let whole = facets
        .iter()
        .find(|facet| {
            facet["id"] == "c06_extract_implicit" && facet["ansi"] == false && facet["batch"] == 1
        })
        .ok_or("missing extraction profile")?;
    let case = &whole["reference"];
    let actual = &whole["observation"];
    assert!(contract_and_tracked_facets_match(case, actual, 1, &baseline)?.0);
    assert!(!required_contract_matches(case, actual));
    let mut changed = actual.clone();
    changed["error"] = json!("Execution error: a different failure");
    assert!(!contract_and_tracked_facets_match(case, &changed, 1, &baseline)?.0);

    let declaration = facets
        .iter()
        .find(|facet| {
            facet["id"] == "c07_yearmonth_implicit" && facet["ansi"] == false && facet["batch"] == 1
        })
        .ok_or("missing year-month declaration")?;
    let case = &declaration["reference"];
    let mut repaired = declaration["observation"].clone();
    assert!(!contract_and_tracked_facets_match(case, &repaired, 1, &baseline)?.0);
    for (stage, key) in [
        ("optimized", "optimized_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ] {
        repaired[key] = case["schema_stages"][stage].clone();
    }
    repaired["rows"] = case["expected"]["rows"].clone();
    repaired["rows_raw"] = case["expected"]["rows"].clone();
    repaired["native_phase"] = json!("encode");
    repaired["error"] = Value::Null;
    repaired["optimized_error"] = Value::Null;
    for key in ["rows_match", "ordered_rows_match", "optimized_matches"] {
        repaired[key] = json!(true);
    }
    assert!(contract_and_tracked_facets_match(case, &repaired, 1, &baseline)?.0);
    assert!(!required_contract_matches(case, &repaired));
    let mut changed = repaired.clone();
    changed["public_schema"]["fields"][0]["metadata"] = json!({"unexpected":"value"});
    assert!(!contract_and_tracked_facets_match(case, &changed, 1, &baseline)?.0);
    repaired["rows_match"] = json!(false);
    repaired["rows"] = json!([["wrong interval"]]);
    assert!(!contract_and_tracked_facets_match(case, &repaired, 1, &baseline)?.0);

    let temporal = facets
        .iter()
        .find(|facet| {
            facet["id"] == "c07_timestampadd_implicit"
                && facet["ansi"] == true
                && facet["batch"] == 1
        })
        .ok_or("missing timestampadd profile")?;
    let case = &temporal["reference"];
    let mut improved = temporal["observation"].clone();
    improved["error"] = json!(format!(
        "Execution error: [DATETIME_OVERFLOW] Datetime operation overflow: {}. SQLSTATE: 22008",
        case["spark_error_details"]["parameters"]["operation"]
            .as_str()
            .ok_or("missing temporal operation")?
    ));
    improved["optimized_error"] = improved["error"].clone();
    assert!(contract_and_tracked_facets_match(case, &improved, 1, &baseline)?.0);
    assert!(!required_contract_matches(case, &improved));
    Ok(())
}
