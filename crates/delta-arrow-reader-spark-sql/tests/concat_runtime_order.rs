//! C06 CONCAT preserves runtime argument order and selection without replay.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, BinaryArray, Int32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Expr, Volatility, create_udf};
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

fn session(batch_size: usize, target_partitions: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(target_partitions),
                ),
        )
        .build(),
    )
}

fn row_counts(rows: &Value) -> TestResult<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for row in rows.as_array().ok_or("missing rows")? {
        *counts.entry(serde_json::to_string(row)?).or_default() += 1;
    }
    Ok(counts)
}

fn matches_reference(case: &Value, actual: &Value) -> TestResult<bool> {
    let expected = &case["expected"];
    if actual["analyzed_schema"] != expected["analyzed_schema"]
        || actual["analyzed_arrow_schema"]
            .as_str()
            .is_none_or(|schema| schema.is_empty())
        || !actual["physical_schema"].is_object()
        || actual["physical_arrow_schema"]
            .as_str()
            .is_none_or(|schema| schema.is_empty())
    {
        return Ok(false);
    }
    if let Some(physical) = expected.get("physical_schema")
        && actual.get("physical_schema") != Some(physical)
    {
        return Ok(false);
    }
    Ok(match expected["status"].as_str() {
        Some("ok") => {
            actual.get("error").is_none()
                && actual.get("error_stage").is_none()
                && actual["physical_schema"] == expected["physical_schema"]
                && actual["rows"].is_array()
                && row_counts(&actual["rows"])? == row_counts(&expected["rows"])?
        }
        Some("error") => {
            expected["phase"] == "collect"
                && expected["condition"] == "DIVIDE_BY_ZERO"
                && actual["error_stage"] == "execute"
                && actual["error"] == "execute: Arrow error: Divide by zero error"
                && actual.get("rows").is_none()
        }
        _ => false,
    })
}

fn has_arrow_receipt(actual: &Value, key: &str) -> bool {
    actual[key]
        .as_str()
        .is_some_and(|schema| !schema.is_empty())
}

// Spark reports these local VALUES errors while obtaining the optimized schema;
// the host reports the same required cause during physical preparation. Keep the
// different public stage names visible instead of calling them a full stage match.
fn matches_local_preexecution_divide(case: &Value, actual: &Value) -> bool {
    matches!(
        case["id"].as_str(),
        Some("concat_885_local_mask_before_division" | "concat_885_local_division_before_mask")
    ) && case["ansi"] == true
        && case["expected"]["status"] == "error"
        && case["expected"]["phase"] == "optimized_schema"
        && case["expected"]["condition"] == "DIVIDE_BY_ZERO"
        && case["expected"].get("physical_schema").is_none()
        && actual["analyzed_schema"] == case["expected"]["analyzed_schema"]
        && has_arrow_receipt(actual, "analyzed_arrow_schema")
        && actual.get("physical_schema").is_some_and(Value::is_null)
        && actual
            .get("physical_arrow_schema")
            .is_some_and(Value::is_null)
        && actual["error_stage"] == "physical"
        && actual["error"]
            == "physical: preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error"
        && actual.get("rows").is_none()
}

fn public_native_observation(actual: &Value) -> TestResult<Value> {
    let mut public = actual.as_object().ok_or("missing observation")?.clone();
    // Complete raw Arrow receipts stay in the fixture and every execution log.
    // Internal #n aliases and buffer addresses are not public output fields.
    public.remove("analyzed_arrow_schema");
    public.remove("physical_arrow_schema");
    if let Some(rows) = actual.get("rows") {
        public.insert("rows".to_owned(), json!(row_counts(rows)?));
    }
    Ok(Value::Object(public))
}

// This checks active native expectations; the captured receipts remain unchanged.
fn matches_retained_native_negative(
    context: &Value,
    case: &Value,
    actual: &Value,
    profiles: &[Value],
) -> TestResult<bool> {
    if !is_active_native_negative_context(context)
        || matches_reference(case, actual)?
        || matches_local_preexecution_divide(case, actual)
        || !has_arrow_receipt(actual, "analyzed_arrow_schema")
        || !has_arrow_receipt(actual, "physical_arrow_schema")
    {
        return Ok(false);
    }
    let mut matching = profiles
        .iter()
        .filter(|profile| &profile["context"] == context);
    let Some(profile) = matching.next() else {
        return Ok(false);
    };
    if matching.next().is_some()
        || profile["strict_meaningful_match"] != false
        || profile["full_compatibility_pass"] != false
    {
        return Ok(false);
    }
    let captured = &profile["captured_observation"];
    if captured["expected"] != case["expected"]
        || ["id", "ansi", "sql", "representation", "batch_size"]
            .iter()
            .any(|key| captured[*key] != context[*key])
    {
        return Ok(false);
    }
    let mut expected = public_native_observation(&captured["actual"])?;
    if context["matrix"] == "one_partition_rng"
        && context["id"] == "rng_885_one_partition_direct"
        && context["batch_size"] == 1
    {
        // #838 repairs the declaration while the captured seeded-row gap remains.
        for stage in ["analyzed_schema", "physical_schema"] {
            if expected[stage]["fields"][1]["nullable"] != true
                || case["expected"][stage]["fields"][1]["nullable"] != false
            {
                return Ok(false);
            }
            expected[stage]["fields"][1]["nullable"] = json!(false);
        }
    }
    Ok(public_native_observation(actual)? == expected)
}

fn runtime_control_context(
    matrix: &str,
    case: &Value,
    representation: usize,
    batch_size: usize,
) -> Value {
    let partitions = if matrix == "one_partition_rng" { 1 } else { 2 };
    json!({"matrix":matrix,"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
        "representation":representation,"batch_size":batch_size,"input_partitions":partitions,
        "target_partitions":partitions,"timezone":"UTC"})
}

fn is_active_native_negative_context(context: &Value) -> bool {
    if context["matrix"] == "one_partition_rng" {
        context["batch_size"] == 1
    } else {
        (context["id"] == "concat_885_seeded_random_before_mask" && context["batch_size"] == 1)
            || (context["id"] == "concat_885_competing_cast_before_division"
                && context["ansi"] == true
                && context["batch_size"] == 32)
    }
}

fn register_rng_inputs(session: &SessionContext, header: &Value, batch_size: usize) -> TestResult {
    let inputs = header["registered_inputs"]
        .as_array()
        .ok_or("missing RNG inputs")?;
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input["name"], "concat_rng_state_one");
    assert_eq!(input["schema"]["type"], "struct");
    let source_fields = input["schema"]["fields"]
        .as_array()
        .ok_or("missing RNG fields")?;
    assert_eq!(source_fields.len(), 2);
    let mut fields = Vec::new();
    for (field, (name, kind, data_type, nullable)) in source_fields.iter().zip([
        ("row_id", "integer", DataType::Int32, false),
        ("s", "string", DataType::Utf8, true),
    ]) {
        assert_eq!(field["name"], name);
        assert_eq!(field["type"], kind);
        assert_eq!(field["nullable"], nullable);
        let metadata = serde_json::from_value::<std::collections::HashMap<String, String>>(
            field["metadata"].clone(),
        )?;
        fields.push(Field::new(name, data_type, nullable).with_metadata(metadata));
    }
    let rows = input["rows"].as_array().ok_or("missing RNG source rows")?;
    assert_eq!(rows.len(), 4);
    for row in rows {
        assert_eq!(row.as_array().ok_or("missing RNG row")?.len(), 2);
    }
    let row_ids = rows
        .iter()
        .map(|row| {
            Ok(row[0]
                .as_str()
                .ok_or("missing RNG row_id")?
                .parse::<i32>()?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(row_ids.iter().collect::<HashSet<_>>().len(), rows.len());
    let strings = rows
        .iter()
        .map(|row| {
            if row[1].is_null() {
                Ok(None)
            } else {
                Ok(Some(row[1].as_str().ok_or("invalid RNG source string")?))
            }
        })
        .collect::<TestResult<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(row_ids)),
            Arc::new(StringArray::from(strings)),
        ],
    )?;
    let batches = (0..batch.num_rows())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
        .collect();
    // This is one real input partition, independent of target_partitions=1.
    session.register_table(
        input["name"].as_str().ok_or("missing RNG table name")?,
        Arc::new(MemTable::try_new(schema, vec![batches])?),
    )?;
    Ok(())
}

async fn run_runtime_control_matrix(rng: bool) -> TestResult {
    let (matrix, fixture, representations, target_partitions) = if rng {
        (
            "one_partition_rng",
            include_str!("fixtures/rng-one-partition-spark.jsonl"),
            1,
            1,
        )
    } else {
        (
            "supplementary",
            include_str!("fixtures/885-cse-order-state-spark.jsonl"),
            3,
            2,
        )
    };
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let profile_records = include_str!("fixtures/concat-runtime-native-negative-profiles.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let profiles = &profile_records[1..];
    let case_ids: &[&str] = if rng {
        &[
            "rng_885_one_partition_direct",
            "rng_885_one_partition_before_mask",
            "rng_885_one_partition_after_mask",
        ]
    } else {
        &[
            "concat_885_cse_suffixes",
            "concat_885_cse_distinct_orders",
            "concat_885_competing_cast_before_division",
            "concat_885_competing_division_before_cast",
            "concat_885_seeded_random_after_mask",
            "concat_885_seeded_random_before_mask",
            "concat_885_local_mask_before_division",
            "concat_885_local_division_before_mask",
        ]
    };
    let expected_references = case_ids
        .iter()
        .flat_map(|id| {
            [
                json!([id, false]).to_string(),
                json!([id, true]).to_string(),
            ]
        })
        .collect::<HashSet<_>>();
    let reference_keys = records[1..]
        .iter()
        .map(|case| json!([case["id"], case["ansi"]]).to_string())
        .collect::<HashSet<_>>();
    let mut keys = HashSet::new();
    let mut expected_keys = HashSet::new();
    for id in case_ids {
        for ansi in [false, true] {
            for representation in 0..representations {
                for batch_size in [1, 32] {
                    expected_keys.insert(json!([id, ansi, representation, batch_size]).to_string());
                }
            }
        }
    }
    let mut expected_negative_keys = HashSet::new();
    let mut retained_keys = HashSet::new();
    let mut counts = [0_usize; 5];
    let mut raw_success_only_matches = 0;
    let mut row_matches = 0;
    let mut failures = Vec::new();
    for representation in 0..representations {
        for batch_size in [1, 32] {
            let session = session(batch_size, target_partitions);
            if rng {
                register_rng_inputs(&session, &records[0], batch_size)?;
            } else {
                register_inputs(&session, &records[0], representation, batch_size)?;
            }
            for case in &records[1..] {
                let context = runtime_control_context(matrix, case, representation, batch_size);
                let key = context.to_string();
                if is_active_native_negative_context(&context) {
                    expected_negative_keys.insert(key.clone());
                }
                let observation_key =
                    json!([case["id"], case["ansi"], representation, batch_size]).to_string();
                if !keys.insert(observation_key) {
                    failures
                        .push(json!({"context":context,"failure":"duplicate observation context"}));
                }
                let actual = match capture_observation(&session, case).await {
                    Ok(actual) => actual,
                    Err(error) => json!({"capture_failure":error.to_string()}),
                };
                let comparison: TestResult<(bool, bool, bool, bool)> = (|| {
                    let strict = matches_reference(case, &actual)?;
                    let local = matches_local_preexecution_divide(case, &actual);
                    let retained =
                        matches_retained_native_negative(&context, case, &actual, profiles)?;
                    let rows = actual["rows"].is_array()
                        && case["expected"]["rows"].is_array()
                        && row_counts(&actual["rows"])? == row_counts(&case["expected"]["rows"])?;
                    Ok((strict, local, retained, rows))
                })();
                let (strict, local, retained, rows) = match comparison {
                    Ok(comparison) => comparison,
                    Err(error) => {
                        failures.push(
                            json!({"context":context,"comparison_failure":error.to_string()}),
                        );
                        (false, false, false, false)
                    }
                };
                let raw_full_match = case["expected"]["status"] == "ok"
                    && actual.get("error").is_none()
                    && actual["analyzed_schema"] == case["expected"]["analyzed_schema"]
                    && actual["physical_schema"] == case["expected"]["physical_schema"]
                    && actual["rows"].is_array()
                    && actual["rows"] == case["expected"]["rows"];
                raw_success_only_matches += usize::from(raw_full_match);
                row_matches += usize::from(rows);
                let category = if strict {
                    if case["expected"]["status"] == "ok" {
                        0
                    } else {
                        1
                    }
                } else if local {
                    2
                } else if retained {
                    3
                } else {
                    4
                };
                counts[category] += 1;
                if retained {
                    retained_keys.insert(key);
                }
                let observation = json!({"context":context,"expected":case["expected"],"actual":actual,
                    "full_match":if rng { strict } else { raw_full_match },
                    "strict_meaningful_match":strict,"full_compatibility_pass":strict,
                    "local_preexecution_divide_match":local,
                    "local_phase_names_match":if local { Some(false) } else { None },
                    "retained_native_negative_match":retained,
                    "regression_invariant_pass":strict || local || retained});
                eprintln!("c06 concat permanent runtime control observation: {observation}");
                if category == 4 || observation["actual"].get("capture_failure").is_some() {
                    failures.push(observation);
                }
            }
        }
    }
    let profile_keys = profiles
        .iter()
        .filter(|profile| {
            profile["context"]["matrix"] == matrix
                && is_active_native_negative_context(&profile["context"])
        })
        .map(|profile| profile["context"].to_string())
        .collect::<Vec<_>>();
    let unique_profile_keys = profile_keys.iter().cloned().collect::<HashSet<_>>();
    let expected_counts = if rng {
        [6, 0, 0, 6, 0]
    } else {
        [54, 21, 12, 9, 0]
    };
    let expected_contexts = if rng { 12 } else { 96 };
    eprintln!(
        "c06 concat permanent runtime control summary: {}",
        json!({
        "matrix":matrix,"observations":counts.iter().sum::<usize>(),"unique_contexts":keys.len(),
        "exact_successes":counts[0],"exact_collection_divides":counts[1],
        "separate_local_preexecution_divides":counts[2],"retained_native_negatives":counts[3],
        "unrecognized_outcomes":counts[4],"raw_success_only_matches":raw_success_only_matches,
        "row_matches":row_matches,"strict_spark_comparison_false_count":counts[2]+counts[3]+counts[4],
        "full_compatibility_pass":counts[2]+counts[3]+counts[4] == 0,
        "regression_invariant_pass":failures.is_empty() && counts == expected_counts})
    );
    // Check completeness and disposition only after every context has been observed.
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(reference_keys, expected_references);
    assert_eq!(records.len(), expected_references.len() + 1);
    assert_eq!(profile_records.len(), 18);
    assert_eq!(profile_records[0]["profile_count"], 17);
    assert_eq!(keys.len(), expected_contexts);
    assert_eq!(keys, expected_keys);
    assert_eq!(counts, expected_counts);
    assert_eq!(profile_keys.len(), expected_counts[3]);
    assert_eq!(unique_profile_keys, expected_negative_keys);
    assert_eq!(retained_keys, expected_negative_keys);
    assert_eq!(row_matches, if rng { 6 } else { 54 });
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[tokio::test]
async fn concat_runtime_supplementary_controls_retain_all_96_outcomes() -> TestResult {
    run_runtime_control_matrix(false).await
}

#[tokio::test]
async fn concat_runtime_one_partition_rng_controls_retain_all_12_outcomes() -> TestResult {
    run_runtime_control_matrix(true).await
}

#[test]
fn retained_runtime_profiles_reject_changed_public_results_and_contexts() -> TestResult {
    let records = include_str!("fixtures/concat-runtime-native-negative-profiles.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let profiles = &records[1..];
    assert_eq!(profiles.len(), 17);
    let mut retired_rng_profiles = 0;
    for profile in profiles {
        // These are mutations of real captured native receipts, not new engine observations.
        let context = &profile["context"];
        let case = &profile["captured_observation"];
        let mut active_actual = case["actual"].clone();
        if context["matrix"] == "one_partition_rng"
            && context["id"] == "rng_885_one_partition_direct"
        {
            assert!(!matches_retained_native_negative(
                context, case, &active_actual, profiles
            )?);
            // Current Fields come from Spark; captured rows and raw receipts stay exact.
            active_actual["analyzed_schema"] = case["expected"]["analyzed_schema"].clone();
            active_actual["physical_schema"] = case["expected"]["physical_schema"].clone();
        }
        let actual = &active_actual;
        if !is_active_native_negative_context(context) {
            assert_eq!(context["matrix"], "one_partition_rng");
            assert_eq!(context["id"], "rng_885_one_partition_direct");
            assert_eq!(context["batch_size"], 32);
            assert!(matches_reference(case, actual)?);
            assert!(!matches_retained_native_negative(context, case, actual, profiles)?);
            retired_rng_profiles += 1;
            continue;
        }
        assert!(!matches_reference(case, actual)?);
        assert!(matches_retained_native_negative(
            context, case, actual, profiles
        )?);
        if context["matrix"] == "supplementary" {
            assert_eq!(case["full_match"], false);
        } else {
            assert_eq!(case["compatibility_assessed"], false);
        }
        for (key, value) in [
            ("error", json!("execute: unrelated error")),
            ("error_stage", json!("physical")),
            ("rows", json!([])),
        ] {
            let mut changed = actual.clone();
            changed[key] = value;
            assert!(
                !matches_retained_native_negative(context, case, &changed, profiles)?,
                "{key}"
            );
        }
        for stage in ["analyzed_schema", "physical_schema"] {
            let fields = actual[stage]["fields"]
                .as_array()
                .ok_or("missing captured public fields")?;
            for (index, field) in fields.iter().enumerate() {
                for (key, value) in [
                    ("name", json!("wrong_name")),
                    ("type", json!("wrong_type")),
                    (
                        "nullable",
                        json!(!field["nullable"].as_bool().ok_or("missing nullability")?),
                    ),
                    ("metadata", json!({"wrong_metadata":"true"})),
                ] {
                    let mut changed = actual.clone();
                    changed[stage]["fields"][index][key] = value;
                    assert!(
                        !matches_retained_native_negative(context, case, &changed, profiles)?,
                        "{stage}, {index}, {key}"
                    );
                }
            }
        }
        for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
            let mut changed = actual.clone();
            changed[key] = json!(format!(
                "{} [internal #999 @0x1]",
                actual[key].as_str().ok_or("missing Arrow receipt")?
            ));
            assert!(matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            changed[key] = json!("");
            assert!(!matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            changed
                .as_object_mut()
                .ok_or("missing observation")?
                .remove(key);
            assert!(!matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
        }
        if let Some(rows) = actual.get("rows") {
            let rows = rows.as_array().ok_or("missing captured rows")?;
            let mut changed = actual.clone();
            changed["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
                .reverse();
            assert!(matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            changed["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
                .push(rows[0].clone());
            assert!(!matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            changed = actual.clone();
            changed["rows"].as_array_mut().ok_or("missing rows")?.pop();
            assert!(!matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            let row = rows
                .iter()
                .position(|row| {
                    row.as_array()
                        .is_some_and(|cells| cells.last().is_some_and(|value| !value.is_null()))
                })
                .ok_or("missing seeded value")?;
            let last = rows[row].as_array().ok_or("missing row")?.len() - 1;
            changed = actual.clone();
            changed["rows"][row][last] = json!("wrong_seeded_value");
            assert!(!matches_retained_native_negative(
                context, case, &changed, profiles
            )?);
            if context["matrix"] == "one_partition_rng" {
                changed = actual.clone();
                changed["rows"][0][0] = json!("wrong_row_id");
                assert!(!matches_retained_native_negative(
                    context, case, &changed, profiles
                )?);
            }
        } else {
            for key in ["error", "error_stage"] {
                let mut changed = actual.clone();
                changed
                    .as_object_mut()
                    .ok_or("missing observation")?
                    .remove(key);
                assert!(!matches_retained_native_negative(
                    context, case, &changed, profiles
                )?);
            }
        }
        for key in [
            "matrix",
            "id",
            "ansi",
            "sql",
            "representation",
            "batch_size",
            "input_partitions",
            "target_partitions",
            "timezone",
        ] {
            let mut changed = context.clone();
            changed[key] = json!("wrong_context");
            assert!(
                !matches_retained_native_negative(&changed, case, actual, profiles)?,
                "{key}"
            );
        }
        let mut duplicate_profiles = profiles.to_vec();
        duplicate_profiles.push(profile.clone());
        assert!(!matches_retained_native_negative(
            context,
            case,
            actual,
            &duplicate_profiles
        )?);
        let mut changed_case = case.clone();
        changed_case["expected"]["status"] = json!("wrong_reference");
        assert!(!matches_retained_native_negative(
            context,
            &changed_case,
            actual,
            profiles
        )?);
    }

    assert_eq!(retired_rng_profiles, 2);
    let local = &records[0]["local_preexecution_control"];
    let actual = &local["actual"];
    assert!(!matches_reference(local, actual)?);
    assert!(matches_local_preexecution_divide(local, actual));
    for (key, value) in [
        ("error", json!("physical: unrelated divide error")),
        ("error_stage", json!("execute")),
        ("rows", Value::Null),
        ("physical_schema", json!({"fields":[]})),
        (
            "physical_arrow_schema",
            json!("unexpected completed physical stage"),
        ),
        ("analyzed_arrow_schema", json!("")),
    ] {
        let mut changed = actual.clone();
        changed[key] = value;
        assert!(!matches_local_preexecution_divide(local, &changed), "{key}");
    }
    for key in [
        "physical_schema",
        "physical_arrow_schema",
        "error",
        "error_stage",
        "analyzed_arrow_schema",
    ] {
        let mut changed = actual.clone();
        changed
            .as_object_mut()
            .ok_or("missing observation")?
            .remove(key);
        assert!(
            !matches_local_preexecution_divide(local, &changed),
            "missing {key}"
        );
    }
    for (key, value) in [
        ("name", json!("wrong_name")),
        ("type", json!("wrong_type")),
        ("nullable", json!(false)),
        ("metadata", json!({"wrong_metadata":"true"})),
    ] {
        let mut changed = actual.clone();
        changed["analyzed_schema"]["fields"][0][key] = value;
        assert!(!matches_local_preexecution_divide(local, &changed), "{key}");
    }
    for (key, value) in [
        ("status", json!("ok")),
        ("phase", json!("collect")),
        ("condition", json!("CAST_INVALID_INPUT")),
    ] {
        let mut changed = local.clone();
        changed["expected"][key] = value;
        assert!(
            !matches_local_preexecution_divide(&changed, actual),
            "expected {key}"
        );
    }
    let mut changed = local.clone();
    changed["id"] = json!("unrelated_local_query");
    assert!(!matches_local_preexecution_divide(&changed, actual));
    changed = local.clone();
    changed["ansi"] = json!(false);
    assert!(!matches_local_preexecution_divide(&changed, actual));
    Ok(())
}

#[tokio::test]
async fn concat_runtime_order_matches_all_spark_observations() -> TestResult {
    let records = include_str!("fixtures/concat-runtime-order-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 33);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let mut keys = HashSet::new();
    let mut differences = Vec::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size, 2);
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                assert!(keys.insert((
                    case["id"].as_str().ok_or("missing ID")?.to_owned(),
                    case["ansi"].as_bool().ok_or("missing ANSI")?,
                    representation,
                    batch_size
                )));
                let actual = capture_observation(&session, case).await?;
                let matches = matches_reference(case, &actual)?;
                let observation = json!({"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
                    "representation":representation,"batch_size":batch_size,"expected":case["expected"],
                    "actual":actual,"full_match":matches});
                eprintln!("c06 concat runtime order observation: {observation}");
                if !matches {
                    differences.push(observation);
                }
            }
        }
    }
    assert_eq!(keys.len(), 192);
    eprintln!(
        "c06 concat runtime order summary: {}",
        json!({"observations":keys.len(),
        "strict_failure_count":differences.len(),"full_compatibility_pass":differences.is_empty()})
    );
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[test]
fn runtime_order_comparison_rejects_unrelated_errors_and_changed_rows() -> TestResult {
    let records = include_str!("fixtures/concat-runtime-order-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let error = records
        .iter()
        .find(|case| case["expected"]["status"] == "error")
        .ok_or("missing error")?;
    // Synthetic comparator checks only; these are not native observations.
    let actual = json!({"analyzed_schema":error["expected"]["analyzed_schema"],
        "analyzed_arrow_schema":"completed analyzed Arrow schema",
        "physical_schema":error["expected"]["analyzed_schema"],
        "physical_arrow_schema":"completed physical Arrow schema", "error_stage":"execute",
        "error":"execute: Arrow error: Divide by zero error"});
    assert!(matches_reference(error, &actual)?);
    for (key, value) in [
        ("error", json!("execute: unrelated error")),
        ("error_stage", json!("physical")),
        ("rows", json!([[null], [null]])),
        ("physical_schema", Value::Null),
        ("physical_arrow_schema", json!("")),
    ] {
        let mut changed = actual.clone();
        changed[key] = value;
        assert!(!matches_reference(error, &changed)?, "{key}: {changed}");
    }
    for key in ["name", "type", "nullable", "metadata"] {
        let mut changed = actual.clone();
        changed["analyzed_schema"]["fields"][0][key] = json!("wrong");
        assert!(!matches_reference(error, &changed)?, "{key}: {changed}");
    }
    let success = records
        .iter()
        .find(|case| case["id"] == "concat_885_binary_masked_column_first" && case["ansi"] == true)
        .ok_or("missing binary mask reference")?;
    let actual = json!({"analyzed_schema":success["expected"]["analyzed_schema"],
        "analyzed_arrow_schema":"completed analyzed Arrow schema",
        "physical_schema":success["expected"]["physical_schema"],
        "physical_arrow_schema":"completed physical Arrow schema","rows":success["expected"]["rows"]});
    assert!(matches_reference(success, &actual)?);
    for rows in [
        json!([[null], ["61312e31"]]),
        json!([[null]]),
        json!([[null], [null], ["61312e30"]]),
    ] {
        let mut changed = actual.clone();
        changed["rows"] = rows;
        assert!(!matches_reference(success, &changed)?);
    }
    Ok(())
}

#[test]
fn ordered_concat_keeps_prefix_work_and_constant_descendants() -> TestResult {
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl, cast as expr_cast, col, lit};
    let info = SimplifyContext::builder()
        .with_schema(Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
        ]))?))
        .build();
    let concat = SparkConcat::new().with_ordered_evaluation();
    let null = lit(ScalarValue::Utf8(None));
    // Spark may eagerly hoist common children across distinct CONCATs. Runtime
    // row selection is installed after that logical optimization boundary.
    assert!(!concat.short_circuits());
    assert!(!SparkConcat::new().short_circuits());
    let legacy = ScalarUDF::from(SparkConcat::new()).call(vec![col("v"), col("v")]);
    let boundary = vec![legacy, null.clone()];
    match concat.simplify(boundary.clone(), &info)? {
        ExprSimplifyResult::Original(actual) => assert_eq!(actual, boundary),
        other => return Err(format!("evaluation modes were flattened together: {other:?}").into()),
    }
    for args in [
        vec![col("v"), null.clone()],
        vec![col("v"), null.clone(), col("v")],
    ] {
        match concat.simplify(args.clone(), &info)? {
            ExprSimplifyResult::Original(actual) => assert_eq!(actual, args),
            other => return Err(format!("preceding work was discarded: {other:?}").into()),
        }
    }
    assert!(matches!(
        concat.simplify(vec![null.clone(), col("v")], &info)?,
        ExprSimplifyResult::Simplified(_)
    ));
    let pending = expr_cast(lit(1_i32) / lit(0_i32), DataType::Utf8);
    let nested = ScalarUDF::from(concat.clone()).call(vec![col("v"), pending]);
    assert!(matches!(
        concat.simplify(vec![null, nested], &info)?,
        ExprSimplifyResult::Original(_)
    ));
    Ok(())
}

#[tokio::test]
async fn concat_runtime_keeps_opaque_null_boundaries() -> TestResult {
    use std::sync::LazyLock;

    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, cast as expr_cast, col, lit,
    };

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OpaqueOperand(DataType);

    impl ScalarUDFImpl for OpaqueOperand {
        fn name(&self) -> &str {
            "concat_runtime_opaque"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: LazyLock<Signature> =
                LazyLock::new(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable));
            &SIGNATURE
        }

        fn supports_constant_folding(&self) -> bool {
            false
        }

        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(self.0.clone())
        }

        fn invoke_with_args(
            &self,
            _: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            datafusion::common::exec_err!("opaque runtime CONCAT operand was invoked")
        }
    }

    // These are host extension controls, with no custom-UDF Spark oracle.
    // A known NULL retains #883's whole-call boundary, including earlier work.
    for data_type in [DataType::Utf8, DataType::Binary] {
        let opaque = ScalarUDF::from(OpaqueOperand(data_type.clone()));
        let info = SimplifyContext::builder()
            .with_schema(Arc::new(DFSchema::try_from(Schema::new(vec![
                Field::new("v", data_type.clone(), false),
            ]))?))
            .build();
        let null = lit(ScalarValue::try_from(&data_type)?);
        let required = expr_cast(lit(1_i32) / lit(0_i32), data_type.clone());
        let operand = opaque.call(vec![lit(1_f64) / lit(0_f64)]);
        for args in [
            vec![col("v"), operand.clone(), null.clone()],
            vec![required.clone(), operand.clone(), null.clone()],
            vec![null.clone(), operand, required],
        ] {
            match SparkConcat::new()
                .with_ordered_evaluation()
                .simplify(args, &info)?
            {
                ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, null),
                other => return Err(format!("opaque NULL boundary changed: {other:?}").into()),
            }
        }
        for batch_size in [1, 32] {
            let session = session(batch_size, 1);
            session.register_udf(opaque.clone());
            session.register_udf(create_udf(
                "concat_runtime_allowed",
                vec![data_type.clone()],
                data_type.clone(),
                Volatility::Immutable,
                Arc::new(|args| Ok(args[0].clone())),
            ));
            let schema = Arc::new(Schema::new(vec![Field::new("z", DataType::Int32, false)]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![0, 1]))])?;
            let batches = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect();
            session.register_table(
                "concat_opaque_prefix",
                Arc::new(MemTable::try_new(schema, vec![batches])?),
            )?;
            let (prefix, null) = if data_type == DataType::Binary {
                (
                    "CAST(CAST(1/z AS STRING) AS BINARY)",
                    "CAST(NULL AS BINARY)",
                )
            } else {
                ("CAST(1/z AS STRING)", "CAST(NULL AS STRING)")
            };
            for ansi in [false, true] {
                for operands in [
                    format!("{null},concat_runtime_opaque(1/0)"),
                    format!("concat_runtime_opaque(1/0),{null}"),
                    format!("{prefix},concat_runtime_opaque(1/1),{null}"),
                    format!("{null},concat_runtime_opaque(1/1),{prefix}"),
                    format!("{prefix},concat_runtime_allowed(concat_runtime_opaque(1/1)),{null}"),
                    format!("{prefix},concat_runtime_opaque(1/0),{null}"),
                ] {
                    let sql = format!("SELECT concat({operands}) AS r FROM concat_opaque_prefix");
                    let before_planning = CALLS.load(Ordering::SeqCst);
                    assert_eq!(before_planning, 0, "before planning: {sql}");
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            session_timezone: "UTC".into(),
                            ..Default::default()
                        },
                    )
                    .await?;
                    let after_planning = CALLS.load(Ordering::SeqCst);
                    assert_eq!(after_planning, 0, "after planning: {sql}");
                    assert_eq!(query.column_names, ["r"]);
                    assert_eq!(query.logical_plan.schema().fields().len(), 1);
                    let analyzed_arrow_schema = format!("{:?}", query.logical_plan.schema());
                    let field = query.logical_plan.schema().field(0);
                    assert_eq!(field.data_type(), &data_type);
                    assert!(field.is_nullable());
                    assert!(field.metadata().is_empty());
                    let physical = query.create_physical_plan(&session).await?;
                    let after_physical = CALLS.load(Ordering::SeqCst);
                    assert_eq!(after_physical, 0, "after physical: {sql}");
                    let expected =
                        Arc::new(Schema::new(vec![Field::new("r", data_type.clone(), true)]));
                    assert_eq!(physical.schema(), expected);
                    let physical_arrow_schema = format!("{:?}", physical.schema());
                    let batches =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
                    let after_collect = CALLS.load(Ordering::SeqCst);
                    assert_eq!(after_collect, 0, "after collect: {sql}");
                    let mut rows = Vec::new();
                    let mut batch_schemas = Vec::new();
                    for batch in batches {
                        assert_eq!(batch.schema(), expected);
                        assert_eq!(batch.num_columns(), 1);
                        batch_schemas.push(format!("{:?}", batch.schema()));
                        for row in 0..batch.num_rows() {
                            assert!(batch.column(0).is_null(row), "{sql}, row {row}");
                            rows.push(json!([null]));
                        }
                    }
                    assert_eq!(Value::Array(rows.clone()), json!([[null], [null]]));
                    eprintln!(
                        "c06 concat runtime opaque NULL control: {}",
                        json!({"sql":sql,"ansi":ansi,"batch_size":batch_size,
                            "analyzed_arrow_schema":analyzed_arrow_schema,
                            "physical_arrow_schema":physical_arrow_schema,
                            "batch_schemas":batch_schemas,"rows":rows,
                            "before_planning":before_planning,"after_planning":after_planning,
                            "after_physical":after_physical,"after_collect":after_collect})
                    );
                }
            }
        }
    }
    Ok(())
}

fn storage(array: &ArrayRef) -> (DataType, usize, Vec<(usize, usize)>) {
    let data = array.to_data();
    (
        array.data_type().clone(),
        data.offset(),
        data.buffers()
            .iter()
            .map(|buffer| (buffer.as_ptr() as usize, buffer.len()))
            .collect(),
    )
}

fn repeated_values(array: &ArrayRef, count: usize) -> TestResult<Value> {
    let string = array.data_type().is_string();
    let values = cast(
        array,
        &if string {
            DataType::Utf8
        } else {
            DataType::Binary
        },
    )?;
    Ok(Value::Array(
        (0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    return Ok(json!([null]));
                }
                Ok(json!([if string {
                    values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("expected STRING")?
                        .value(row)
                        .repeat(count)
                } else {
                    values
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or("expected BINARY")?
                        .value(row)
                        .repeat(count)
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                }]))
            })
            .collect::<TestResult<Vec<_>>>()?,
    ))
}

#[tokio::test]
async fn concat_and_substring_preserve_existing_async_operand_planning() -> TestResult {
    use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
    use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AsyncIdentity(Signature);
    impl ScalarUDFImpl for AsyncIdentity {
        fn name(&self) -> &str {
            "concat_async_identity"
        }
        fn signature(&self) -> &Signature {
            &self.0
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke_with_args(
            &self,
            _: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            datafusion::common::exec_err!("async identity requires asynchronous invocation")
        }
    }
    impl AsyncScalarUDFImpl for AsyncIdentity {
        fn invoke_async_with_args<'life0, 'async_trait>(
            &'life0 self,
            args: ScalarFunctionArgs,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = datafusion::common::Result<ColumnarValue>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { Ok(args.args[0].clone()) })
        }
    }
    let session = session(32, 1);
    session.register_udf(
        AsyncScalarUDF::new(Arc::new(AsyncIdentity(Signature::exact(
            vec![DataType::Utf8],
            Volatility::Volatile,
        ))))
        .into_scalar_udf(),
    );
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
    )?;
    session.register_table(
        "concat_async",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    for ansi in [false, true] {
        let actual = capture_result(&session,&json!({"sql":"SELECT concat(v,concat_async_identity(v)) AS r FROM concat_async","ansi":ansi})).await?;
        assert_eq!(actual["rows"], json!([["aa"], [null]]));
        for function in ["substring", "substr"] {
            for arguments in [
                "concat_async_identity(v),1,2",
                "v,cast(length(concat_async_identity(v)) AS BIGINT),2",
                "v,1,cast(length(concat_async_identity(v)) AS BIGINT)",
            ] {
                let sql = format!("SELECT {function}({arguments}) AS r FROM concat_async");
                let actual = capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                let schema = json!({"type":"struct","fields":[
                    {"name":"r","type":"string","nullable":true,"metadata":{}}
                ]});
                assert_eq!(actual["analyzed_schema"], schema);
                assert_eq!(actual["physical_schema"], schema);
                assert_eq!(actual["rows"], json!([["a"], [null]]));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn concat_keeps_needed_row_counts_types_and_flat_wide_calls() -> TestResult {
    for data_type in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        for batch_size in [1, 32] {
            let source: ArrayRef = if data_type.is_string() {
                Arc::new(StringArray::from(vec![
                    Some("padding"),
                    Some("abcdefghijklmnop"),
                    None,
                    Some("qrstuvwxyz012345"),
                    Some("padding"),
                ]))
            } else {
                Arc::new(BinaryArray::from(vec![
                    Some(b"padding".as_slice()),
                    Some(b"\x00\xffabcdefghijklmn".as_slice()),
                    None,
                    Some(b"\x80aABCDEFGHIJKLMN".as_slice()),
                    Some(b"padding".as_slice()),
                ]))
            };
            let source = cast(&source, &data_type)?.slice(1, 3);
            let before = storage(&source);
            let session = session(batch_size, 1);
            let schema = Arc::new(Schema::new(vec![
                Field::new("v", data_type.clone(), true).with_metadata(
                    std::collections::HashMap::from([("c06_source".to_owned(), "885".to_owned())]),
                ),
            ]));
            let batch = RecordBatch::try_new(schema.clone(), vec![source.clone()])?;
            let batches = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect();
            session.register_table(
                "concat_counted",
                Arc::new(MemTable::try_new(schema.clone(), vec![batches])?),
            )?;
            let rows = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&rows);
            session.register_udf(create_udf(
                "concat_observed",
                vec![data_type.clone()],
                data_type.clone(),
                Volatility::Volatile,
                Arc::new(move |args| {
                    let arrays = ColumnarValue::values_to_arrays(args)?;
                    observed.fetch_add(arrays[0].len(), Ordering::SeqCst);
                    Ok(args[0].clone())
                }),
            ));
            let staged = [
                ("a", vec![None, Some("a1"), Some("a2"), Some("a3")]),
                ("b", vec![Some("b0"), None, Some("b2"), Some("b3")]),
                ("c", vec![Some("c0"), Some("c1"), None, Some("c3")]),
            ];
            let mut columns = Vec::new();
            for (_, values) in &staged {
                let values: ArrayRef = if data_type.is_string() {
                    Arc::new(StringArray::from(values.clone()))
                } else {
                    Arc::new(BinaryArray::from(
                        values
                            .iter()
                            .map(|value| value.map(str::as_bytes))
                            .collect::<Vec<_>>(),
                    ))
                };
                columns.push(cast(&values, &data_type)?);
            }
            let schema = Arc::new(Schema::new(
                staged
                    .iter()
                    .map(|(name, _)| Field::new(*name, data_type.clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            let batches = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect();
            session.register_table(
                "concat_staged_masks",
                Arc::new(MemTable::try_new(schema, vec![batches])?),
            )?;
            for ansi in [false, true] {
                for (sql, expected_count) in [
                    (
                        "SELECT concat(v,concat_observed(v)) AS r FROM concat_counted",
                        2,
                    ),
                    (
                        "SELECT concat(concat_observed(v),NULL) AS r FROM concat_counted",
                        3,
                    ),
                    (
                        "SELECT concat(NULL,concat_observed(v)) AS r FROM concat_counted",
                        0,
                    ),
                    (
                        "SELECT concat(v,NULL,concat_observed(v)) AS r FROM concat_counted",
                        0,
                    ),
                    (
                        "SELECT concat(concat_observed(v),v,concat_observed(v)) AS r FROM concat_counted",
                        5,
                    ),
                ] {
                    rows.store(0, Ordering::SeqCst);
                    let actual = capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                    assert_eq!(
                        rows.load(Ordering::SeqCst),
                        expected_count,
                        "{data_type:?}, batch={batch_size}, {sql}"
                    );
                    assert_eq!(actual["rows"].as_array().ok_or("missing rows")?.len(), 3);
                    assert_eq!(
                        actual["rows"],
                        if sql.contains("NULL") {
                            json!([[null], [null], [null]])
                        } else {
                            repeated_values(&source, if expected_count == 5 { 3 } else { 2 })?
                        }
                    );
                }
                rows.store(0, Ordering::SeqCst);
                let actual = capture_result(&session,&json!({"sql":"SELECT concat(concat_observed(a),concat_observed(b),concat_observed(c)) AS r FROM concat_staged_masks","ansi":ansi})).await?;
                assert_eq!(
                    rows.load(Ordering::SeqCst),
                    9,
                    "each cumulative mask must skip later source rows"
                );
                assert_eq!(
                    actual["rows"],
                    json!([
                        [null],
                        [null],
                        [null],
                        [if data_type.is_string() {
                            "a3b3c3"
                        } else {
                            "613362336333"
                        }]
                    ])
                );
                for width in [3, 16, 64] {
                    let mut expression = "concat_observed(v)".to_owned();
                    for _ in 1..width {
                        expression = format!("concat({expression},v)");
                    }
                    let sql = format!("SELECT {expression} AS r FROM concat_counted");
                    rows.store(0, Ordering::SeqCst);
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let expected_type = if data_type == DataType::BinaryView {
                        DataType::Binary
                    } else {
                        data_type.clone()
                    };
                    assert_eq!(
                        query.logical_plan.schema().field(0).data_type(),
                        &expected_type
                    );
                    let mut state = session.state();
                    state
                        .config_mut()
                        .options_mut()
                        .extensions
                        .insert(SparkSqlExpressionSemantics::default());
                    state
                        .config_mut()
                        .options_mut()
                        .extensions
                        .insert(PreserveAggregateStatisticsSchema::default());
                    let optimized = state.optimize(&query.logical_plan)?;
                    let mut arities = Vec::new();
                    optimized.apply(|plan| {
                        plan.apply_expressions(|expr| {
                            expr.apply(|expr| {
                                if let Expr::ScalarFunction(function) = expr
                                    && function.func.inner().is::<SparkConcat>()
                                {
                                    arities.push(function.args.len());
                                }
                                Ok(TreeNodeRecursion::Continue)
                            })
                        })?;
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert_eq!(arities, vec![width], "{sql}");
                    assert_eq!(rows.load(Ordering::SeqCst), 0, "planning evaluated source");
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(physical.schema().field(0).data_type(), &expected_type);
                    assert_eq!(
                        rows.load(Ordering::SeqCst),
                        0,
                        "physical planning evaluated source"
                    );
                    let batches =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
                    assert_eq!(
                        rows.load(Ordering::SeqCst),
                        3,
                        "source was evaluated more than once"
                    );
                    let mut actual_rows = Vec::new();
                    for batch in batches {
                        assert_eq!(batch.column(0).data_type(), &expected_type);
                        actual_rows.extend(
                            repeated_values(batch.column(0), 1)?
                                .as_array()
                                .ok_or("missing rows")?
                                .iter()
                                .cloned(),
                        );
                    }
                    assert_eq!(Value::Array(actual_rows), repeated_values(&source, width)?);
                }
            }
            assert_eq!(storage(&source), before, "CONCAT changed provider storage");
        }
    }
    Ok(())
}
