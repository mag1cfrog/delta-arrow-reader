//! Frozen Unicode and binary substring comparisons with retained compatibility gaps.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "This target selects the additive C06 observation APIs."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{
    TestResult, c06_receipts, capture_observation_with_receipt, register_inputs_with_receipts,
};

#[allow(
    dead_code,
    reason = "Only the shared outcome checks are used by this suite."
)]
#[path = "support/string_binary_contracts.rs"]
mod contracts;
#[path = "support/substring.rs"]
mod substring;

#[test]
fn substring_repaired_cast_fields_and_error_causes_stay_strict() -> TestResult {
    let records = include_str!("fixtures/substring-arguments-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let fractional = records
        .iter()
        .find(|case| case["id"] == "double_fraction_direct_cast" && case["ansi"] == true)
        .ok_or("missing direct CAST reference")?;
    // The historical R2 analyzed declaration must no longer pass as a retained gap.
    let mut actual = json!({
        "analyzed_schema":{"type":"struct","fields":[{"name":"r","type":"integer","nullable":false,"metadata":{}}]},
        "physical_schema":{"type":"struct","fields":[{"name":"r","type":"integer","nullable":false,"metadata":{}}]},
        "rows":[["1"]]
    });
    assert!(!substring::matches_reference(fractional, &actual));
    assert!(substring::known_difference(fractional, &actual).is_none());
    actual["analyzed_schema"] = fractional["expected"]["analyzed_schema"].clone();
    assert!(substring::matches_reference(fractional, &actual));
    for stage in ["analyzed_schema", "physical_schema"] {
        for property in ["name", "type", "nullable", "metadata"] {
            let mut changed = actual.clone();
            changed[stage]["fields"][0][property] = json!("unrelated");
            assert!(!substring::matches_reference(fractional, &changed));
            assert!(substring::known_difference(fractional, &changed).is_none());
        }
    }
    let mut changed = actual;
    changed["rows"] = json!([["2"]]);
    assert!(!substring::matches_reference(fractional, &changed));
    assert!(substring::known_difference(fractional, &changed).is_none());
    let overflow = records
        .iter()
        .find(|case| case["id"] == "float_nan_substring" && case["ansi"] == true)
        .ok_or("missing overflow reference")?;
    assert!(substring::meaningful_error(
        overflow,
        "execute: [CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value NaN to type Int32"
    ));
    for error in [
        "execute: Arrow error: Cast error: Can't cast value NaN to type Int32",
        "execute: [CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value 1 to type Int32",
        "execute: [CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value NaN to type Int64",
        "execute: [UNRELATED] arbitrary failure",
    ] {
        assert!(!substring::meaningful_error(overflow, error));
    }
    Ok(())
}

#[test]
fn overflow_notation_comparison_is_literal_specific_and_keeps_strict_failure() -> TestResult {
    let current: Value = serde_json::from_str(include_str!(
        "fixtures/substring-literal-preparation-current-native.json"
    ))?;
    let records = include_str!("fixtures/substring-arguments-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for (id, stage, cause, physical) in [
        (
            "double_infinity_direct_cast",
            "physical",
            "physical: Optimizer rule 'simplify_expressions' failed\ncaused by\n[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value inf to type Int32",
            false,
        ),
        (
            "double_infinity_substring",
            "physical",
            "physical: Optimizer rule 'simplify_expressions' failed\ncaused by\n[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value inf to type Int32",
            false,
        ),
        (
            "decimal_overflow_direct_cast",
            "physical",
            "physical: preserve_spark_expression_errors\ncaused by\n[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: value of 2147483648 is out of range Int32",
            false,
        ),
        (
            "decimal_overflow_substring",
            "physical",
            "physical: preserve_spark_expression_errors\ncaused by\n[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: value of 2147483648 is out of range Int32",
            false,
        ),
    ] {
        let case = records
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == true)
            .ok_or("missing notation control")?;
        let kind = if id.ends_with("direct_cast") {
            "integer"
        } else {
            "string"
        };
        // Infinity substring uses a whole current observation. The other
        // notation controls retain their historical synthetic mutation inputs.
        let mut actual = if id == "double_infinity_substring" {
            current["substring"]
                .as_array()
                .ok_or("missing current substring controls")?
                .iter()
                .find(|record| {
                    record["id"] == id
                        && record["ansi"] == true
                        && record["representation"] == 0
                        && record["batch_size"] == 1
                })
                .ok_or("missing measured Infinity preparation")?["actual"]
                .clone()
        } else {
            json!({
                "analyzed_schema":{"type":"struct","fields":[{"name":"r","type":kind,"nullable":true,"metadata":{}}]},
                "analyzed_arrow_schema":"synthetic DFSchema for mutation controls",
                "physical_schema":null,"physical_arrow_schema":null,"error_stage":stage,"error":cause
            })
        };
        if physical {
            actual["physical_schema"] = json!({"type":"struct","fields":[{"name":"r","type":"string","nullable":true,"metadata":{}}]});
            actual["physical_arrow_schema"] = json!("synthetic Arrow schema for mutation controls");
        }
        assert!(!substring::matches_reference(case, &actual));
        assert!(substring::matches_meaningful_contract(case, &actual));
        if id == "double_infinity_substring" {
            for key in ["status", "phase"] {
                let mut changed = case.clone();
                changed["expected"]
                    .as_object_mut()
                    .ok_or("missing expected object")?
                    .remove(key);
                assert!(!substring::matches_meaningful_contract(&changed, &actual));
            }
            for (key, wrong) in [
                ("status", json!("ok")),
                ("status", json!(true)),
                ("phase", json!("collect")),
            ] {
                let mut changed = case.clone();
                changed["expected"][key] = wrong;
                assert!(!substring::matches_meaningful_contract(&changed, &actual));
            }
            for key in ["physical_schema", "physical_arrow_schema"] {
                let mut changed = actual.clone();
                changed
                    .as_object_mut()
                    .ok_or("missing actual object")?
                    .remove(key);
                assert!(!substring::matches_meaningful_contract(case, &changed));
            }
            for (component, wrong) in [
                ("nullable", json!(false)),
                ("name", json!("wrong")),
                ("type", json!("integer")),
                ("metadata", json!({"wrong":"metadata"})),
            ] {
                let mut changed = actual.clone();
                changed["analyzed_schema"]["fields"][0][component] = wrong;
                assert!(!substring::matches_meaningful_contract(case, &changed));
            }
            let mut late = actual.clone();
            late["error_stage"] = json!("execute");
            late["error"] = json!(
                "execute: [CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value inf to type Int32"
            );
            late["physical_schema"] = actual["analyzed_schema"].clone();
            late["physical_arrow_schema"] = actual["analyzed_arrow_schema"].clone();
            assert!(!substring::matches_meaningful_contract(case, &late));
            let mut rows = actual.clone();
            rows["rows"] = json!([[null]]);
            assert!(!substring::matches_meaningful_contract(case, &rows));
        }
        if physical {
            for (component, wrong) in [
                ("nullable", json!(false)),
                ("name", json!("wrong")),
                ("type", json!("integer")),
                ("metadata", json!({"wrong":"metadata"})),
            ] {
                let mut changed = actual.clone();
                changed["physical_schema"]["fields"][0][component] = wrong;
                assert!(!substring::matches_meaningful_contract(case, &changed));
            }
        }
        for wrong in [
            cause.replace("[CAST_OVERFLOW]", ""),
            cause
                .replace("inf to", "-inf to")
                .replace("2147483648", "-2147483648"),
            cause
                .replace("inf to", "NaN to")
                .replace("2147483648", "2147483649"),
            cause.replace("Int32", "Int64"),
            "execute: [UNRELATED] arbitrary failure".to_owned(),
        ] {
            assert_ne!(wrong, cause);
            let mut changed = actual.clone();
            changed["error"] = json!(wrong);
            assert!(!substring::matches_meaningful_contract(case, &changed));
        }
        for parameter in ["value", "sourceType", "targetType", "ansiConfig"] {
            let mut changed = case.clone();
            changed["expected"]["parameters"][parameter] = json!("unrelated");
            assert!(!substring::matches_meaningful_contract(&changed, &actual));
        }
        for field in ["analyzed_schema", "analyzed_arrow_schema"] {
            let mut changed = actual.clone();
            changed[field] = Value::Null;
            assert!(!substring::matches_meaningful_contract(case, &changed));
        }
        let mut changed = actual;
        changed["error_stage"] = json!("resolve");
        assert!(!substring::matches_meaningful_contract(case, &changed));
    }
    Ok(())
}

#[tokio::test]
async fn substring_retains_complete_spark_comparisons() -> TestResult {
    use std::collections::{BTreeMap, HashSet};
    let fixtures = [
        (
            "substring-original-spark.jsonl",
            include_str!("fixtures/substring-original-spark.jsonl"),
            340,
        ),
        (
            "substring-boundaries-spark.jsonl",
            include_str!("fixtures/substring-boundaries-spark.jsonl"),
            194,
        ),
        (
            "substring-arguments-spark.jsonl",
            include_str!("fixtures/substring-arguments-spark.jsonl"),
            122,
        ),
    ];
    let run_label = std::env::var("C06_ARGS_RUN_LABEL").unwrap_or_else(|_| "retained".into());
    let manifest_path = std::env::var("C06_ARGS_SOURCE_MANIFEST").ok();
    let source_manifest = manifest_path
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?
        .map(|source| serde_json::from_str::<Value>(&source))
        .transpose()?;
    let mut keys = HashSet::new();
    let mut profiles = BTreeMap::new();
    let mut unexpected = Vec::new();
    let mut strict_differences = 0;
    let mut meaningful_differences = 0;
    let mut capture_failures = 0;
    for (name, capture, count) in fixtures {
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        eprintln!(
            "c06 arguments capture header: {}",
            json!({"run_label":run_label,"fixture":fixture,
            "spark_capture_header":records[0],"native_target_partitions":2,"native_session_timezone":"UTC",
            "native_manifest_path":manifest_path,"native_source_manifest":source_manifest,
            "native_crate_cwd":env!("CARGO_MANIFEST_DIR")})
        );
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
                let providers = register_inputs_with_receipts(
                    &session,
                    &records[0],
                    representation,
                    batch_size,
                )
                .await?;
                for provider in providers.as_array().ok_or("missing provider receipts")? {
                    c06_receipts::emit(
                        "provider",
                        json!({"family":"arguments","fixture":fixture,
                        "representation":representation,"batch_size":batch_size,"target_partitions":2}),
                        provider.clone(),
                    );
                }
                for case in &records[1..] {
                    assert!(keys.insert((
                        case["id"].as_str().ok_or("missing ID")?.to_owned(),
                        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        representation,
                        batch_size
                    )));
                    let task_session = session.clone();
                    let task_case = case.clone();
                    let (actual, receipt) = match tokio::spawn(async move {
                        capture_observation_with_receipt(&task_session, &task_case).await
                    })
                    .await
                    {
                        Ok(Ok(actual)) => actual,
                        Ok(Err(error)) => {
                            capture_failures += 1;
                            (
                                json!({"capture_error":error.to_string()}),
                                c06_receipts::failure(error.as_ref(), "capture"),
                            )
                        }
                        Err(error) => {
                            capture_failures += 1;
                            (
                                json!({"capture_task_error":error.to_string()}),
                                c06_receipts::failure(&error, "capture_task"),
                            )
                        }
                    };
                    c06_receipts::emit(
                        "query",
                        json!({"family":"arguments","run_label":run_label,"fixture":fixture,
                        "id":case["id"],"ansi":case["ansi"],"sql":case["sql"],"representation":representation,
                        "batch_size":batch_size,"target_partitions":2}),
                        receipt,
                    );
                    let matches = substring::matches_reference(case, &actual);
                    let meaningful = substring::matches_meaningful_contract(case, &actual);
                    strict_differences += usize::from(!matches);
                    meaningful_differences += usize::from(!meaningful);
                    let profile = if !contracts::completed_stages_are_retained(&actual) {
                        "missing_completed_stage"
                    } else if matches {
                        "full_spark_match"
                    } else {
                        substring::known_difference(case, &actual)
                            .unwrap_or("unexpected_difference")
                    };
                    *profiles.entry(profile).or_insert(0usize) += 1;
                    eprintln!(
                        "c06 arguments observation: {}",
                        json!({"run_label":run_label,"fixture":fixture,
                        "id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,
                        "matches_reference":matches,"meaningful_contract_match":meaningful,"profile":profile,"sql":case["sql"],"actual":actual})
                    );
                    if matches!(profile, "missing_completed_stage" | "unexpected_difference") {
                        unexpected.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {}, actual {actual}",case["id"],case["ansi"],case["expected"]));
                    }
                }
            }
        }
    }
    let full_compatibility_pass = strict_differences == 0;
    eprintln!(
        "c06 arguments capture summary: {}",
        json!({"run_label":run_label,"references":656,
        "observations":keys.len(),"capture_failures":capture_failures,"differences":strict_differences,
        "capture_only":false,"full_compatibility_pass":full_compatibility_pass,"profiles":profiles})
    );
    eprintln!(
        "C06 substring meaningful comparison: {}",
        json!({"observations":keys.len(),
        "meaningful_contract_matches":keys.len()-meaningful_differences,
        "meaningful_contract_differences":meaningful_differences,
        "strict_receipt_differences":strict_differences,"full_compatibility_pass":full_compatibility_pass})
    );
    assert_eq!(keys.len(), 3936);
    assert_eq!(capture_failures, 0);
    assert!(unexpected.is_empty(), "{}", unexpected.join("\n"));
    assert_eq!(
        profiles,
        BTreeMap::from([
            ("full_spark_match", 3900),
            ("open_780_invalid_utf8", 12),
            ("strict_172_infinity_literal_notation", 12),
            ("strict_decimal_literal_notation", 12),
        ])
    );
    assert_eq!(strict_differences, 36);
    assert_eq!(meaningful_differences, 12);
    assert!(
        !full_compatibility_pass,
        "the known failures remain incompatible"
    );
    Ok(())
}

#[tokio::test]
async fn substring_retains_all_cross_row_error_observations() -> TestResult {
    let records = include_str!("fixtures/substring-first-error-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 15);
    let mut compared = 0;
    let mut strict_differences = 0;
    let mut unexpected = Vec::new();
    let mut keys = std::collections::HashSet::new();
    let mut capture_failures = 0;
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
            // Preserve the original two provider partitions and their batch assignment.
            let providers =
                register_inputs_with_receipts(&session, &records[0], representation, batch_size)
                    .await?;
            for provider in providers.as_array().ok_or("missing provider receipts")? {
                c06_receipts::emit(
                    "provider",
                    json!({"family":"first_error","fixture":"substring-first-error-spark.jsonl",
                    "representation":representation,"batch_size":batch_size,"target_partitions":2}),
                    provider.clone(),
                );
            }
            for case in &records[1..] {
                assert!(keys.insert((
                    case["id"].as_str().ok_or("missing ID")?.to_owned(),
                    case["ansi"].as_bool().ok_or("missing ANSI")?,
                    representation,
                    batch_size
                )));
                let task_session = session.clone();
                let task_case = case.clone();
                let (actual, receipt) = match tokio::spawn(async move {
                    capture_observation_with_receipt(&task_session, &task_case).await
                })
                .await
                {
                    Ok(Ok(actual)) => actual,
                    Ok(Err(error)) => {
                        capture_failures += 1;
                        (
                            json!({"capture_error":error.to_string()}),
                            c06_receipts::failure(error.as_ref(), "capture"),
                        )
                    }
                    Err(error) => {
                        capture_failures += 1;
                        (
                            json!({"capture_task_error":error.to_string()}),
                            c06_receipts::failure(&error, "capture_task"),
                        )
                    }
                };
                c06_receipts::emit(
                    "query",
                    json!({"family":"first_error","fixture":"substring-first-error-spark.jsonl",
                    "id":case["id"],"ansi":case["ansi"],"sql":case["sql"],"representation":representation,
                    "batch_size":batch_size,"target_partitions":2}),
                    receipt,
                );
                let matches = substring::matches_reference(case, &actual);
                strict_differences += usize::from(!matches);
                compared += 1;
                let known =
                    !matches && substring::known_first_error_difference(case, &actual, batch_size);
                eprintln!(
                    "c06 substring first error observation: {}",
                    json!({"id":case["id"],"ansi":case["ansi"],
                    "sql":case["sql"],"representation":representation,"batch_size":batch_size,"target_partitions":2,
                    "expected":case["expected"],"actual":actual,"full_match":matches,
                    "profile":if known {"open_151_unordered_cross_row_winner"}else if matches {"full_spark_match"}else {"unexpected_difference"}})
                );
                if !contracts::completed_stages_are_retained(&actual) || (!matches && !known) {
                    unexpected.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {}, actual {actual}",case["id"],case["ansi"],case["expected"]));
                }
            }
        }
    }
    eprintln!(
        "c06 substring first error summary: {}",
        json!({"observations":compared,
        "strict_reference_matches":compared-strict_differences,"strict_reference_differences":strict_differences,
        "differences":strict_differences,"capture_failures":capture_failures,"full_compatibility_pass":strict_differences==0})
    );
    assert_eq!(compared, 84);
    assert_eq!(keys.len(), 84);
    assert_eq!(capture_failures, 0);
    assert!(unexpected.is_empty(), "{}", unexpected.join("\n"));
    assert_eq!(strict_differences, 3);
    Ok(())
}

#[tokio::test]
async fn substring_preserves_native_sql_and_fixed_binary_inputs() -> TestResult {
    use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    async fn native(
        session: &SessionContext,
        sql: &str,
        control: &str,
    ) -> TestResult<Vec<RecordBatch>> {
        let frame = session.sql(sql).await?;
        let mut receipt = c06_receipts::new("native_dataframe_physical_collect");
        receipt["analyzed"] = c06_receipts::schema(frame.schema().as_arrow());
        receipt["public_names"] = json!(
            frame
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>()
        );
        let physical = frame.create_physical_plan().await?;
        receipt["before_collect"] = c06_receipts::schema(physical.schema().as_ref());
        receipt["physical"] = c06_receipts::schema(physical.schema().as_ref());
        receipt["physical_plan"] = json!(format!("{physical:?}"));
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        receipt["output"] = c06_receipts::batches(&batches, string_binary::encode_value)?;
        c06_receipts::finish(&mut receipt);
        c06_receipts::emit(
            "native",
            json!({"control":control,"sql":sql,
            "batch_size":session.state().config().batch_size(),"target_partitions":session.state().config().target_partitions()}),
            receipt,
        );
        Ok(batches)
    }

    let native_sql =
        "SELECT substr('abcdef',-10,6) AS a,substr('abcdef',0,2) AS b,concat('a',NULL) AS c";
    let plain = SessionContext::new();
    let expected = native(&plain, native_sql, "plain_before").await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(
        native(&session, native_sql, "configured_before").await?,
        expected
    );
    let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [
            Some([0u8, 0xff, 0x61]),
            Some([0, 0xff, 0x61]),
            None,
            Some([0, 0xff, 0x61]),
            Some([0, 0xff, 0x61]),
        ]
        .into_iter(),
        3,
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("b", DataType::FixedSizeBinary(3), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
            Arc::new(values),
        ],
    )?;
    let sliced = batch.slice(1, 3);
    assert_eq!(sliced.column(1).null_count(), 1);
    session.register_batch("fixed_binary", sliced.clone())?;
    let empty = sliced.slice(0, 0);
    session.register_batch("fixed_empty", empty.clone())?;
    let mut fixed_receipt = c06_receipts::registration(
        &json!({"name":"fixed_binary"}),
        &batch,
        &sliced,
        &[vec![sliced.clone()]],
        string_binary::encode_value,
    )?;
    fixed_receipt["registration"]["slice"] =
        json!({"offset":1,"length":3,"storage_rows":batch.num_rows()});
    let mut empty_receipt = c06_receipts::registration(
        &json!({"name":"fixed_empty"}),
        &batch,
        &empty,
        &[vec![empty.clone()]],
        string_binary::encode_value,
    )?;
    empty_receipt["registration"]["slice"] =
        json!({"offset":1,"length":0,"storage_rows":batch.num_rows()});
    let registrations = vec![fixed_receipt, empty_receipt];
    let providers =
        c06_receipts::provider_readbacks(&session, registrations, string_binary::encode_value)
            .await?;
    for provider in providers
        .as_array()
        .ok_or("missing fixed provider receipts")?
    {
        c06_receipts::emit(
            "provider",
            json!({"family":"fixed_binary","fixture":"native_fixed_binary",
            "batch_size":session.state().config().batch_size(),"target_partitions":session.state().config().target_partitions()}),
            provider.clone(),
        );
    }
    for ansi in [true, false] {
        let (result, receipt) = capture_observation_with_receipt(
            &session,
            &json!({"sql":"SELECT substring(b,2,2) AS r FROM fixed_binary ORDER BY k","ansi":ansi}),
        )
        .await?;
        c06_receipts::emit(
            "native",
            json!({"control":"fixed_binary","ansi":ansi,
            "sql":"SELECT substring(b,2,2) AS r FROM fixed_binary ORDER BY k",
            "batch_size":session.state().config().batch_size(),"target_partitions":session.state().config().target_partitions()}),
            receipt,
        );
        assert!(result.get("error").is_none(), "{result}");
        // The independent nul_ff_a_middle reference observes these same byte boundaries.
        assert_eq!(result["rows"], json!([["ff61"], [null], ["ff61"]]));
        let expected_schema = json!({"type":"struct","fields":[{"name":"r","type":"binary","nullable":true,"metadata":{}}]});
        assert_eq!(result["analyzed_schema"], expected_schema);
        assert_eq!(result["physical_schema"], expected_schema);
        let (empty, receipt) = capture_observation_with_receipt(
            &session,
            &json!({"sql":"SELECT substring(b,2,2) AS r FROM fixed_empty","ansi":ansi}),
        )
        .await?;
        c06_receipts::emit(
            "native",
            json!({"control":"fixed_empty","ansi":ansi,
            "sql":"SELECT substring(b,2,2) AS r FROM fixed_empty",
            "batch_size":session.state().config().batch_size(),"target_partitions":session.state().config().target_partitions()}),
            receipt,
        );
        assert!(empty.get("error").is_none(), "{empty}");
        assert_eq!(empty["rows"], json!([]));
        assert_eq!(empty["analyzed_schema"], expected_schema);
        assert_eq!(empty["physical_schema"], expected_schema);
    }
    assert_eq!(
        native(&session, native_sql, "configured_after").await?,
        expected
    );
    Ok(())
}
