//! Compare NULL ROWS admission with Spark and require explicit typed-NULL rejection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/null_rows.rs"]
mod null_rows;
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    STAGES, TestResult, case_identity, compare_exact_spark_query_stages_with_optimized,
    native_result_matches_spark_reference, validate_spark_reference_result,
};

const REQUEST: &str = include_str!("fixtures/null-rows-1085-cases.json");
const REFERENCE: &str = include_str!("fixtures/null-rows-1085-spark.jsonl");
const STRING_PARTNER_REQUEST: &str =
    include_str!("fixtures/null-rows-1085-string-partner-cases.json");
const STRING_PARTNER_REFERENCE: &str =
    include_str!("fixtures/null-rows-1085-string-partner-spark.jsonl");
const ORIGINAL: &str = include_str!("fixtures/null-range-754-cases.json");

fn parse_fixture(request_text: &str, reference_text: &str) -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(request_text)?;
    let records = reference_text
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    validate_references(&request, &records)?;
    Ok((request, records))
}

fn validate_references(request: &Value, records: &[Value]) -> TestResult {
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    let string_partner =
        corpora.len() == 1 && corpora[0]["corpus"] == "null-rows-1085-string-partner";
    let expected_records = if string_partner { 8 } else { 92 };
    let expected_sql = if string_partner { 4 } else { 46 };
    let expected_corpora = if string_partner { 1 } else { 2 };
    let expected_outcomes = if string_partner {
        [8, 0, 0]
    } else {
        [72, 10, 10]
    };
    let original: Value = serde_json::from_str(ORIGINAL)?;
    let original_corpus = original["corpora"]
        .as_array()
        .ok_or("missing original corpora")?
        .iter()
        .find(|corpus| corpus["corpus"] == "null-range-754-supplement")
        .ok_or("missing original corpus")?;
    if request["issue"] != 1085
        || request["expected_record_count"] != expected_records
        || request["sql_source_count"] != expected_sql
        || request["batch_sizes"] != json!([1, 32])
        || request["provider_partitions"] != 2
        || request["target_partitions"] != 2
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || corpora.len() != expected_corpora
        || records.len() != expected_records + 1
    {
        return Err("NULL ROWS source or reference inventory differs".into());
    }
    let original_cases = original_corpus["cases"]
        .as_array()
        .ok_or("missing original cases")?
        .iter()
        .filter(|case| {
            case["id"].as_str().is_some_and(|id| {
                request["original_source_manifest"]["ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|value| value == id))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let original_inputs = original_corpus["metadata"]["inputs"]
        .as_array()
        .ok_or("missing original inputs")?
        .iter()
        .filter(|input| {
            matches!(
                input["name"].as_str(),
                Some("null_range_mixed" | "null_range_empty")
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    if (!string_partner
        && (original_cases.len() != 8 || corpora[0]["cases"] != json!(original_cases)))
        || corpora
            .iter()
            .any(|corpus| corpus["metadata"]["inputs"] != json!(original_inputs))
    {
        return Err("original NULL ROWS SQL or providers changed".into());
    }
    let header = &records[0];
    if header["issue"] != 1085
        || header["spark_version"] != request["spark_version"]
        || header["spark_build"]["SPARK_REVISION"]["value"] != request["spark_revision"]
        || header["expected_record_count"] != expected_records
        || header["spark_only"] != true
        || header["reads_expected_outputs"] != false
        || header["schema_stage_protocol"] != json!(STAGES.map(|(name, _)| name))
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
    {
        return Err("NULL ROWS independent runtime differs".into());
    }
    let receipts = header["corpora"]
        .as_array()
        .ok_or("missing provider receipts")?;
    if receipts.len() != corpora.len() {
        return Err("NULL ROWS receipt count differs".into());
    }
    let mut definitions = BTreeMap::new();
    let mut modes = BTreeMap::<String, BTreeSet<bool>>::new();
    for corpus in corpora {
        let receipt = receipts
            .iter()
            .find(|receipt| receipt["corpus"] == corpus["corpus"])
            .ok_or("missing provider receipt")?;
        if ["source", "metadata"]
            .iter()
            .any(|field| receipt[*field] != corpus[*field])
        {
            return Err("NULL ROWS provider source differs".into());
        }
        let providers = receipt["registered_inputs"]
            .as_array()
            .ok_or("missing providers")?;
        if providers.len() != original_inputs.len() {
            return Err("NULL ROWS provider count differs".into());
        }
        for (provider, input) in providers.iter().zip(&original_inputs) {
            if ["name", "schema", "rows"]
                .iter()
                .any(|field| provider[*field] != input[*field])
                || provider["schema_observed"] != input["schema"]
                || provider["num_partitions"]
                    .as_u64()
                    .is_none_or(|count| count == 0)
            {
                return Err("NULL ROWS registered provider differs".into());
            }
        }
        for case in corpus["cases"].as_array().ok_or("missing source cases")? {
            let key = case_identity(case)?;
            if case["corpus"] != corpus["corpus"]
                || case["ordered"] != false
                || ["expected", "status", "spark_error_details"]
                    .iter()
                    .any(|field| case.get(*field).is_some())
                || definitions.insert(key.clone(), case).is_some()
            {
                return Err("NULL ROWS source identity differs".into());
            }
            modes
                .entry(case["sql"].as_str().ok_or("missing SQL")?.into())
                .or_default()
                .insert(key.2);
        }
    }
    let mut seen = BTreeSet::new();
    let mut outcomes = [0, 0, 0];
    for case in &records[1..] {
        let key = case_identity(case)?;
        let source = definitions.get(&key).ok_or("unknown NULL ROWS reference")?;
        if !seen.insert(key.clone())
            || case.get("capture_failure").is_some()
            || ["sql", "ordered", "ownership", "coverage"]
                .iter()
                .any(|field| case.get(*field) != source.get(*field))
        {
            return Err("NULL ROWS reference differs from source".into());
        }
        let settings = case["effective_settings"]
            .as_object()
            .ok_or("missing settings")?;
        let expected_settings = header["effective_settings"]
            .as_object()
            .ok_or("missing header settings")?;
        if settings.len() != 8
            || expected_settings.len() != 8
            || settings.iter().any(|(name, value)| {
                if name == "spark.sql.ansi.enabled" {
                    value.as_str() != Some(if key.2 { "true" } else { "false" })
                } else {
                    expected_settings.get(name) != Some(value)
                }
            })
        {
            return Err("NULL ROWS actual settings differ".into());
        }
        validate_spark_reference_result(case)?;
        null_rows::expected_native_error(case)?;
        if string_partner {
            let (location, source_type) = match case["coverage"]["shape"].as_str() {
                Some("bare_null_lower_string_upper") => ("lower", "\"DOUBLE\""),
                Some("typed_null_lower_string_upper") => ("upper", "\"STRING\""),
                _ => return Err("unknown NULL/STRING source shape".into()),
            };
            let parameters = &case["spark_error_details"]["parameters"];
            if case["expected"]["condition"]
                != "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"
                || parameters["location"] != location
                || parameters["exprType"] != source_type
                || parameters["expectedType"] != "\"INT\""
                || case["spark_error_details"]["sql_state"] != "42K09"
            {
                return Err("NULL/STRING bound admission precedence changed".into());
            }
        }
        if null_rows::unsupported_null_bound_location(case).is_some() {
            let empty = case["coverage"]["provider"] == "null_range_empty";
            let fields = json!({"type":"struct","fields":[
                {"name":"id","type":"integer","nullable":false,"metadata":{}},
                {"name":"n","type":"long","nullable":false,"metadata":{}}
            ]});
            let outcome_matches = if empty {
                case["status"] == "ok" && case["expected"]["rows"] == json!([])
            } else {
                case["expected"]["condition"] == "INTERNAL_ERROR"
                    && case["spark_error_phase"] == "collect"
                    && case["spark_error_details"]["parameters"]
                        == json!({"message":"Unhandled bound in windows expressions: null"})
                    && case["spark_error_details"]["sql_state"] == "XX000"
            };
            if !outcome_matches
                || STAGES
                    .iter()
                    .any(|(stage, _)| case["schema_stages"][*stage] != fields)
            {
                return Err("typed NULL ROWS Spark outcome or Fields changed".into());
            }
            outcomes[if empty { 2 } else { 1 }] += 1;
        } else {
            if case["status"] != "error"
                || case["spark_error_phase"] != "sql"
                || STAGES.iter().any(|(stage, _)| {
                    case["schema_stage_availability"][*stage]["available"] != false
                })
            {
                return Err(
                    "NULL ROWS admission error lost its phase or unavailable Fields".into(),
                );
            }
            outcomes[0] += 1;
        }
    }
    if definitions.len() != expected_records
        || seen.len() != expected_records
        || modes.len() != expected_sql
        || modes
            .values()
            .any(|mode| *mode != BTreeSet::from([false, true]))
        || outcomes != expected_outcomes
    {
        return Err("NULL ROWS source, ANSI or outcome inventory differs".into());
    }
    Ok(())
}

#[tokio::test]
async fn null_rows_admission_matches_spark_with_explicit_typed_null_difference() -> TestResult {
    let fixtures = [
        parse_fixture(REQUEST, REFERENCE)?,
        parse_fixture(STRING_PARTNER_REQUEST, STRING_PARTNER_REFERENCE)?,
    ];
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
    let mut admission = 0;
    let mut typed = 0;
    let mut raw_strict = 0;
    let mut native_aliases = 0;
    for (request, records) in fixtures {
        for corpus in request["corpora"].as_array().ok_or("missing corpora")? {
            for batch in [1, 32] {
                let session = SessionContext::new_with_state(
                    configure_spark_sql(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(
                                SessionConfig::new()
                                    .with_batch_size(batch)
                                    .with_target_partitions(2),
                            ),
                    )
                    .build(),
                );
                let provider_error =
                    spark_inputs::register_recorded_inputs(&session, &corpus["metadata"], batch)
                        .err()
                        .map(|error| error.to_string());
                for case in records[1..]
                    .iter()
                    .filter(|case| case["corpus"] == corpus["corpus"])
                {
                    let observation = match &provider_error {
                        Some(error) => {
                            json!({"observer_error":error,"observer_phase":"register_inputs"})
                        }
                        None => compare_exact_spark_query_stages_with_optimized(&session, case)
                            .await
                            .unwrap_or_else(|error| json!({"observer_error":error.to_string()})),
                    };
                    let intentional_difference =
                        null_rows::unsupported_null_bound_location(case).is_some();
                    let legacy_alias = matches!(
                        case["expected"]["condition"].as_str(),
                        Some("_LEGACY_ERROR_TEMP_1036" | "_LEGACY_ERROR_TEMP_1035")
                    );
                    let raw_spark_matches =
                        native_result_matches_spark_reference(&observation, case);
                    let exact_rejection = null_rows::matches_contract(case, &observation)?;
                    let matches = exact_rejection
                        && (if intentional_difference {
                            !raw_spark_matches
                        } else {
                            raw_spark_matches || legacy_alias
                        });
                    if intentional_difference {
                        typed += 1;
                    } else {
                        admission += 1;
                    }
                    raw_strict += usize::from(raw_spark_matches);
                    native_aliases +=
                        usize::from(legacy_alias && !raw_spark_matches && exact_rejection);
                    let unique = completed.insert((case_identity(case)?, batch));
                    if !matches || !unique {
                        differences.push(format!(
                            "{}:{} ANSI={} batch={batch}",
                            case["corpus"], case["id"], case["ansi"]
                        ));
                    }
                    eprintln!(
                        "NULL_ROWS_OBSERVATION {}",
                        json!({"issue":1085,"corpus":case["corpus"],"id":case["id"],
                        "ansi":case["ansi"],"batch":batch,"provider_partitions":2,"target_partitions":2,
                        "reference":case,"observation":observation,"contract_matches":matches,
                        "intentional_typed_null_difference":intentional_difference,"raw_strict_spark_matches":raw_spark_matches,
                        "native_condition_alias":legacy_alias,"spark_admission_agreement":!intentional_difference && matches})
                    );
                }
            }
        }
    }
    eprintln!(
        "NULL_ROWS_SUMMARY {}",
        json!({"observations":completed.len(),"spark_admission_contexts":admission,
        "raw_strict_spark_contexts":raw_strict,"native_condition_alias_contexts":native_aliases,
        "intentional_typed_null_contexts":typed,"differences":differences.len()})
    );
    assert_eq!((completed.len(), admission, typed), (200, 160, 40));
    assert_eq!(raw_strict + native_aliases, admission);
    assert!(
        differences.is_empty(),
        "NULL ROWS differences:\n{}",
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn null_rows_contract_rejects_wrong_diagnostics_phase_and_missing_observations() -> TestResult {
    let cases = [
        parse_fixture(REQUEST, REFERENCE)?,
        parse_fixture(STRING_PARTNER_REQUEST, STRING_PARTNER_REFERENCE)?,
    ]
    .into_iter()
    .flat_map(|(_, records)| records.into_iter().skip(1))
    .collect::<Vec<_>>();
    for case in &cases {
        let error = null_rows::expected_native_error(case)?;
        let observation = json!({"native_phase":"resolve","error":error,"optimized_error":error,
            "public_schema":null,"optimized_schema":null,"before_collect":null,"physical_schema":null,
            "optimized_observation":null,"rows":null,"rows_raw":null});
        assert!(null_rows::matches_contract(case, &observation)?);
        let wrong_messages = [
            error.replace("lower", "upper"),
            error.replace("upper", "lower"),
            error.replace("DOUBLE", "VOID"),
            error.replace("VOID", "DOUBLE"),
            error.replace("BIGINT", "DOUBLE"),
            error.replace("STRING", "INT"),
            format!("{error} unrelated failure"),
            error
                .split_once(']')
                .map(|(condition, _)| format!("{condition}] another cause"))
                .unwrap_or_else(|| "External error: not supported: another NULL ROWS cause".into()),
        ];
        for wrong in wrong_messages.into_iter().filter(|wrong| *wrong != error) {
            let mut changed = observation.clone();
            changed["error"] = json!(wrong);
            changed["optimized_error"] = changed["error"].clone();
            assert!(
                !null_rows::matches_contract(case, &changed)?,
                "accepted wrong diagnostic for {}",
                case["id"]
            );
        }
        for (field, value) in [
            ("native_phase", json!("collect")),
            ("optimized_error", json!("another cause")),
            ("rows", json!([])),
            ("rows_raw", json!([])),
            ("observer_error", json!("failed")),
            ("native_panic", json!("failed")),
        ] {
            let mut changed = observation.clone();
            changed[field] = value;
            assert!(
                !null_rows::matches_contract(case, &changed)?,
                "accepted wrong {field}"
            );
        }
        for field in [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
            "optimized_observation",
            "rows",
            "rows_raw",
        ] {
            let mut changed = observation.clone();
            changed
                .as_object_mut()
                .ok_or("missing observation")?
                .remove(field);
            assert!(
                !null_rows::matches_contract(case, &changed)?,
                "accepted missing {field}"
            );
            let mut changed = observation.clone();
            changed[field] = json!({"type":"struct","fields":[]});
            assert!(
                !null_rows::matches_contract(case, &changed)?,
                "accepted available {field}"
            );
        }
    }
    Ok(())
}

#[test]
fn null_rows_references_reject_lost_fields_causes_settings_and_identities() -> TestResult {
    let (request, records) = parse_fixture(REQUEST, REFERENCE)?;
    for pointer in [
        "/schema_stages/public/fields/0/metadata",
        "/schema_stages/optimized",
        "/spark_error_details/parameters",
        "/spark_error_details/java_causes",
        "/spark_error_details/cause_chain_complete",
        "/spark_error_phase",
        "/effective_settings/spark.sql.ansi.enabled",
    ] {
        let mut changed = records.clone();
        *changed[1]
            .pointer_mut(pointer)
            .ok_or("missing fault target")? = Value::Null;
        assert!(
            validate_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let mut changed = records.clone();
    changed.pop();
    assert!(validate_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[2] = changed[1].clone();
    assert!(validate_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[0]["corpora"][0]["registered_inputs"][0]["schema_observed"] = Value::Null;
    assert!(validate_references(&request, &changed).is_err());
    let (request, records) = parse_fixture(STRING_PARTNER_REQUEST, STRING_PARTNER_REFERENCE)?;
    for (field, value) in [
        ("location", json!("upper")),
        ("exprType", json!("\"STRING\"")),
        ("expectedType", json!("\"BIGINT\"")),
    ] {
        let mut changed = records.clone();
        changed[1]["spark_error_details"]["parameters"][field] = value;
        assert!(
            validate_references(&request, &changed).is_err(),
            "accepted wrong NULL/STRING {field}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn non_null_lower_conversion_still_precedes_upper_evaluation() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_upper_bound",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))
        }),
    ));
    for ansi in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode: ansi,
            ..SparkSqlOptions::default()
        };
        let control = plan_query(&session,"SELECT COUNT(*) OVER (ORDER BY v ROWS BETWEEN CAST(1E100 AS DOUBLE) PRECEDING AND CURRENT ROW) FROM VALUES(1) inputs(v)",&options).await;
        let control = control
            .err()
            .ok_or("lower conversion unexpectedly succeeded")?
            .to_string();
        let result = plan_query(&session,"SELECT COUNT(*) OVER (ORDER BY v ROWS BETWEEN CAST(1E100 AS DOUBLE) PRECEDING AND observed_upper_bound() FOLLOWING) FROM VALUES(1) inputs(v)",&options).await;
        assert_eq!(
            result
                .err()
                .ok_or("lower conversion unexpectedly succeeded")?
                .to_string(),
            control
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_rows_sql_keeps_its_own_planner() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT v,COUNT(*) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n FROM VALUES(1),(2),(3) inputs(v)",
        "SELECT v,COUNT(*) OVER (ORDER BY v ROWS BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS n FROM VALUES(1),(2),(3) inputs(v)",
        "SELECT v,COUNT(*) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n FROM VALUES(1),(2),(3) inputs(v)",
    ] {
        assert_eq!(
            native.sql(sql).await?.collect().await?,
            configured.sql(sql).await?.collect().await?,
            "{sql}"
        );
    }
    Ok(())
}
