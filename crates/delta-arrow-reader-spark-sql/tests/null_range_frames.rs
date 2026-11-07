//! Compare explicit NULL RANGE rows, complete Fields and failures with independent Spark records.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, compare_exact_spark_query_stages_with_optimized};

const REQUEST: &str = include_str!("fixtures/null-range-754-cases.json");
const REFERENCE: &str = include_str!("fixtures/null-range-754-spark.jsonl");
const ORIGINAL_REQUEST: &str = include_str!("fixtures/date-range-747-cases.json");
const ORIGINAL_REFERENCE: &str = include_str!("fixtures/date-range-747-spark.jsonl");
const STAGES: [(&str, &str); 5] = [
    ("public", "public_schema"),
    ("analyzed", "public_schema"),
    ("optimized", "optimized_schema"),
    ("executed_before_collect", "before_collect"),
    ("executed", "physical_schema"),
];

fn case_identity(case: &Value) -> TestResult<(String, String, bool)> {
    Ok((
        case["corpus"].as_str().ok_or("missing corpus")?.into(),
        case["id"].as_str().ok_or("missing ID")?.into(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn multiset(rows: &Value) -> TestResult<Value> {
    let mut rows = rows.as_array().ok_or("missing rows")?.clone();
    rows.sort_by_key(Value::to_string);
    Ok(json!(rows))
}

fn complete_schema(schema: &Value) -> bool {
    schema["type"] == "struct"
        && schema["fields"].as_array().is_some_and(|fields| {
            !fields.is_empty()
                && fields.iter().all(|field| {
                    field["name"].is_string()
                        && field["type"].is_string()
                        && field["nullable"].is_boolean()
                        && field["metadata"].is_object()
                })
        })
}

fn validate_null_range_references(request: &Value, records: &[Value]) -> TestResult {
    if request["issue"] != 754
        || request["expected_record_count"] != 582
        || request["original_record_count"] != 14
        || request["supplement_record_count"] != 568
        || request["batch_sizes"] != json!([1, 32])
        || request["target_partitions"] != 2
        || request["provider_partitions"] != 2
        || records.len() != 583
    {
        return Err("NULL RANGE source or reference inventory differs".into());
    }
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    let original: Value = serde_json::from_str(ORIGINAL_REQUEST)?;
    let mut original_corpora = Vec::new();
    for corpus in original["corpora"]
        .as_array()
        .ok_or("missing original corpora")?
    {
        let cases = corpus["cases"]
            .as_array()
            .ok_or("missing original cases")?
            .iter()
            .filter(|case| case["ownership"] == "null_offset_owner")
            .cloned()
            .collect::<Vec<_>>();
        if !cases.is_empty() {
            let mut selected = corpus.clone();
            selected["cases"] = json!(cases);
            original_corpora.push(selected);
        }
    }
    if corpora.len() != 3
        || &corpora[..2] != original_corpora.as_slice()
        || corpora[2]["corpus"] != "null-range-754-supplement"
        || corpora[2]["cases"]
            .as_array()
            .is_none_or(|cases| cases.len() != 568)
    {
        return Err("original NULL RANGE SQL, identities or providers changed".into());
    }
    let mut definitions = BTreeMap::new();
    for corpus in corpora {
        for case in corpus["cases"].as_array().ok_or("missing source cases")? {
            if case["corpus"] != corpus["corpus"]
                || !case["ordered"].is_boolean()
                || !case["sql"].is_string()
                || case["ownership"] != "null_offset_owner"
                || definitions.insert(case_identity(case)?, case).is_some()
            {
                return Err("malformed or duplicate NULL RANGE source identity".into());
            }
        }
    }
    if definitions.len() != 582
        || definitions
            .values()
            .filter(|case| case["ordered"] == true)
            .count()
            != 24
    {
        return Err("NULL RANGE source identity or ordering inventory differs".into());
    }
    let header = &records[0];
    if header["issue"] != 754
        || header["spark_version"] != "4.2.0"
        || header["spark_build"]["SPARK_REVISION"]["value"]
            != "32f7299601108917fb01920a54e084595b7b3bf8"
        || header["expected_record_count"] != 582
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
        || header["effective_settings"]["spark.sql.shuffle.partitions"] != "1"
    {
        return Err("unexpected NULL RANGE Spark reference runtime".into());
    }
    let captured = header["corpora"]
        .as_array()
        .ok_or("missing provider receipts")?;
    if captured.len() != corpora.len() {
        return Err("NULL RANGE captured provider inventory differs".into());
    }
    for corpus in corpora {
        let receipt = captured
            .iter()
            .find(|item| item["corpus"] == corpus["corpus"])
            .ok_or("missing captured corpus")?;
        let inputs = corpus["metadata"]["inputs"]
            .as_array()
            .ok_or("missing inputs")?;
        let providers = receipt["registered_inputs"]
            .as_array()
            .ok_or("missing actual providers")?;
        if receipt["metadata"] != corpus["metadata"]
            || receipt["source"] != corpus["source"]
            || providers.len() != inputs.len()
        {
            return Err("NULL RANGE captured providers differ from source".into());
        }
        for (input, provider) in inputs.iter().zip(providers) {
            if ["name", "schema", "rows"]
                .iter()
                .any(|key| input[*key] != provider[*key])
                || provider["schema_observed"] != input["schema"]
                || provider["num_partitions"]
                    .as_u64()
                    .is_none_or(|count| count == 0)
            {
                return Err("NULL RANGE actual provider receipt differs".into());
            }
        }
    }
    let original_records = ORIGINAL_REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let originals = original_records
        .iter()
        .filter(|case| case["ownership"] == "null_offset_owner")
        .map(|case| Ok((case_identity(case)?, case)))
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    let mut seen = BTreeSet::new();
    let mut original_count = 0;
    for case in &records[1..] {
        let key = case_identity(case)?;
        let definition = definitions
            .get(&key)
            .ok_or("unexpected NULL RANGE reference")?;
        if !seen.insert(key.clone())
            || ["sql", "ordered", "ownership", "coverage"]
                .iter()
                .any(|field| case.get(*field) != definition.get(*field))
            || !matches!(case["status"].as_str(), Some("ok" | "error"))
            || case.get("capture_failure").is_some()
        {
            return Err("NULL RANGE reference identity or source contract differs".into());
        }
        let settings = case["effective_settings"]
            .as_object()
            .ok_or("missing actual settings")?;
        let header_settings = header["effective_settings"]
            .as_object()
            .ok_or("missing header settings")?;
        if settings.len() != 8
            || header_settings.len() != 8
            || settings.iter().any(|(name, value)| {
                if name == "spark.sql.ansi.enabled" {
                    value.as_str() != Some(if key.2 { "true" } else { "false" })
                } else {
                    header_settings.get(name) != Some(value)
                }
            })
        {
            return Err("NULL RANGE actual settings differ from source ANSI/runtime".into());
        }
        if let Some(original) = originals.get(&key) {
            original_count += 1;
            if [
                "status",
                "expected",
                "schema_stages",
                "schema_stage_availability",
                "rows_multiset",
            ]
            .iter()
            .any(|field| case.get(*field) != original.get(*field))
            {
                return Err("original independent NULL RANGE outcome changed".into());
            }
        }
        let schemas = case["schema_stages"]
            .as_object()
            .ok_or("missing schema stages")?;
        let availability = case["schema_stage_availability"]
            .as_object()
            .ok_or("missing schema availability")?;
        if availability.len() != STAGES.len()
            || schemas
                .keys()
                .any(|stage| !STAGES.iter().any(|(name, _)| *name == stage.as_str()))
        {
            return Err("NULL RANGE schema stage inventory differs".into());
        }
        for (stage, _) in STAGES {
            let state = &case["schema_stage_availability"][stage];
            match state["available"].as_bool() {
                Some(true) if complete_schema(&case["schema_stages"][stage]) => {}
                Some(false)
                    if case["status"] == "error"
                        && !schemas.contains_key(stage)
                        && state["reason"]
                            .as_str()
                            .is_some_and(|reason| !reason.is_empty()) => {}
                _ => return Err(format!("incomplete NULL RANGE stage {stage}: {key:?}").into()),
            }
        }
        if case["status"] == "ok" {
            let width = case["schema_stages"]["public"]["fields"]
                .as_array()
                .ok_or("missing public Fields")?
                .len();
            if schemas.len() != STAGES.len()
                || case["expected"]["schema"] != case["schema_stages"]["public"]
                || case["expected"].get("condition").is_some()
                || ["rows_raw", "rows_multiset"].iter().any(|field| {
                    !case[*field].as_array().is_some_and(|rows| {
                        rows.iter().all(|row| {
                            row.as_array().is_some_and(|row| {
                                row.len() == width
                                    && row.iter().all(|value| value.is_null() || value.is_string())
                            })
                        })
                    })
                })
                || multiset(&case["rows_raw"])? != case["rows_multiset"]
                || case["expected"].get("rows")
                    != Some(if case["ordered"] == true {
                        &case["rows_raw"]
                    } else {
                        &case["rows_multiset"]
                    })
            {
                return Err(format!("incomplete NULL RANGE success or rows: {key:?}").into());
            }
        } else {
            let details = &case["spark_error_details"];
            if !case["expected"]["condition"].is_string()
                || case["expected"].get("rows").is_some()
                || !case["spark_error_phase"].is_string()
                || details["condition"] != case["expected"]["condition"]
                || details["phase"] != case["spark_error_phase"]
                || !details["message"].is_string()
                || !details["java_causes"].is_array()
                || details["cause_chain_complete"] != true
                || ["parameters", "sql_state", "python_class"]
                    .iter()
                    .any(|field| details.get(*field).is_none())
            {
                return Err(format!("incomplete NULL RANGE failure: {key:?}").into());
            }
        }
    }
    if seen.len() != definitions.len() || originals.len() != 14 || original_count != 14 {
        return Err("missing original or supplementary NULL RANGE reference identity".into());
    }
    Ok(())
}

fn parse_null_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(REQUEST)?;
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    validate_null_range_references(&request, &records)?;
    Ok((request, records))
}

#[tokio::test]
async fn explicit_null_range_frames_match_spark() -> TestResult {
    let (request, records) = parse_null_range_fixture()?;
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
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
                let identity = (case_identity(case)?, batch);
                let observation = match &provider_error {
                    Some(error) => {
                        json!({"observer_error":error,"observer_phase":"register_inputs"})
                    }
                    None => compare_exact_spark_query_stages_with_optimized(&session, case)
                        .await
                        .unwrap_or_else(|error| json!({"observer_error":error.to_string()})),
                };
                let matches = observation["matches"] == true
                    && observation["optimized_matches"] == true
                    && STAGES.iter().all(|(stage, observed)| {
                        match case["schema_stages"].get(*stage) {
                            Some(expected) => observation.get(*observed) == Some(expected),
                            None => observation.get(*observed).is_some_and(Value::is_null),
                        }
                    });
                // Validated source identities cannot abort observation collection on a query difference.
                let unique = completed.insert(identity);
                if !matches || !unique {
                    differences.push(format!(
                        "{}:{} ANSI={} batch={batch}",
                        case["corpus"], case["id"], case["ansi"]
                    ));
                }
                let record = json!({"issue":754,"corpus":case["corpus"],"id":case["id"],
                    "ansi":case["ansi"],"batch":batch,"target_partitions":2,"provider_partitions":2,
                    "source_ownership_tag":case["ownership"],"primary":true,
                    "reference":case,"observation":observation,"spark_matches":matches});
                eprintln!("NULL_RANGE_OBSERVATION {record}");
            }
        }
    }
    eprintln!(
        "NULL_RANGE_SUMMARY {}",
        json!({"observations":completed.len(),
        "differences":differences.len(),"expected_observations":1164})
    );
    assert_eq!(completed.len(), 1164);
    assert!(
        differences.is_empty(),
        "{} NULL RANGE differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn null_range_references_reject_contract_loss() -> TestResult {
    let (request, records) = parse_null_range_fixture()?;
    let success = records
        .iter()
        .position(|case| case["corpus"] == "null-range-754-supplement" && case["status"] == "ok")
        .ok_or("missing successful supplemental reference")?;
    for field in [
        "id",
        "schema_stages",
        "schema_stage_availability",
        "rows_raw",
    ] {
        let mut changed = records.clone();
        changed[success]
            .as_object_mut()
            .ok_or("missing record")?
            .remove(field);
        assert!(
            validate_null_range_references(&request, &changed).is_err(),
            "accepted missing {field}"
        );
    }
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_null_range_references(&request, &missing).is_err());
    let mut duplicate = records.clone();
    duplicate[2] = duplicate[1].clone();
    assert!(validate_null_range_references(&request, &duplicate).is_err());
    for pointer in [
        "/schema_stages/public/fields/0/metadata",
        "/schema_stages/optimized",
        "/expected/rows",
    ] {
        let mut changed = records.clone();
        *changed[success]
            .pointer_mut(pointer)
            .ok_or("missing schema/row fault target")? = Value::Null;
        assert!(
            validate_null_range_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let ordered = records
        .iter()
        .position(|case| {
            case["ordered"] == true
                && case["rows_raw"]
                    .as_array()
                    .is_some_and(|rows| rows.len() > 1)
        })
        .ok_or("missing ordered reference")?;
    let mut changed = records.clone();
    changed[ordered]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing ordered rows")?
        .reverse();
    assert!(validate_null_range_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[ordered]["ordered"] = json!(false);
    assert!(validate_null_range_references(&request, &changed).is_err());
    let mut changed_source = request.clone();
    changed_source["corpora"][0]["cases"][0]["sql"] = json!("changed original SQL");
    assert!(validate_null_range_references(&changed_source, &records).is_err());
    let mut changed = records.clone();
    changed[0]["corpora"][0]["registered_inputs"][0]["schema_observed"] = Value::Null;
    assert!(validate_null_range_references(&request, &changed).is_err());
    Ok(())
}
