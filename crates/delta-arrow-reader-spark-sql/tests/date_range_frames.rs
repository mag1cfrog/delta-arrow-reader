//! Compare numeric DATE RANGE membership and complete Fields with independent Spark observations.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_native_query_observation, compare_exact_spark_query_stages_with_optimized,
};

const REFERENCE: &str = include_str!("fixtures/date-range-747-spark.jsonl");
const MANIFEST: &str = include_str!("fixtures/date-range-747-cases.json");
const STAGES: [&str; 5] = [
    "public",
    "analyzed",
    "optimized",
    "executed_before_collect",
    "executed",
];

fn case_identity(case: &Value) -> TestResult<(String, String, bool)> {
    Ok((
        case["corpus"].as_str().ok_or("missing corpus")?.into(),
        case["id"].as_str().ok_or("missing ID")?.into(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn validate_prior_owner_controls(records: &[Value], controls: &[Value]) -> TestResult {
    let mut expected = BTreeSet::new();
    for case in &records[1..] {
        if matches!(
            case["ownership"].as_str(),
            Some("null_offset_owner" | "date_interval_owner")
        ) {
            for batch in [1, 32] {
                expected.insert((case_identity(case)?, batch));
            }
        }
    }
    let mut seen = BTreeSet::new();
    for control in controls {
        let key = case_identity(control)?;
        let batch = control["batch"].as_u64().ok_or("missing prior batch")?;
        if !seen.insert((key.clone(), batch)) {
            return Err("duplicate prior owner identity/batch".into());
        }
        let reference = records[1..]
            .iter()
            .find(|case| case_identity(case).ok().as_ref() == Some(&key))
            .ok_or("missing prior owner reference")?;
        if control["reference"] != *reference
            || control["primary"] != false
            || control["replay_scope"] != "all"
            || control["target_partitions"] != 2
            || control["provider_partitions"] != 2
            || control["ownership"] != reference["ownership"]
            || !control["observation"].is_object()
            || control["observation"].get("observer_error").is_some()
        {
            return Err("prior owner reference or observation profile differs".into());
        }
    }
    if controls.len() != 112 || expected.len() != 112 || seen != expected {
        return Err("prior owner corpus/ANSI/batch inventory differs".into());
    }
    Ok(())
}

fn prior_owner_observation_matches(actual: &Value, control: &Value) -> TestResult<bool> {
    let mut actual = actual.clone();
    let mut control = control.clone();
    if actual["reference"]["ordered"] == false && control["reference"]["ordered"] == false {
        for record in [&mut actual, &mut control] {
            if record["observation"]["rows_raw"].is_array() {
                record["observation"]["rows_raw"] = multiset(&record["observation"]["rows_raw"])?;
            }
        }
    }
    Ok(actual == control)
}

fn parse_prior_native_controls() -> TestResult<Vec<Value>> {
    let controls: Vec<Value> = serde_json::from_str(include_str!(
        "fixtures/date-range-747-prior-native-controls.json"
    ))?;
    let expected = ["DATE", "TIMESTAMP", "INT"]
        .into_iter()
        .flat_map(|kind| [1, 32].map(move |batch| (kind.to_owned(), batch)))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for control in &controls {
        let key = (
            control["key_type"]
                .as_str()
                .ok_or("missing native key type")?
                .to_owned(),
            control["batch"].as_u64().ok_or("missing native batch")?,
        );
        if !seen.insert(key)
            || control["target_partitions"] != 2
            || !control["sql"].is_string()
            || ["native_observation", "configured_observation"]
                .iter()
                .any(|profile| {
                    let profile = &control[*profile];
                    profile["phase"] != "encode"
                        || profile["error"] != Value::Null
                        || !profile["rows"].is_array()
                        || ["public_schema", "before_collect", "physical_schema"]
                            .iter()
                            .any(|stage| !complete_schema(&profile[*stage]))
                })
        {
            return Err("prior native profile is missing or incomplete".into());
        }
    }
    if controls.len() != 6 || seen != expected {
        return Err("prior native type/batch inventory differs".into());
    }
    Ok(controls)
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

fn multiset(rows: &Value) -> TestResult<Value> {
    let mut rows = rows.as_array().ok_or("missing rows")?.clone();
    rows.sort_by_key(Value::to_string);
    Ok(json!(rows))
}

fn validate_date_range_references(records: &[Value], manifest: &Value) -> TestResult {
    if records.len() != 549 {
        return Err("expected exactly 548 Spark records and one header".into());
    }
    let header = &records[0];
    if header["spark_version"] != "4.2.0"
        || header["spark_build"]["SPARK_REVISION"]["value"]
            != "32f7299601108917fb01920a54e084595b7b3bf8"
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
        || header["effective_settings"]["spark.sql.shuffle.partitions"] != "1"
    {
        return Err("unexpected DATE RANGE Spark runtime or timezone".into());
    }
    let corpora = manifest["corpora"]
        .as_array()
        .ok_or("missing corpus definitions")?;
    let definitions = corpora
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().into_iter().flatten())
        .collect::<Vec<_>>();
    let expected = definitions
        .iter()
        .map(|case| case_identity(case))
        .collect::<TestResult<BTreeSet<_>>>()?;
    if definitions.len() != 548 || expected.len() != 548 || corpora.len() != 7 {
        return Err("malformed original DATE RANGE identity inventory".into());
    }
    let captured_corpora = header["corpora"]
        .as_array()
        .ok_or("missing captured corpus providers")?;
    if captured_corpora.len() != corpora.len() {
        return Err("captured corpus/provider inventory differs".into());
    }
    for corpus in corpora {
        let captured = captured_corpora
            .iter()
            .find(|item| item["corpus"] == corpus["corpus"])
            .ok_or("missing captured corpus")?;
        if captured["metadata"]["inputs"] != corpus["metadata"]["inputs"]
            || captured["source"]["sha256"] != corpus["source"]["sha256"]
        {
            return Err("original provider schema, rows or source provenance differs".into());
        }
        let inputs = corpus["metadata"]["inputs"]
            .as_array()
            .ok_or("missing provider definitions")?;
        let receipts = captured["registered_inputs"]
            .as_array()
            .ok_or("missing actual provider receipts")?;
        if receipts.len() != inputs.len() {
            return Err("actual Spark provider receipt count differs".into());
        }
        for (input, receipt) in inputs.iter().zip(receipts) {
            if ["name", "schema", "rows"]
                .iter()
                .any(|field| input[*field] != receipt[*field])
                || receipt["schema_observed"] != input["schema"]
                || !receipt["num_partitions"]
                    .as_u64()
                    .is_some_and(|count| count > 0)
            {
                return Err("actual Spark provider receipt differs from original inputs".into());
            }
        }
    }
    let mut seen = BTreeSet::new();
    let mut ownership_counts = BTreeMap::<String, usize>::new();
    let mut ordered_count = 0;
    for case in &records[1..] {
        let key = case_identity(case)?;
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate DATE RANGE identity: {key:?}").into());
        }
        let definition = definitions
            .iter()
            .find(|item| case_identity(item).ok().as_ref() == Some(&key))
            .ok_or("unexpected DATE RANGE identity")?;
        for field in ["sql", "ordered", "ownership"] {
            if case.get(field).is_none() || case[field] != definition[field] {
                return Err(format!("original DATE RANGE {field} differs: {key:?}").into());
            }
        }
        let owner = case["ownership"].as_str().ok_or("missing ownership")?;
        *ownership_counts.entry(owner.into()).or_default() += 1;
        ordered_count += usize::from(case["ordered"] == true);
        if !matches!(case["status"].as_str(), Some("ok" | "error")) {
            return Err(format!("incomplete Spark capture: {key:?}").into());
        }
        let settings = case["effective_settings"]
            .as_object()
            .ok_or("missing actual Spark settings")?;
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
            return Err(format!("effective Spark settings differ: {key:?}").into());
        }
        let schemas = case["schema_stages"]
            .as_object()
            .ok_or("missing schema stages")?;
        let availability = case["schema_stage_availability"]
            .as_object()
            .ok_or("missing stage availability")?;
        if availability.len() != STAGES.len()
            || schemas.keys().any(|key| !STAGES.contains(&key.as_str()))
        {
            return Err("unexpected DATE RANGE schema stage inventory".into());
        }
        for stage in STAGES {
            let state = &case["schema_stage_availability"][stage];
            match state["available"].as_bool() {
                Some(true) if complete_schema(&case["schema_stages"][stage]) => {}
                Some(false)
                    if case["status"] == "error"
                        && !schemas.contains_key(stage)
                        && state["reason"]
                            .as_str()
                            .is_some_and(|reason| !reason.is_empty()) => {}
                _ => return Err(format!("incomplete stage {stage}: {key:?}").into()),
            }
        }
        if case["status"] == "ok" {
            if schemas.len() != STAGES.len()
                || case["expected"]["schema"] != case["schema_stages"]["public"]
                || case["expected"].get("condition").is_some()
            {
                return Err(format!("incomplete successful Fields: {key:?}").into());
            }
            let width = case["schema_stages"]["public"]["fields"]
                .as_array()
                .ok_or("missing Fields")?
                .len();
            for field in ["rows_raw", "rows_multiset"] {
                let rows = case[field].as_array().ok_or("missing captured rows")?;
                if rows.iter().any(|row| {
                    !row.as_array().is_some_and(|row| {
                        row.len() == width
                            && row.iter().all(|cell| cell.is_null() || cell.is_string())
                    })
                }) {
                    return Err(format!("invalid row width or encoding: {key:?}").into());
                }
            }
            let expected_rows = if case["ordered"] == true {
                &case["rows_raw"]
            } else {
                &case["rows_multiset"]
            };
            if multiset(&case["rows_raw"])? != case["rows_multiset"]
                || case["expected"].get("rows") != Some(expected_rows)
            {
                return Err(format!("raw/ordered/multiset rows differ: {key:?}").into());
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
                || !details["cause_chain_complete"].is_boolean()
                || ["parameters", "sql_state", "python_class"]
                    .iter()
                    .any(|field| details.get(field).is_none())
            {
                return Err(format!("incomplete structured Spark failure: {key:?}").into());
            }
        }
    }
    if seen != expected
        || json!(ownership_counts) != manifest["ownership_counts"]
        || ordered_count != 24
    {
        return Err("missing DATE RANGE identity, owner or ordered consumer".into());
    }
    Ok(())
}

fn parse_date_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let manifest = serde_json::from_str::<Value>(MANIFEST)?;
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    validate_date_range_references(&records, &manifest)?;
    Ok((manifest, records))
}

#[test]
fn date_range_reference_rejects_missing_contracts() -> TestResult {
    let (manifest, records) = parse_date_range_fixture()?;
    for field in [
        "rows_raw",
        "rows_multiset",
        "schema_stages",
        "schema_stage_availability",
        "id",
        "sql",
    ] {
        let mut malformed = records.clone();
        malformed[1]
            .as_object_mut()
            .ok_or("missing object")?
            .remove(field);
        assert!(
            validate_date_range_references(&malformed, &manifest).is_err(),
            "accepted missing {field}"
        );
    }
    for pointer in [
        "/expected/rows",
        "/schema_stages/analyzed",
        "/schema_stages/public/fields/0/metadata",
    ] {
        let mut malformed = records.clone();
        *malformed[1]
            .pointer_mut(pointer)
            .ok_or("missing mutation target")? = Value::Null;
        assert!(
            validate_date_range_references(&malformed, &manifest).is_err(),
            "accepted malformed {pointer}"
        );
    }
    let mut duplicated = records.clone();
    duplicated[2] = duplicated[1].clone();
    assert!(validate_date_range_references(&duplicated, &manifest).is_err());
    let mut provider_change = manifest.clone();
    provider_change["corpora"][6]["metadata"]["inputs"][0]["schema"]["fields"]
        .as_array_mut()
        .ok_or("missing original provider Fields")?
        .pop();
    assert!(validate_date_range_references(&records, &provider_change).is_err());
    let ordered = records
        .iter()
        .position(|case| {
            case["ordered"] == true
                && case["rows_raw"]
                    .as_array()
                    .is_some_and(|rows| rows.len() > 1)
        })
        .ok_or("missing ordered consumer")?;
    let mut reversed = records.clone();
    reversed[ordered]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing ordered rows")?
        .reverse();
    assert!(validate_date_range_references(&reversed, &manifest).is_err());
    Ok(())
}

fn new_spark_session(batch_size: usize) -> SessionContext {
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

async fn replay_date_range_cases(original_consumers_only: bool) -> TestResult {
    let (manifest, records) = parse_date_range_fixture()?;
    let owner_controls: Vec<Value> = serde_json::from_str(include_str!(
        "fixtures/date-range-747-prior-owner-controls.json"
    ))?;
    validate_prior_owner_controls(&records, &owner_controls)?;
    let mut owner_differences = Vec::new();
    let mut owner_contexts = 0;
    let mut primary_contexts = 0;
    let mut completed = BTreeSet::new();
    let mut primary_differences = Vec::new();
    for corpus in manifest["corpora"].as_array().ok_or("missing corpora")? {
        for batch_size in [1, 32] {
            let session = new_spark_session(batch_size);
            spark_inputs::register_recorded_inputs(&session, &corpus["metadata"], batch_size)?;
            for case in records[1..]
                .iter()
                .filter(|case| case["corpus"] == corpus["corpus"])
            {
                if original_consumers_only
                    && case["ownership"] != "original_five_column_primary"
                    && case["corpus"] != "date-range-consumers-spark"
                {
                    continue;
                }
                let (source, id, ansi) = case_identity(case)?;
                if !completed.insert((source.clone(), id.clone(), ansi, batch_size)) {
                    return Err("duplicate native DATE RANGE context".into());
                }
                let result = compare_exact_spark_query_stages_with_optimized(&session, case).await;
                let matches = result.as_ref().is_ok_and(|actual| {
                    actual["matches"] == true
                        && actual["optimized_matches"] == true
                        && case["schema_stages"]
                            .get("public")
                            .is_none_or(|public| actual["public_schema"] == *public)
                        && (case["status"] == "error"
                            || actual["public_schema"] == case["expected"]["schema"])
                });
                let actual =
                    result.unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
                let independent_owner = matches!(
                    case["ownership"].as_str(),
                    Some("null_offset_owner" | "date_interval_owner")
                );
                let observation = json!({
                    "corpus":source,"id":id,"ansi":ansi,"batch":batch_size,
                    "target_partitions":2,"provider_partitions":2,
                    "replay_scope":if original_consumers_only { "original_consumers" } else { "all" },
                    "ownership":case["ownership"],"reference":case,"observation":actual,
                    "spark_matches":matches,"primary":!independent_owner,
                });
                eprintln!("DATE_RANGE_OBSERVATION {observation}");
                if independent_owner {
                    owner_contexts += 1;
                    let control = owner_controls
                        .iter()
                        .find(|control| {
                            control["corpus"] == case["corpus"]
                                && control["id"] == case["id"]
                                && control["ansi"] == case["ansi"]
                                && control["batch"] == batch_size
                        })
                        .ok_or("missing prior owner context")?;
                    if !prior_owner_observation_matches(&observation, control)? {
                        owner_differences
                            .push(format!("{source}:{id} ANSI={ansi} batch={batch_size}"));
                    }
                }
                if !independent_owner && !matches {
                    primary_differences
                        .push(format!("{source}:{id} ANSI={ansi} batch={batch_size}"));
                }
                primary_contexts += usize::from(!independent_owner);
            }
        }
    }
    let expected_contexts = if original_consumers_only { 108 } else { 1096 };
    assert_eq!(completed.len(), expected_contexts);
    assert_eq!(
        primary_contexts,
        if original_consumers_only { 108 } else { 984 }
    );
    assert_eq!(
        owner_contexts,
        if original_consumers_only { 0 } else { 112 }
    );
    assert!(
        primary_differences.is_empty() && owner_differences.is_empty(),
        "primary differences:\n{}\nprior owner changes:\n{}",
        primary_differences.join("\n"),
        owner_differences.join("\n"),
    );
    Ok(())
}

#[tokio::test]
async fn date_finite_numeric_range_frames_match_spark() -> TestResult {
    replay_date_range_cases(false).await
}

#[tokio::test]
async fn date_range_original_consumers_match_spark() -> TestResult {
    replay_date_range_cases(true).await
}

#[tokio::test]
async fn native_range_controls_match_unconfigured_session() -> TestResult {
    let controls = parse_prior_native_controls()?;
    for batch_size in [1, 32] {
        let native = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
        let configured = new_spark_session(batch_size);
        for (key_type, values, offset) in [
            (
                "DATE",
                "(DATE '2020-01-01'),(DATE '2020-01-02'),(DATE '2020-01-04')",
                "INTERVAL '1 day'",
            ),
            (
                "TIMESTAMP",
                "(TIMESTAMP '2020-01-01'),(TIMESTAMP '2020-01-02'),(TIMESTAMP '2020-01-04')",
                "INTERVAL '1 day'",
            ),
            ("INT", "(1),(2),(4)", "1"),
        ] {
            let sql = format!(
                "SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN {offset} PRECEDING AND CURRENT ROW) AS n FROM (VALUES {values}) inputs(k) ORDER BY k"
            );
            let native_observation = capture_native_query_observation(&native, &sql).await?;
            let configured_observation =
                capture_native_query_observation(&configured, &sql).await?;
            let observation = json!({
                "key_type":key_type,"batch":batch_size,"target_partitions":2,"sql":sql,
                "native_observation":native_observation,"configured_observation":configured_observation,
            });
            eprintln!("NATIVE_DATE_RANGE_CONTROL {observation}");
            let control = controls
                .iter()
                .find(|control| control["key_type"] == key_type && control["batch"] == batch_size)
                .ok_or("missing prior native context")?;
            assert_eq!(
                &observation, control,
                "native prior profile {key_type} batch={batch_size}"
            );
            assert_eq!(
                native_observation, configured_observation,
                "{key_type} batch={batch_size}"
            );
            assert_eq!(
                configured_observation["error"],
                Value::Null,
                "{configured_observation}"
            );
            assert_eq!(configured_observation["rows"], json!([["1"], ["1"], ["2"]]));
            assert!(complete_schema(&configured_observation["public_schema"]));
            for context in [&native, &configured] {
                let batches = context.sql(&sql).await?.collect().await?;
                let counts = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_primitive::<Int64Type>()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    counts,
                    [1, 2, 1],
                    "native ordered control {key_type} batch={batch_size}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn prior_owner_controls_reject_changes_and_only_relax_unordered_raw_rows() -> TestResult {
    let (_, records) = parse_date_range_fixture()?;
    let controls: Vec<Value> = serde_json::from_str(include_str!(
        "fixtures/date-range-747-prior-owner-controls.json"
    ))?;
    validate_prior_owner_controls(&records, &controls)?;
    let mut missing = controls.clone();
    missing.pop();
    assert!(validate_prior_owner_controls(&records, &missing).is_err());
    let mut duplicate = controls.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(validate_prior_owner_controls(&records, &duplicate).is_err());
    let control = controls
        .iter()
        .find(|control| {
            control["observation"]["rows_raw"]
                .as_array()
                .is_some_and(|rows| rows.len() > 2)
        })
        .ok_or("missing unordered prior rows")?;
    let mut reordered = control.clone();
    reordered["observation"]["rows_raw"]
        .as_array_mut()
        .ok_or("missing raw rows")?
        .reverse();
    assert!(prior_owner_observation_matches(&reordered, control)?);
    for pointer in [
        "/observation/public_schema",
        "/observation/optimized_schema",
        "/observation/before_collect",
        "/observation/physical_schema",
        "/observation/error",
        "/observation/native_phase",
        "/reference/sql",
    ] {
        let mut changed = control.clone();
        *changed.pointer_mut(pointer).ok_or("missing fault target")? = json!("changed prior facet");
        assert!(
            !prior_owner_observation_matches(&changed, control)?,
            "accepted changed {pointer}"
        );
    }
    let mut duplicate_row = control.clone();
    let row = duplicate_row["observation"]["rows_raw"][0].clone();
    duplicate_row["observation"]["rows_raw"]
        .as_array_mut()
        .ok_or("missing raw rows")?
        .push(row);
    assert!(!prior_owner_observation_matches(&duplicate_row, control)?);
    let mut changed_reference = control.clone();
    changed_reference["reference"]["rows_raw"]
        .as_array_mut()
        .ok_or("missing Spark raw rows")?
        .reverse();
    assert!(!prior_owner_observation_matches(
        &changed_reference,
        control
    )?);
    let mut ordered = control.clone();
    ordered["reference"]["ordered"] = json!(true);
    let mut reordered = ordered.clone();
    reordered["observation"]["rows_raw"]
        .as_array_mut()
        .ok_or("missing raw rows")?
        .reverse();
    assert!(!prior_owner_observation_matches(&reordered, &ordered)?);
    Ok(())
}
