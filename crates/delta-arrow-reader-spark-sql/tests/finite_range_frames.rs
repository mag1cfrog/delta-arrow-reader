//! Compare signed-integer RANGE frames with independently captured Spark observations.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    STAGES, TestResult, case_identity, compare_exact_spark_query_stages_with_optimized,
    native_result_matches_spark_reference, validate_spark_reference_result,
};

const REQUEST: &str = include_str!("fixtures/finite-range-1086-cases.json");
const REFERENCE: &str = include_str!("fixtures/finite-range-1086-spark.jsonl");
const LIMITED_REQUEST: &str = include_str!("fixtures/finite-range-1086-limited-cases.json");
const LIMITED_REFERENCE: &str = include_str!("fixtures/finite-range-1086-limited-spark.jsonl");
const LIMITED_CORPUS: &str = "finite-range-1086-limited-output-diagnostics";
const NARROWING_REQUEST: &str = include_str!("fixtures/finite-range-1086-narrowing-cases.json");
const NARROWING_REFERENCE: &str = include_str!("fixtures/finite-range-1086-narrowing-spark.jsonl");
const ZERO_CURRENT_REQUEST: &str =
    include_str!("fixtures/finite-range-1086-zero-current-cases.json");
const ZERO_CURRENT_REFERENCE: &str =
    include_str!("fixtures/finite-range-1086-zero-current-spark.jsonl");

fn validate_finite_range_references(request: &Value, records: &[Value]) -> TestResult {
    if request["issue"] != 1086
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || request["original_record_count"] != 16
        || request["supplement_record_count"] != 172
        || request["expected_record_count"] != 188
        || request["original_source_request"]["sha256"]
            != "b565f897d99c64c8363bc0f33950a7435f392f2495847cbe09565f74cd6fb2c1"
        || request["original_independent_spark_capture"]["sha256"]
            != "01c7a336fcf75597ea255397d9938d5eb19d2583adbfcef4c87bf1502e892f2c"
        || records.len() != 189
    {
        return Err("finite RANGE source or reference inventory differs".into());
    }
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    if corpora.len() != 2
        || corpora[0]["corpus"] != "finite-numeric-range-754-controls"
        || corpora[0]["cases"]
            .as_array()
            .is_none_or(|cases| cases.len() != 16)
        || corpora[1]["corpus"] != "finite-range-1086-supplement"
        || corpora[1]["cases"]
            .as_array()
            .is_none_or(|cases| cases.len() != 172)
    {
        return Err("finite RANGE corpus inventory differs".into());
    }
    let mut definitions = BTreeMap::new();
    let mut families = BTreeMap::<String, usize>::new();
    let mut modes = BTreeMap::<(String, String), BTreeSet<bool>>::new();
    for corpus in corpora {
        for case in corpus["cases"].as_array().ok_or("missing source cases")? {
            let key = case_identity(case)?;
            if case["corpus"] != corpus["corpus"]
                || !case["sql"].is_string()
                || !case["ordered"].is_boolean()
                || ["expected", "status", "rows_raw", "spark_error_details"]
                    .iter()
                    .any(|field| case.get(*field).is_some())
                || definitions.insert(key.clone(), case).is_some()
            {
                return Err("malformed or duplicate finite RANGE source identity".into());
            }
            modes.entry((key.0, key.1)).or_default().insert(key.2);
            if corpus["corpus"] == "finite-range-1086-supplement" {
                let family = case["coverage"]["family"]
                    .as_str()
                    .ok_or("missing finite RANGE family")?;
                *families.entry(family.into()).or_default() += 1;
                if case["ownership"]
                    != if family == "cross_width_null_controls" {
                        "null_offset_control"
                    } else {
                        "finite_signed_integer_owner"
                    }
                {
                    return Err("finite RANGE source ownership differs".into());
                }
            }
        }
    }
    let expected_families = json!({
        "same_width_stages":36,"all_null_min":16,"empty_desc_min":8,
        "key_edge_arithmetic":32,"regular_signs_max":32,"special_coercion":32,
        "consumers":8,"cross_width_null_controls":8,
    });
    if definitions.len() != 188
        || modes.len() != 94
        || modes
            .values()
            .any(|ansi| *ansi != BTreeSet::from([false, true]))
        || json!(families) != expected_families
        || request["supplement_family_counts"] != expected_families
    {
        return Err("finite RANGE source family or ANSI inventory differs".into());
    }
    validate_range_capture(request, records, &definitions)
}

fn validate_range_capture(
    request: &Value,
    records: &[Value],
    definitions: &BTreeMap<(String, String, bool), &Value>,
) -> TestResult {
    let corpora = request["corpora"].as_array().ok_or("missing corpora")?;
    for ((corpus, id, ansi), source) in definitions {
        let partner = definitions
            .get(&(corpus.clone(), id.clone(), !*ansi))
            .ok_or("missing ANSI source partner")?;
        if ["sql", "ordered", "ownership", "coverage"]
            .iter()
            .any(|field| source.get(*field) != partner.get(*field))
        {
            return Err("finite RANGE ANSI source partners differ".into());
        }
    }
    let header = &records[0];
    if header["issue"] != 1086
        || header["spark_version"] != request["spark_version"]
        || header["spark_build"]["SPARK_REVISION"]["value"] != request["spark_revision"]
        || header["expected_record_count"] != request["expected_record_count"]
        || header["spark_only"] != true
        || header["reads_expected_outputs"] != false
        || header["schema_stage_protocol"] != json!(STAGES.map(|(stage, _)| stage))
        || header["effective_settings"]["spark.sql.session.timeZone"] != "UTC"
        || header["effective_settings"]["spark.sql.shuffle.partitions"] != "1"
    {
        return Err("unexpected finite RANGE Spark reference runtime".into());
    }
    let captured = header["corpora"]
        .as_array()
        .ok_or("missing provider receipts")?;
    if captured.len() != corpora.len() {
        return Err("finite RANGE captured corpus inventory differs".into());
    }
    for corpus in corpora {
        let receipt = captured
            .iter()
            .find(|receipt| receipt["corpus"] == corpus["corpus"])
            .ok_or("missing captured corpus")?;
        let inputs = corpus["metadata"]["inputs"]
            .as_array()
            .ok_or("missing inputs")?;
        let providers = receipt["registered_inputs"]
            .as_array()
            .ok_or("missing providers")?;
        if receipt["source"] != corpus["source"]
            || receipt["metadata"] != corpus["metadata"]
            || providers.len() != inputs.len()
        {
            return Err("finite RANGE captured providers differ from source".into());
        }
        for (input, provider) in inputs.iter().zip(providers) {
            if ["name", "schema", "rows"]
                .iter()
                .any(|field| provider[*field] != input[*field])
                || provider["schema_observed"] != input["schema"]
                || provider["num_partitions"]
                    .as_u64()
                    .is_none_or(|count| count == 0)
            {
                return Err("finite RANGE provider receipt differs".into());
            }
        }
    }
    let settings = header["effective_settings"]
        .as_object()
        .ok_or("missing header settings")?;
    let mut seen = BTreeSet::new();
    for case in &records[1..] {
        let key = case_identity(case)?;
        let source = definitions
            .get(&key)
            .ok_or("unexpected finite RANGE reference identity")?;
        if !seen.insert(key.clone())
            || !matches!(case["status"].as_str(), Some("ok" | "error"))
            || case.get("capture_failure").is_some()
            || ["sql", "ordered", "ownership", "coverage"]
                .iter()
                .any(|field| case.get(*field) != source.get(*field))
        {
            return Err("finite RANGE reference differs from source or capture failed".into());
        }
        let actual_settings = case["effective_settings"]
            .as_object()
            .ok_or("missing case settings")?;
        if settings.len() != 8
            || actual_settings.len() != 8
            || actual_settings.iter().any(|(name, value)| {
                if name == "spark.sql.ansi.enabled" {
                    value.as_str() != Some(if key.2 { "true" } else { "false" })
                } else {
                    settings.get(name) != Some(value)
                }
            })
        {
            return Err("finite RANGE actual settings differ from source ANSI/runtime".into());
        }
        validate_spark_reference_result(case)?;
    }
    if seen != definitions.keys().cloned().collect() {
        return Err("missing finite RANGE reference identity".into());
    }
    Ok(())
}

fn validate_limited_range_references(request: &Value, records: &[Value]) -> TestResult {
    let corpora = request["corpora"]
        .as_array()
        .ok_or("missing limited corpora")?;
    if request["issue"] != 1086
        || request["expected_record_count"] != 8
        || request["sql_source_count"] != 4
        || corpora.len() != 1
        || records.len() != 9
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || request["frozen_primary_request"]["sha256"]
            != "e4d0fc47801a667224180372684a1f6c0533b80d0a9ac054b20f8f1e230480ec"
        || corpora[0]["corpus"] != LIMITED_CORPUS
        || corpora[0]["metadata"] != json!({"spark_version":"4.2.0","timezone":"UTC","inputs":[]})
    {
        return Err("limited finite RANGE inventory differs".into());
    }
    let limits = BTreeMap::from([
        ("limited_a_null_intmax", 1),
        ("limited_b_zero_intmax", 1),
        ("limited_c_null_intmin_desc", 1),
        ("limited_d_upper_min_preceding_limit_zero", 0),
    ]);
    let mut definitions = BTreeMap::new();
    for case in corpora[0]["cases"]
        .as_array()
        .ok_or("missing limited cases")?
    {
        let key = case_identity(case)?;
        let limit = limits.get(key.1.as_str()).ok_or("unexpected limited ID")?;
        let coverage = &case["coverage"];
        let order = coverage["window_order"]
            .as_str()
            .ok_or("missing window order")?;
        let frame = coverage["frame"].as_str().ok_or("missing limited frame")?;
        let sql = case["sql"].as_str().ok_or("missing limited SQL")?;
        if case["corpus"] != corpora[0]["corpus"]
            || case["ordered"] != true
            || case["ownership"] != "finite_signed_integer_diagnostic"
            || coverage["family"] != "limited_output"
            || coverage["global_window"] != true
            || coverage["key_type"] != "INT"
            || coverage["outer_order"] != coverage["window_order"]
            || coverage["limit"] != *limit
            || !sql.contains(&format!("OVER (ORDER BY {order} {frame})"))
            || !sql.ends_with(&format!("ORDER BY {order} LIMIT {limit}"))
            || ["expected", "status", "rows_raw", "spark_error_details"]
                .iter()
                .any(|field| case.get(*field).is_some())
            || definitions.insert(key, case).is_some()
        {
            return Err("limited finite RANGE source clauses differ".into());
        }
    }
    if definitions.len() != 8
        || limits.keys().any(|id| {
            [true, false].iter().any(|ansi| {
                !definitions.contains_key(&(LIMITED_CORPUS.into(), (*id).into(), *ansi))
            })
        })
    {
        return Err("limited finite RANGE ANSI inventory differs".into());
    }
    validate_range_capture(request, records, &definitions)
}

fn parse_finite_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(REQUEST)?;
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    validate_finite_range_references(&request, &records)?;
    Ok((request, records))
}

fn parse_narrowing_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request: Value = serde_json::from_str(NARROWING_REQUEST)?;
    let records = NARROWING_REFERENCE
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    validate_narrowing_range_references(&request, &records)?;
    Ok((request, records))
}

fn validate_narrowing_range_references(request: &Value, records: &[Value]) -> TestResult {
    let corpora = request["corpora"]
        .as_array()
        .ok_or("missing narrowing corpora")?;
    if request["issue"] != 1086
        || request["expected_record_count"] != 24
        || request["sql_source_count"] != 12
        || corpora.len() != 1
        || records.len() != 25
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || corpora[0]["corpus"] != "finite-range-1086-narrowing-controls"
        || corpora[0]["metadata"] != json!({"spark_version":"4.2.0","timezone":"UTC","inputs":[]})
    {
        return Err("narrowing finite RANGE inventory differs".into());
    }
    let pairs = BTreeMap::from([
        (("TINYINT", "SMALLINT"), "128"),
        (("TINYINT", "INT"), "128"),
        (("TINYINT", "BIGINT"), "128"),
        (("SMALLINT", "INT"), "32768"),
        (("SMALLINT", "BIGINT"), "32768"),
        (("INT", "BIGINT"), "2147483648"),
    ]);
    let mut contexts = BTreeSet::new();
    let mut definitions = BTreeMap::new();
    for case in corpora[0]["cases"]
        .as_array()
        .ok_or("missing narrowing cases")?
    {
        let identity = case_identity(case)?;
        let coverage = &case["coverage"];
        let key_type = coverage["key_type"]
            .as_str()
            .ok_or("missing narrowing key type")?;
        let source_type = coverage["source_offset_type"]
            .as_str()
            .ok_or("missing narrowing source type")?;
        let offset = pairs
            .get(&(key_type, source_type))
            .ok_or("unexpected narrowing pair")?;
        let shape = coverage["shape"]
            .as_str()
            .ok_or("missing narrowing shape")?;
        let lower = match shape {
            "unbounded" => "UNBOUNDED PRECEDING".into(),
            "zero_following" => format!("CAST(0 AS {source_type}) FOLLOWING"),
            _ => return Err("unexpected narrowing shape".into()),
        };
        let frame = format!("RANGE BETWEEN {lower} AND CAST({offset} AS {source_type}) FOLLOWING");
        let values = format!(
            "(0,CAST(-1 AS {key_type})),(1,CAST(0 AS {key_type})),(2,CAST(1 AS {key_type}))"
        );
        let sql = format!(
            "SELECT id,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST {frame}) AS n FROM VALUES {values} AS t(id,k) ORDER BY id"
        );
        if case["corpus"] != corpora[0]["corpus"]
            || case["ordered"] != true
            || case["ownership"] != "finite_signed_integer_narrowing_control"
            || coverage["family"] != "narrowing_coercion"
            || coverage["positive_offset"] != *offset
            || coverage["order"] != "ASC NULLS FIRST"
            || coverage["outer_order"] != "id"
            || coverage["frame"] != frame
            || case["sql"] != sql
            || ["expected", "status", "rows_raw", "spark_error_details"]
                .iter()
                .any(|field| case.get(*field).is_some())
            || !contexts.insert((key_type, source_type, shape, identity.2))
            || definitions.insert(identity, case).is_some()
        {
            return Err("narrowing finite RANGE source clauses differ".into());
        }
    }
    if contexts.len() != 24 || definitions.len() != 24 {
        return Err("narrowing finite RANGE source context inventory differs".into());
    }
    validate_range_capture(request, records, &definitions)
}

fn parse_limited_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(LIMITED_REQUEST)?;
    let records = LIMITED_REFERENCE
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    validate_limited_range_references(&request, &records)?;
    Ok((request, records))
}

fn parse_zero_current_range_fixture() -> TestResult<(Value, Vec<Value>)> {
    let request = serde_json::from_str(ZERO_CURRENT_REQUEST)?;
    let records = ZERO_CURRENT_REFERENCE
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    validate_zero_current_range_references(&request, &records)?;
    Ok((request, records))
}

fn validate_zero_current_range_references(request: &Value, records: &[Value]) -> TestResult {
    let corpora = request["corpora"]
        .as_array()
        .ok_or("missing zero/current corpora")?;
    let key_types = ["TINYINT", "SMALLINT", "INT", "BIGINT"];
    let wrapped = BTreeMap::from([
        (("TINYINT", "INT"), "256"),
        (("SMALLINT", "INT"), "65536"),
        (("INT", "BIGINT"), "4294967296"),
    ]);
    if request["version"] != 1
        || request["issue"] != 1086
        || request["expected_record_count"] != 30
        || request["sql_source_count"] != 15
        || request["spark_version"] != "4.2.0"
        || request["spark_revision"] != "32f7299601108917fb01920a54e084595b7b3bf8"
        || request["signed_key_types"] != json!(key_types)
        || corpora.len() != 1
        || records.len() != 31
        || corpora[0]["corpus"] != "finite-range-1086-zero-current-controls"
        || corpora[0]["metadata"] != json!({"spark_version":"4.2.0","timezone":"UTC","inputs":[]})
    {
        return Err("zero/current finite RANGE inventory differs".into());
    }
    let mut contexts = BTreeSet::new();
    let mut definitions = BTreeMap::new();
    for case in corpora[0]["cases"]
        .as_array()
        .ok_or("missing zero/current cases")?
    {
        let identity = case_identity(case)?;
        let coverage = &case["coverage"];
        let key = coverage["key_type"]
            .as_str()
            .ok_or("missing zero/current key")?;
        let source = coverage["source_offset_type"]
            .as_str()
            .ok_or("missing zero/current source")?;
        let offset = coverage["positive_offset"]
            .as_str()
            .ok_or("missing zero/current offset")?;
        let shape = coverage["shape"]
            .as_str()
            .ok_or("missing zero/current shape")?;
        let (family, frame) = if shape == "wrapped_preceding_current" {
            if wrapped.get(&(key, source)) != Some(&offset) {
                return Err("unexpected wrapped-zero coercion pair".into());
            }
            (
                "wrapped_zero_current",
                format!("RANGE BETWEEN CAST({offset} AS {source}) PRECEDING AND CURRENT ROW"),
            )
        } else {
            if !key_types.contains(&key) || source != "INT" || offset != "0" {
                return Err("unexpected zero/current operand type".into());
            }
            let frame = match shape {
                "zero_preceding_current" => "RANGE BETWEEN 0 PRECEDING AND CURRENT ROW",
                "zero_preceding_shorthand" => "RANGE 0 PRECEDING",
                "zero_following_current" => "RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW",
                _ => return Err("unexpected zero/current frame shape".into()),
            };
            ("zero_current", frame.into())
        };
        let values = ["NULL", "NULL", "0", "0", "1"]
            .iter()
            .enumerate()
            .map(|(id, value)| (id, format!("CAST({value} AS {key})")))
            .collect::<Vec<_>>();
        let sql_values = values
            .iter()
            .map(|(id, value)| format!("({id},{value})"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT id,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST {frame}) AS n FROM VALUES {sql_values} AS t(id,k) ORDER BY id"
        );
        if case["corpus"] != corpora[0]["corpus"]
            || case["ordered"] != true
            || case["ownership"] != "finite_signed_integer_zero_current_control"
            || coverage["family"] != family
            || coverage["frame"] != frame
            || coverage["inline_values"] != json!(values)
            || coverage["order"] != "ASC NULLS FIRST"
            || coverage["outer_order"] != "id"
            || case["sql"] != sql
            || ["expected", "status", "rows_raw", "spark_error_details"]
                .iter()
                .any(|field| case.get(*field).is_some())
            || !contexts.insert((key, source, offset, shape, identity.2))
            || definitions.insert(identity, case).is_some()
        {
            return Err("zero/current finite RANGE source clauses differ".into());
        }
    }
    if contexts.len() != 30 || definitions.len() != 30 {
        return Err("zero/current finite RANGE source context inventory differs".into());
    }
    validate_range_capture(request, records, &definitions)
}

fn matches_numeric_error_cause(reference: &Value, observation: &Value) -> bool {
    let condition = reference["expected"]["condition"].as_str();
    if !matches!(
        condition,
        Some("ARITHMETIC_OVERFLOW" | "BINARY_ARITHMETIC_OVERFLOW" | "CAST_OVERFLOW")
    ) {
        return true;
    }
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let parameters = &reference["spark_error_details"]["parameters"];
    let root_matches = |cause: &str| error.lines().any(|line| line.ends_with(cause));
    let integer = |name: &str| {
        parameters[name]
            .as_str()
            .map(|value| value.trim_end_matches(['S', 'L']))
    };
    match condition {
        Some("ARITHMETIC_OVERFLOW") if parameters["message"] == "overflow" => {
            match parameters["alternative"].as_str() {
                Some(alternative) if alternative.contains("try_add") => {
                    error.contains("[ARITHMETIC_OVERFLOW] RANGE comparison target")
                        && error.lines().any(|line| {
                            line.contains(
                                "Arrow error: Arithmetic overflow: Overflow happened on: ",
                            ) && line.contains(" + ")
                        })
                }
                Some("") => {
                    error.contains("[ARITHMETIC_OVERFLOW] RANGE offset negation")
                        && error.lines().any(|line| {
                            line.contains(" overflow on negative(") && line.ends_with(')')
                        })
                }
                _ => false,
            }
        }
        Some("BINARY_ARITHMETIC_OVERFLOW") => {
            let (Some(left), Some(right)) = (integer("value1"), integer("value2")) else {
                return false;
            };
            parameters["symbol"] == "+"
                && parameters["functionName"] == "`try_add`"
                && error.contains("[BINARY_ARITHMETIC_OVERFLOW] RANGE comparison target")
                && root_matches(&format!(
                    "Arrow error: Arithmetic overflow: Overflow happened on: {left} + {right}"
                ))
        }
        Some("CAST_OVERFLOW") => {
            let Some(value) = integer("value") else {
                return false;
            };
            let target = match parameters["targetType"].as_str() {
                Some("\"TINYINT\"") => "Int8",
                Some("\"SMALLINT\"") => "Int16",
                Some("\"INT\"") => "Int32",
                Some("\"BIGINT\"") => "Int64",
                _ => return false,
            };
            // Native cast roots report the value and target, but do not expose sourceType.
            error.contains("[CAST_OVERFLOW]")
                && root_matches(&format!(
                    "Arrow error: Cast error: Can't cast value {value} to type {target}"
                ))
        }
        _ => false,
    }
}

#[tokio::test]
async fn finite_signed_integer_range_frames_match_spark() -> TestResult {
    let fixtures = [
        parse_finite_range_fixture()?,
        parse_limited_range_fixture()?,
        parse_narrowing_range_fixture()?,
        parse_zero_current_range_fixture()?,
    ];
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
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
                    let identity = (case_identity(case)?, batch);
                    let observation = match &provider_error {
                        Some(error) => {
                            json!({"observer_error":error,"observer_phase":"register_inputs"})
                        }
                        None => match std::panic::AssertUnwindSafe(
                            compare_exact_spark_query_stages_with_optimized(&session, case),
                        )
                        .catch_unwind()
                        .await
                        {
                            Ok(result) => result.unwrap_or_else(
                                |error| json!({"observer_error":error.to_string()}),
                            ),
                            Err(payload) => {
                                let panic = payload
                                    .downcast_ref::<String>()
                                    .cloned()
                                    .or_else(|| {
                                        payload
                                            .downcast_ref::<&str>()
                                            .map(|message| (*message).to_owned())
                                    })
                                    .unwrap_or_else(|| "non-string panic payload".into());
                                json!({"native_panic":panic,"matches":false,"optimized_matches":false})
                            }
                        },
                    };
                    let cause_matches = matches_numeric_error_cause(case, &observation);
                    let matches =
                        native_result_matches_spark_reference(&observation, case) && cause_matches;
                    let unique = completed.insert(identity);
                    if !matches || !unique {
                        differences.push(format!(
                            "{}:{} ANSI={} batch={batch}",
                            case["corpus"], case["id"], case["ansi"],
                        ));
                    }
                    eprintln!(
                        "FINITE_RANGE_OBSERVATION {}",
                        json!({"issue":1086,"corpus":case["corpus"],"id":case["id"],
                    "ansi":case["ansi"],"batch":batch,"target_partitions":2,"provider_partitions":(!corpus["metadata"]["inputs"].as_array().ok_or("missing provider inputs")?.is_empty()).then_some(2),
                    "source_ownership_tag":case["ownership"],"reference":case,
                    "observation":observation,"numeric_cause_matches":cause_matches,
                    "unexposed_cause_parameters":if case["expected"]["condition"] == "CAST_OVERFLOW" { Some(["sourceType"]) } else { None },
                    "spark_matches":matches}),
                    );
                }
            }
        }
    }
    eprintln!(
        "FINITE_RANGE_SUMMARY {}",
        json!({"observations":completed.len(),"expected_observations":500,
        "differences":differences.len()}),
    );
    assert_eq!(completed.len(), 500);
    assert!(
        differences.is_empty(),
        "finite RANGE differences:\n{}",
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn numeric_error_causes_reject_same_condition_phase_and_fields_mutations() -> TestResult {
    let (_, finite) = parse_finite_range_fixture()?;
    let (_, narrowing) = parse_narrowing_range_fixture()?;
    let addition = finite
        .iter()
        .find(|case| {
            case["expected"]["condition"] == "ARITHMETIC_OVERFLOW"
                && case["spark_error_details"]["parameters"]["alternative"]
                    .as_str()
                    .is_some_and(|value| value.contains("try_add"))
        })
        .ok_or("missing captured target addition error")?;
    let negation = finite
        .iter()
        .find(|case| {
            case["expected"]["condition"] == "ARITHMETIC_OVERFLOW"
                && case["spark_error_details"]["parameters"]["alternative"] == ""
        })
        .ok_or("missing captured offset negation error")?;
    let binary = finite
        .iter()
        .find(|case| {
            case["expected"]["condition"] == "BINARY_ARITHMETIC_OVERFLOW"
                && case["spark_error_details"]["parameters"]["value1"] == "-4S"
                && case["spark_error_details"]["parameters"]["value2"] == "-128S"
        })
        .ok_or("missing captured binary addition operands")?;
    let cast = narrowing
        .iter()
        .find(|case| {
            case["expected"]["condition"] == "CAST_OVERFLOW"
                && case["spark_error_details"]["parameters"]["value"] == "128S"
                && case["spark_error_details"]["parameters"]["targetType"] == "\"TINYINT\""
        })
        .ok_or("missing captured narrowing input and target")?;
    let add_error = "[ARITHMETIC_OVERFLOW] RANGE comparison target\ncaused by\nArrow error: Arithmetic overflow: Overflow happened on: -4 + -2147483648";
    let negate_error = "[ARITHMETIC_OVERFLOW] RANGE offset negation\ncaused by\nExecution error: Int32 overflow on negative(-2147483648)";
    for (reference, error, mutations) in [
        (
            addition,
            add_error,
            vec![
                negate_error,
                "[ARITHMETIC_OVERFLOW] RANGE comparison target\nArrow error: Arithmetic overflow: Overflow happened on: -4 - -2147483648",
            ],
        ),
        (negation, negate_error, vec![add_error]),
        (
            binary,
            "[BINARY_ARITHMETIC_OVERFLOW] RANGE comparison target\nArrow error: Arithmetic overflow: Overflow happened on: -4 + -128",
            vec![
                "[BINARY_ARITHMETIC_OVERFLOW] RANGE comparison target\nArrow error: Arithmetic overflow: Overflow happened on: -3 + -128",
                "[BINARY_ARITHMETIC_OVERFLOW] RANGE comparison target\nArrow error: Arithmetic overflow: Overflow happened on: -128 + -4",
                "[BINARY_ARITHMETIC_OVERFLOW] RANGE offset negation\nExecution error: Int8 overflow on negative(-128)",
            ],
        ),
        (
            cast,
            "[CAST_OVERFLOW]\nArrow error: Cast error: Can't cast value 128 to type Int8",
            vec![
                "[CAST_OVERFLOW]\nArrow error: Cast error: Can't cast value 129 to type Int8",
                "[CAST_OVERFLOW]\nArrow error: Cast error: Can't cast value 128 to type Int16",
                "[CAST_OVERFLOW] RANGE offset negation\nExecution error: Int8 overflow on negative(-128)",
            ],
        ),
    ] {
        let phase = match reference["spark_error_phase"].as_str() {
            Some("sql") => "resolve",
            Some("optimized_schema") => "create_physical_plan",
            Some("collect") => "collect",
            _ => return Err("unexpected arithmetic error phase".into()),
        };
        let mut observation =
            json!({"matches":true,"optimized_matches":true,"native_phase":phase,"error":error});
        for (stage, observed) in STAGES {
            observation[observed] = reference["schema_stages"][stage].clone();
        }
        assert!(native_result_matches_spark_reference(
            &observation,
            reference
        ));
        assert!(matches_numeric_error_cause(reference, &observation));
        for mutation in mutations {
            let mut changed = observation.clone();
            changed["error"] = json!(mutation);
            assert!(native_result_matches_spark_reference(&changed, reference));
            assert!(
                !matches_numeric_error_cause(reference, &changed),
                "accepted wrong numeric cause {mutation}"
            );
        }
    }
    Ok(())
}

#[test]
fn finite_range_references_reject_lost_captured_contracts() -> TestResult {
    let (request, records) = parse_finite_range_fixture()?;
    let success = records
        .iter()
        .position(|case| {
            case["status"] == "ok"
                && case["ordered"] == true
                && case["rows_raw"]
                    .as_array()
                    .is_some_and(|rows| rows.len() > 1)
        })
        .ok_or("missing nonempty ordered finite RANGE reference")?;
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_finite_range_references(&request, &missing).is_err());
    let mut duplicate = records.clone();
    duplicate[2] = duplicate[1].clone();
    assert!(validate_finite_range_references(&request, &duplicate).is_err());
    for pointer in [
        "/id",
        "/ordered",
        "/rows_raw",
        "/schema_stage_availability",
        "/schema_stages/public/fields/0/metadata",
        "/schema_stages/analyzed/fields/0/metadata",
        "/schema_stages/optimized/fields/0/metadata",
        "/schema_stages/executed_before_collect/fields/0/metadata",
        "/schema_stages/executed/fields/0/metadata",
    ] {
        let mut changed = records.clone();
        *changed[success]
            .pointer_mut(pointer)
            .ok_or("missing success fault target")? = Value::Null;
        assert!(
            validate_finite_range_references(&request, &changed).is_err(),
            "accepted lost {pointer}"
        );
    }
    let mut changed = records.clone();
    changed[success]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing expected rows")?
        .reverse();
    assert!(validate_finite_range_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[success]["expected"]["rows"]
        .as_array_mut()
        .ok_or("missing expected rows")?
        .pop();
    assert!(validate_finite_range_references(&request, &changed).is_err());
    let mut changed = records.clone();
    changed[0]["corpora"][0]["registered_inputs"][0]["schema_observed"] = Value::Null;
    assert!(validate_finite_range_references(&request, &changed).is_err());
    let mut changed_request = request.clone();
    changed_request["corpora"][1]["cases"][0]["sql"] = json!("changed source SQL");
    assert!(validate_finite_range_references(&changed_request, &records).is_err());
    let consumer = records
        .iter()
        .position(|case| {
            case["status"] == "ok"
                && case["schema_stages"]["public"]["fields"]
                    .as_array()
                    .is_some_and(|fields| fields.iter().any(|field| field["name"] == "member_ids"))
        })
        .ok_or("missing nested finite RANGE consumer reference")?;
    for (stage, _) in STAGES {
        let field = records[consumer]["schema_stages"][stage]["fields"]
            .as_array()
            .ok_or("missing consumer Fields")?
            .iter()
            .position(|field| field["name"] == "member_ids")
            .ok_or("missing member_ids Field")?;
        for property in ["elementType", "containsNull"] {
            let mut changed = records.clone();
            changed[consumer]["schema_stages"][stage]["fields"][field]["type"][property] =
                Value::Null;
            assert!(
                validate_finite_range_references(&request, &changed).is_err(),
                "accepted lost {stage} array {property}"
            );
        }
    }

    for phase in ["sql", "optimized_schema", "collect"] {
        let error = records
            .iter()
            .position(|case| case["status"] == "error" && case["spark_error_phase"] == phase)
            .ok_or_else(|| format!("missing finite RANGE error at {phase}"))?;
        for pointer in [
            "/expected/condition",
            "/spark_error_phase",
            "/spark_error_details/phase",
            "/spark_error_details/message",
            "/spark_error_details/java_causes",
            "/spark_error_details/cause_chain_complete",
        ] {
            let mut changed = records.clone();
            *changed[error]
                .pointer_mut(pointer)
                .ok_or("missing error fault target")? = Value::Null;
            assert!(
                validate_finite_range_references(&request, &changed).is_err(),
                "accepted lost {phase} {pointer}"
            );
        }
        for (stage, _) in STAGES {
            if records[error]["schema_stage_availability"][stage]["available"] == true {
                let mut changed = records.clone();
                changed[error]["schema_stages"][stage]["fields"][0]["metadata"] = Value::Null;
                assert!(
                    validate_finite_range_references(&request, &changed).is_err(),
                    "accepted lost {phase} {stage} Field"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn finite_range_member_ids_keep_the_recorded_array_codec() -> TestResult {
    let (_, records) = parse_finite_range_fixture()?;
    let reference = records
        .iter()
        .find(|case| case["id"] == "consumers_tinyint_sliding" && case["status"] == "ok")
        .ok_or("missing captured member_ids consumer")?;
    let field = reference["schema_stages"]["public"]["fields"]
        .as_array()
        .ok_or("missing consumer Fields")?
        .iter()
        .position(|field| field["name"] == "member_ids")
        .ok_or("missing member_ids Field")?;
    assert_eq!(reference["rows_raw"][0][field], json!(["0", "70"]));
    assert_eq!(reference["ordered"], true);
    validate_spark_reference_result(reference)?;
    for value in [
        Value::Null,
        json!("scalar"),
        json!([0, "70"]),
        json!([null, "70"]),
        json!([["0"], "70"]),
        json!({"unexpected":["0", "70"]}),
    ] {
        let mut changed = reference.clone();
        changed["rows_raw"][0][field] = value;
        let mut rows = changed["rows_raw"]
            .as_array()
            .ok_or("missing recorded rows")?
            .clone();
        rows.sort_by_key(Value::to_string);
        changed["rows_multiset"] = json!(rows);
        changed["expected"]["rows"] = changed["rows_raw"].clone();
        assert!(
            validate_spark_reference_result(&changed).is_err(),
            "accepted malformed member_ids codec"
        );
    }
    Ok(())
}

#[test]
fn narrowing_range_references_reject_lost_source_and_capture_contracts() -> TestResult {
    let (request, records) = parse_narrowing_range_fixture()?;
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_narrowing_range_references(&request, &missing).is_err());
    let mut changed = request.clone();
    changed["corpora"][0]["cases"][0]["coverage"]["positive_offset"] = json!("0");
    assert!(validate_narrowing_range_references(&changed, &records).is_err());
    let mut changed = records.clone();
    changed[1]["spark_error_details"]["cause_chain_complete"] = Value::Null;
    assert!(validate_narrowing_range_references(&request, &changed).is_err());
    Ok(())
}

#[test]
fn limited_range_references_reject_lost_source_and_capture_contracts() -> TestResult {
    let (request, records) = parse_limited_range_fixture()?;
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_limited_range_references(&request, &missing).is_err());
    let mut changed = request.clone();
    changed["corpora"][0]["cases"][0]["coverage"]["limit"] = json!(0);
    assert!(validate_limited_range_references(&changed, &records).is_err());
    let mut changed = records.clone();
    changed[1]["schema_stages"]["optimized"] = Value::Null;
    assert!(validate_limited_range_references(&request, &changed).is_err());
    Ok(())
}

#[test]
fn finite_range_matcher_requires_every_available_and_unavailable_stage() -> TestResult {
    let (_, records) = parse_finite_range_fixture()?;
    for (status, phase) in [("ok", None), ("error", Some("sql"))] {
        let reference = records
            .iter()
            .find(|case| {
                case["status"] == status
                    && phase.is_none_or(|phase| case["spark_error_phase"] == phase)
            })
            .ok_or("missing finite RANGE result status")?;
        let mut observation = json!({"matches":true,"optimized_matches":true});
        for (stage, observed) in STAGES {
            observation[observed] = reference["schema_stages"][stage].clone();
        }
        assert!(native_result_matches_spark_reference(
            &observation,
            reference
        ));
        for (_, observed) in STAGES {
            let mut changed = observation.clone();
            changed[observed] = if changed[observed].is_null() {
                json!({})
            } else {
                Value::Null
            };
            assert!(
                !native_result_matches_spark_reference(&changed, reference),
                "accepted changed {status} {observed}"
            );
        }
        for field in ["matches", "optimized_matches"] {
            let mut changed = observation.clone();
            changed[field] = json!(false);
            assert!(!native_result_matches_spark_reference(&changed, reference));
        }
    }
    Ok(())
}

#[test]
fn zero_current_range_references_reject_lost_source_and_capture_contracts() -> TestResult {
    let (request, records) = parse_zero_current_range_fixture()?;
    let mut missing = records.clone();
    missing.pop();
    assert!(validate_zero_current_range_references(&request, &missing).is_err());
    let mut changed = request.clone();
    changed["corpora"][0]["cases"][0]["coverage"]["frame"] =
        json!("RANGE BETWEEN CURRENT ROW AND CURRENT ROW");
    assert!(validate_zero_current_range_references(&changed, &records).is_err());
    let mut changed = records.clone();
    changed[1]["schema_stages"]["public"]["fields"][0]["metadata"] = Value::Null;
    assert!(validate_zero_current_range_references(&request, &changed).is_err());
    Ok(())
}

#[tokio::test]
async fn nonzero_and_zero_range_windows_share_a_query() -> TestResult {
    use datafusion::physical_plan::collect;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    let sql = "SELECT id,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS bounded,COUNT(*) OVER (ORDER BY k ASC NULLS FIRST RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) AS peers FROM VALUES (0,CAST(NULL AS INT)),(1,CAST(NULL AS INT)),(2,CAST(0 AS INT)),(3,CAST(0 AS INT)),(4,CAST(1 AS INT)) AS t(id,k) ORDER BY id";
    for ansi_mode in [true, false] {
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
            let query = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let plan = query.create_physical_plan(&session).await?;
            let batches = collect(plan, session.task_ctx()).await?;
            let mut rows = Vec::new();
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| spark_results::encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?,
                    );
                }
            }
            assert_eq!(
                json!(rows),
                json!([
                    ["0", "2", "2"],
                    ["1", "2", "2"],
                    ["2", "2", "2"],
                    ["3", "2", "2"],
                    ["4", "3", "1"]
                ]),
                "ANSI={ansi_mode} batch={batch}"
            );
        }
    }
    Ok(())
}
