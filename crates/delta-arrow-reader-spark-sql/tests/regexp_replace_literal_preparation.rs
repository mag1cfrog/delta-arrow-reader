//! Reached literal replacement errors must keep their independent preparation boundary.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[expect(
    dead_code,
    reason = "This target uses full observations rather than the result-only helper."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

type QueryKey = (String, bool);

struct References {
    inputs: Value,
    queries: BTreeMap<QueryKey, Value>,
    providers: BTreeMap<(QueryKey, String), Value>,
}

fn key(case: &Value) -> TestResult<QueryKey> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

fn references() -> TestResult<References> {
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-replace-literal-preparation-inputs.json"
    ))?;
    let records = include_str!("fixtures/regexp-replace-literal-preparation-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 138);
    assert_eq!(records[0]["kind"], "header");
    assert_eq!(records[0]["runtime_identity"]["spark_version"], "4.2.0");
    assert_eq!(
        records.last(),
        Some(&json!({"kind":"summary","observations":34,"provider_receipts":102,
            "collection_complete":true,"compatibility_credit":false}))
    );
    let mut queries = BTreeMap::new();
    let mut providers = BTreeMap::new();
    for record in &records[1..records.len() - 1] {
        match record["kind"].as_str() {
            Some("observation") => {
                assert!(queries.insert(key(record)?, record.clone()).is_none());
            }
            Some("provider") => {
                let name = record["name"].as_str().ok_or("missing provider name")?;
                assert!(
                    providers
                        .insert((key(record)?, name.to_owned()), record.clone())
                        .is_none()
                );
            }
            other => return Err(format!("unexpected reference record: {other:?}").into()),
        }
    }
    assert_eq!(queries.len(), 34);
    assert_eq!(providers.len(), 102);
    let cases = inputs["cases"].as_array().ok_or("missing input cases")?;
    let declarations = inputs["providers"].as_array().ok_or("missing providers")?;
    assert_eq!(cases.len(), 34);
    assert_eq!(declarations.len(), 3);
    assert_eq!(
        declarations
            .iter()
            .map(|p| p["rows"].as_array().map(Vec::len))
            .collect::<Vec<_>>(),
        vec![Some(6), Some(4), Some(0)]
    );
    let mut seen = BTreeSet::new();
    for case in cases {
        let case_key = key(case)?;
        assert!(seen.insert(case_key.clone()));
        let record = queries.get(&case_key).ok_or("missing Spark query")?;
        for (name, value) in case.as_object().ok_or("invalid input case")? {
            assert_eq!(record.get(name), Some(value), "input {name}: {case_key:?}");
        }
        assert_eq!(record["compatibility_credit"], false);
        for (name, availability) in record["availability"].as_object().ok_or("missing stage availability")? {
            let observed = match record["actual"].get(name) {
                None => "absent",
                Some(Value::Null) => "null",
                Some(_) => "available",
            };
            assert_eq!(availability.as_str(), Some(observed), "reference stage {name}: {case_key:?}");
        }
        assert_eq!(record["effective_settings"]["spark.sql.session.timeZone"], "UTC");
        assert_eq!(
            record["effective_settings"]["spark.sql.ansi.enabled"],
            case["ansi"].to_string()
        );
        for (name, value) in inputs["settings"].as_object().ok_or("missing settings")? {
            let effective = case["settings"].get(name).unwrap_or(value);
            assert_eq!(record["effective_settings"].get(name), Some(effective));
        }
        for (name, value) in case["settings"].as_object().ok_or("missing case settings")? {
            assert_eq!(record["effective_settings"].get(name), Some(value));
        }
        assert!(record["absent_case_settings"].as_object().ok_or("missing absent settings")?.values().all(Value::is_null));
        for declaration in declarations {
            let name = declaration["name"].as_str().ok_or("missing provider name")?;
            let provider = providers
                .get(&(case_key.clone(), name.to_owned()))
                .ok_or("missing Spark provider receipt")?;
            assert_eq!(provider["actual"]["status"], "ok");
            assert_eq!(provider["actual"]["schema"]["fields"], declaration["fields"]);
            assert_eq!(provider["actual"]["rows"], declaration["rows"]);
        }
    }
    Ok(References { inputs, queries, providers })
}

fn complete_keys(actual: &Value, error: bool) -> bool {
    let mut expected = BTreeSet::from([
        "analyzed_schema", "analyzed_arrow_schema", "physical_schema", "physical_arrow_schema",
    ]);
    if error {
        expected.extend(["error", "error_stage"]);
    } else {
        expected.insert("rows");
    }
    actual.as_object().is_some_and(|object| {
        object.keys().map(String::as_str).collect::<BTreeSet<_>>() == expected
    })
}

fn error_matches(error: &str, expected: &Value) -> bool {
    let Some(cause) = expected["java_causes"].as_array().and_then(|causes| causes.last()) else {
        return false;
    };
    match expected["condition"].as_str() {
        Some("INVALID_REGEXP_REPLACE") => {
            let [Some(source), Some(pattern), Some(replacement), Some(position)] =
                ["source", "pattern", "replacement", "position"]
                    .map(|name| expected["parameters"][name].as_str())
            else {
                return false;
            };
            let (Some(class), Some(message)) = (cause["class"].as_str(), cause["message"].as_str()) else {
                return false;
            };
            error.contains(&format!(
                "[INVALID_REGEXP_REPLACE] source={source:?}, pattern={pattern:?}, replacement={replacement:?}, position={position}: "
            )) && error.ends_with(&format!("{class}: {message}"))
        }
        Some("INVALID_PARAMETER_VALUE.PATTERN") => {
            cause["class"] == "java.util.regex.PatternSyntaxException"
                && cause["message"] == "Unclosed character class near index 0\n[\n^"
                && expected["parameters"]["value"] == "'['"
                && error.contains("[INVALID_PARAMETER_VALUE.PATTERN] regexp_replace pattern \"[\": regex parse error:")
                && error.ends_with("error: unclosed character class")
        }
        _ => false,
    }
}

fn matches_reference(actual: &Value, expected: &Value) -> bool {
    if actual.get("analyzed_schema") != expected.get("analyzed_schema")
        || expected.get("schema").is_some_and(|schema| actual.get("analyzed_schema") != Some(schema))
        || !actual["analyzed_arrow_schema"].as_str().is_some_and(|s| !s.is_empty())
    {
        return false;
    }
    if expected["status"] == "ok" {
        return complete_keys(actual, false)
            && actual.get("physical_schema") == expected.get("physical_schema")
            && actual["physical_arrow_schema"].as_str().is_some_and(|s| !s.is_empty())
            && actual.get("rows") == expected.get("rows");
    }
    let stage = match expected["phase"].as_str() {
        Some("optimized_schema") => "physical",
        Some("collect") => "execute",
        _ => return false,
    };
    complete_keys(actual, true)
        && actual["error_stage"] == stage
        && actual["error"].as_str().is_some_and(|error| {
            error.starts_with(&format!("{stage}: ")) && error_matches(error, expected)
        })
        && if stage == "physical" {
            expected.get("physical_schema").is_none()
                && actual["physical_schema"].is_null()
                && actual["physical_arrow_schema"].is_null()
        } else {
            actual.get("physical_schema") == expected.get("physical_schema")
                && actual["physical_arrow_schema"].as_str().is_some_and(|s| !s.is_empty())
        }
}

fn marker_absent(session: &SessionContext) -> bool {
    session.state().config_options().extensions.get::<SparkSqlExpressionSemantics>().is_none()
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            ),
        ).build(),
    )
}

async fn native_controls(session: &SessionContext) -> TestResult<Value> {
    let mut results = Vec::new();
    for sql in [
        "SELECT regexp_replace('abc123','([0-9]+)','<$1>') AS r",
        "SELECT regexp_replace('a','(a)','$9') AS r",
    ] {
        results.push(spark_results::capture_native_query_observation(session, sql).await?);
    }
    Ok(json!(results))
}

#[tokio::test]
async fn replacement_preparation_matches_all_34_independent_references() -> TestResult {
    let references = references()?;
    let declarations = references.inputs["providers"].as_array().ok_or("missing providers")?;
    let registration = json!({"registered_inputs":declarations.iter().map(|p| {
        json!({"name":p["name"],"schema":{"type":"struct","fields":p["fields"]},"rows":p["rows"]})
    }).collect::<Vec<_>>()});
    let native = native_controls(&SessionContext::new()).await?;
    let mut differences = Vec::new();
    let mut observations = BTreeSet::new();
    let mut provider_receipts = BTreeSet::new();
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            spark_inputs::register_inputs(&session, &registration, representation, batch_size)?;
            assert!(marker_absent(&session));
            assert_eq!(native_controls(&session).await?, native);
            for case in references.inputs["cases"].as_array().ok_or("missing cases")? {
                let case_key = key(case)?;
                let reference = references.queries.get(&case_key).ok_or("missing query")?;
                for declaration in declarations {
                    let name = declaration["name"].as_str().ok_or("missing provider name")?;
                    let expected = &references.providers
                        .get(&(case_key.clone(), name.to_owned())).ok_or("missing provider receipt")?["actual"];
                    let provider = session.table_provider(name).await?;
                    let schema = provider.schema();
                    let fields = spark_results::capture_schema(schema.fields(), None)?;
                    let sql = format!("SELECT * FROM {name} ORDER BY {}", schema.field(0).name());
                    let actual = spark_results::capture_native_query_observation(&session, &sql).await?;
                    let string_type = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View][representation].clone();
                    let storage_matches = schema.fields().iter().all(|field| {
                        field.data_type() == if field.name() == "k" { &DataType::Int32 } else { &string_type }
                    });
                    let matches = storage_matches && fields == expected["schema"]
                        && actual["public_schema"] == expected["analyzed_schema"]
                        && actual["before_collect"] == expected["physical_schema"]
                        && actual["physical_schema"] == expected["physical_schema"]
                        && actual["rows"] == expected["rows"] && actual["error"].is_null();
                    assert!(provider_receipts.insert((case_key.clone(), representation, batch_size, name.to_owned())));
                    eprintln!("c06 replacement provider regression: {}", json!({
                        "id":case["id"],"ansi":case["ansi"],"name":name,"representation":representation,
                        "batch_size":batch_size,"actual":actual,"provider_schema":fields,
                        "provider_raw_schema":format!("{schema:?}"),"matches_reference":matches
                    }));
                    if !matches { differences.push(format!("provider {name}: {case_key:?}/{representation}/{batch_size}")); }
                }
                assert!(marker_absent(&session));
                let actual = capture_observation(&session, case).await?;
                assert!(marker_absent(&session));
                let matches = matches_reference(&actual, &reference["actual"]);
                assert!(observations.insert((case_key.clone(), representation, batch_size)));
                eprintln!("c06 replacement preparation regression: {}", json!({
                    "input_case":case,"representation":representation,"batch_size":batch_size,
                    "actual":actual,"matches_reference":matches,"native_optimized_schema":"unexposed"
                }));
                if !matches { differences.push(format!("query {case_key:?}/{representation}/{batch_size}: {actual}")); }
            }
            assert_eq!(native_controls(&session).await?, native);
            assert!(marker_absent(&session));
        }
    }
    assert_eq!(observations.len(), 204);
    assert_eq!(provider_receipts.len(), 612);
    assert!(differences.is_empty(), "{} mismatches:\n{}", differences.len(), differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn replacement_null_no_match_and_pattern_priority_keep_their_boundaries() -> TestResult {
    let mut compared = 0;
    let mut differences = Vec::new();
    for (capture, ids) in [
        (include_str!("fixtures/regexp-replace-spark.jsonl"), vec![
            "regexp_replace_literal_no_match", "regexp_replace_literal_null_source",
            "regexp_replace_literal_null_pattern", "regexp_replace_literal_null_replacement",
            "admission_regexp_replace_position_null", "regexp_replace_literal_invalid_pattern",
        ]),
        (include_str!("fixtures/regexp-replace-expanded-spark.jsonl"), vec![
            "position_invalid_past_end", "position_bad_replacement_past_end", "position_null_invalid",
            "null_source_bad_pattern", "null_replacement_bad_pattern",
        ]),
        (include_str!("fixtures/regexp-replace-empty-utf16-spark.jsonl"), vec!["bad_pattern_beyond"]),
    ] {
        let records = capture.lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>, _>>()?;
        let cases = records.iter().skip(1).filter(|case| case["id"].as_str().is_some_and(|id| ids.contains(&id))).collect::<Vec<_>>();
        assert_eq!(cases.len(), ids.len() * 2);
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                spark_inputs::register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &cases {
                    let actual = capture_observation(&session, case).await?;
                    let matches = matches_reference(&actual, &case["expected"]);
                    eprintln!("c06 replacement boundary control: {}", json!({
                        "id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
                        "representation":representation,"batch_size":batch_size,"actual":actual,"matches_reference":matches
                    }));
                    if !matches { differences.push(format!("{} ANSI={} /{representation}/{batch_size}: {actual}", case["id"], case["ansi"])); }
                    compared += 1;
                }
            }
        }
    }
    assert_eq!(compared, 144);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
