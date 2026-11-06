//! Literal extraction preparation retains resolved Fields and native SQL isolation.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::util::display::array_value_to_string;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[expect(
    dead_code,
    reason = "This target captures complete failed-query observations."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_schema, register_inputs};

#[path = "support/regexp_extract_diagnostics.rs"]
mod regexp_diagnostics;
use regexp_diagnostics::{availability, error_cause, group_error_suffix_matches};


#[expect(dead_code, reason = "This target selects the recorded input and Int32 cause helpers.")]
#[path = "support/spark_results.rs"]
mod spark_results;
#[expect(dead_code, reason = "This target registers only the recorded empty floating input.")]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "../vendor/sail/crates/sail-plan/src/test_utils/regexp_literal_producer.rs"]
mod literal_producer;

fn assert_query_marker_absent(session: &SessionContext) {
    assert!(
        session
            .state()
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
    );
}

#[tokio::test]
async fn literal_group_errors_keep_resolved_fields_and_query_local_marker() -> TestResult {
    let references = include_str!("fixtures/regexp-extract-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let native_sql = "SELECT regexp_match('abc123','([0-9]+)') AS r";
    let plain = SessionContext::new();
    let native = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_query_marker_absent(&session);
    assert_eq!(session.sql(native_sql).await?.collect().await?, native);
    let mut checked = 0;
    for case in references.iter().skip(1).filter(|case| {
        case["id"] == "regexp_extract_literal_missing_index"
            || case["id"] == "regexp_extract_default_no_group"
    }) {
        assert_eq!(case["expected"]["phase"], "optimized_schema");
        assert_query_marker_absent(&session);
        let actual = capture_observation(&session, case).await?;
        assert_query_marker_absent(&session);
        assert_eq!(
            actual["analyzed_schema"],
            case["expected"]["analyzed_schema"]
        );
        assert!(actual["analyzed_arrow_schema"].is_string());
        assert_eq!(actual["error_stage"], "physical");
        assert_eq!(actual["physical_schema"], Value::Null);
        assert_eq!(actual["physical_arrow_schema"], Value::Null);
        assert!(actual.get("rows").is_none() && actual.get("optimized_schema").is_none());
        let count = case["expected"]["parameters"]["groupCount"]
            .as_str()
            .ok_or("missing group count")?;
        let index = case["expected"]["parameters"]["groupIndex"]
            .as_str()
            .ok_or("missing group index")?;
        let error = actual["error"].as_str().ok_or("missing error")?;
        assert!(error.starts_with("physical: "));
        assert!(
            error.ends_with(&format!(
                "Spark `regexp_extract` function: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] Expects group index between 0 and {count}, but got {index}"
            )),
            "{error}"
        );
        assert_eq!(session.sql(native_sql).await?.collect().await?, native);
        eprintln!(
            "c06 literal preparation regression: {}",
            json!({
                "id":case["id"],"sql":case["sql"],"ansi":case["ansi"],"actual":actual,
                "full_compatibility_pass":false
            })
        );
        checked += 1;
    }
    assert_eq!(checked, 4);
    Ok(())
}

#[tokio::test]
async fn set_resolution_keeps_literal_fields_before_preparation_failure() -> TestResult {
    let references = include_str!("fixtures/regexp-extract-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut checked = 0;
    for case in references
        .iter()
        .skip(1)
        .filter(|case| case["id"] == "regexp_extract_literal_missing_index")
    {
        assert_query_marker_absent(&session);
        let sql = format!(
            "{} UNION SELECT 'b' AS r",
            case["sql"].as_str().ok_or("missing SQL")?
        );
        let query = plan_query(
            &session,
            &sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                ..Default::default()
            },
        )
        .await?;
        let resolved = query.logical_plan.schema().clone();
        assert_eq!(resolved.fields().len(), 1);
        assert_eq!(resolved.field(0).data_type(), &DataType::Utf8);
        assert_eq!(query.column_names, vec!["r".to_string()]);
        assert_query_marker_absent(&session);
        let error = match query.create_physical_plan(&session).await {
            Ok(_) => return Err("expected literal group preparation failure".into()),
            Err(error) => error.to_string(),
        };
        assert_eq!(query.logical_plan.schema(), &resolved);
        assert_query_marker_absent(&session);
        let count = case["expected"]["parameters"]["groupCount"]
            .as_str()
            .ok_or("missing group count")?;
        let index = case["expected"]["parameters"]["groupIndex"]
            .as_str()
            .ok_or("missing group index")?;
        assert!(
            error.ends_with(&format!(
                "Spark `regexp_extract` function: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] Expects group index between 0 and {count}, but got {index}"
            )),
            "{error}"
        );
        // The set-op exercises resolver simplification; only the underlying
        // literal group cause is taken from the independent Spark reference.
        eprintln!(
            "c06 literal set resolution: {}",
            json!({
                "sql":sql,"ansi":case["ansi"],"resolved_fields":format!("{resolved:?}"),
                "column_names":query.column_names,"physical_error":error,"spark_set_oracle":false
            })
        );
        checked += 1;
    }
    assert_eq!(checked, 2);
    Ok(())
}
fn fields(values: &[Arc<Field>], names: Option<&[String]>) -> Value {
    json!(
        values
            .iter()
            .enumerate()
            .map(|(i, f)| json!({
                "name":names.map_or(f.name(), |v| &v[i]),"type":format!("{:?}",f.data_type()),
                "nullable":f.is_nullable(),"metadata":f.metadata()
            }))
            .collect::<Vec<_>>()
    )
}
fn failure(error: &(dyn std::error::Error + 'static), stage: &str) -> Value {
    let mut causes = Vec::new();
    let mut current = Some(error);
    while let Some(value) = current {
        causes.push(value.to_string());
        current = value.source();
    }
    json!({"status":"error","error_stage":stage,"error":error.to_string(),
           "error_debug":format!("{error:?}"),"error_chain":causes})
}
fn retain(actual: &mut Value, error: Value) -> TestResult {
    for (k, v) in error.as_object().ok_or("invalid native failure object")? {
        actual[k] = v.clone();
    }
    Ok(())
}
async fn provider(session: &SessionContext, name: &str) -> TestResult<Value> {
    let p = session.table_provider(name).await?;
    let schema = p.schema();
    let mut rows = Vec::new();
    let mut batches = Vec::new();
    for b in session
        .sql(&format!("SELECT * FROM {name} ORDER BY {}", schema.field(0).name()))
        .await?
        .collect()
        .await?
    {
        batches.push(
            json!({"rows":b.num_rows(),"fields":fields(b.schema().fields(),None),
            "raw_schema":format!("{:?}",b.schema())}),
        );
        for r in 0..b.num_rows() {
            rows.push(
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(r) {
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(a.as_ref(), r).map(Value::String)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    let actual_schema = if schema.fields().iter().any(|field| {
        matches!(field.data_type(), DataType::Float32 | DataType::Float64)
    }) {
        spark_results::capture_schema(schema.fields(),None)?
    } else {
        capture_schema(schema.fields(),None)?
    };
    Ok(json!({"name":name,"fields":fields(schema.fields(),None),
        "schema":actual_schema,"raw_schema":format!("{schema:?}"),"rows":rows,"batches":batches}))
}
async fn observe(session: &SessionContext, sql: &str, ansi: bool) -> TestResult<Value> {
    observe_with_producer(session, sql, ansi, None).await
}

async fn observe_with_producer(
    session: &SessionContext,
    sql: &str,
    ansi: bool,
    producer: Option<&literal_producer::Counts>,
) -> TestResult<Value> {
    if let Some(counts) = producer { counts.phase.store(0, std::sync::atomic::Ordering::SeqCst); }
    let mut actual = json!({"analyzed_schema":null,"analyzed_fields":null,
        "analyzed_arrow_schema":null,"analyzed_plan":null,"query_column_names":null,"physical_schema":null,
        "physical_fields":null,"physical_arrow_schema":null,"physical_plan":null,
        "rand_seed":{"status":"unavailable","reason":"No assigned native RAND seed is exposed by these APIs."}});
    let query = match plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode: ansi,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(q) => q,
        Err(e) => {
            retain(&mut actual, failure(&e, "resolve"))?;
            return Ok(actual);
        }
    };
    actual["query_column_names"] = json!(query.column_names);
    actual["analyzed_schema"] = capture_schema(
        query.logical_plan.schema().fields(),
        Some(&query.column_names),
    )?;
    actual["analyzed_fields"] = fields(query.logical_plan.schema().fields(), None);
    actual["analyzed_arrow_schema"] = json!(format!("{:?}", query.logical_plan.schema()));
    actual["analyzed_plan"] = json!(format!("{:?}", query.logical_plan));
    if let Some(counts) = producer { counts.phase.store(1, std::sync::atomic::Ordering::SeqCst); }
    let physical = match query.create_physical_plan(session).await {
        Ok(p) => p,
        Err(e) => {
            retain(&mut actual, failure(&e, "physical"))?;
            return Ok(actual);
        }
    };
    actual["physical_schema"] = capture_schema(physical.schema().fields(), None)?;
    actual["physical_fields"] = fields(physical.schema().fields(), None);
    actual["physical_arrow_schema"] = json!(format!("{:?}", physical.schema()));
    actual["physical_plan"] = json!(format!("{physical:?}"));
    let captured_physical_schema = physical.schema();
    if let Some(counts) = producer { counts.phase.store(2, std::sync::atomic::Ordering::SeqCst); }
    let batches = match datafusion::physical_plan::collect(physical, session.task_ctx()).await {
        Ok(b) => b,
        Err(e) => {
            retain(&mut actual, failure(&e, "execute"))?;
            return Ok(actual);
        }
    };
    let mut rows = Vec::new();
    let mut batch_receipts = Vec::new();
    for b in batches {
        assert_eq!(b.schema(), captured_physical_schema);
        batch_receipts.push(
            json!({"rows":b.num_rows(),"fields":fields(b.schema().fields(),None),
            "metadata":b.schema().metadata(),"raw_schema":format!("{:?}",b.schema())}),
        );
        for r in 0..b.num_rows() {
            rows.push(
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(r) {
                            Ok(Value::Null)
                        } else {
                            array_value_to_string(a.as_ref(), r).map(Value::String)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    actual["rows"] = json!(rows);
    actual["batches"] = json!(batch_receipts);
    actual["status"] = json!("ok");
    Ok(actual)
}

fn literal_key(value: &Value) -> TestResult<String> {
    let sql = value["sql"].as_str().ok_or("missing literal SQL")?;
    let ansi = value["ansi"].as_bool().ok_or("missing literal ANSI mode")?;
    let representation = value["representation"]
        .as_u64()
        .filter(|value| *value < 3)
        .ok_or("invalid literal representation")?;
    let batch_size = value["batch_size"]
        .as_u64()
        .filter(|value| [1, 32].contains(value))
        .ok_or("invalid literal batch size")?;
    Ok(json!([sql, ansi, representation, batch_size]).to_string())
}

fn literal_controls() -> TestResult<BTreeMap<String, Value>> {
    let mut controls = BTreeMap::new();
    for (fixture, execution, count) in [
        (
            include_str!("fixtures/regexp-extract-literal-preparation-native.jsonl"),
            "bfaafcd009de427ea2611d8bad8a71afa1290e7c",
            42,
        ),
        (
            include_str!("fixtures/regexp-extract-local-values-native.jsonl"),
            "73423f2450992d5e1463f8922c2568a0c51a6525",
            30,
        ),
    ] {
        let mut records = fixture.lines().map(serde_json::from_str::<Value>);
        let header = records.next().ok_or("missing literal native header")??;
        assert_eq!(
            header["kind"],
            "complete-native-regression-controls-not-spark-oracle"
        );
        assert_eq!(
            header["source_product"],
            "0a3fa8605d8056f24c035dbba0c752857d158674"
        );
        assert_eq!(header["source_execution"], execution);
        assert_eq!(header["actuals_normalized"], false);
        let before = controls.len();
        for control in records {
            let control = control?;
            assert_eq!(control["accepted_as_spark_compatibility"], false);
            assert_eq!(control["full_compatibility"], false);
            assert!(
                controls
                    .insert(literal_key(&control["context"])?, control)
                    .is_none()
            );
        }
        assert_eq!(controls.len() - before, count);
        assert_eq!(header["observations"], count);
    }
    assert_eq!(controls.len(), 72);
    Ok(controls)
}

fn literal_fields_match(actual: &Value, physical: bool) -> bool {
    let (schema, raw) = if physical {
        (&actual["physical_schema"], &actual["physical_fields"])
    } else {
        (&actual["analyzed_schema"], &actual["analyzed_fields"])
    };
    let (Some(fields), Some(raw)) = (schema["fields"].as_array(), raw.as_array()) else {
        return false;
    };
    fields.len() == raw.len()
        && (physical
            || actual["query_column_names"]
                .as_array()
                .is_some_and(|names| names.len() == fields.len()))
        && fields
            .iter()
            .zip(raw)
            .enumerate()
            .all(|(index, (field, raw))| {
                let data_type = match field["type"].as_str() {
                    Some("string") => matches!(
                        raw["type"].as_str(),
                        Some("Utf8" | "LargeUtf8" | "Utf8View")
                    ),
                    Some("integer") => raw["type"] == "Int32",
                    _ => false,
                };
                data_type
                    && raw["name"].is_string()
                    && field["nullable"] == raw["nullable"]
                    && field["metadata"] == raw["metadata"]
                    && if physical {
                        field["name"] == raw["name"]
                    } else {
                        field["name"] == actual["query_column_names"][index]
                    }
            })
}

fn literal_complete(actual: &Value) -> bool {
    let mut keys: BTreeSet<_> = [
        "status",
        "analyzed_schema",
        "analyzed_fields",
        "analyzed_arrow_schema",
        "analyzed_plan",
        "query_column_names",
        "physical_schema",
        "physical_fields",
        "physical_arrow_schema",
        "physical_plan",
        "rand_seed",
    ]
    .into_iter()
    .collect();
    let analyzed = literal_fields_match(actual, false)
        && actual["analyzed_arrow_schema"].is_string()
        && actual["analyzed_plan"].is_string();
    let physical = literal_fields_match(actual, true)
        && actual["physical_arrow_schema"].is_string()
        && actual["physical_plan"].is_string();
    let completed = match actual["status"].as_str() {
        Some("error") => {
            keys.extend(["error_stage", "error", "error_debug", "error_chain"]);
            let chain = actual["error_chain"].as_array();
            let error = chain.is_some_and(|chain| {
                !chain.is_empty()
                    && chain.first() == actual.get("error")
                    && chain.iter().all(Value::is_string)
            }) && actual["error_debug"]
                .as_str()
                .is_some_and(|value| !value.is_empty());
            error
                && match actual["error_stage"].as_str() {
                    Some("physical") => {
                        analyzed
                            && [
                                "physical_schema",
                                "physical_fields",
                                "physical_arrow_schema",
                                "physical_plan",
                            ]
                            .into_iter()
                            .all(|key| actual.get(key).is_some_and(Value::is_null))
                    }
                    Some("execute") => analyzed && physical,
                    _ => false,
                }
        }
        Some("ok") => {
            keys.extend(["rows", "batches"]);
            analyzed
                && physical
                && actual["rows"].is_array()
                && actual["batches"].as_array().is_some_and(|batches| {
                    batches.iter().all(|batch| {
                        batch["fields"] == actual["physical_fields"]
                            && batch["metadata"].is_object()
                            && batch["raw_schema"].is_string()
                            && batch["rows"].is_u64()
                    }) && batches
                        .iter()
                        .filter_map(|batch| batch["rows"].as_u64())
                        .sum::<u64>()
                        == actual["rows"]
                            .as_array()
                            .map_or(0, |rows| rows.len() as u64)
                })
        }
        _ => false,
    };
    completed
        && actual.as_object().is_some_and(|actual| {
            actual.keys().map(String::as_str).collect::<BTreeSet<_>>() == keys
        })
        && availability(actual, "optimized_schema") == "absent"
}

fn literal_comparison(actual: &Value, expected: &Value) -> Value {
    let error = expected["status"] == "error";
    let mut facets = json!({
        "status":actual["status"] == expected["status"],
        "analyzed_schema":actual["analyzed_schema"] == expected["analyzed_schema"],
        "rows":if error {actual.get("rows").is_none()} else {actual.get("rows") == expected.get("rows")},
    });
    if let Some(physical) = expected.get("physical_schema") {
        facets["physical_schema"] = json!(actual.get("physical_schema") == Some(physical));
    }
    if error {
        let stage = match expected["phase"].as_str() {
            Some("optimized_schema") => "physical",
            Some("collect") => "execute",
            _ => "unknown Spark phase",
        };
        let message = actual["error"].as_str().unwrap_or("");
        let condition = expected["condition"].as_str().unwrap_or("");
        let identifier = if condition == "INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX" {
            group_error_suffix_matches(message, &expected["parameters"], Some(condition))
        } else if condition == "CAST_OVERFLOW" {
            actual["error_chain"].as_array().is_some_and(|chain| {
                chain.last().and_then(Value::as_str).is_some_and(|cause| {
                    let tagged = format!("[CAST_OVERFLOW]\ncaused by\nArrow error: {cause}");
                    message.ends_with(&tagged)
                        && chain
                            .iter()
                            .any(|value| value.as_str() == Some(tagged.as_str()))
                })
            })
        } else {
            false
        };
        let cause = if condition == "INVALID_PARAMETER_VALUE.PATTERN" {
            expected["parameters"]["functionName"] == "`regexp_extract`"
                && expected["parameters"]["value"] == "'['"
                && message.starts_with("Execution error: Spark `regexp_extract` function: Invalid regex pattern: regex parse error:")
                && message.ends_with("error: unclosed character class")
        } else {
            error_cause(actual, expected)
                && (condition != "CAST_OVERFLOW"
                    || (expected["parameters"]["sourceType"] == "\"DOUBLE\""
                        && actual["analyzed_plan"]
                            .as_str()
                            .is_some_and(|plan| plan.contains("Float64"))))
        };
        facets["phase"] = json!(actual["error_stage"] == stage);
        facets["diagnostic_identifier"] = json!(identifier);
        facets["meaningful_cause"] = json!(cause);
        facets["terminal_stage_availability"] = json!(
            actual["status"] == "error"
                && actual.get("rows").is_none()
                && actual.get("batches").is_none()
                && if stage == "physical" {
                    [
                        "physical_schema",
                        "physical_fields",
                        "physical_arrow_schema",
                        "physical_plan",
                    ]
                    .into_iter()
                    .all(|key| actual.get(key).is_some_and(Value::is_null))
                } else {
                    [
                        "physical_schema",
                        "physical_fields",
                        "physical_arrow_schema",
                        "physical_plan",
                    ]
                    .into_iter()
                    .all(|key| availability(actual, key) == "available")
                }
        );
    }
    json!({"facets":facets,"complete_actual":literal_complete(actual)})
}

fn literal_false_facets(comparison: &Value) -> TestResult<BTreeSet<String>> {
    let facets = comparison["facets"]
        .as_object()
        .ok_or("missing literal comparison facets")?;
    let mut result = BTreeSet::new();
    for (key, value) in facets {
        if !value.as_bool().ok_or("invalid literal comparison facet")? {
            result.insert(key.clone());
        }
    }
    if !comparison["complete_actual"]
        .as_bool()
        .ok_or("missing literal completeness facet")?
    {
        result.insert("complete_actual".to_owned());
    }
    Ok(result)
}

fn literal_satisfied(actual: &Value, comparison: &Value, control: Option<&Value>) -> bool {
    if comparison["complete_actual"] != true {
        return false;
    }
    let Ok(false_facets) = literal_false_facets(comparison) else {
        return false;
    };
    match control {
        None => false_facets.is_empty(),
        Some(control) => {
            control["actual"] == *actual
                && control["false_facets"].as_array().is_some_and(|facets| {
                    let declared = facets
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<BTreeSet<_>>();
                    declared.len() == facets.len() && declared == false_facets
                })
        }
    }
}

fn literal_provider_matches(actual: &Value, expected: &Value, representation: usize) -> bool {
    let Some(fields) = actual["fields"].as_array() else {
        return false;
    };
    let Some(declared) = expected["schema"]["fields"].as_array() else {
        return false;
    };
    let Some(rows) = actual["rows"].as_array() else {
        return false;
    };
    let Some(batches) = actual["batches"].as_array() else {
        return false;
    };
    actual["schema"] == expected["schema"]
        && actual["rows"] == expected["rows"]
        && actual["raw_schema"].is_string()
        && fields.len() == declared.len()
        && fields.iter().zip(declared).all(|(field, declared)| {
            let storage = match declared["type"].as_str() {
                Some("integer") => "Int32",
                Some("float") => "Float32",
                Some("double") => "Float64",
                Some("string") => ["Utf8", "LargeUtf8", "Utf8View"][representation],
                _ => return false,
            };
            field["name"] == declared["name"]
                && field["type"] == storage
                && field["nullable"] == declared["nullable"]
                && field["metadata"] == declared["metadata"]
        })
        && batches.iter().all(|batch| {
            batch["fields"] == actual["fields"]
                && batch["raw_schema"].is_string()
                && batch["rows"].is_u64()
        })
        && batches
            .iter()
            .filter_map(|batch| batch["rows"].as_u64())
            .sum::<u64>()
            == rows.len() as u64
}

fn literal_catalog() -> TestResult<(Value, Vec<Value>)> {
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-literal-preparation-inputs.json"
    ))?;
    let local_inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-local-values-inputs.json"
    ))?;
    assert_eq!(inputs["providers"], local_inputs["providers"]);
    let mut cases = Vec::new();
    let mut seen = HashSet::new();
    for (inputs, fixture, count) in [
        (
            &inputs,
            include_str!("fixtures/regexp-extract-literal-preparation-spark.jsonl"),
            34,
        ),
        (
            &local_inputs,
            include_str!("fixtures/regexp-extract-local-values-spark.jsonl"),
            12,
        ),
    ] {
        let mut records = fixture.lines().map(serde_json::from_str::<Value>);
        let header = records.next().ok_or("missing literal reference header")??;
        let additions = records.collect::<Result<Vec<_>, _>>()?;
        let catalog = inputs["cases"]
            .as_array()
            .ok_or("missing literal catalog")?;
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(additions.len(), count);
        assert_eq!(catalog.len(), count);
        for case in &additions {
            assert!(case["sql"].is_string() && case["ansi"].is_boolean());
            assert!(seen.insert(json!([case["sql"], case["ansi"]]).to_string()));
            assert_eq!(case["capture_observation"]["actual"], case["expected"]);
            assert_eq!(case["capture_observation"]["sql"], case["sql"]);
            assert_eq!(case["capture_observation"]["ansi"], case["ansi"]);
            assert_eq!(
                catalog
                    .iter()
                    .filter(|input| {
                        input["sql"] == case["sql"]
                            && input["ansi"] == case["ansi"]
                            && input["id"] == case["id"]
                    })
                    .count(),
                1
            );
            let providers = case["capture_providers"]
                .as_array()
                .ok_or("missing frozen providers")?;
            assert_eq!(providers.len(), 2);
            for provider in providers {
                assert_eq!(provider["id"], case["id"]);
                assert_eq!(provider["ansi"], case["ansi"]);
            }
            assert_eq!(
                providers
                    .iter()
                    .filter_map(|provider| provider["name"].as_str())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from(["c06_re_empty", "c06_re_mixed"])
            );
        }
        cases.extend(additions);
    }
    assert_eq!(cases.len(), 46);
    Ok((inputs, cases))
}

fn literal_input_metadata(inputs: &Value) -> TestResult<Value> {
    let mut definitions = Vec::new();
    for input in inputs["providers"]
        .as_array()
        .ok_or("missing literal providers")?
    {
        let declared = input["fields"]
            .as_array()
            .ok_or("missing provider declaration")?;
        let mut rows = input["rows"]
            .as_array()
            .ok_or("missing provider rows")?
            .clone();
        for row in &mut rows {
            for (index, value) in row
                .as_array_mut()
                .ok_or("invalid provider row")?
                .iter_mut()
                .enumerate()
            {
                if !value.is_null() && declared[index]["type"] == "integer" {
                    *value = json!(
                        value
                            .as_i64()
                            .ok_or("invalid typed integer input")?
                            .to_string()
                    );
                }
            }
        }
        definitions.push(
            json!({"name":input["name"],"schema":{"type":"struct","fields":declared},"rows":rows}),
        );
    }
    Ok(json!({"registered_inputs":definitions}))
}

#[tokio::test]
async fn literal_preparation_references_keep_fields_stages_and_native_limits() -> TestResult {
    let (inputs, cases) = literal_catalog()?;
    let mut controls = literal_controls()?;
    let metadata = literal_input_metadata(&inputs)?;
    let mut seen = HashSet::new();
    let mut providers = HashSet::new();
    let mut failures = Vec::new();
    let mut strict = 0;
    let mut retained = 0;
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
            register_inputs(&session, &metadata, representation, batch_size)?;
            for case in &cases {
                let context = json!({"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
                    "representation":representation,"batch_size":batch_size});
                let key = literal_key(&context)?;
                assert!(seen.insert(key.clone()));
                for receipt in case["capture_providers"]
                    .as_array()
                    .ok_or("missing Spark provider receipts")?
                {
                    let name = receipt["name"].as_str().ok_or("missing provider name")?;
                    assert!(providers.insert((key.clone(), name.to_owned())));
                    let actual = provider(&session, name).await?;
                    let matches =
                        literal_provider_matches(&actual, &receipt["actual"], representation);
                    eprintln!(
                        "c06 literal provider regression: {}",
                        json!({
                            "context":context,"name":name,"actual":actual,"matches_reference":matches
                        })
                    );
                    if !matches {
                        failures.push(format!("provider {name}: {context}"));
                    }
                }
                assert_query_marker_absent(&session);
                let actual = observe(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                )
                .await?;
                assert_query_marker_absent(&session);
                let comparison = literal_comparison(&actual, &case["expected"]);
                let control = controls.remove(&key);
                if let Some(control) = &control {
                    assert_eq!(control["context"], context);
                }
                let satisfied = literal_satisfied(&actual, &comparison, control.as_ref());
                let false_facets = literal_false_facets(&comparison)?;
                let strict_match = false_facets.is_empty();
                if strict_match && case["expected"]["condition"] == "CAST_OVERFLOW" {
                    for (before, after) in [("NaN", "Infinity"), ("Int32", "Int64")] {
                        let mut changed = actual.clone();
                        changed["error"] =
                            json!(actual["error"].as_str().unwrap().replace(before, after));
                        let changed_comparison = literal_comparison(&changed, &case["expected"]);
                        assert_eq!(changed_comparison["facets"]["meaningful_cause"], false);
                        assert!(!literal_satisfied(&changed, &changed_comparison, None));
                    }
                }
                strict += usize::from(strict_match);
                retained += usize::from(control.is_some());
                eprintln!(
                    "c06 literal preparation reference: {}",
                    json!({
                        "context":context,"actual":actual,"comparison":comparison,
                        "required_spark_facts":case["expected"],
                        "native_regression_contract":control.as_ref(),
                        "strict_reference_match":strict_match,"native_regression_satisfied":satisfied,
                        "remaining_differences":false_facets,
                        "retained_native_control":control.is_some(),
                        "native_control_classification":control.as_ref().map(|control| &control["classification"]),
                        "actual_phase":actual.get("error_stage"),
                        "native_optimized_schema_available":actual.get("optimized_schema").is_some(),
                        "full_compatibility_pass":false
                    })
                );
                if !satisfied {
                    failures.push(format!("query {context}: {comparison}"));
                }
            }
        }
    }
    assert_eq!(seen.len(), 276);
    assert_eq!(providers.len(), 552);
    assert!(controls.is_empty());
    assert_eq!((strict, retained), (204, 72));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn literal_preparation_contracts_reject_changed_facets() -> TestResult {
    let mut actual = json!({});
    assert!(retain(&mut actual, json!([])).is_err());
    assert_eq!(actual, json!({}));
    for malformed in [
        json!({}),
        json!({"facets":[],"complete_actual":true}),
        json!({"facets":{"status":null},"complete_actual":true}),
    ] {
        assert!(literal_false_facets(&malformed).is_err());
        assert!(!literal_satisfied(&json!({}), &malformed, None));
    }
    let (_, cases) = literal_catalog()?;
    let controls = literal_controls()?;
    for control in controls.values() {
        let case = cases
            .iter()
            .find(|case| {
                case["sql"] == control["context"]["sql"]
                    && case["ansi"] == control["context"]["ansi"]
            })
            .ok_or("unknown native control")?;
        let actual = &control["actual"];
        let comparison = literal_comparison(actual, &case["expected"]);
        assert!(literal_satisfied(actual, &comparison, Some(control)));
        for facets in [
            json!(["unknown"]),
            json!([null]),
            json!(["physical_schema", "physical_schema"]),
        ] {
            let mut changed = control.clone();
            changed["false_facets"] = facets;
            assert!(!literal_satisfied(actual, &comparison, Some(&changed)));
        }
        for (field, value) in [
            ("status", json!("changed")),
            ("rows", json!([["changed"]])),
            ("analyzed_arrow_schema", Value::Null),
            ("physical_fields", json!([])),
            ("error_stage", json!("resolve")),
            ("unexpected", json!(true)),
        ] {
            let mut changed = actual.clone();
            changed[field] = value;
            assert!(!literal_satisfied(
                &changed,
                &literal_comparison(&changed, &case["expected"]),
                Some(control)
            ));
        }
        let mut changed = actual.clone();
        changed.as_object_mut().unwrap().remove("status");
        assert!(!literal_satisfied(
            &changed,
            &literal_comparison(&changed, &case["expected"]),
            Some(control)
        ));
        let mut changed = control["context"].clone();
        changed["representation"] = json!(3);
        assert!(literal_key(&changed).is_err());
        if actual["error"]
            .as_str()
            .is_some_and(|error| error.contains("REGEX_GROUP_INDEX"))
        {
            for (before, after) in [
                ("REGEX_GROUP_INDEX", "WRONG_CONDITION"),
                ("`regexp_extract`", "`regexp_extract_all`"),
                ("between 0 and 1", "between 0 and 2"),
                ("got ", "got changed "),
                ("Expects group index", "changed Expects group index"),
                ("[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] ", ""),
            ] {
                let mut changed = actual.clone();
                changed["error"] = json!(actual["error"].as_str().unwrap().replace(before, after));
                let comparison = literal_comparison(&changed, &case["expected"]);
                assert_eq!(comparison["facets"]["diagnostic_identifier"], false);
                assert!(!literal_satisfied(&changed, &comparison, Some(control)));
            }
            let mut changed = actual.clone();
            changed["error"] = json!(format!(
                "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] {}",
                actual["error"]
                    .as_str()
                    .unwrap()
                    .replace("[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] ", "")
            ));
            let comparison = literal_comparison(&changed, &case["expected"]);
            assert_eq!(comparison["facets"]["diagnostic_identifier"], false);
            assert_eq!(comparison["facets"]["meaningful_cause"], true);
            assert!(!literal_satisfied(&changed, &comparison, Some(control)));
        }
    }
    Ok(())
}


// Exact producer priority is separate from the retained historical native profiles.
fn producer_comparison(actual: &Value, expected: &Value) -> Value {
    let mut comparison = literal_comparison(actual, expected);
    if expected["condition"] != "CAST_INVALID_INPUT" { return comparison; }
    let parameters = &expected["parameters"];
    let cause = actual["error_chain"].as_array().and_then(|chain| chain.last()).and_then(Value::as_str);
    let input = json!({"expected":expected,"spark_error_details":{"message_parameters":parameters}});
    let meaningful = parameters["sourceType"] == "\"STRING\""
        && parameters["targetType"] == "\"INT\""
        && spark_results::matches_spark_int32_cast_input(&input,actual)
        && cause.is_some_and(|cause| {
            parameters["expression"].as_str().is_some_and(|value| {
                cause == format!("Cast error: Cannot cast string {value} to value of Int32 type")
            })
        });
    let identifier = cause.is_some_and(|cause| {
        let tagged = format!("[CAST_INVALID_INPUT]\ncaused by\nArrow error: {cause}");
        actual["error"].as_str().is_some_and(|message| message.ends_with(&tagged))
            && actual["error_chain"].as_array().is_some_and(|chain| {
                chain.iter().any(|value| value.as_str() == Some(tagged.as_str()))
            })
    });
    comparison["facets"]["meaningful_cause"] = json!(meaningful);
    comparison["facets"]["diagnostic_identifier"] = json!(identifier);
    comparison
}

fn producer_catalog() -> TestResult<(Value, Vec<Value>)> {
    let (inputs, original) = literal_catalog()?;
    let local: Value = serde_json::from_str(include_str!("fixtures/regexp-extract-local-values-inputs.json"))?;
    let local_keys = local["cases"].as_array().ok_or("missing local cases")?.iter()
        .map(|case| json!([case["sql"],case["ansi"]]).to_string()).collect::<HashSet<_>>();
    let mut cases = original.into_iter().filter(|case| {
        local_keys.contains(&json!([case["sql"],case["ansi"]]).to_string())
    }).collect::<Vec<_>>();
    assert_eq!(cases.len(),12);
    let providers = include_str!("fixtures/regexp-extract-local-priority-providers.jsonl")
        .lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>,_>>()?;
    assert_eq!(providers.len(),30);
    for line in include_str!("fixtures/regexp-extract-local-priority-spark.jsonl").lines() {
        let observation: Value = serde_json::from_str(line)?;
        assert_eq!(observation["issue"],1049);
        let receipts = providers.iter().filter(|provider| provider["id"] == observation["id"]
            && provider["ansi"] == observation["ansi"]).cloned().collect::<Vec<_>>();
        assert_eq!(receipts.len(),3);
        cases.push(json!({"id":observation["id"],"sql":observation["sql"],"ansi":observation["ansi"],
            "expected":observation["actual"],"capture_observation":observation,
            "capture_providers":receipts}));
    }
    let identities = cases.iter().map(|case| json!([case["sql"],case["ansi"]]).to_string()).collect::<HashSet<_>>();
    assert_eq!(cases.len(),22);assert_eq!(identities.len(),22);
    for case in &cases {
        assert_eq!(case["expected"],case["capture_observation"]["actual"]);
        assert_eq!(case["sql"],case["capture_observation"]["sql"]);
        assert_eq!(case["ansi"],case["capture_observation"]["ansi"]);
        let receipts = case["capture_providers"].as_array().ok_or("missing provider receipts")?;
        let names = receipts.iter().map(|p| p["name"].as_str()).collect::<BTreeSet<_>>();
        assert_eq!(names.len(),receipts.len());
        for receipt in receipts {
            assert_eq!(receipt["id"],case["id"]);assert_eq!(receipt["ansi"],case["ansi"]);
        }
    }
    Ok((inputs,cases))
}

fn producer_actual_faults(actual: &Value, expected: &Value, control: Option<&Value>) -> Vec<Value> {
    let mut faults = Vec::new();
    let mut changed = actual.clone();changed["unexpected_key"] = json!(true);faults.push(changed);
    let mut changed = actual.clone();changed["analyzed_schema"]["fields"][0]["metadata"] = json!({"wrong":"metadata"});faults.push(changed);
    let mut changed = actual.clone();changed["analyzed_fields"][0]["nullable"] = json!(!actual["analyzed_fields"][0]["nullable"].as_bool().unwrap());faults.push(changed);
    let mut changed = actual.clone();changed["analyzed_schema"]["fields"][0]["name"] = json!("wrong");faults.push(changed);
    if actual["status"] == "error" {
        for (from,to) in [("REGEX_GROUP_INDEX","WRONG_CONDITION"),("regexp_extract","regexp_extract_all"),
            ("between 0 and 1","between 0 and 8"),("got 2","got 9"),
            ("CAST_INVALID_INPUT","WRONG_CONDITION"),("'bad'","'wrong'"),("Int32","Int64")] {
            if !actual["error"].as_str().unwrap().contains(from) { continue; }
            let mut changed = actual.clone();
            changed["error"] = json!(actual["error"].as_str().unwrap().replace(from,to));
            changed["error_chain"] = json!(actual["error_chain"].as_array().unwrap().iter()
                .map(|cause| cause.as_str().unwrap().replace(from,to)).collect::<Vec<_>>());
            faults.push(changed);
        }
        let tag = format!("[{}]",expected["condition"].as_str().unwrap());
        for detached in [false,true] {
            let mut changed = actual.clone();
            let bare = actual["error"].as_str().unwrap().replace(&tag,"");
            changed["error"] = json!(if detached {format!("{tag}\n{bare}")} else {bare});
            changed["error_chain"] = json!(actual["error_chain"].as_array().unwrap().iter()
                .map(|cause| cause.as_str().unwrap().replace(&tag,"")).collect::<Vec<_>>());
            faults.push(changed);
        }
        let mut changed = actual.clone();changed["error"] = json!(format!("{} changed cause",actual["error"].as_str().unwrap()));faults.push(changed);
        let mut changed = actual.clone();changed["error_stage"] = json!("execute");faults.push(changed);
        let mut changed = actual.clone();changed["physical_fields"] = json!([]);faults.push(changed);
        let mut changed = actual.clone();changed["rows"] = json!([]);faults.push(changed);
    } else {
        let mut changed = actual.clone();changed["rows"] = if actual["rows"] == json!([]) {json!([[null]])} else {json!([])};faults.push(changed);
        let mut changed = actual.clone();changed["physical_schema"]["fields"][0]["metadata"] = json!({"wrong":"metadata"});faults.push(changed);
    }
    for fault in &faults {
        assert!(!literal_satisfied(fault,&producer_comparison(fault,expected),control),"fault accepted: {fault}");
    }
    faults
}

#[tokio::test]
async fn local_regexp_producer_references_keep_all_132_contexts() -> TestResult {
    let (inputs,cases) = producer_catalog()?;
    let metadata = literal_input_metadata(&inputs)?;
    let floating: Value = serde_json::from_str(include_str!("fixtures/regexp-extract-local-priority-inputs.json"))?;
    let mut refinements = literal_controls()?.into_iter().filter(|(_,control)| {
        control["context"]["id"] == "values_literal_double_nan" && control["context"]["ansi"] == false
    }).collect::<BTreeMap<_,_>>();
    assert_eq!(refinements.len(),6);
    for control in refinements.values() {assert_eq!(control["false_facets"],json!(["physical_schema"]));}
    let mut seen = HashSet::new();let mut receipts = HashSet::new();let mut failures = Vec::new();
    let mut captured = Vec::new();
    for representation in 0..3 {
        for batch_size in [1,32] {
            let session = SessionContext::new_with_state(configure_spark_sql(SessionStateBuilder::new()
                .with_default_features().with_config(SessionConfig::new().with_batch_size(batch_size)
                    .with_target_partitions(2))).build());
            register_inputs(&session,&metadata,representation,batch_size)?;
            spark_inputs::register_recorded_inputs(&session,&floating,batch_size)?;
            for case in &cases {
                let context = json!({"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
                    "representation":representation,"batch_size":batch_size});
                let key = literal_key(&context)?;assert!(seen.insert(key.clone()));
                for receipt in case["capture_providers"].as_array().ok_or("missing providers")? {
                    let name = receipt["name"].as_str().ok_or("missing provider name")?;
                    assert!(receipts.insert((key.clone(),name.to_owned())));
                    let actual = provider(&session,name).await?;
                    let matches = literal_provider_matches(&actual,&receipt["actual"],representation);
                    eprintln!("c06 local producer provider: {}",json!({"context":context,"name":name,
                        "actual":actual,"matches_reference":matches}));
                    if !matches {failures.push(format!("provider {name}: {context}"));}
                }
                assert_query_marker_absent(&session);
                let actual = observe(&session,case["sql"].as_str().ok_or("missing SQL")?,
                    case["ansi"].as_bool().ok_or("missing ANSI")?).await?;
                assert_query_marker_absent(&session);
                let comparison = producer_comparison(&actual,&case["expected"]);
                let control = refinements.remove(&key);
                let satisfied = literal_satisfied(&actual,&comparison,control.as_ref());
                let remaining = literal_false_facets(&comparison)?;
                eprintln!("c06 local producer reference: {}",json!({"context":context,"actual":actual,
                    "comparison":comparison,"required_spark_facts":case["expected"],
                    "strict_reference_match":remaining.is_empty(),"remaining_differences":remaining,
                    "native_regression_satisfied":satisfied,"whole_native_control":control,
                    "compatibility_credit":false}));
                if !satisfied {failures.push(format!("query: {context}"));}
                captured.push((actual,case.clone(),control,satisfied));
            }
        }
    }
    assert_eq!(seen.len(),132);assert_eq!(receipts.len(),324);assert!(refinements.is_empty());
    // All observations exist before fault checks or semantic failure assertions.
    let mut faults = 0;
    for (actual,case,control,satisfied) in &captured {
        if *satisfied {faults += producer_actual_faults(actual,&case["expected"],control.as_ref()).len();}
    }
    eprintln!("c06 local producer summary: {}",json!({"observations":seen.len(),"providers":receipts.len(),
        "faults_checked":faults,"failures":failures,"compatibility_credit":false}));
    assert!(failures.is_empty(),"{}",failures.join("\n"));
    Ok(())
}

// This records native baselines, including unresolved async outcomes. It does
// not grant an exception to a Spark reference or predict a baseline profile.
#[tokio::test]
async fn local_literal_producer_native_profiles() -> TestResult {
    use literal_producer::{Kind, producer};
    let mut seen = HashSet::new();
    for kind in [Kind::ImmutableNoFold,Kind::VolatileFold,Kind::AsyncFold,Kind::ImmutableFold] {
        for ansi in [true,false] {
            let session = SessionContext::new_with_state(configure_spark_sql(
                SessionStateBuilder::new().with_default_features().with_config(
                    SessionConfig::new().with_batch_size(32).with_target_partitions(1),
                )).build());
            let (callback,counts) = producer(kind);
            let name = callback.name().to_owned();
            assert_eq!(callback.inner().supports_constant_folding(),kind != Kind::ImmutableNoFold);
            assert_eq!(callback.signature().volatility == datafusion::logical_expr::Volatility::Volatile,
                kind == Kind::VolatileFold);
            assert_eq!(callback.as_async().is_some(),kind == Kind::AsyncFold);
            session.register_udf(callback);
            let (index,limit) = if kind == Kind::ImmutableFold {(1,"")} else {(2," LIMIT 0")};
            let produced = if kind == Kind::ImmutableFold {", produced"} else {""};
            let sql = format!("SELECT regexp_extract('a','(a)',{index}) AS r{produced} FROM \
                (SELECT {name}(k) AS produced FROM VALUES(1) AS t(k)) AS q{limit}");
            assert!(seen.insert((name.clone(),ansi)));
            assert_query_marker_absent(&session);
            let actual = observe_with_producer(&session,&sql,ansi,Some(&counts)).await?;
            assert_query_marker_absent(&session);
            let snapshot = counts.snapshot();
            let events = snapshot["events"].as_array().ok_or("missing actual producer events")?;
            let phase_calls = (0..3).map(|phase| events.iter()
                .filter(|event| event["phase"] == phase).count()).collect::<Vec<_>>();
            eprintln!("c06 local literal producer native profile: {}",json!({
                "kind":format!("{kind:?}"),"sql":sql,"ansi":ansi,"actual":actual,
                "availability":availability_record(&actual),"counts":snapshot,
                "calls_by_phase_resolve_physical_execute":phase_calls,
                "native_baseline_profile_required":true,"compatibility_credit":false}));
        }
    }
    assert_eq!(seen.len(),8);
    Ok(())
}

fn availability_record(actual: &Value) -> Value {
    let mut values = serde_json::Map::new();
    for key in ["analyzed_schema","analyzed_fields","analyzed_arrow_schema","analyzed_plan",
        "physical_schema","physical_fields","physical_arrow_schema","physical_plan",
        "rows","batches","optimized_schema"] {
        values.insert(key.to_owned(),json!(availability(actual,key)));
    }
    Value::Object(values)
}

#[test]
fn local_producer_diagnostics_reject_changed_real_causes() -> TestResult {
    let (_,cases) = producer_catalog()?;
    let samples = include_str!("fixtures/regexp-extract-local-producer-diagnostic-samples.jsonl")
        .lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>,_>>()?;
    assert_eq!(samples.len(),2);
    for sample in samples {
        assert_eq!(sample["product_commit"],"5ca1b9d461dcaec1c883f17e9a0ae1212a5a8f23");
        assert_eq!(sample["execution_commit"],"3b0c5fd1b98a88ae93a7a7d228f50e7ae8a33d50");
        let record = &sample["raw_complete_observation"];
        let expected = &cases.iter().find(|case| case["sql"] == record["sql"]
            && case["ansi"] == record["ansi"]).ok_or("missing exact sample reference")?["expected"];
        let actual = &record["actual"];
        let comparison = producer_comparison(actual,expected);
        assert_eq!(comparison["complete_actual"],true);
        assert_eq!(comparison["facets"]["meaningful_cause"],true);
        assert_eq!(comparison["facets"]["diagnostic_identifier"],true);
        let condition = expected["condition"].as_str().ok_or("missing condition")?;
        let tag = format!("[{condition}]");
        for replacement in ["","[WRONG_CONDITION]"] {
            let mut changed = actual.clone();
            changed["error"] = json!(actual["error"].as_str().unwrap().replace(&tag,replacement));
            changed["error_chain"] = json!(actual["error_chain"].as_array().unwrap().iter()
                .map(|cause| cause.as_str().unwrap().replace(&tag,replacement)).collect::<Vec<_>>());
            assert_eq!(producer_comparison(&changed,expected)["facets"]["diagnostic_identifier"],false);
        }
        let mut detached = actual.clone();
        detached["error_chain"] = json!(actual["error_chain"].as_array().unwrap().iter()
            .map(|cause| cause.as_str().unwrap().replace(&tag,"")).collect::<Vec<_>>());
        detached["error"] = json!(format!("{tag}\n{}",detached["error_chain"][0].as_str().unwrap()));
        detached["error_chain"][0] = detached["error"].clone();
        assert_eq!(producer_comparison(&detached,expected)["facets"]["diagnostic_identifier"],false);
        let replacements = if condition == "CAST_INVALID_INPUT" {
            vec![("'bad'","'wrong'"),("Int32","Int64")]
        } else {
            vec![("regexp_extract","regexp_extract_all"),("between 0 and 1","between 0 and 8"),("got 2","got 9")]
        };
        for (from,to) in replacements {
            assert!(actual["error"].as_str().unwrap().contains(from));
            let mut changed = actual.clone();
            changed["error"] = json!(actual["error"].as_str().unwrap().replace(from,to));
            changed["error_chain"] = json!(actual["error_chain"].as_array().unwrap().iter()
                .map(|cause| cause.as_str().unwrap().replace(from,to)).collect::<Vec<_>>());
            assert_eq!(producer_comparison(&changed,expected)["facets"]["meaningful_cause"],false);
        }
        if condition == "CAST_INVALID_INPUT" {
            assert!(literal_false_facets(&comparison)?.is_empty());
            assert!(!producer_actual_faults(actual,expected,None).is_empty());
        } else {
            // This real before-repair group sample still has an execution-phase gap.
            assert_eq!(comparison["facets"]["phase"],false);
        }
    }
    Ok(())
}
