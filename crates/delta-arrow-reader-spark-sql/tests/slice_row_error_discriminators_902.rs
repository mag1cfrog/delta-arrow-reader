//! Replay verified #902 Spark records with one partition of ordered input batches.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::physical_plan::{ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SPARK_REFERENCE_SHA256: &str =
    "4d1f61567e1f460561a931b55e805318dde7b09b91c55735b9a9fe97abb5ebc6";
const MANIFEST_SHA256: &str = "e95ecc0a4eaab782343f5d15ae00797ec2e8ab2bf52da8737c79de5f46e20f05";
const CAPTURE_SHA256: &str = "bac7a1ec14f49b23e56cdbf4b173c66892defc26acd8f28e0d62d8ae56a38a45";
const COMPARISON_FLAGS: &[&str] = &[
    "matches",
    "optimized_matches",
    "public_matches",
    "before_matches",
    "physical_matches",
    "rows_match",
    "ordered_rows_match",
    "error_matches",
    "cast_input_matches",
];
const SCHEMA_STAGES: &[&str] = &[
    "public",
    "resolved",
    "analyzed",
    "optimized",
    "physical",
    "executed_before_collect",
    "executed",
];
const PROVIDER_INPUT_SQL: &str =
    "SELECT k, array_text, start_text, length_text FROM slice_row_priority";

fn recorded_source_sha256(source: &str) -> TestResult<String> {
    let digest = datafusion::functions::crypto::sha256().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            source.into(),
        )))],
        arg_fields: vec![Arc::new(Field::new("source", DataType::Utf8, false))],
        number_rows: 1,
        return_field: Arc::new(Field::new("digest", DataType::Binary, false)),
        config_options: Default::default(),
    })?;
    let ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) = digest else {
        return Err("SHA256 did not return a binary scalar".into());
    };
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn observer_panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).into())
        })
        .unwrap_or_else(|| "non-string panic payload".into())
}

fn strict_discriminator_matches(case: &Value, observation: &Value) -> bool {
    let required_stages = if case["expected"]["condition"].is_string() {
        6
    } else {
        7
    };
    COMPARISON_FLAGS
        .iter()
        .all(|flag| observation[flag] == true)
        && observation["provider_input_matches"] == true
        && observation["scenario_input_matches"] == true
        && observation["schema_stage_matches"]
            .as_object()
            .is_some_and(|stages| {
                stages.len() == required_stages
                    && SCHEMA_STAGES[..required_stages]
                        .iter()
                        .all(|stage| stages.get(*stage) == Some(&Value::Bool(true)))
            })
        && observation.get("observer_error").is_none()
        && observation.get("panic").is_none()
}

async fn observe_input_partition(session: &SessionContext, sql: &str) -> Value {
    let mut observation = json!({"sql":sql});
    let mut phase = "sql";
    let result: TestResult = async {
        let frame = session.sql(sql).await?;
        phase = "public_schema";
        observation["public_schema"] =
            spark_results::capture_schema(frame.schema().fields(), None)?;
        phase = "create_physical_plan";
        let physical = frame.create_physical_plan().await?;
        let schema = physical.schema();
        observation["physical_schema"] = spark_results::capture_schema(schema.fields(), None)?;
        observation["partitions"] = json!(physical.output_partitioning().partition_count());
        observation["physical_plan"] =
            json!(displayable(physical.as_ref()).indent(true).to_string());
        phase = "collect";
        let batches = collect(physical, session.task_ctx()).await?;
        phase = "encode_rows";
        let mut rows = Vec::new();
        let mut batch_rows = Vec::new();
        for batch in batches {
            spark_results::validate_batch(schema.as_ref(), &batch)?;
            batch_rows.push(batch.num_rows());
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
        observation["rows_raw"] = json!(rows);
        observation["batch_rows"] = json!(batch_rows);
        Ok(())
    }
    .await;
    observation["phase"] = json!(phase);
    observation["error"] = json!(result.as_ref().err().map(ToString::to_string));
    observation
}

fn input_partition_matches(observation: &Value, expected: &Value) -> bool {
    observation.get("observer_error").is_none()
        && observation.get("panic").is_none()
        && observation["error"].is_null()
        && observation["phase"] == "encode_rows"
        && observation["partitions"] == 1
        && observation["public_schema"] == expected["schema"]
        && observation["physical_schema"] == expected["schema"]
        && observation["rows_raw"] == expected["rows_in_partition_order"]
}

async fn catch_input_observation(session: &SessionContext, sql: &str) -> Value {
    match std::panic::AssertUnwindSafe(observe_input_partition(session, sql))
        .catch_unwind()
        .await
    {
        Ok(observation) => observation,
        Err(payload) => json!({"sql":sql,"panic":observer_panic_message(payload)}),
    }
}

#[test]
fn discriminator_gates_reject_incomplete_stages_cast_causes_and_input_order() -> TestResult {
    assert_eq!(
        recorded_source_sha256("abc")?,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    for error_case in [false, true] {
        let case = if error_case {
            json!({"expected":{"condition":"CAST_INVALID_INPUT"}})
        } else {
            json!({"expected":{"rows":[]}})
        };
        let mut observation = json!({"provider_input_matches":true,"scenario_input_matches":true});
        for flag in COMPARISON_FLAGS {
            observation[flag] = json!(true);
        }
        let stages = if error_case {
            &SCHEMA_STAGES[..6]
        } else {
            SCHEMA_STAGES
        };
        for stage in stages {
            observation["schema_stage_matches"][stage] = json!(true);
        }
        assert!(strict_discriminator_matches(&case, &observation));
        for flag in COMPARISON_FLAGS
            .iter()
            .copied()
            .chain(["provider_input_matches", "scenario_input_matches"])
        {
            let mut wrong = observation.clone();
            wrong[flag] = json!(false);
            assert!(!strict_discriminator_matches(&case, &wrong), "{flag}");
        }
        for stage in stages {
            let mut wrong = observation.clone();
            wrong["schema_stage_matches"][stage] = Value::Null;
            assert!(!strict_discriminator_matches(&case, &wrong), "{stage}");
        }
    }
    let case = json!({"expected":{"condition":"CAST_INVALID_INPUT"},
        "spark_error_details":{"message_parameters":{"expression":"'start_bad'"}}});
    for input in ["start_bad", "length_bad", "array_bad"] {
        let observation = json!({"error":format!(
            "[CAST_INVALID_INPUT]\nArrow error: Cast error: Cannot cast string '{input}' to value of Int32 type")});
        assert_eq!(
            spark_results::matches_spark_int32_cast_input(&case, &observation),
            input == "start_bad"
        );
    }
    let expected =
        json!({"schema":{"type":"struct","fields":[]},"rows_in_partition_order":[["0"],["1"]]});
    let input = json!({"public_schema":expected["schema"],"physical_schema":expected["schema"],
        "partitions":1,"phase":"encode_rows","error":null,"rows_raw":[["0"],["1"]]});
    assert!(input_partition_matches(&input, &expected));
    let mut reversed = input.clone();
    reversed["rows_raw"] = json!([["1"], ["0"]]);
    assert!(!input_partition_matches(&reversed, &expected));
    let mut partitions = input;
    partitions["partitions"] = json!(2);
    assert!(!input_partition_matches(&partitions, &expected));
    Ok(())
}

#[tokio::test]
async fn slice_new_row_error_discriminators_match_verified_spark() -> TestResult {
    let raw = include_str!("fixtures/slice-row-priority-902/spark-reference.jsonl");
    let manifest_source = include_str!("fixtures/slice-row-priority-902/manifest.json");
    let capture_source = include_str!("fixtures/slice-row-priority-902/capture_spark.py");
    assert_eq!(recorded_source_sha256(raw)?, SPARK_REFERENCE_SHA256);
    assert_eq!(recorded_source_sha256(manifest_source)?, MANIFEST_SHA256);
    assert_eq!(recorded_source_sha256(capture_source)?, CAPTURE_SHA256);
    let manifest: Value = serde_json::from_str(manifest_source)?;
    let records = raw
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 25);
    let header = &records[0];
    assert_eq!(header["group"], "slice-902-row-priority-missing-v1");
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        header["source_commit"],
        "d2f9e5eb50d50f9eae0c285b51bc52ab4ecf5cdf"
    );
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["process_timezone"], "UTC");
    assert_eq!(header["provider_input_roundtrip_checked"], true);
    assert_eq!(header["spark_master"], "local[1]");
    assert_eq!(header["capture_sha256"], CAPTURE_SHA256);
    assert_eq!(header["manifest_sha256"], MANIFEST_SHA256);
    assert_eq!(manifest["capture_script_sha256"], CAPTURE_SHA256);
    for key in [
        "inputs",
        "input_encoding",
        "group",
        "spark_version",
        "spark_revision",
        "source_commit",
        "timezone",
        "spark_config",
    ] {
        assert_eq!(header[key], manifest[key], "{key}");
    }
    let inputs = header["inputs"].as_array().ok_or("missing inputs")?;
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input["name"], "slice_row_priority");
    assert_eq!(
        input["schema"],
        json!({"type":"struct","fields":[
            {"name":"k","type":"integer","nullable":false,"metadata":{}},
            {"name":"array_text","type":"string","nullable":false,"metadata":{}},
            {"name":"start_text","type":"string","nullable":false,"metadata":{}},
            {"name":"length_text","type":"string","nullable":false,"metadata":{}},
        ]})
    );
    let input_rows = input["rows"].as_array().ok_or("missing provider rows")?;
    assert_eq!(input_rows.len(), 24);
    for (index, row) in input_rows.iter().enumerate() {
        assert_eq!(row[0], index.to_string());
        assert_eq!(row.as_array().ok_or("missing provider row")?.len(), 4);
        assert!(
            row.as_array()
                .ok_or("missing provider row")?
                .iter()
                .all(Value::is_string)
        );
    }
    let provider_orders = header["provider_partition_orders"]
        .as_array()
        .ok_or("missing provider order")?;
    assert_eq!(provider_orders.len(), 1);
    assert_eq!(provider_orders[0]["name"], input["name"]);
    assert_eq!(provider_orders[0]["partitions"], 1);
    assert_eq!(provider_orders[0]["rows_in_partition_order"], input["rows"]);
    let provider_expected =
        json!({"schema":input["schema"],"rows_in_partition_order":input["rows"]});
    let definitions = manifest["scenarios"]
        .as_array()
        .ok_or("missing scenarios")?;
    let input_roundtrips = header["scenario_input_roundtrips"]
        .as_array()
        .ok_or("missing scenario inputs")?;
    assert_eq!(definitions.len(), 12);
    assert_eq!(input_roundtrips.len(), 12);
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let mut outcomes = (0, 0);
    for (definition, roundtrip) in definitions.iter().zip(input_roundtrips) {
        assert_eq!(roundtrip["id"], definition["id"]);
        assert_eq!(roundtrip["partitions"], 1);
        assert_eq!(roundtrip["schema"], input["schema"]);
        let keys = definition["input_keys"]
            .as_array()
            .ok_or("missing input keys")?;
        assert_eq!(keys.len(), 2);
        let selected = input_rows
            .iter()
            .filter(|row| keys.contains(&row[0]))
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), 2);
        assert_eq!(json!(selected), roundtrip["rows_in_partition_order"]);
        assert_eq!(
            json!(
                selected
                    .iter()
                    .map(|row| row[0].clone())
                    .collect::<Vec<_>>()
            ),
            definition["input_keys"]
        );
        assert!(
            !definition["input_sql"]
                .as_str()
                .ok_or("missing input SQL")?
                .contains("ORDER BY")
        );
    }
    for case in &records[1..] {
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("unknown case")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{key}");
        }
        let required_stages = if case["expected"]["condition"].is_string() {
            outcomes.0 += 1;
            assert_eq!(case["spark_error_phase"], "collect");
            assert_eq!(case["query_failure_kind"], "sql_error");
            assert_eq!(case["spark_error_details"]["metadata_errors"], json!([]));
            6
        } else {
            outcomes.1 += 1;
            assert_eq!(case["ansi"], false);
            assert_eq!(case["expected"]["rows"], case["spark_collected_rows"]);
            7
        };
        let schemas = case["schema_stages"]
            .as_object()
            .ok_or("missing reference schemas")?;
        assert_eq!(schemas.len(), required_stages);
        for stage in &SCHEMA_STAGES[..required_stages] {
            assert!(schemas.get(*stage).is_some_and(Value::is_object), "{stage}");
        }
    }
    assert_eq!(outcomes, (20, 4));
    let expected_completions = [1_usize, 32]
        .into_iter()
        .flat_map(|batch| {
            [true, false].into_iter().flat_map(move |ansi| {
                definitions.iter().map(move |case| {
                    (
                        case["id"].as_str().unwrap_or_default().to_owned(),
                        ansi,
                        batch,
                    )
                })
            })
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(expected_completions.len(), 48);
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let setup = std::panic::catch_unwind(|| -> TestResult<SessionContext> {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch_size)
                                .with_target_partitions(1),
                        ),
                )
                .build(),
            );
            let batch = spark_inputs::decode_recorded_batch(input)?;
            let chunks = (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect::<Vec<_>>();
            session.register_table(
                "slice_row_priority",
                Arc::new(MemTable::try_new(batch.schema(), vec![chunks])?),
            )?;
            Ok(session)
        });
        let setup = match setup {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(payload) => Err(observer_panic_message(payload)),
        };
        let mut provider_observation = match &setup {
            Ok(session) => catch_input_observation(session, PROVIDER_INPUT_SQL).await,
            Err(error) => json!({"observer_error":error,"observer_phase":"setup"}),
        };
        let provider_matches = input_partition_matches(&provider_observation, &provider_expected);
        provider_observation["input_scope"] = json!("provider");
        provider_observation["batch"] = json!(batch_size);
        provider_observation["input_matches"] = json!(provider_matches);
        eprintln!("SLICE_ROW_PRIORITY_902_INPUT_OBSERVATION {provider_observation}");
        for case in &records[1..] {
            let expected_input = input_roundtrips
                .iter()
                .find(|roundtrip| roundtrip["id"] == case["id"])
                .ok_or("missing case input")?;
            let mut input_observation = match &setup {
                Ok(session) => {
                    catch_input_observation(
                        session,
                        case["input_sql"].as_str().ok_or("missing input SQL")?,
                    )
                    .await
                }
                Err(error) => json!({"observer_error":error,"observer_phase":"setup"}),
            };
            let scenario_matches = input_partition_matches(&input_observation, expected_input);
            input_observation["input_scope"] = json!("scenario");
            input_observation["id"] = case["id"].clone();
            input_observation["ansi"] = case["ansi"].clone();
            input_observation["batch"] = json!(batch_size);
            input_observation["input_matches"] = json!(scenario_matches);
            eprintln!("SLICE_ROW_PRIORITY_902_INPUT_OBSERVATION {input_observation}");
            let mut observation = match &setup {
                Ok(session) if provider_matches && scenario_matches => {
                    match std::panic::AssertUnwindSafe(
                        spark_results::compare_exact_spark_query_stages_with_optimized(
                            session, case,
                        ),
                    )
                    .catch_unwind()
                    .await
                    {
                        Ok(Ok(observation)) => observation,
                        Ok(Err(error)) => json!({"observer_error":error.to_string()}),
                        Err(payload) => json!({"panic":observer_panic_message(payload)}),
                    }
                }
                Ok(_) => {
                    json!({"observer_error":"input roundtrip differs","observer_phase":"input_roundtrip"})
                }
                Err(error) => json!({"observer_error":error,"observer_phase":"setup"}),
            };
            observation["group"] = header["group"].clone();
            observation["id"] = case["id"].clone();
            observation["sql"] = case["sql"].clone();
            observation["ansi"] = case["ansi"].clone();
            observation["batch"] = json!(batch_size);
            observation["spark_reference_sha256"] = json!(SPARK_REFERENCE_SHA256);
            observation["cast_input_matches"] = json!(
                spark_results::matches_spark_int32_cast_input(case, &observation)
            );
            observation["provider_input_matches"] = json!(provider_matches);
            observation["scenario_input_matches"] = json!(scenario_matches);
            let mut schemas = serde_json::Map::new();
            let mut stage_matches = serde_json::Map::new();
            let required_stages = if case["expected"]["condition"].is_string() {
                6
            } else {
                7
            };
            for stage in &SCHEMA_STAGES[..required_stages] {
                let actual = match *stage {
                    "public" | "resolved" | "analyzed" => &observation["public_schema"],
                    "optimized" => &observation["optimized_schema"],
                    "physical" | "executed_before_collect" => &observation["before_collect"],
                    "executed" => &observation["physical_schema"],
                    _ => &Value::Null,
                };
                schemas.insert((*stage).into(), actual.clone());
                stage_matches.insert(
                    (*stage).into(),
                    json!(actual == &case["schema_stages"][stage]),
                );
            }
            observation["schema_stage_observations"] = json!(schemas);
            observation["schema_stage_matches"] = json!(stage_matches);
            eprintln!("SLICE_ROW_PRIORITY_902_DISCRIMINATOR_OBSERVATION {observation}");
            if !strict_discriminator_matches(case, &observation) {
                differences.push(observation.to_string());
            }
            let identity = (
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            );
            if !completed.insert(identity) {
                differences.push("duplicate completion identity".into());
            }
        }
    }
    eprintln!(
        "SLICE_ROW_PRIORITY_902_DISCRIMINATOR_COMPLETION {}",
        json!({
            "expected_identities":expected_completions,"completed_identities":completed,
            "comparisons":completed.len(),"strict_differences":differences.len(),
        })
    );
    assert_eq!(completed, expected_completions);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
