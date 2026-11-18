//! GET_JSON_OBJECT preserves Spark path selection and exact extracted STRING contents.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

use super::{spark_inputs, spark_results};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

fn references() -> TestResult<Vec<Value>> {
    let request: Value = serde_json::from_str(include_str!(
        "../fixtures/get-json-object-extraction/requests.json"
    ))?;
    let records = include_str!("../fixtures/get-json-object-extraction/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 182);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 182);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate {id} ANSI={ansi}");
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id && definition["ansi"] == ansi)
            .ok_or("unknown reference case")?;
        assert_eq!(case["sql"], definition["sql"], "{id}");
        assert_eq!(case["ordered"], definition["ordered"], "{id}");
        assert_eq!(case["status"], "ok", "{case}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string()
        );
        assert_eq!(
            case["schema_stage_availability"]["executed"]["available"],
            true
        );
    }
    assert_eq!(identities.len(), definitions.len());
    Ok(records)
}

fn result_types(schema: &Value) -> TestResult<Vec<Value>> {
    Ok(schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| field["type"].clone())
        .collect())
}

#[tokio::test]
async fn get_json_object_matches_spark_values_and_executed_types() -> TestResult {
    let records = references()?;
    let inputs = &records[0]["corpora"][0]["metadata"];
    let native_sql = "SELECT 0 AS source, id, json_as_text(j) AS r FROM path_inputs UNION ALL SELECT 1 AS source, id, json_as_text(j) AS r FROM writer_inputs ORDER BY source, id";
    let mut differences = Vec::new();
    for (provider_partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let config = SessionConfig::new()
            .with_batch_size(output_batch)
            .with_target_partitions(target_partitions);
        let native = SessionContext::new_with_config(config.clone());
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        for context in [&native, &session] {
            context.register_udf(
                sail_function::scalar::json::json_as_text_udf()
                    .as_ref()
                    .clone(),
            );
            spark_inputs::register_recorded_inputs_with_partition_count(
                context,
                inputs,
                input_batch,
                provider_partitions,
            )?;
        }
        let state = session.state();
        for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
            let name = input["name"].as_str().ok_or("missing input name")?;
            let scan = session
                .table_provider(name)
                .await?
                .scan(&state, None, &[], None)
                .await?;
            assert_eq!(
                scan.output_partitioning().partition_count(),
                provider_partitions
            );
            let readback = capture_native_query_observation(
                &session,
                &format!("SELECT id, j, p FROM {name} ORDER BY id"),
            )
            .await?;
            let mut expected_rows = input["rows"]
                .as_array()
                .ok_or("missing provider rows")?
                .clone();
            expected_rows.sort_by_key(Value::to_string);
            assert_eq!(readback["rows"], json!(expected_rows));
        }
        let native_expected = capture_native_query_observation(&native, native_sql).await?;
        let native_before = capture_native_query_observation(&session, native_sql).await?;
        let native_text_sql = "SELECT json_as_text('{ \"o\" : { \"x\" : 1 } }', 'o') AS r";
        let native_text_before =
            capture_native_query_observation(&session, native_text_sql).await?;
        assert_eq!(native_text_before["rows"], json!([["{ \"x\" : 1 }"]]));
        let native_number_sql =
            "SELECT json_as_text('{\"n\":123456789012345678901234567890}', 'n') AS r";
        let native_number_before =
            capture_native_query_observation(&session, native_number_sql).await?;
        assert_eq!(
            native_number_before["rows"],
            json!([["123456789012345678901234567890"]])
        );
        assert!(native_expected["error"].is_null(), "{native_expected}");
        assert_eq!(native_before, native_expected);
        for case in &records[1..] {
            let actual = capture_executed_query_result(&session, case).await;
            let matches = match &actual {
                Ok(actual) => {
                    actual["rows"] == case["expected"]["rows"]
                        && result_types(&actual["schema"])?
                            == result_types(&case["schema_stages"]["executed"])?
                }
                Err(_) => false,
            };
            println!(
                "GET_JSON_OBJECT_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                    "provider_partitions":provider_partitions,"input_batch":input_batch,
                    "target_partitions":target_partitions,"output_batch":output_batch,
                    "matches":matches,"actual":actual.as_ref().map_err(ToString::to_string)})
            );
            if !matches {
                differences.push(format!(
                    "{} ANSI={} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected {}, got {actual:?}",
                    case["id"],case["ansi"],case["expected"]
                ));
            }
        }
        for ansi_mode in [false, true] {
            for sql in [
                "SELECT get_json_object()",
                "SELECT get_json_object(j) FROM path_inputs",
            ] {
                assert!(
                    plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err()
                );
            }
            let deep_document = format!("{}0{}", "[".repeat(256), "]".repeat(256));
            let depth_control = json!({"ansi":ansi_mode,"ordered":true,
                "sql":format!("SELECT get_json_object('{deep_document}', '$') AS r")});
            assert_eq!(
                capture_executed_query_result(&session, &depth_control).await?["rows"],
                json!([[null]])
            );
            let native_depth = capture_native_query_observation(
                &session,
                &format!("SELECT json_as_text('{deep_document}') AS r"),
            )
            .await?;
            assert_eq!(native_depth["rows"], json!([[null]]));
        }
        let native_after = capture_native_query_observation(&session, native_sql).await?;
        assert_eq!(native_after, native_before);
        assert_eq!(
            capture_native_query_observation(&session, native_text_sql).await?,
            native_text_before
        );
        assert_eq!(
            capture_native_query_observation(&session, native_number_sql).await?,
            native_number_before
        );
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
