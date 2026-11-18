//! JSON_ARRAY_LENGTH accepts array roots while native generic JSON lengths keep their contract.

use std::collections::BTreeSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::json::{JsonLength, json_array_length_udf, json_length_udf};
use serde_json::{Value, json};

use super::{spark_inputs, spark_results};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

fn references() -> TestResult<Vec<Value>> {
    let request: Value = serde_json::from_str(include_str!(
        "../fixtures/json-array-length-roots/requests.json"
    ))?;
    let records = include_str!("../fixtures/json-array-length-roots/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 40);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 40);
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

async fn assert_native_length_contract(session: &SessionContext) -> TestResult {
    let sql = r#"SELECT
        CAST(json_length('{"x":1,"y":2}') AS BIGINT) AS object_length,
        CAST(json_len('{}') AS BIGINT) AS empty_object,
        CAST(json_length('[1,null,{}]') AS BIGINT) AS array_length,
        CAST(json_len('{"o":{"x":1,"y":2},"a":[1,2,3]}','o') AS BIGINT) AS object_path,
        CAST(json_length('{"o":{"x":1,"y":2},"a":[1,2,3]}','a') AS BIGINT) AS array_path,
        CAST(json_len('[[1,2],[3]]',0) AS BIGINT) AS index_path,
        CAST(json_length('{"a":1}','missing') AS BIGINT) AS missing_path,
        CAST(json_len('17') AS BIGINT) AS scalar_root,
        CAST(json_length('[]') AS BIGINT) AS empty_array"#;
    let actual = capture_native_query_observation(session, sql).await?;
    assert!(actual["error"].is_null(), "{actual}");
    assert_eq!(
        actual["rows"],
        json!([["2", "0", "3", "2", "3", "2", null, null, "0"]])
    );
    let batches = session
        .sql("SELECT json_length('{\"x\":1,\"y\":2}'),json_len('{}')")
        .await?
        .collect()
        .await?;
    let batch = batches.first().ok_or("missing native length batch")?;
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::UInt64)
    );
    Ok(())
}

#[tokio::test]
async fn json_array_length_roots_match_spark_and_preserve_native_lengths() -> TestResult {
    let records = references()?;
    let generic = json_length_udf();
    let default = ScalarUDF::from(JsonLength::default());
    let arrays_only = json_array_length_udf();
    for function in [generic.as_ref(), &default] {
        assert_eq!(function.name(), "json_length");
        assert_eq!(function.aliases(), &["json_length", "json_len"]);
    }
    assert_eq!(arrays_only.name(), "json_array_length");
    assert!(arrays_only.aliases().is_empty());
    let inputs = &records[0]["corpora"][0]["metadata"];
    let mut differences = Vec::new();
    for (provider_partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let config = SessionConfig::new()
            .with_batch_size(output_batch)
            .with_target_partitions(target_partitions);
        let reverse = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config.clone()),
            )
            .build(),
        );
        let spark_probe = json!({"ansi":false,"ordered":true,
            "sql":"SELECT json_array_length('{\"x\":1,\"y\":2}') AS object_length,json_array_length('[1,2,3]') AS array_length"});
        assert_eq!(
            capture_executed_query_result(&reverse, &spark_probe).await?["rows"],
            json!([[null, "3"]])
        );
        reverse.register_udf(json_length_udf().as_ref().clone());
        assert_native_length_contract(&reverse).await?;
        assert_eq!(
            capture_executed_query_result(&reverse, &spark_probe).await?["rows"],
            json!([[null, "3"]])
        );
        assert_native_length_contract(&reverse).await?;
        let native = SessionContext::new_with_config(config.clone());
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        native.register_udf(ScalarUDF::from(JsonLength::default()));
        session.register_udf(json_length_udf().as_ref().clone());
        for context in [&native, &session] {
            spark_inputs::register_recorded_inputs_with_partition_count(
                context,
                inputs,
                input_batch,
                provider_partitions,
            )?;
        }
        let state = session.state();
        let scan = session
            .table_provider("json_length_inputs")
            .await?
            .scan(&state, None, &[], None)
            .await?;
        assert_eq!(
            scan.output_partitioning().partition_count(),
            provider_partitions
        );
        let readback = capture_native_query_observation(
            &session,
            "SELECT id, j FROM json_length_inputs ORDER BY id",
        )
        .await?;
        let mut expected_rows = inputs["inputs"][0]["rows"]
            .as_array()
            .ok_or("missing provider rows")?
            .clone();
        expected_rows.sort_by_key(Value::to_string);
        assert_eq!(readback["rows"], json!(expected_rows));
        assert_native_length_contract(&native).await?;
        assert_native_length_contract(&session).await?;
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
                "JSON_ARRAY_LENGTH_OBSERVATION {}",
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
                "SELECT json_array_length()",
                "SELECT json_array_length(j,'x') FROM json_length_inputs",
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
                    .is_err(),
                    "{sql}"
                );
            }
        }
        assert_native_length_contract(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
