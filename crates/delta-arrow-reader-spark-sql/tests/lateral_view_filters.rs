//! WHERE filters see every preceding LATERAL VIEW output.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, capture_native_query_observation, encode_value,
    remove_nullability_and_metadata, validate_batch,
};

const REQUEST: &str = include_str!("fixtures/lateral-view-filters/requests.json");
const REFERENCE: &str = include_str!("fixtures/lateral-view-filters/spark-results.jsonl");

fn case_key(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn references() -> TestResult<(Vec<Value>, Value)> {
    let request: Value = serde_json::from_str(REQUEST)?;
    let corpus = &request["corpora"][0];
    let mut records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let header = records.remove(0);
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(header["corpora"][0]["metadata"], corpus["metadata"]);
    assert_eq!(
        header["expected_record_count"],
        request["expected_record_count"]
    );
    let mut definitions = BTreeMap::new();
    for case in corpus["cases"].as_array().ok_or("missing source cases")? {
        assert!(definitions.insert(case_key(case)?, case).is_none());
    }
    for (id, _) in definitions.keys() {
        for ansi in [false, true] {
            assert!(
                definitions.contains_key(&(id.clone(), ansi)),
                "{id} ANSI={ansi}"
            );
        }
    }
    let mut seen = BTreeSet::new();
    for record in &records {
        let key = case_key(record)?;
        assert!(seen.insert(key.clone()), "duplicate reference {key:?}");
        let definition = definitions.get(&key).ok_or("unknown reference")?;
        for field in ["corpus", "sql", "ordered"] {
            assert_eq!(record[field], definition[field], "{key:?}: {field}");
        }
        assert_eq!(record["status"], "ok", "{key:?}");
        assert_eq!(
            record["effective_settings"]["spark.sql.ansi.enabled"],
            key.1.to_string()
        );
        assert_eq!(
            record["effective_settings"]["spark.sql.session.timeZone"],
            "UTC"
        );
        assert!(record["expected"]["rows"].is_array());
        assert!(record["schema_stages"]["executed"]["fields"].is_array());
    }
    assert_eq!(seen, definitions.into_keys().collect());
    assert_eq!(json!(seen.len()), request["expected_record_count"]);
    Ok((records, corpus["metadata"].clone()))
}

fn executed_types(schema: &Value) -> TestResult<Value> {
    let mut types = json!(
        schema["fields"]
            .as_array()
            .ok_or("missing executed fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    );
    remove_nullability_and_metadata(&mut types);
    Ok(types)
}

fn session(target_partitions: usize, output_batch: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(output_batch)
                        .with_target_partitions(target_partitions),
                ),
        )
        .build(),
    )
}

async fn assert_provider_inputs(
    session: &SessionContext,
    inputs: &Value,
    partitions: usize,
) -> TestResult {
    let state = session.state();
    for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing provider name")?;
        let scan = session
            .table_provider(name)
            .await?
            .scan(&state, None, &[], None)
            .await?;
        assert_eq!(
            scan.output_partitioning().partition_count(),
            partitions,
            "{name}"
        );
        let schema = scan.schema();
        let mut rows = Vec::new();
        for batch in collect(scan, session.task_ctx()).await? {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(json!(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?
                ));
            }
        }
        rows.sort_by_key(Value::to_string);
        let mut expected = input["rows"]
            .as_array()
            .ok_or("missing input rows")?
            .clone();
        expected.sort_by_key(Value::to_string);
        assert_eq!(rows, expected, "{name} source readback");
    }
    Ok(())
}

#[tokio::test]
async fn lateral_view_filters_match_spark_values_and_executed_types() -> TestResult {
    let (cases, inputs) = references()?;
    let mut differences = Vec::new();
    for (provider_partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let session = session(target_partitions, output_batch);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &inputs,
            input_batch,
            provider_partitions,
        )?;
        assert_provider_inputs(&session, &inputs, provider_partitions).await?;
        for case in &cases {
            let actual = capture_executed_query_result(&session, case).await;
            let mut expected_rows = case["expected"]["rows"].clone();
            if case["ordered"] != true {
                expected_rows
                    .as_array_mut()
                    .ok_or("missing rows")?
                    .sort_by_key(Value::to_string);
            }
            let matches = match &actual {
                Ok(actual) => {
                    actual["rows"] == expected_rows
                        && executed_types(&actual["schema"])?
                            == executed_types(&case["schema_stages"]["executed"])?
                }
                Err(_) => false,
            };
            println!(
                "LATERAL_FILTER_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
                "provider_partitions":provider_partitions,"input_batch":input_batch,"target_partitions":target_partitions,
                "output_batch":output_batch,"ordered":case["ordered"],"matches":matches,
                "actual":actual.as_ref().map_err(ToString::to_string)})
            );
            if !matches {
                differences.push(format!("{} ANSI={} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected rows={expected_rows} types={}, got {actual:?}",
                    case["id"],case["ansi"],executed_types(&case["schema_stages"]["executed"])?));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn unresolved_filter_columns_still_fail_and_native_sql_is_unchanged() -> TestResult {
    let (_, inputs) = references()?;
    let native = SessionContext::new();
    let configured = session(2, 32);
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &inputs, 2)?;
    }
    let sql =
        "SELECT id, a[1] AS item, m['a'] AS value FROM nested_inputs WHERE id = 0 ORDER BY id";
    let expected = capture_native_query_observation(&native, sql).await?;
    assert_eq!(expected["rows"], json!([["0", "7", "7"], ["0", "7", "7"]]));
    assert_eq!(
        capture_native_query_observation(&configured, sql).await?,
        expected
    );
    for ansi_mode in [false, true] {
        for sql in [
            "SELECT id, item FROM nested_inputs LATERAL VIEW explode(a) x AS item WHERE unknown_item = 7",
            "SELECT id FROM nested_inputs WHERE unknown_item = 7",
        ] {
            match plan_query(
                &configured,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await
            {
                Err(error) => {
                    let error = error.to_string();
                    assert!(
                        error.contains("unknown_item") && error.contains("UNRESOLVED_COLUMN"),
                        "{error}"
                    );
                }
                Ok(_) => return Err(format!("unresolved column was admitted: {sql}").into()),
            }
        }
    }
    assert_eq!(
        capture_native_query_observation(&configured, sql).await?,
        expected
    );
    Ok(())
}
