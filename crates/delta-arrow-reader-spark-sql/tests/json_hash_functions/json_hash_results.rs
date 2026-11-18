//! Complete JSON/hash results, retaining only the documented TO_JSON cell differences.

use std::collections::BTreeSet;

use datafusion::arrow::compute::concat_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect_partitioned};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

use super::{spark_inputs, spark_results};
use spark_results::{TestResult, capture_executed_query_result, capture_native_query_observation};

fn references() -> TestResult<Vec<Value>> {
    let request: Value =
        serde_json::from_str(include_str!("../fixtures/json-hash-results/requests.json"))?;
    let records = include_str!("../fixtures/json-hash-results/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["expected_record_count"], 126);
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(definitions.len(), 126);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate {id} ANSI={ansi}");
        let definition = definitions
            .iter()
            .find(|q| q["id"] == id && q["ansi"] == ansi)
            .ok_or("unknown case")?;
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
    let mut schema = schema.clone();
    spark_results::remove_nullability_and_metadata(&mut schema);
    Ok(schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| field["type"].clone())
        .collect())
}

fn normalize_logical_maps(value: &mut Value, data_type: &Value) -> TestResult {
    if value.is_null() {
        return Ok(());
    }
    match data_type["type"].as_str() {
        Some("map") => {
            let entries = value.as_array_mut().ok_or("expected MAP entry pairs")?;
            for entry in entries.iter_mut() {
                let pair = entry.as_array_mut().ok_or("expected MAP pair")?;
                assert_eq!(pair.len(), 2);
                normalize_logical_maps(&mut pair[0], &data_type["keyType"])?;
                normalize_logical_maps(&mut pair[1], &data_type["valueType"])?;
            }
            entries.sort_by_cached_key(|entry| entry[0].to_string());
        }
        Some("array") => {
            for item in value.as_array_mut().ok_or("expected ARRAY values")? {
                normalize_logical_maps(item, &data_type["elementType"])?;
            }
        }
        Some("struct") => {
            let values = value.as_array_mut().ok_or("expected STRUCT fields")?;
            let fields = data_type["fields"]
                .as_array()
                .ok_or("missing STRUCT fields")?;
            assert_eq!(values.len(), fields.len());
            for (value, field) in values.iter_mut().zip(fields) {
                normalize_logical_maps(value, &field["type"])?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn normalize_result_maps(rows: &mut Value, schema: &Value) -> TestResult {
    let fields = schema["fields"].as_array().ok_or("missing result fields")?;
    for row in rows.as_array_mut().ok_or("missing result rows")? {
        let values = row.as_array_mut().ok_or("missing row values")?;
        assert_eq!(values.len(), fields.len());
        for (value, field) in values.iter_mut().zip(fields) {
            normalize_logical_maps(value, &field["type"])?;
        }
    }
    Ok(())
}

fn is_documented_to_json_difference(
    id: &str,
    row: usize,
    column: usize,
    spark: &Value,
    actual: &Value,
) -> bool {
    // #1094 keeps six fractional digits; #1284 keeps the VOID null field.
    matches!(
        (id, row, column, spark.as_str(), actual.as_str()),
        (
            "to_json_columns",
            0,
            1,
            Some(r#"{"a":1,"s":"x","ts":"2024-02-29T12:34:56.123Z"}"#),
            Some(r#"{"a":1,"s":"x","ts":"2024-02-29T12:34:56.123000Z"}"#)
        ) | (
            "to_json_columns",
            0,
            3,
            Some(r#"[{"a":1,"s":"x","ts":"2024-02-29T12:34:56.123Z"},null]"#),
            Some(r#"[{"a":1,"s":"x","ts":"2024-02-29T12:34:56.123000Z"},null]"#)
        ) | (
            "to_json_void_boundary",
            0,
            0,
            Some("{}"),
            Some(r#"{"void":null}"#)
        )
    )
}

struct RowComparison {
    accepted: bool,
    documented_to_json_difference_cells: usize,
}

fn compare_rows_with_documented_differences(
    id: &str,
    actual_rows: &[Value],
    spark_rows: &[Value],
) -> TestResult<RowComparison> {
    let mut comparison = RowComparison {
        accepted: actual_rows.len() == spark_rows.len(),
        documented_to_json_difference_cells: 0,
    };
    for (row_index, (actual_row, spark_row)) in actual_rows.iter().zip(spark_rows).enumerate() {
        let actual_values = actual_row.as_array().ok_or("missing actual values")?;
        let spark_values = spark_row.as_array().ok_or("missing Spark values")?;
        comparison.accepted &= actual_values.len() == spark_values.len();
        for (column, (actual, spark)) in actual_values.iter().zip(spark_values).enumerate() {
            if actual != spark {
                if is_documented_to_json_difference(id, row_index, column, spark, actual) {
                    comparison.documented_to_json_difference_cells += 1;
                } else {
                    comparison.accepted = false;
                }
            }
        }
    }
    Ok(comparison)
}

async fn observe_provider_inputs(
    session: &SessionContext,
    inputs: &Value,
    input_batch: usize,
    provider_partitions: usize,
) -> TestResult<Value> {
    let mut observations = Vec::new();
    for input in inputs["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing input name")?;
        let expected = spark_inputs::decode_recorded_batch(input)?;
        let scan = session
            .table_provider(name)
            .await?
            .scan(&session.state(), None, &[], None)
            .await?;
        assert_eq!(
            scan.output_partitioning().partition_count(),
            provider_partitions
        );
        let partitions = collect_partitioned(scan, session.task_ctx()).await?;
        let actual_sizes = partitions
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut expected_sizes = vec![Vec::new(); provider_partitions];
        for (chunk, offset) in (0..expected.num_rows()).step_by(input_batch).enumerate() {
            expected_sizes[chunk % provider_partitions]
                .push(input_batch.min(expected.num_rows() - offset));
        }
        assert_eq!(actual_sizes, expected_sizes, "{name}");
        let batches = session
            .sql(&format!("SELECT * FROM {name} ORDER BY id"))
            .await?
            .collect()
            .await?;
        assert_eq!(
            concat_batches(&expected.schema(), &batches)?,
            expected,
            "{name}"
        );
        observations.push(json!({"table":name,"partition_batch_rows":actual_sizes}));
    }
    Ok(json!(observations))
}

#[tokio::test]
async fn json_and_hash_results_preserve_spark_and_documented_boundaries() -> TestResult {
    let records = references()?;
    let inputs = &records[0]["corpora"][0]["metadata"];
    let native_sql = "SELECT id,md5(s) AS digest,sha256(b) AS sha FROM hash_inputs ORDER BY id";
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
            spark_inputs::register_recorded_inputs_with_partition_count(
                context,
                inputs,
                input_batch,
                provider_partitions,
            )?;
        }
        let observed_inputs =
            observe_provider_inputs(&session, inputs, input_batch, provider_partitions).await?;
        println!(
            "JSON_HASH_PROVIDER_OBSERVATION {}",
            json!({"provider_partitions":provider_partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"inputs":observed_inputs})
        );
        let native_expected = capture_native_query_observation(&native, native_sql).await?;
        let native_before = capture_native_query_observation(&session, native_sql).await?;
        assert!(native_expected["error"].is_null(), "{native_expected}");
        assert_eq!(native_before, native_expected);
        for case in &records[1..] {
            let actual = capture_executed_query_result(&session, case).await;
            let mut spark_matches = false;
            let mut accepted = false;
            let mut documented_difference_cells = 0;
            if let Ok(actual) = &actual {
                let mut actual_rows = actual["rows"].clone();
                let mut spark_rows = case["expected"]["rows"].clone();
                normalize_result_maps(&mut actual_rows, &actual["schema"])?;
                normalize_result_maps(&mut spark_rows, &case["schema_stages"]["executed"])?;
                let types_match = result_types(&actual["schema"])?
                    == result_types(&case["schema_stages"]["executed"])?;
                spark_matches = types_match && actual_rows == spark_rows;
                let actual_rows = actual_rows.as_array().ok_or("missing actual rows")?;
                let spark_rows = spark_rows.as_array().ok_or("missing Spark rows")?;
                let comparison = compare_rows_with_documented_differences(
                    case["id"].as_str().ok_or("missing case ID")?,
                    actual_rows,
                    spark_rows,
                )?;
                accepted = types_match && comparison.accepted;
                documented_difference_cells = comparison.documented_to_json_difference_cells;
            }
            println!(
                "JSON_HASH_RESULT_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"provider_partitions":provider_partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"spark_matches":spark_matches,"accepted":accepted,"documented_to_json_boundary_cells":documented_difference_cells,"actual":actual.as_ref().map_err(ToString::to_string)})
            );
            if !accepted {
                differences.push(format!("{} ANSI={} layout=({provider_partitions},{input_batch},{target_partitions},{output_batch}): expected {}, got {actual:?}",case["id"],case["ansi"],case["expected"]));
            }
        }
        assert_eq!(
            capture_native_query_observation(&session, native_sql).await?,
            native_before
        );
        observe_provider_inputs(&session, inputs, input_batch, provider_partitions).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn documented_to_json_differences_reject_other_cases_cells_and_values() -> TestResult {
    let records = references()?;
    let case = records[1..]
        .iter()
        .find(|case| case["id"] == "to_json_columns" && case["ansi"] == false)
        .ok_or("missing TO_JSON reference")?;
    let spark = &case["expected"]["rows"][0][1];
    let current = json!(r#"{"a":1,"s":"x","ts":"2024-02-29T12:34:56.123000Z"}"#);
    assert!(is_documented_to_json_difference(
        "to_json_columns",
        0,
        1,
        spark,
        &current
    ));
    assert!(!is_documented_to_json_difference(
        "to_json_literal",
        0,
        1,
        spark,
        &current
    ));
    assert!(!is_documented_to_json_difference(
        "to_json_columns",
        1,
        1,
        spark,
        &current
    ));
    assert!(!is_documented_to_json_difference(
        "to_json_columns",
        0,
        2,
        spark,
        &current
    ));
    let extra_change = json!(r#"{"a":2,"s":"x","ts":"2024-02-29T12:34:56.123000Z"}"#);
    assert!(!is_documented_to_json_difference(
        "to_json_columns",
        0,
        1,
        spark,
        &extra_change
    ));
    let changed_map = json!(r#"{"a":9,"b":2}"#);
    assert!(!is_documented_to_json_difference(
        "to_json_columns",
        0,
        2,
        &case["expected"]["rows"][0][2],
        &changed_map
    ));
    let spark_rows = case["expected"]["rows"]
        .as_array()
        .ok_or("missing Spark rows")?;
    let mut actual_rows = spark_rows.clone();
    actual_rows[0][1] = current;
    let allowed =
        compare_rows_with_documented_differences("to_json_columns", &actual_rows, spark_rows)?;
    assert!(allowed.accepted);
    assert_eq!(allowed.documented_to_json_difference_cells, 1);
    actual_rows[0][2] = changed_map;
    assert!(
        !compare_rows_with_documented_differences("to_json_columns", &actual_rows, spark_rows)?
            .accepted
    );
    let exact =
        compare_rows_with_documented_differences("to_json_columns", spark_rows, spark_rows)?;
    assert!(exact.accepted);
    assert_eq!(exact.documented_to_json_difference_cells, 0);
    Ok(())
}

#[test]
fn logical_map_normalization_keeps_json_string_bytes() -> TestResult {
    let text = " { \"b\": 2, \"a\": 1 } ";
    let schema = json!({"fields":[
        {"name":"logical_map","type":{"type":"map","keyType":"string","valueType":"integer"}},
        {"name":"json_text","type":"string"}
    ]});
    let mut rows = json!([[[["b", "2"], ["a", "1"]], text]]);
    normalize_result_maps(&mut rows, &schema)?;
    assert_eq!(rows[0][0], json!([["a", "1"], ["b", "2"]]));
    assert_eq!(
        rows[0][1].as_str().ok_or("missing JSON text")?.as_bytes(),
        text.as_bytes()
    );
    Ok(())
}
