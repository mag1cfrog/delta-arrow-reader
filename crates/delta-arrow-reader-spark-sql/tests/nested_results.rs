//! Ordinary nested values retain NULLs, multiplicity and executed types.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, AsArray, StructArray};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, capture_native_query_observation, encode_value,
    remove_nullability_and_metadata,
};

const REQUEST: &str = include_str!("fixtures/nested-results-coverage/requests.json");
const REFERENCE: &str = include_str!("fixtures/nested-results-coverage/spark-results.jsonl");
const GENERATOR_INPUTS: &str = include_str!("fixtures/array-map-explode/inputs.json");
const GENERATOR_REFERENCE: &str = include_str!("fixtures/array-map-explode/spark-results.jsonl");

fn case_key(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
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

fn load_references() -> TestResult<(Vec<Value>, Value)> {
    let request: Value = serde_json::from_str(REQUEST)?;
    let corpus = &request["corpora"][0];
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["expected_record_count"],
        request["expected_record_count"]
    );
    assert_eq!(records[0]["corpora"][0]["metadata"], corpus["metadata"]);
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
    let mut cases = Vec::new();
    for record in &records[1..] {
        let key = case_key(record)?;
        assert!(
            seen.insert(key.clone()),
            "duplicate Spark reference {key:?}"
        );
        let definition = definitions.get(&key).ok_or("unknown Spark reference")?;
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
        assert!(record["schema_stages"]["executed"]["fields"].is_array());
        assert!(record["expected"]["rows"].is_array());
        cases.push(
            json!({"id":record["id"],"ansi":record["ansi"],"sql":record["sql"],
            "ordered":record["ordered"],"rows":record["expected"]["rows"],
            "schema":record["schema_stages"]["executed"]}),
        );
    }
    assert_eq!(seen, definitions.into_keys().collect());
    assert_eq!(json!(seen.len()), request["expected_record_count"]);

    // Reuse the accepted primitive generator matrix and its exact source definitions.
    let generators: Value = serde_json::from_str(GENERATOR_INPUTS)?;
    assert_eq!(generators["spark_version"], "4.2.0");
    let definitions = generators["cases"]
        .as_array()
        .ok_or("missing generator cases")?;
    let mut expected = BTreeSet::new();
    for definition in definitions {
        let id = definition["id"].as_str().ok_or("missing generator ID")?;
        for ansi in [false, true] {
            assert!(
                expected.insert((id.to_owned(), ansi)),
                "duplicate generator {id}"
            );
        }
    }
    let mut seen = BTreeSet::new();
    for line in GENERATOR_REFERENCE.lines() {
        let record: Value = serde_json::from_str(line)?;
        let key = case_key(&record)?;
        assert!(seen.insert(key.clone()));
        let definition = definitions
            .iter()
            .find(|case| case["id"] == key.0)
            .ok_or("unknown generator")?;
        assert_eq!(record["sql"], definition["sql"]);
        assert_eq!(record["order"], definition["order"]);
        assert_eq!(record["status"], "ok");
        assert!(matches!(
            record["order"].as_str(),
            Some("ordered" | "multiset")
        ));
        cases.push(
            json!({"id":record["id"],"ansi":record["ansi"],"sql":record["sql"],
            "ordered":record["order"] == "ordered", "rows":record["rows"],
            "schema":{"fields":record["fields"]}}),
        );
    }
    assert_eq!(seen, expected);
    let mut inputs = corpus["metadata"]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .clone();
    inputs.extend(
        generators["registered_inputs"]
            .as_array()
            .ok_or("missing generator inputs")?
            .clone(),
    );
    Ok((cases, json!({"inputs":inputs})))
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
            spark_results::validate_batch(schema.as_ref(), &batch)?;
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
            .ok_or("missing provider rows")?
            .clone();
        expected.sort_by_key(Value::to_string);
        assert_eq!(rows, expected, "{name} source readback");
    }
    Ok(())
}

async fn compare_cases(session: &SessionContext, cases: &[Value], layout: Value) -> TestResult {
    let mut differences = Vec::new();
    for case in cases {
        let actual = capture_executed_query_result(session, case).await;
        let mut expected_rows = case["rows"].clone();
        if case["ordered"] != true {
            expected_rows
                .as_array_mut()
                .ok_or("missing expected rows")?
                .sort_by_key(Value::to_string);
        }
        // Seeded RAND draws may differ across native execution layouts.
        let volatile_layout =
            case["id"] == "outer_volatile_array" && layout["target_partitions"] != 1;
        let matches = match &actual {
            Ok(actual) => {
                executed_types(&actual["schema"])? == executed_types(&case["schema"])?
                    && if volatile_layout {
                        volatile_outer_rows_match(&actual["rows"], &expected_rows)?
                    } else {
                        actual["rows"] == expected_rows
                    }
            }
            Err(_) => false,
        };
        println!(
            "NESTED_RESULTS_OBSERVATION {}",
            json!({"id":case["id"],"ansi":case["ansi"],
            "sql":case["sql"],"layout":layout,"ordered":case["ordered"],"matches":matches,
            "comparison":if volatile_layout {"volatile_layout_invariant"} else {"exact_spark"},
            "spark":case,"actual":actual.as_ref().map_err(ToString::to_string)})
        );
        if !matches {
            differences.push(format!(
                "{} ANSI={} layout={layout}: expected {case}, got {actual:?}",
                case["id"], case["ansi"]
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn volatile_outer_rows_match(rows: &Value, reference: &Value) -> TestResult<bool> {
    let rows = rows.as_array().ok_or("missing volatile rows")?;
    let ids = rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    let expected = reference
        .as_array()
        .ok_or("missing reference rows")?
        .iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    Ok(ids == expected
        && rows.iter().all(|row| {
            row.as_array().is_some_and(|row| {
                row.len() == 3
                    && ((row[1].is_null() && row[2].is_null()) || (row[1] == "0" && row[2] == "x"))
            })
        }))
}

#[test]
fn volatile_outer_invariant_retains_source_rows_and_position_value_pairs() -> TestResult {
    let reference = json!([["0", null, null], ["1", "0", "x"]]);
    assert!(volatile_outer_rows_match(
        &json!([["0", "0", "x"], ["1", null, null]]),
        &reference
    )?);
    for rows in [
        json!([["0", null, null]]),
        json!([["0", null, null], ["0", "0", "x"]]),
        json!([["0", null, "x"], ["1", "0", "x"]]),
        json!([["0", "1", "x"], ["1", null, null]]),
        json!([["1", "0", "x"], ["0", null, null]]),
    ] {
        assert!(!volatile_outer_rows_match(&rows, &reference)?, "{rows}");
    }
    Ok(())
}

#[tokio::test]
async fn nested_results_match_spark_values_and_executed_types() -> TestResult {
    let (cases, inputs) = load_references()?;
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
        compare_cases(&session, &cases, json!({"provider_partitions":provider_partitions,
            "input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch})).await?;
    }
    Ok(())
}

#[tokio::test]
async fn nullable_struct_parent_masks_stored_children() -> TestResult {
    let (cases, inputs) = load_references()?;
    let input = &inputs["inputs"][0];
    let logical = spark_inputs::decode_recorded_batch(input)?;
    let index = logical.schema().index_of("s")?;
    let row = input["rows"]
        .as_array()
        .ok_or("missing rows")?
        .iter()
        .position(|row| row[index].is_null())
        .ok_or("missing NULL STRUCT")?;
    let mut storage = input.clone();
    storage["rows"][row][index] = json!(["31337", "hidden", ["919"]]);
    let storage = spark_inputs::decode_recorded_batch(&storage)?;
    let original = logical.column(index).as_struct();
    let stored = storage.column(index).as_struct();
    let nested = StructArray::try_new(
        stored.fields().clone(),
        stored.columns().to_vec(),
        original.nulls().cloned(),
    )?;
    assert!(nested.is_null(row));
    assert_eq!(
        encode_value(nested.column(0).as_ref(), row)?,
        json!("31337")
    );
    let mut columns = logical.columns().to_vec();
    columns[index] = Arc::new(nested);
    let batch = RecordBatch::try_new(logical.schema(), columns)?;
    spark_results::validate_batch(batch.schema().as_ref(), &batch)?;
    for row in 0..batch.num_rows() {
        assert_eq!(
            encode_value(batch.column(index).as_ref(), row)?,
            input["rows"][row][index]
        );
    }
    let cases = cases
        .into_iter()
        .filter(|case| {
            matches!(
                case["id"].as_str(),
                Some(
                    "registered_containers"
                        | "struct_dotted_fields"
                        | "struct_bracket_fields"
                        | "filtered_nested_values"
                )
            )
        })
        .collect::<Vec<_>>();
    for output_batch in [1, 32] {
        let session = session(2, output_batch);
        session.register_table(
            "nested_inputs",
            Arc::new(MemTable::try_new(
                batch.schema(),
                vec![vec![batch.clone()]],
            )?),
        )?;
        compare_cases(
            &session,
            &cases,
            json!({"variant":"hidden_struct_children","output_batch":output_batch}),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_nested_queries_keep_their_values_and_types() -> TestResult {
    let (_, inputs) = load_references()?;
    let native = SessionContext::new();
    let configured = session(2, 32);
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &inputs, 2)?;
    }
    let sql = "SELECT id, a[1] AS first, m['a'] AS item, s['code'] AS code FROM nested_inputs ORDER BY id";
    let expected = capture_native_query_observation(&native, sql).await?;
    let actual = capture_native_query_observation(&configured, sql).await?;
    assert!(expected["rows"].is_array(), "{expected}");
    assert_eq!(actual, expected);
    Ok(())
}
