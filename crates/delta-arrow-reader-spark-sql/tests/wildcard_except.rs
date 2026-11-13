//! Wildcard exclusions resolve visible names before expanding projections and calls.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Int32Array, StructArray};
use arrow::buffer::NullBuffer;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, encode_value, remove_nullability_and_metadata,
};

#[tokio::test]
async fn wildcard_exclusions_match_spark() -> TestResult {
    let records = include_str!("fixtures/wildcard-except/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let deep_records = include_str!("fixtures/wildcard-except/deep-spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(deep_records[0]["spark_version"], "4.2.0");
    assert_eq!(deep_records.len() - 1, 4);
    // Depth24 establishes Spark reachability; depth12 keeps a regressed runtime planner bounded.
    let cases = records
        .iter()
        .skip(1)
        .chain(
            deep_records
                .iter()
                .skip(1)
                .filter(|case| case["id"] == "deep_12"),
        )
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 144);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 142);
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let mut identities = BTreeSet::new();
    for &case in &cases {
        assert!(case["rows"].is_array());
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
    }
    let mut differences = Vec::new();
    for (partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let session = SessionContext::new_with_state(
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
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0]["corpora"][0]["metadata"],
            input_batch,
            partitions,
        )?;
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &deep_records[0]["corpora"][0]["metadata"],
            input_batch,
            partitions,
        )?;
        assert_native_sql_controls(&session).await?;
        for &case in &cases {
            let observation = capture_executed_query_result(&session, case).await;
            println!(
                "EXCEPT_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"source_partitions":partitions,
                    "input_batch":input_batch,"target_partitions":target_partitions,
                    "output_batch":output_batch,
                    "actual":observation.as_ref().map_err(ToString::to_string)})
            );
            match observation {
                Ok(actual) => {
                    if actual["rows"] != case["rows"]
                        || result_types(&actual["schema"])? != result_types(&case["schema"])?
                    {
                        differences.push(format!("{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected {case}, got {actual}", case["id"], case["ansi"]));
                    }
                }
                Err(error) => differences.push(format!(
                    "{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): {error}",
                    case["id"], case["ansi"]
                )),
            }
        }
        for ansi_mode in [false, true] {
            for sql in [
                "SELECT * EXCEPT(b) FROM numbers x JOIN numbers y USING(a)",
                "SELECT * EXCEPT(missing) FROM numbers",
                "SELECT s.* EXCEPT(missing) FROM nested",
                "SELECT * EXCEPT(s.missing) FROM nested",
                "SELECT * EXCEPT(s.x.y) FROM nested",
                "SELECT a EXCEPT(b) FROM numbers",
                "SELECT * EXCEPT(a) EXCEPT(b) FROM numbers",
                "SELECT * EXCEPT(x.a) FROM (SELECT * FROM numbers x JOIN numbers y USING(a)) q",
            ] {
                assert!(
                    plan_query(
                        &session,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..SparkSqlOptions::default()
                        }
                    )
                    .await
                    .is_err(),
                    "accepted invalid exclusion: {sql}"
                );
            }
        }
        assert_native_sql_controls(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn assert_native_sql_controls(session: &SessionContext) -> TestResult {
    let native = session
        .sql("SELECT count(*) AS n FROM numbers")
        .await?
        .collect()
        .await?;
    assert_eq!(encode_value(native[0].column(0).as_ref(), 0)?, json!("5"));
    for sql in [
        "SELECT 1 AS x EXCEPT (SELECT 1 AS x)",
        "SELECT 1 EXCEPT (SELECT 1)",
    ] {
        assert!(
            session
                .sql(sql)
                .await?
                .collect()
                .await?
                .iter()
                .all(|batch| batch.num_rows() == 0)
        );
    }
    Ok(())
}

fn result_types(schema: &Value) -> TestResult<Value> {
    let mut types = json!(
        schema["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    );
    remove_nullability_and_metadata(&mut types);
    Ok(types)
}

#[tokio::test]
async fn nested_stars_mask_stored_children_under_null_structs() -> TestResult {
    let records = include_str!("fixtures/wildcard-except/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let input = records[0]["corpora"][0]["metadata"]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "nested")
        .ok_or("missing nested input")?;
    let batch = spark_inputs::decode_recorded_batch(input)?;
    let s = batch.column(1).as_struct();
    let child = s.column(1).as_struct();
    let child = StructArray::try_new(
        child.fields().clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(20), None, Some(700), Some(800)])),
            Arc::new(Int32Array::from(vec![
                Some(30),
                Some(31),
                Some(701),
                Some(801),
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, true, false])),
    )?;
    let s = StructArray::try_new(
        s.fields().clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(10), None, Some(900), Some(11)])),
            Arc::new(child),
            Arc::new(Int32Array::from(vec![Some(40), None, Some(901), Some(41)])),
        ],
        s.nulls().cloned(),
    )?;
    let batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            batch.column(0).clone(),
            Arc::new(s),
            batch.column(2).clone(),
        ],
    )?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_table(
        "nested",
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    for case in records.iter().skip(1).filter(|case| {
        case["sql"]
            .as_str()
            .is_some_and(|sql| sql.contains("FROM nested"))
    }) {
        let actual = capture_executed_query_result(&session, case).await?;
        println!(
            "EXCEPT_HIDDEN_CHILD_OBSERVATION {}",
            json!({"id":case["id"],"ansi":case["ansi"],"actual":actual})
        );
        assert_eq!(
            actual["rows"], case["rows"],
            "hidden children: {} ANSI={}",
            case["id"], case["ansi"]
        );
        assert_eq!(
            result_types(&actual["schema"])?,
            result_types(&case["schema"])?
        );
    }
    Ok(())
}
