//! Repeated bare columns keep Spark's resolution identity and every physical output slot.

use std::collections::{BTreeSet, HashSet};

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

async fn assert_native_count(session: &SessionContext) -> TestResult {
    let batches = session
        .sql("SELECT count(*) FROM left_inputs")
        .await?
        .collect()
        .await?;
    assert_eq!(encode_value(batches[0].column(0).as_ref(), 0)?, json!("5"));
    Ok(())
}

#[tokio::test]
async fn repeated_source_columns_match_spark() -> TestResult {
    let records = include_str!("fixtures/repeated-source-columns/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 114);
    let mut identities = BTreeSet::new();
    for case in records.iter().skip(1) {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?
        )));
        assert!(matches!(case["status"].as_str(), Some("ok" | "error")));
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
                            .with_target_partitions(target_partitions)
                            .with_batch_size(output_batch),
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
        assert_native_count(&session).await?;
        for sql in [
            "SELECT k,* FROM left_inputs",
            "SELECT id,id FROM left_mixed",
        ] {
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            let schema = query.logical_plan.schema();
            let physical_names = schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<HashSet<_>>();
            assert_eq!(
                physical_names.len(),
                schema.fields().len(),
                "physical IDs repeated: {sql}"
            );
            assert_eq!(
                query.column_names.iter().collect::<HashSet<_>>().len() + 1,
                query.column_names.len(),
                "repeated visible entry lost: {sql}"
            );
        }
        for case in records.iter().skip(1) {
            let observation = capture_executed_query_result(&session, case).await;
            println!(
                "REPEATED_SOURCE_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"source_partitions":partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"actual":observation.as_ref().map_err(ToString::to_string)})
            );
            match observation {
                Ok(actual) if case["status"]=="ok" => {
                    if actual["rows"]!=case["rows"] || result_types(&actual["schema"])?!=result_types(&case["schema"])? {
                        differences.push(format!("{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected {case}, got {actual}",case["id"],case["ansi"]));
                    }
                }
                Err(error) if case["status"]=="error" => {
                    if !error.to_string().to_ascii_lowercase().contains("ambiguous") {
                        differences.push(format!("{} failed without DAR ambiguity validation: {error}", case["id"]));
                    }
                },
                other => differences.push(format!("{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected {}, got {other:?}",case["id"],case["ansi"],case["status"])),
            }
        }
        assert_native_count(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
