//! Empty grouping-set rows and global aggregate controls from Spark 4.2.0.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn grouping_sets_match_spark_on_empty_and_nonempty_input() -> TestResult {
    let references = include_str!("fixtures/empty-grouping-sets/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(
        references[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(references.len() - 1, 140);
    let mut differences = Vec::new();
    for (batch, target_partitions, input_batch, provider_partitions) in
        [(1, 1, 1, 1), (1, 4, 1, 4), (32, 2, 1, 2), (32, 4, 32, 4)]
    {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(target_partitions),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &references[0],
            input_batch,
            provider_partitions,
        )?;
        for case in &references[1..] {
            let mut actual = spark_results::capture_executed_query_result(&session, case)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "EMPTY_GROUPING_SETS {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,
                    "target_partitions":target_partitions,"input_batch":input_batch,
                    "provider_partitions":provider_partitions,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ANSI={} batch={batch} target={target_partitions} input_batch={input_batch} providers={provider_partitions}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_sql_keeps_empty_totals_before_and_after_spark_queries() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let references = include_str!("fixtures/empty-grouping-sets/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    spark_inputs::register_recorded_inputs(&session, &references[0], 1)?;
    let mut before = Vec::new();
    for grouping in ["GROUPING SETS((g),())", "ROLLUP(g)", "CUBE(g)"] {
        let sql = format!("SELECT g,COUNT(*) AS n FROM empty_samples GROUP BY {grouping}");
        let actual = spark_results::capture_native_query_observation(&session, &sql).await?;
        assert_eq!(actual["rows"], json!([[null, "0"]]), "{sql}: {actual}");
        before.push((sql, actual));
    }
    for ansi in [false, true] {
        let case = references[1..]
            .iter()
            .find(|case| case["id"] == "issue_sets" && case["ansi"] == ansi)
            .ok_or("missing Spark issue control")?;
        let actual = spark_results::capture_executed_query_result(&session, case).await?;
        assert_eq!(actual["rows"], json!([]));
        for (sql, expected) in &before {
            assert_eq!(
                spark_results::capture_native_query_observation(&session, sql).await?,
                *expected,
                "{sql} after Spark ANSI={ansi}"
            );
        }
    }
    Ok(())
}
