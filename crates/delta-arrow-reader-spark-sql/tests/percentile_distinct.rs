//! Compare numeric DISTINCT percentiles and MEDIAN with independent Spark results.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
#[allow(
    dead_code,
    reason = "This corpus varies provider partition counts explicitly."
)]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn distinct_percentiles_match_spark_values_and_executed_types() -> TestResult {
    let references = include_str!("fixtures/percentile-distinct/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    let cases = references[1..]
        .iter()
        .filter(|case| case["spark_rejected"] != true)
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 112);
    assert_eq!(references.len() - 1 - cases.len(), 14);
    let mut differences = Vec::new();
    for partitions in [1, 3] {
        for batch in [1, 8] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_batch_size(batch)
                                .with_target_partitions(partitions),
                        ),
                )
                .build(),
            );
            spark_inputs::register_recorded_inputs_with_partition_count(
                &session,
                &references[0],
                batch,
                partitions,
            )?;
            for case in &cases {
                let mut actual = spark_results::capture_executed_query_result(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
                spark_results::remove_nullability_and_metadata(&mut actual);
                spark_results::remove_nullability_and_metadata(&mut expected);
                println!(
                    "PERCENTILE_DISTINCT {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,
                        "partitions":partitions,"actual":actual})
                );
                if actual != expected {
                    differences.push(format!(
                        "id={} ansi={} batch={batch} partitions={partitions}: expected {expected}; actual {actual}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
