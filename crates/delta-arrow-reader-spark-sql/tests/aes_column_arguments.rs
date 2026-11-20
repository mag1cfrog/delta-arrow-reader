//! Verify Spark AES results with one-row and multi-row source batches.

use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn aes_column_arguments_match_spark_4_2() -> TestResult {
    let records = include_str!("fixtures/aes-column-arguments/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let header = &records[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["expected_cases"], records.len() - 1);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(1),
        );
        spark_inputs::register_recorded_inputs(&session, header, batch_size)?;
        for case in &records[1..] {
            if case["scope"] == "spark-surface" {
                assert_eq!(case["expected"]["condition"], "UNRESOLVED_ROUTINE");
                continue;
            }
            let mut expected = case["expected"].clone();
            let actual = spark_results::capture_executed_query_result(&session, case).await;
            match (case["status"].as_str(), actual) {
                (Some("error"), Err(_)) => {}
                (Some("ok"), Ok(mut actual)) => {
                    spark_results::remove_nullability_and_metadata(&mut actual);
                    spark_results::remove_nullability_and_metadata(&mut expected);
                    if actual != expected {
                        differences.push(format!(
                            "{} ANSI {} batch {}\nexpected {expected}\nactual {actual}",
                            case["id"], case["ansi"], batch_size
                        ));
                    }
                }
                (_, actual) => differences.push(format!(
                    "{} ANSI {} batch {}: expected {:?}, got {actual:?}",
                    case["id"], case["ansi"], batch_size, case["status"]
                )),
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
