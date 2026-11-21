//! Compare serialized count-min sketches with the pre-retraction implementation.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

const INPUT: &str = "WITH samples AS (SELECT * FROM VALUES
    (0L,-9223372036854775807L), (1L,NULL), (3L,1L), (5L,1L),
    (6L,9223372036854775807L), (10L,0L), (13L,-7L), (14L,NULL) AS rows(id,v))";
const SKETCH: &str = "count_min_sketch(v,0.9D,0.5D,-7)";
const FRAMES: &[&str] = &[
    "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    "RANGE BETWEEN 4 PRECEDING AND CURRENT ROW",
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    "ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
];

fn check_staging(observations: Vec<Value>) -> TestResult {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/sliding-count-min-sketch/staging.json"
    ))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn count_min_sketch_outputs_match_staging_in_both_ansi_modes() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(3)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    );
    let mut observations = Vec::new();
    for ansi in [false, true] {
        let windows = FRAMES.iter().map(|frame| {
            format!(
                "SELECT id,{SKETCH} OVER (ORDER BY id {frame}) AS sketch FROM samples ORDER BY id"
            )
        });
        let plain = ["", " WHERE false"]
            .map(|filter| format!("SELECT {SKETCH} AS sketch FROM samples{filter}"));
        for query in windows.chain(plain) {
            let sql = format!("{INPUT} {query}");
            let mut result = spark_results::capture_executed_query_result(
                &session,
                &json!({"sql":sql,"ansi":ansi,"ordered":true}),
            )
            .await?;
            spark_results::remove_nullability_and_metadata(&mut result);
            observations.push(json!({"query":query,"ansi":ansi,"result":result}));
        }
    }
    check_staging(observations)
}
