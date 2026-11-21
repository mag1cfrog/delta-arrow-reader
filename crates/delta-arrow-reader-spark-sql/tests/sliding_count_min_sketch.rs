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
const SKETCH: &str = "count_min_sketch(v,0.9D,0.9D,-7)";
const FRAMES: &[&str] = &[
    "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    "RANGE BETWEEN 4 PRECEDING AND CURRENT ROW",
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    "ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
];
const PARAMETER_BOUNDARIES: &[&str] = &[
    "count_min_sketch(v,2.0D,0.00001D,-2147483648L)",
    "count_min_sketch(v,1.1D,0.999D,2147483647)",
];
const INVALID_ARGUMENTS: &[&str] = &[
    "count_min_sketch(v,0.0D,0.5D,0)",
    "count_min_sketch(v,1e-12D,0.5D,0)",
    "count_min_sketch(v,0.9D,0.0D,0)",
    "count_min_sketch(v,0.9D,1.0D,0)",
    "count_min_sketch(v,0.9D,0.5D,CAST(NULL AS BIGINT))",
    "count_min_sketch(v,CAST(id AS DOUBLE),0.5D,0)",
    "count_min_sketch(CAST(v AS DOUBLE),0.9D,0.5D,0)",
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
        let boundaries = PARAMETER_BOUNDARIES.iter().map(|expression| {
            format!(
                "SELECT id,{expression} FILTER (WHERE id <> 0L) OVER (PARTITION BY id % 2 ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sketch FROM samples ORDER BY id"
            )
        });
        let errors = INVALID_ARGUMENTS.iter().map(|expression| {
            (
                format!("SELECT {expression} OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM samples"),
                true,
            )
        });
        for (query, expects_error) in windows
            .chain(plain)
            .chain(boundaries)
            .map(|query| (query, false))
            .chain(errors)
        {
            let sql = format!("{INPUT} {query}");
            let result = spark_results::capture_executed_query_result(
                &session,
                &json!({"sql":sql,"ansi":ansi,"ordered":true}),
            )
            .await;
            let result = if expects_error {
                json!({"error":result.expect_err(&query).to_string()})
            } else {
                let mut result = result?;
                spark_results::remove_nullability_and_metadata(&mut result);
                result
            };
            observations.push(json!({"query":query,"ansi":ansi,"result":result}));
        }
    }
    check_staging(observations)
}
