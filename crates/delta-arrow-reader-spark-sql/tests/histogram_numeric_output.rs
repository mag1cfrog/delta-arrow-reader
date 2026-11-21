//! Compare histogram values and bin field types with staging.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

const INPUT: &str = "WITH samples AS (SELECT * FROM VALUES
    (0L,CAST('-0.0' AS DOUBLE)), (1L,NULL), (3L,0D), (4L,6D), (6L,12D),
    (10L,-6D), (12L,6D), (13L,18.25D), (14L,NULL) AS rows(id,v))";
const FRAMES: &[&str] = &[
    "ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "ORDER BY id RANGE BETWEEN 4 PRECEDING AND CURRENT ROW",
    "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    "ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
];
const PLAIN_QUERIES: &[&str] = &[
    "SELECT histogram_numeric(v,4) AS bins FROM samples",
    "SELECT histogram_numeric(v,4) AS bins FROM samples WHERE false",
    "SELECT histogram_numeric(CAST(v AS INT),2) AS integer_bins,histogram_numeric(CAST(v AS DECIMAL(8,2)),2) AS decimal_bins FROM samples",
    "SELECT histogram_numeric(CAST(v AS INT),2) AS bins FROM samples WHERE v IS NULL",
];
const INVALID_QUERIES: &[&str] = &[
    "SELECT histogram_numeric(v,0) FROM samples",
    "SELECT histogram_numeric(v,CAST(NULL AS INT)) FROM samples",
    "SELECT histogram_numeric(v,id) FROM samples",
];

fn check_staging(observations: Vec<Value>) -> TestResult {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/histogram-numeric-output/staging.json"
    ))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn histogram_bins_and_types_match_staging_in_both_ansi_modes() -> TestResult {
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
                "SELECT id,histogram_numeric(v,4) OVER w AS bins,histogram_numeric(v,1) OVER w AS single_bin FROM samples WINDOW w AS ({frame}) ORDER BY id"
            )
        });
        let queries = windows
            .chain(PLAIN_QUERIES.iter().map(|sql| sql.to_string()))
            .map(|sql| (sql, false))
            .chain(INVALID_QUERIES.iter().map(|sql| (sql.to_string(), true)));
        for (query, expects_error) in queries {
            let sql = format!("{INPUT} {query}");
            let result = spark_results::capture_executed_query_result(
                &session,
                &json!({"sql":sql,"ansi":ansi,"ordered":true}),
            )
            .await;
            let result = if expects_error {
                json!({"error":result.expect_err(&query).to_string()})
            } else {
                result?
            };
            observations.push(json!({"query":query,"ansi":ansi,"result":result}));
        }
    }
    check_staging(observations)
}
