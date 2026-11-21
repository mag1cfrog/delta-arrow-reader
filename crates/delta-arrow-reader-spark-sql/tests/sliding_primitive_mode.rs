//! Compare sliding mode values and tie rules with the previous implementation.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

const INPUT: &str = "WITH samples AS (SELECT * FROM VALUES
    (0L,CAST('-9223372036854775808' AS BIGINT)), (1L,NULL), (3L,7L), (4L,7L),
    (6L,9223372036854775807L), (10L,-7L), (12L,-7L), (13L,NULL) AS rows(id,v))";
const FRAMES: &[&str] = &[
    "ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
    "ORDER BY id RANGE BETWEEN 4 PRECEDING AND CURRENT ROW",
    "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    "ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
    "PARTITION BY id % 2 ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
];
const ADDITIONAL_QUERIES: &[&str] = &[
    "SELECT mode(v) AS highest,mode(v,true) AS lowest,mode(v,false) AS unrestricted FROM samples",
    "SELECT mode(v) AS highest,mode(v,true) AS lowest,mode(v,false) AS unrestricted FROM samples WHERE false",
    "SELECT id,mode(v,true) FILTER (WHERE id <> 0L) OVER (PARTITION BY id % 2 ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS filtered FROM samples ORDER BY id",
    "SELECT id,mode(CAST(v AS STRING)) OVER w AS text_mode,mode(CAST(v AS DOUBLE),true) OVER w AS double_mode FROM samples WINDOW w AS (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ORDER BY id",
];
const INVALID_QUERIES: &[&str] = &[
    "SELECT mode(v,CAST(NULL AS BOOLEAN)) OVER () FROM samples",
    "SELECT mode(v,id < 3L) OVER () FROM samples",
];

fn check_staging(observations: Vec<Value>) -> TestResult {
    let staging: Value =
        serde_json::from_str(include_str!("fixtures/sliding-primitive-mode/staging.json"))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn sliding_primitive_modes_match_staging_in_both_ansi_modes() -> TestResult {
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
                "SELECT id,mode(v) OVER w AS highest,mode(v,true) OVER w AS lowest,mode(v,false) OVER w AS unrestricted FROM samples WINDOW w AS ({frame}) ORDER BY id"
            )
        });
        let queries = windows
            .chain(ADDITIONAL_QUERIES.iter().map(|sql| sql.to_string()))
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
                let mut result = result?;
                spark_results::remove_nullability_and_metadata(&mut result);
                result
            };
            observations.push(json!({"query":query,"ansi":ansi,"result":result}));
        }
    }
    check_staging(observations)
}
