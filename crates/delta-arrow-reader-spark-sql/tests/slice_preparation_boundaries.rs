//! Paired DAR observations for SLICE preparation reachability, without a Spark oracle.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn observe_slice_preparation_boundaries() -> TestResult {
    let header: Value = serde_json::from_str(
        include_str!("fixtures/slice-parameters/spark-reference.jsonl")
            .lines()
            .next()
            .ok_or("missing recorded inputs")?,
    )?;
    let scenarios = [
        ("literal_empty_lambda", "SELECT TRANSFORM(CAST(ARRAY() AS ARRAY<INT>), x -> SLICE(ARRAY(1, 2), 0, 1)) AS r"),
        ("provider_empty_lambda", "SELECT TRANSFORM(a_nn, x -> SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields WHERE k = 1"),
        ("provider_null_lambda", "SELECT TRANSFORM(a_pn, x -> SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields WHERE k = 1"),
        ("provider_valid_lambda", "SELECT TRANSFORM(a_nn, x -> SLICE(ARRAY(1, 2), 1, 1)) AS r FROM slice_fields WHERE k = 0"),
        ("required_count_start", "SELECT COUNT(SLICE(ARRAY(1, 2), 0, 1)) AS r FROM slice_fields"),
        ("required_count_length", "SELECT COUNT(SLICE(ARRAY(1, 2), 1, -1)) AS r FROM slice_fields"),
        ("unused_projection", "SELECT 1 AS r FROM (SELECT SLICE(ARRAY(1, 2), 0, 1) AS unused FROM slice_fields) q"),
        ("limit_zero", "SELECT SLICE(ARRAY(1, 2), 0, 1) AS r FROM slice_fields LIMIT 0"),
        ("discarded_scalar_one_row", "SELECT CASE WHEN false THEN (SELECT SLICE(ARRAY(1, 2), 0, 1)) ELSE ARRAY(9) END AS r"),
        ("discarded_scalar_provider", "SELECT CASE WHEN false THEN (SELECT SLICE(ARRAY(1, 2), 0, 1) FROM slice_fields WHERE k = 0) ELSE ARRAY(9) END AS r"),
        ("valid_scalar_provider", "SELECT (SELECT SLICE(ARRAY(1, 2), 1, 1) FROM slice_fields WHERE k = 0) AS r"),
        ("valid_scalar_empty_provider", "SELECT (SELECT SLICE(ARRAY(1, 2), 1, 1) FROM slice_empty) AS r"),
    ];
    let mut completed = std::collections::BTreeSet::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch)),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &header, batch)?;
        for ansi in [true, false] {
            for (id, sql) in scenarios {
                let case = json!({"id":id,"sql":sql,"ansi":ansi,"timezone":"UTC",
                    "expected":{},"schema_stages":{}});
                let mut observation =
                    spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case)
                        .await?;
                assert!(observation["public_schema"].is_object(), "{id}: {observation}");
                observation["id"] = json!(id);
                observation["sql"] = json!(sql);
                observation["ansi"] = json!(ansi);
                observation["batch"] = json!(batch);
                assert!(completed.insert((id, ansi, batch)));
                eprintln!("SLICE_PREPARATION_BOUNDARY_OBSERVATION {observation}");
            }
        }
    }
    assert_eq!(completed.len(), 48);
    Ok(())
}
