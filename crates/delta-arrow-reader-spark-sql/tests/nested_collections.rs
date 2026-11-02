//! Collection aggregates keep nested values and their Spark result schemas.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::cast;
use datafusion::prelude::{SessionConfig, SessionContext, col};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn canonicalize_unordered_collections(result: &mut Value, case: &Value) -> TestResult {
    // Only unordered aggregate outputs are bags; nested values and ordered windows stay intact.
    for name in case["unordered_fields"]
        .as_array()
        .ok_or("missing unordered fields")?
    {
        let index = result["schema"]["fields"]
            .as_array()
            .ok_or("missing schema")?
            .iter()
            .position(|field| field["name"] == *name)
            .ok_or("missing collection field")?;
        for row in result["rows"].as_array_mut().ok_or("missing rows")? {
            row[index]
                .as_array_mut()
                .ok_or("expected collection")?
                .sort_by_key(Value::to_string);
        }
    }
    result["rows"]
        .as_array_mut()
        .ok_or("missing rows")?
        .sort_by_key(Value::to_string);
    Ok(())
}

#[tokio::test]
async fn nested_collections_match_spark() -> TestResult {
    let records = include_str!("fixtures/nested-collections-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 722);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case).await;
            let mut expected = case["expected"].clone();
            canonicalize_unordered_collections(&mut expected, case)?;
            let matches = if let Ok(value) = &actual {
                let mut comparable = value.clone();
                canonicalize_unordered_collections(&mut comparable, case)?;
                comparable == expected
            } else {
                false
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn collection_results_preserve_native_sql_and_cast_validation() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/nested-collections-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 2)?;
    }
    for kind in ["integer", "string"] {
        for shape in ["struct", "nested", "struct_array"] {
            for source in ["mixed", "nulls", "empty"] {
                let table = format!("{kind}_{shape}_{source}");
                capture_spark_query_result(&configured, &json!({
                    "ansi":true,"sql":format!("SELECT COLLECT_LIST(k) AS collected FROM {table}")
                })).await?;
                let sql = format!("SELECT ARRAY_SORT(ARRAY_AGG(k)) AS collected FROM {table}");
                assert_eq!(
                    configured.sql(&sql).await?.collect().await?,
                    native.sql(&sql).await?.collect().await?,
                    "{sql}"
                );
            }
        }
    }
    for session in [&native, &configured] {
        let frame = session
            .sql("SELECT ARRAY_AGG(k) AS collected FROM integer_struct_mixed")
            .await?;
        let DataType::List(element) = frame.schema().field(0).data_type() else {
            return Err("expected collection list".into());
        };
        let required = DataType::List(Arc::new(element.as_ref().clone().with_nullable(false)));
        let result = frame
            .select(vec![cast(col("collected"), required)])?
            .create_physical_plan()
            .await;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("Unsupported CAST")),
            "{result:?}"
        );
    }
    Ok(())
}
