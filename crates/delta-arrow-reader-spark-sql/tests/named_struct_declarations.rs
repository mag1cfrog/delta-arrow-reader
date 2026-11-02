//! Spark named_struct preserves its child field declarations.

use std::collections::{BTreeMap, BTreeSet};

use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn references() -> TestResult<Vec<Value>> {
    Ok(
        include_str!("fixtures/named-struct-declarations-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?,
    )
}

fn fixture_identity(case: &Value) -> TestResult<(&str, &str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?,
        case["input"].as_str().ok_or("missing input")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

#[test]
fn named_struct_fixture_partition_preserves_every_original_record() -> TestResult {
    let original = include_str!("fixtures/named-struct-fields-spark.jsonl");
    let scoped = include_str!("fixtures/named-struct-declarations-spark.jsonl");
    assert_eq!(scoped.lines().next(), original.lines().next());
    let original_records = original
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let scoped_records = references()?;
    assert_eq!(original_records.len() - 1, 110);
    assert_eq!(scoped_records.len() - 1, 104);
    assert_eq!(
        original_records[0]["capture_sha256"],
        "c17ba5f306d52622bf655f155cf9556114c4bfa08de4b60d148820947e8198d4"
    );
    let mut original_lines = BTreeMap::new();
    for (case, line) in original_records[1..].iter().zip(original.lines().skip(1)) {
        assert!(
            original_lines
                .insert(fixture_identity(case)?, line)
                .is_none()
        );
    }
    // These six unchanged generated-label records remain owned by #766/#710.
    let pending = BTreeSet::from([
        ("generated_label", "inputs", true),
        ("generated_label", "inputs", false),
        ("generated_label", "null_inputs", true),
        ("generated_label", "null_inputs", false),
        ("generated_label", "empty_inputs", true),
        ("generated_label", "empty_inputs", false),
    ]);
    let mut selected = BTreeSet::new();
    for (case, line) in scoped_records[1..].iter().zip(scoped.lines().skip(1)) {
        let identity = fixture_identity(case)?;
        assert!(selected.insert(identity));
        assert!(!pending.contains(&identity));
        assert_eq!(original_lines.get(&identity), Some(&line));
    }
    assert_eq!(selected.len(), 104);
    assert_eq!(pending.len(), 6);
    assert_eq!(
        selected.union(&pending).copied().collect::<BTreeSet<_>>(),
        original_lines.keys().copied().collect::<BTreeSet<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn named_struct_declarations_and_values_match_spark() -> TestResult {
    let records = references()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 104);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        assert!(identities.insert((
            id,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("condition").is_some() {
            assert!(matches!(
                id,
                "odd_arity" | "numeric_name" | "null_name" | "column_name"
            ));
        } else {
            assert_eq!(case["expected"]["schema"]["fields"][0]["name"], "result");
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
        }
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
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
            let id = case["id"].as_str().ok_or("missing ID")?;
            if case["expected"].get("condition").is_some() {
                continue;
            }
            comparisons += 1;
            let actual = capture_spark_query_result(&session, case).await;
            match actual {
                Ok(actual) if actual == case["expected"] => {}
                Ok(actual) => differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["input"], case["ansi"], case["expected"]
                )),
                Err(error) => differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: {error}",
                    case["input"], case["ansi"]
                )),
            }
        }
    }
    assert_eq!(comparisons, 192);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn named_struct_rejects_invalid_names_and_arity() -> TestResult {
    let records = references()?;
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            let message = match id {
                "odd_arity" => "named_struct requires an even number of arguments",
                "numeric_name" | "null_name" | "column_name" => {
                    "named_struct requires 0-th (0-indexed) field name as constant string"
                }
                _ => continue,
            };
            assert!(case["expected"].get("condition").is_some());
            comparisons += 1;
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let error = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..SparkSqlOptions::default()
                },
            )
            .await
            .expect_err(sql);
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
    }
    assert_eq!(comparisons, 16);
    Ok(())
}

async fn native_snapshot(
    session: &SessionContext,
    sql: &str,
) -> TestResult<(String, Vec<RecordBatch>)> {
    let frame = session.sql(sql).await?;
    let analyzed = frame.schema().clone();
    let physical = frame.create_physical_plan().await?;
    let declaration = format!(
        "{analyzed:?}\n{:?}\n{}",
        physical.schema(),
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    Ok((declaration, collect(physical, session.task_ctx()).await?))
}

#[tokio::test]
async fn spark_named_struct_leaves_native_sql_unchanged() -> TestResult {
    let records = references()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 32)?;
    }
    for id in [
        "literal_no_input",
        "nonnull_child",
        "nullable_child",
        "null_child",
        "nested_constructor",
        "nested_provider",
        "extension_metadata",
    ] {
        let case = records[1..]
            .iter()
            .find(|case| {
                case["id"] == id
                    && case["ansi"] == true
                    && (case["input"] == "inputs" || case["input"] == "none")
            })
            .ok_or("missing native control")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let expected = native_snapshot(&native, sql).await?;
        let before = native_snapshot(&configured, sql).await?;
        assert_eq!(before, expected, "{sql}");
        assert_eq!(
            capture_spark_query_result(&configured, case).await?,
            case["expected"],
            "{sql}"
        );
        let after = native_snapshot(&configured, sql).await?;
        assert_eq!(after, before, "{sql}: Spark planning changed native SQL");
    }
    Ok(())
}
