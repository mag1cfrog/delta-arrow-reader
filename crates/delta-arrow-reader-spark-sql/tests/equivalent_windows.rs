//! Equivalent window offsets retain every output after expression simplification.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Dependency, FunctionalDependence, FunctionalDependencies};
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::logical_plan::Window;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit};
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

#[tokio::test]
async fn equivalent_windows_match_spark() -> TestResult {
    let records = include_str!("fixtures/equivalent-windows-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 66);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
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
            let expected = &case["expected"];
            if let Some(condition) = expected["condition"].as_str() {
                let expected_cause = match condition {
                    "_LEGACY_ERROR_TEMP_1035" => "[WINDOW_OFFSET_FRAME_MISMATCH]",
                    "WINDOW_FUNCTION_FRAME_NOT_ORDERED" => "[WINDOW_FUNCTION_FRAME_NOT_ORDERED]",
                    _ => return Err(format!("unclassified reference error: {condition}").into()),
                };
                let planned = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                match planned {
                    Err(error) if error.to_string().contains(expected_cause) => {}
                    other => differences.push(format!(
                        "{} input={} ANSI={} batch={batch_size}: expected {condition} during planning, got {other:?}",
                        case["id"], case["input"], case["ansi"]
                    )),
                }
            } else {
                let actual = capture_spark_query_result(&session, case).await;
                if actual.as_ref().ok() != Some(expected) {
                    differences.push(format!(
                        "{} input={} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                        case["id"], case["input"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn equivalent_windows_keep_native_sql_unchanged() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/equivalent-windows-spark.jsonl")
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
    for table in ["inputs", "empty_inputs", "null_inputs"] {
        for offset in ["2", "1+1", "CAST(2 AS INT)"] {
            let sql = format!(
                "SELECT id,LEAD(v,{offset}) OVER w AS value,\
                 LEAD(v,{offset}) OVER w AS duplicate \
                 FROM {table} WINDOW w AS (ORDER BY id) ORDER BY id"
            );
            let mut results = Vec::new();
            for session in [&native, &configured] {
                let query = session.sql(&sql).await?;
                let logical_schema = query.schema().clone();
                let physical = query.create_physical_plan().await?;
                assert_eq!(logical_schema.as_arrow(), physical.schema().as_ref());
                results.push(collect(physical, session.task_ctx()).await?);
            }
            assert_eq!(results[0], results[1], "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn window_aliases_preserve_row_number_dependencies() -> TestResult {
    let input = Arc::new(LogicalPlanBuilder::values(vec![vec![lit(1)], vec![lit(2)]])?.build()?);
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for partitioned in [false, true] {
        let expected = if partitioned {
            FunctionalDependencies::empty()
        } else {
            FunctionalDependencies::new(vec![
                FunctionalDependence::new(vec![1], vec![0, 1], false).with_mode(Dependency::Single),
            ])
        };
        let mut function = WindowFunction::new(row_number_udwf(), vec![]);
        if partitioned {
            function.params.partition_by = vec![col("column1")];
        }
        let expression = Expr::WindowFunction(Box::new(function));
        for expression in [
            expression.clone(),
            expression.clone().alias("number"),
            expression.alias("inner").alias("outer"),
        ] {
            let window = Window::try_new(vec![expression], input.clone())?;
            assert_eq!(window.schema.functional_dependencies(), &expected);
        }
        let partition = if partitioned { "PARTITION BY id" } else { "" };
        let sql = format!(
            "SELECT id,ROW_NUMBER() OVER ({partition} ORDER BY id) AS number \
             FROM VALUES(1),(2) inputs(id)"
        );
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        let mut windows = 0;
        query.logical_plan.apply(|plan| {
            if let LogicalPlan::Window(window) = plan {
                assert_eq!(window.schema.functional_dependencies(), &expected, "{sql}");
                windows += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(windows, 1, "{sql}");
    }
    Ok(())
}
