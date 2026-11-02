//! Equivalent window offsets retain every output after expression simplification.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_window::lead_lag::lead_udwf;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, col, lit};
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

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
async fn native_window_cse_preserves_scalar_inputs_and_filter_boundaries() -> TestResult {
    let session = SessionContext::new();
    let input = session
        .sql(
            "SELECT * FROM VALUES(1,0,1,10),(2,0,2,20),(3,0,1,30),\
             (4,1,1,40),(5,1,2,50) inputs(id,g,k,v)",
        )
        .await?
        .into_unoptimized_plan();
    let expression = Expr::from(WindowFunction::new(
        lead_udwf(),
        vec![col("v") + lit(1_i64), lit(1_i64)],
    ));
    let window = expression
        .clone()
        .partition_by(vec![col("g")])
        .order_by(vec![col("id").sort(true, true)])
        .build()?;
    let other = expression
        .partition_by(vec![col("g"), col("k")])
        .order_by(vec![col("id").sort(true, true)])
        .build()?;
    let plan = LogicalPlanBuilder::from(input)
        .window(vec![other.alias("other")])?
        .window(vec![
            window.clone().alias("first"),
            window.alias("duplicate"),
        ])?
        .filter(
            col("g")
                .eq(lit(0_i64))
                .and(col("k").eq(lit(1_i64)))
                .and(col("first").lt(lit(25_i64))),
        )?
        .project(vec![
            col("id"),
            col("first"),
            col("duplicate"),
            col("other"),
        ])?
        .sort(vec![col("id").sort(true, true)])?
        .build()?;
    let optimized = Optimizer::with_rules(vec![
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(PushDownFilter::new()),
    ])
    .optimize(plan, &OptimizerContext::new(), |_, _| {})?;
    let mut scalar_inputs = 0;
    optimized.apply(|plan| {
        if let LogicalPlan::Projection(projection) = plan {
            for expression in &projection.expr {
                assert!(!expression.exists(|expr| Ok(matches!(expr, Expr::WindowFunction(_))))?);
                if expression.exists(|expr| Ok(matches!(expr, Expr::BinaryExpr(_))))? {
                    scalar_inputs += 1;
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(
        scalar_inputs > 0,
        "window arguments should still share a scalar input"
    );
    let physical = session
        .execute_logical_plan(optimized)
        .await?
        .create_physical_plan()
        .await?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let column = column.as_primitive::<Int64Type>();
                        column.is_valid(row).then(|| column.value(row))
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    assert_eq!(json!(rows), json!([[1, 21, 21, 31]]));
    Ok(())
}
