//! Preceding SELECT aliases retain values, scope, types and one computed producer.

use std::collections::BTreeSet;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use datafusion::common::{
    ScalarValue,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Expr, LogicalPlan, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_executed_query_result, remove_nullability_and_metadata};

fn case_key(case: &Value) -> TestResult<(&str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

fn session(partitions: usize, batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(partitions)
                        .with_batch_size(batch_size),
                ),
        )
        .build(),
    )
}

fn assert_no_alias_references(plan: &LogicalPlan) -> TestResult {
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                assert!(
                    !matches!(expr, Expr::Placeholder(_)),
                    "private alias reference survived: {expr}"
                );
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn request() -> TestResult<Value> {
    let mut request: Value = serde_json::from_str(include_str!(
        "fixtures/lateral-select-aliases/requests.json"
    ))?;
    let supplemental: Value = serde_json::from_str(include_str!(
        "fixtures/lateral-select-aliases/tuple-subquery-requests.json"
    ))?;
    assert_eq!(
        request["corpora"][0]["metadata"],
        supplemental["corpora"][0]["metadata"]
    );
    request["corpora"][0]["cases"]
        .as_array_mut()
        .ok_or("missing cases")?
        .extend(
            supplemental["corpora"][0]["cases"]
                .as_array()
                .ok_or("missing supplemental cases")?
                .iter()
                .cloned(),
        );
    Ok(request)
}

#[tokio::test]
async fn preceding_select_aliases_match_spark() -> TestResult {
    let request = request()?;
    let mut records = include_str!("fixtures/lateral-select-aliases/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let supplemental =
        include_str!("fixtures/lateral-select-aliases/tuple-subquery-spark-reference.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(supplemental[0]["spark_version"], "4.2.0");
    assert_eq!(
        supplemental[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    records.extend(supplemental.into_iter().skip(1));
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let cases = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    let expected = cases
        .iter()
        .map(case_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    let observed = records[1..]
        .iter()
        .map(case_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(expected.len(), cases.len());
    assert_eq!(observed.len(), records.len() - 1);
    assert_eq!(observed, expected);
    for case in &records[1..] {
        let source = cases
            .iter()
            .find(|source| case_key(source).ok() == case_key(case).ok())
            .ok_or("unrequested reference")?;
        assert_eq!(case["sql"], source["sql"]);
        assert_eq!(case["ordered"], source["ordered"]);
        assert_eq!(case["status"], "ok");
    }

    // The first layout matches Spark's seeded stream; the second checks its direct producer.
    for (input_batch, partitions, output_batch) in [(1, 1, 1), (2, 3, 3)] {
        let ctx = session(partitions, output_batch);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &ctx,
            &request["corpora"][0]["metadata"],
            input_batch,
            1,
        )?;
        for case in &records[1..] {
            let (id, ansi) = case_key(case)?;
            let query = plan_query(
                &ctx,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await?;
            assert_no_alias_references(&query.logical_plan)?;
            let actual = capture_executed_query_result(&ctx, case).await?;
            if id == "rand_reuse" {
                let control = serde_json::json!({"sql":"SELECT a,rand(17) AS b FROM numbers","ansi":ansi,"ordered":false});
                let direct = capture_executed_query_result(&ctx, &control).await?;
                let producer_rows = actual["rows"]
                    .as_array()
                    .ok_or("missing alias random rows")?
                    .iter()
                    .map(|row| serde_json::json!([row[0], row[1]]))
                    .collect::<Vec<_>>();
                assert_eq!(
                    serde_json::json!(producer_rows),
                    direct["rows"],
                    "alias path changed RAND producer samples"
                );
                let direct_query = plan_query(
                    &ctx,
                    control["sql"].as_str().ok_or("missing direct SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                let direct_plan = direct_query.create_physical_plan(&ctx).await?;
                let alias_plan = query.create_physical_plan(&ctx).await?;
                println!(
                    "SELECT_ALIAS_RAND_LAYOUT {}",
                    serde_json::json!({"ansi":ansi,"input_batch":input_batch,"target_partitions":partitions,"output_batch":output_batch,"direct_rows":direct["rows"],"alias_rows":actual["rows"],"direct_plan":datafusion::physical_plan::displayable(direct_plan.as_ref()).indent(true).to_string(),"alias_plan":datafusion::physical_plan::displayable(alias_plan.as_ref()).indent(true).to_string()})
                );
            }
            if id == "rand_reuse" && input_batch != 1 {
                // #1248 retains random sample differences between execution layouts.
                let rows = actual["rows"].as_array().ok_or("missing random rows")?;
                let expected = case["expected"]["rows"]
                    .as_array()
                    .ok_or("missing reference rows")?;
                assert_eq!(rows.len(), expected.len());
                for (row, expected) in rows.iter().zip(expected) {
                    assert_eq!(row[0], expected[0]);
                    assert_eq!(row[1], row[2]);
                    let value = |column: usize| -> TestResult<f64> {
                        Ok(f64::from_bits(u64::from_str_radix(
                            row[column].as_str().ok_or("missing DOUBLE bits")?,
                            16,
                        )?))
                    };
                    assert_eq!(value(1)? * 2.0, value(3)?);
                }
            } else {
                assert_eq!(
                    actual["rows"], case["expected"]["rows"],
                    "{id}, ANSI={ansi}, input_batch={input_batch}, output_batch={output_batch}"
                );
            }
            let mut actual_schema = actual["schema"].clone();
            let mut spark_schema = case["schema_stages"]["executed"].clone();
            remove_nullability_and_metadata(&mut actual_schema);
            remove_nullability_and_metadata(&mut spark_schema);
            assert_eq!(actual_schema, spark_schema, "{id}, ANSI={ansi}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn reused_volatile_alias_retains_same_row_values() -> TestResult {
    let request = request()?;
    for ansi_mode in [false, true] {
        for (partitions, batch_size) in [(1, 1), (3, 2)] {
            let ctx = session(partitions, batch_size);
            spark_inputs::register_recorded_inputs_with_partition_count(
                &ctx,
                &request["corpora"][0]["metadata"],
                batch_size,
                partitions,
            )?;
            let evaluated_rows = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&evaluated_rows);
            ctx.register_udf(create_udf(
                "observed_alias",
                vec![DataType::Int32],
                DataType::Int32,
                Volatility::Volatile,
                Arc::new(move |args| {
                    let values = args.first().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "missing observed alias input".to_string(),
                        )
                    })?;
                    Ok(match values {
                        ColumnarValue::Array(array) => {
                            let start = observed.fetch_add(array.len(), Ordering::SeqCst);
                            ColumnarValue::Array(Arc::new(Int32Array::from_iter_values(
                                (start..start + array.len()).map(|value| value as i32 + 1),
                            )))
                        }
                        ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::Int32(
                            Some(observed.fetch_add(1, Ordering::SeqCst) as i32 + 1),
                        )),
                    })
                }),
            ));
            let query = plan_query(
                &ctx,
                "SELECT a,observed_alias(a) AS b,b AS c,b*2 AS d FROM numbers",
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await?;
            assert_no_alias_references(&query.logical_plan)?;
            let physical = query.create_physical_plan(&ctx).await?;
            let planning_rows = evaluated_rows.load(Ordering::SeqCst);
            assert!(
                physical
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| field.data_type() == &DataType::Int32)
            );
            let mut rows = 0;
            for batch in collect(physical, ctx.task_ctx()).await? {
                let b = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("missing b INT values")?;
                let c = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("missing c INT values")?;
                let d = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("missing d INT values")?;
                for row in 0..batch.num_rows() {
                    assert_eq!(b.value(row), c.value(row));
                    assert_eq!(b.value(row) * 2, d.value(row));
                }
                rows += batch.num_rows();
            }
            assert_eq!(rows, 4);
            println!(
                "SELECT_ALIAS_REUSE_OBSERVATION {}",
                serde_json::json!({"ansi":ansi_mode,"input_partitions":partitions,"batch_size":batch_size,"rows":rows,"evaluated_rows_at_planning":planning_rows,"total_evaluated_rows":evaluated_rows.load(Ordering::SeqCst)})
            );
        }
    }
    Ok(())
}
