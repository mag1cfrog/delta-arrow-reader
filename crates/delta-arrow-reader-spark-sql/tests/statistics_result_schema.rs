//! Public result schemas when exact provider statistics replace aggregates.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::common::{ScalarValue, metadata::FieldMetadata, stats::Precision};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

fn references() -> TestResult<Vec<Value>> {
    Ok(
        include_str!("fixtures/statistics-result-schema-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?,
    )
}

async fn register_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    spark_inputs::register_recorded_inputs(session, metadata, batch_size)?;
    for view in metadata["views"].as_array().ok_or("missing views")? {
        let frame = session
            .sql(view["sql"].as_str().ok_or("missing view SQL")?)
            .await?;
        session.register_table(
            view["name"].as_str().ok_or("missing view name")?,
            frame.into_view(),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn statistics_results_match_independent_spark_schemas_and_rows() -> TestResult {
    let records = references()?;
    assert_eq!(records.len() - 1, 262);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
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
        register_inputs(&session, &records[0], batch_size).await?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case)
                .await
                .unwrap_or_else(|error| json!({"unexpected_error": error.to_string()}));
            if actual != case["expected"] {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
        let inexact = session
            .table("statistics_inexact")
            .await?
            .create_physical_plan()
            .await?
            .partition_statistics(None)?;
        assert!(matches!(inexact.num_rows, Precision::Inexact(_)));
        let unknown = session
            .table("statistics_unknown_bounds")
            .await?
            .create_physical_plan()
            .await?
            .partition_statistics(None)?;
        assert_eq!(unknown.num_rows, Precision::Exact(3));
        assert!(!matches!(
            unknown.column_statistics[0].min_value,
            Precision::Exact(_)
        ));
        for (id, shortcut) in [
            ("statistics_many_numeric_min", true),
            ("statistics_one_numeric_max", true),
            ("statistics_many_numeric_counts", true),
            ("statistics_empty_numeric_combined", true),
            ("statistics_inexact_min", false),
            ("statistics_unknown_bounds_min", false),
        ] {
            let case = records[1..]
                .iter()
                .find(|case| case["id"] == id && case["ansi"] == true)
                .ok_or("missing statistics control")?;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions::default(),
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            let plan = datafusion::physical_plan::displayable(physical.as_ref())
                .indent(true)
                .to_string();
            assert_eq!(!plan.contains("AggregateExec:"), shortcut, "{id}: {plan}");
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn native_snapshot(session: &SessionContext, sql: &str) -> TestResult<Value> {
    let frame = session.sql(sql).await?;
    let analyzed = format!("{:?}", frame.schema().as_arrow());
    let physical = frame.create_physical_plan().await?;
    let schema = format!("{:?}", physical.schema());
    let plan = datafusion::physical_plan::displayable(physical.as_ref())
        .indent(true)
        .to_string();
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        Ok(if column.is_null(row) {
                            Value::Null
                        } else if let Some(values) = column.as_any().downcast_ref::<Float64Array>()
                        {
                            json!(format!("{:016x}", values.value(row).to_bits()))
                        } else {
                            json!(array_value_to_string(column.as_ref(), row)?)
                        })
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"analyzed_schema":analyzed,"physical_schema":schema,"plan":plan,"rows":rows}))
}

#[tokio::test]
async fn spark_output_schema_keeps_native_statistics_plans_and_values() -> TestResult {
    let records = references()?;
    let config = SessionConfig::new()
        .with_batch_size(32)
        .with_target_partitions(2);
    let plain = SessionContext::new_with_config(config.clone());
    let configured = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config),
        )
        .build(),
    );
    for session in [&plain, &configured] {
        register_inputs(session, &records[0], 32).await?;
    }
    for id in [
        "statistics_many_numeric_combined",
        "statistics_many_zero_combined",
        "statistics_many_nan_combined",
        "statistics_input_true_combined",
        "statistics_inexact_combined",
    ] {
        let case = records[1..]
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == true)
            .ok_or("missing native statistics control")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let native = native_snapshot(&plain, sql).await?;
        let before = native_snapshot(&configured, sql).await?;
        assert_eq!(before, native, "{sql}: native contexts differ");
        let spark = capture_spark_query_result(&configured, case).await?;
        assert_eq!(spark, case["expected"], "{sql}");
        let after = native_snapshot(&configured, sql).await?;
        assert_eq!(
            after, before,
            "{sql}: Spark planning changed native behavior"
        );
    }
    Ok(())
}

#[tokio::test]
async fn exact_statistics_keep_analyzed_result_schema() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_batch(
        "statistics_ids",
        RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from_iter_values(0..3)) as ArrayRef,
        )])?,
    )?;
    let values = session
        .sql("SELECT CAST(1.5 AS DOUBLE) AS k FROM statistics_ids")
        .await?;
    session.register_table("statistics_values", values.into_view())?;
    let source = session
        .table("statistics_values")
        .await?
        .create_physical_plan()
        .await?;
    let statistics = source.partition_statistics(None)?;
    assert_eq!(statistics.num_rows, Precision::Exact(3));
    assert_eq!(
        statistics.column_statistics[0].min_value,
        Precision::Exact(ScalarValue::Float64(Some(1.5)))
    );
    let sql = "SELECT MIN(k) AS result FROM statistics_values";
    let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
    let physical = query.create_physical_plan(&session).await?;
    assert!(query.logical_plan.schema().field(0).is_nullable());
    assert!(physical.schema().field(0).is_nullable());
    let actual = capture_spark_query_result(&session, &json!({"sql":sql,"ansi":true})).await?;
    assert_eq!(actual["rows"], json!([["3ff8000000000000"]]));
    Ok(())
}

#[tokio::test]
async fn statistics_replacement_preserves_aggregate_metadata_and_float_bits() -> TestResult {
    use datafusion::common::DFSchema;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::expr_fn::min;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::physical_expr::aggregate::LoweredAggregateBuilder;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::aggregate_statistics::{
        AggregateStatistics, PreserveAggregateStatisticsSchema,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_planner::DefaultPhysicalPlanner;

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int32, false)],
        [("origin".into(), "provider".into())].into(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2]))],
    )?;
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
    let host_rules = session.state().physical_optimizers().to_vec();
    for value in [
        1.5,
        -0.0,
        0.0,
        f64::from_bits(0xfff8000000004321),
        f64::from_bits(0x7ff8000000001234),
    ] {
        let metadata = HashMap::from([("unit".to_owned(), "reading".to_owned())]);
        let input = Arc::new(ProjectionExec::try_new(
            [(lit(ScalarValue::Float64(Some(value))), "k".to_owned())],
            source.clone(),
        )?);
        let input_schema = input.schema();
        let expr = min(col("k"))
            .alias_with_metadata("result", Some(FieldMetadata::from(metadata.clone())));
        let aggregate = LoweredAggregateBuilder::new(
            &expr,
            &DFSchema::try_from(input_schema.clone())?,
            &input_schema,
            &ExecutionProps::new(),
        )
        .build()?;
        let original: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::default(),
            vec![aggregate.aggregate],
            vec![aggregate.filter],
            input,
            input_schema,
        )?);
        assert!(original.schema().field(0).is_nullable());
        assert_eq!(original.schema().field(0).metadata(), &metadata);
        assert_eq!(original.schema().metadata(), schema.metadata());
        let mut state = session.state();
        let native =
            AggregateStatistics::new().optimize(original.clone(), state.config_options())?;
        assert!(!native.schema().field(0).is_nullable());
        assert!(native.schema().field(0).metadata().is_empty());
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(PreserveAggregateStatisticsSchema::default());
        let replacement =
            AggregateStatistics::new().optimize(original.clone(), state.config_options())?;
        assert_eq!(replacement.schema(), original.schema());
        let physical = DefaultPhysicalPlanner::default().optimize_physical_plan(
            replacement,
            &state,
            |_, _| {},
        )?;
        assert_eq!(physical.schema(), original.schema());
        let plan = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();
        assert!(!plan.contains("AggregateExec:"), "{plan}");
        let rebuilt = physical
            .clone()
            .transform_up(|plan| {
                let children = plan.children().into_iter().cloned().collect();
                plan.with_new_children(children).map(Transformed::yes)
            })?
            .data;
        assert_eq!(rebuilt.schema(), physical.schema());
        let batches = collect(rebuilt, session.task_ctx()).await?;
        assert_eq!(batches.len(), 1);
        for batch in batches {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema(), physical.schema());
            let result = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("expected DOUBLE result")?;
            assert_eq!(result.value(0).to_bits(), value.to_bits());
        }
    }
    assert!(
        session
            .copied_config()
            .options()
            .extensions
            .get::<PreserveAggregateStatisticsSchema>()
            .is_none()
    );
    assert_eq!(
        host_rules.len(),
        session.state().physical_optimizers().len()
    );
    assert!(
        host_rules
            .iter()
            .zip(session.state().physical_optimizers())
            .all(|(before, after)| Arc::ptr_eq(before, after))
    );
    Ok(())
}
