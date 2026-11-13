//! Compare DISTINCT approximate percentile values and executed types with Spark.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

#[tokio::test]
async fn distinct_approximate_percentiles_match_spark() -> TestResult {
    let references = include_str!("fixtures/approx-percentile-distinct/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 88);
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        for case in &references[1..] {
            let mut actual = spark_results::capture_executed_query_result(&session, case)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "APPROX_DISTINCT {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ansi={} batch={batch}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn distinct_percentile_uses_bounded_windows_and_respects_memory_limits() -> TestResult {
    use datafusion::common::DataFusionError;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{collect, displayable};

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(64 * 1024, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let session = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_batch_size(128)
            .with_target_partitions(1),
        runtime,
    );
    for function in [
        "APPROX_PERCENTILE(DISTINCT CAST(id%1001 AS DOUBLE),0.5)",
        "PERCENTILE(DISTINCT CAST(id%1001 AS DOUBLE),0.5,id%3)",
    ] {
        let query = plan_query(
            &session,
            &format!("SELECT {function} AS d FROM range(0,200000)"),
            &SparkSqlOptions::default(),
        )
        .await?;
        let physical = query.create_physical_plan(&session).await?;
        let plan = displayable(physical.as_ref()).indent(true).to_string();
        assert!(plan.contains("BoundedWindowAggExec"), "{plan}");
        assert!(plan.contains("mode=[Sorted]"), "{plan}");
        let error = match collect(physical, session.task_ctx()).await {
            Ok(_) => return Err("expected the DISTINCT sort to exceed the memory limit".into()),
            Err(error) => error,
        };
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_and_native_percentiles_keep_their_original_plans() -> TestResult {
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;
    use datafusion::physical_plan::{collect, displayable};

    let session = SessionContext::new();
    for function in ["APPROX_PERCENTILE(v,0.5)", "PERCENTILE(v,0.5)", "MEDIAN(v)"] {
        let sql = format!("SELECT {function} AS b FROM VALUES (1D),(1D),(1D),(9D),(20D) AS t(v)");
        let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
        let physical = query.create_physical_plan(&session).await?;
        assert!(
            !displayable(physical.as_ref())
                .indent(true)
                .to_string()
                .contains("WindowAggExec")
        );
        let batches = collect(physical, session.task_ctx()).await?;
        assert_eq!(
            batches[0].column(0).as_primitive::<Float64Type>().value(0),
            1.0
        );
    }
    let batches = session.sql("SELECT APPROX_PERCENTILE_CONT(v,0.5) AS b FROM (VALUES (1.0::DOUBLE),(1.0::DOUBLE),(1.0::DOUBLE),(9.0::DOUBLE),(20.0::DOUBLE)) AS t(v)").await?.collect().await?;
    assert_eq!(
        batches[0].column(0).as_primitive::<Float64Type>().value(0),
        1.0
    );
    Ok(())
}
