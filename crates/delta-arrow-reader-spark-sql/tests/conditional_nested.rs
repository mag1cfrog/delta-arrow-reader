//! Independent Spark references for nested conditional common types and admission.

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn nested_conditional_results_match_spark() -> TestResult {
    let mut records = include_str!("fixtures/conditional-nested-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 12);
    for (fixture, count) in [
        (include_str!("fixtures/conditional-nested-supplement-spark.jsonl"), 110),
        (include_str!("fixtures/conditional-nested-order-spark.jsonl"), 16),
        (include_str!("fixtures/conditional-nested-boundaries-spark.jsonl"), 10),
    ] {
        let supplement = fixture.lines().map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(supplement.len() - 1, count);
        assert_eq!(supplement[0]["spark_version"], records[0]["spark_version"]);
        assert_eq!(supplement[0]["spark_revision"], records[0]["spark_revision"]);
        assert_eq!(supplement[0]["inputs"], records[0]["inputs"]);
        records.extend(supplement.into_iter().skip(1));
    }
    assert_eq!(records.len() - 1, 148);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
        }
    }
    let mut failures = Vec::new();
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut phase = "resolve";
            let mut public = None;
            let mut before_collect = None;
            let mut executed = None;
            // Spark can refine nested declarations after analysis; compare each stage independently.
            let actual: TestResult<Vec<Vec<Value>>> = async {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                ).await?;
                phase = "public_schema";
                if query.column_names.len() != query.logical_plan.schema().fields().len() {
                    return Err("public field count differs from output names".into());
                }
                public = Some(spark_results::capture_schema(
                    query.logical_plan.schema().fields(), Some(&query.column_names),
                )?);
                phase = "create_physical_plan";
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                phase = "physical_schema";
                before_collect = Some(spark_results::capture_schema(schema.fields(), None)?);
                phase = "collect";
                let batches = collect(physical, session.task_ctx()).await?;
                phase = "encode";
                let mut rows = Vec::new();
                for batch in batches {
                    spark_results::validate_batch(schema.as_ref(), &batch)?;
                    for row in 0..batch.num_rows() {
                        rows.push(batch.columns().iter().map(|column| {
                            spark_results::encode_value(column.as_ref(), row)
                        }).collect::<TestResult<Vec<_>>>()?);
                    }
                }
                executed = Some(spark_results::capture_schema(schema.fields(), None)?);
                rows.sort_by_key(|row| json!(row).to_string());
                Ok(rows)
            }.await;
            let public_matches = case["schema_stages"].get("analyzed")
                .is_none_or(|expected| public.as_ref() == Some(expected));
            let before_matches = case["schema_stages"].get("executed_before_collect")
                .is_none_or(|expected| before_collect.as_ref() == Some(expected));
            let executed_matches = case["schema_stages"].get("executed")
                .is_none_or(|expected| executed.as_ref() == Some(expected));
            let rows_match = case["expected"].get("rows")
                .is_none_or(|expected| actual.as_ref().is_ok_and(|rows| json!(rows) == *expected));
            let error_matches = if let Some(condition) = case["expected"]["condition"].as_str() {
                let expected_phase = match case["spark_error_phase"].as_str() {
                    Some("sql") => "resolve",
                    Some("collect") => "collect",
                    other => return Err(format!("unsupported reference error phase: {other:?}").into()),
                };
                phase == expected_phase && actual.as_ref().err().is_some_and(|error| {
                    error.to_string().contains(&format!("[{condition}]"))
                })
            } else {
                actual.is_ok()
            };
            let matches = public_matches && before_matches && executed_matches && rows_match && error_matches;
            eprintln!("NESTED_CONDITIONAL_OBSERVATION {}", json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "public_schema":public, "before_collect":before_collect, "physical_schema":executed,
                "rows":actual.as_ref().ok(), "native_phase":phase,
                "error":actual.as_ref().err().map(ToString::to_string),
                "public_matches":public_matches, "before_matches":before_matches,
                "physical_matches":executed_matches, "rows_match":rows_match,
                "error_matches":error_matches, "matches":matches,
            }));
            if !matches {
                failures.push(format!(
                    "{} ANSI={} batch={batch_size}: public={public_matches} before={before_matches} executed={executed_matches} rows={rows_match} error={error_matches}; phase={phase}, expected={}, stages={}, actual={actual:?}, public={public:?}, before={before_collect:?}, executed={executed:?}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, 296);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
