//! Spark CASE merges nested declarations before pruning and uses live fields at execution.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::common::DFSchema;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, ExprSchemable, col, expr::Case, lit};
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

fn references() -> TestResult<Vec<Value>> {
    let mut records = include_str!("fixtures/case-nested-result-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 74);
    for (fixture, count) in [
        (include_str!("fixtures/case-nested-pruning-spark.jsonl"), 22),
        (
            include_str!("fixtures/case-nested-numeric-pruning-spark.jsonl"),
            2,
        ),
    ] {
        let supplement = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(supplement.len() - 1, count);
        for key in ["spark_version", "spark_revision", "inputs"] {
            assert_eq!(supplement[0][key], records[0][key]);
        }
        records.extend(supplement.into_iter().skip(1));
    }
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing provider")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{case}");
            }
            assert!(case["expected"]["rows"].is_array());
        }
    }
    Ok(records)
}

#[test]
fn spark_case_type_merges_nested_child_declarations() -> TestResult {
    for depth in [1, 8, 32] {
        let mut required = DataType::Boolean;
        let mut nullable = DataType::Boolean;
        for level in 0..depth {
            required = DataType::List(Arc::new(Field::new("item", required, false)));
            nullable = DataType::List(Arc::new(Field::new("item", nullable, level == 0)));
        }
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("required", required.clone(), false),
                Field::new("nullable", nullable.clone(), false),
            ]
            .into(),
            Default::default(),
        )?;
        let arms = vec![(Box::new(lit(true)), Box::new(col("required")))];
        let otherwise = Some(Box::new(col("nullable")));
        let spark = Expr::Case(Case::new_spark_searched(arms.clone(), otherwise.clone()));
        assert_eq!(spark.get_type(&schema)?, nullable, "depth={depth}");
        assert!(
            !spark.nullable(&schema)?,
            "literal TRUE keeps the required top-level result"
        );
        let native = Expr::Case(Case::new(None, arms, otherwise));
        assert_eq!(native.get_type(&schema)?, required, "depth={depth}");
    }
    Ok(())
}

#[tokio::test]
async fn case_nested_result_fields_match_spark() -> TestResult {
    let records = references()?;
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["input"] = case["input"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("CASE_NESTED_FIELDS_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 196);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn case_validates_unselected_nested_results_and_conditions() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi_mode in [true, false] {
        for (sql, diagnostic) in [
            (
                "SELECT CASE WHEN TRUE THEN ARRAY(TRUE) ELSE NAMED_STRUCT('leaf', TRUE) END AS r",
                "CASE result types are incompatible",
            ),
            (
                "SELECT CASE WHEN FALSE THEN ARRAY(TRUE) ELSE NAMED_STRUCT('leaf', TRUE) END AS r",
                "CASE result types are incompatible",
            ),
            (
                "SELECT CASE WHEN 1 THEN ARRAY(TRUE) ELSE ARRAY(FALSE) END AS r",
                "CASE WHEN condition must be BOOLEAN",
            ),
            (
                "SELECT CASE WHEN TRUE THEN ARRAY(TRUE) WHEN 1 THEN ARRAY(FALSE) ELSE ARRAY(TRUE) END AS r",
                "CASE WHEN condition must be BOOLEAN",
            ),
            (
                "SELECT CASE WHEN FALSE THEN missing_result ELSE ARRAY(TRUE) END AS r",
                "missing_result",
            ),
        ] {
            let error = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await
            .err()
            .ok_or("invalid CASE reached physical planning")?;
            assert!(error.to_string().contains(diagnostic), "{sql}: {error}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_nested_case() -> TestResult {
    let records = references()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(&configured,
                "SELECT CASE WHEN id=0 THEN ARRAY(TRUE) ELSE ARRAY(CAST(NULL AS BOOLEAN)) END AS r FROM case_nested_mixed",
                &SparkSqlOptions::default()).await?;
            collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
        }
        for sql in [
            "SELECT CASE WHEN TRUE THEN [TRUE] ELSE [CAST(NULL AS BOOLEAN)] END AS r",
            "SELECT CASE WHEN FALSE THEN [CAST(NULL AS BOOLEAN)] ELSE [TRUE] END AS r",
            "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN [CAST(NULL AS BOOLEAN)] ELSE [TRUE] END AS r",
            "SELECT CASE WHEN id=0 THEN [TRUE] ELSE [CAST(NULL AS BOOLEAN)] END AS r FROM case_nested_mixed",
            "SELECT CASE WHEN id=0 THEN [DATE '2024-02-29'] ELSE [CAST(NULL AS DATE)] END AS r FROM case_nested_empty",
            "SELECT CASE WHEN id=0 THEN NAMED_STRUCT('leaf', TRUE) ELSE NAMED_STRUCT('leaf', CAST(NULL AS BOOLEAN)) END AS r FROM case_nested_null",
            "SELECT CASE WHEN TRUE THEN [TRUE] ELSE NAMED_STRUCT('leaf', TRUE) END AS r",
            "SELECT CASE WHEN CAST('bad' AS BOOLEAN) THEN [TRUE] ELSE [FALSE] END AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}
