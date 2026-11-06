//! Equal-result Spark CASE tails retain required volatility and preparation errors.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{ScalarValue, ToDFSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Case, Expr, LogicalPlanBuilder, col, lit, scalar_subquery};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
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

const REFERENCE: &str = include_str!("fixtures/case-result-tails-spark.jsonl");
const VOLATILE_SUBQUERIES: &str =
    include_str!("fixtures/case-result-tails-volatile-subqueries-spark.jsonl");
const RANGE_SUBQUERY: &str = include_str!("fixtures/case-result-tails-range-subquery-spark.jsonl");
const VOLATILE_FLOATS: &str =
    include_str!("fixtures/case-result-tails-volatile-floats-spark.jsonl");
const RAW_FLOATS: &str = include_str!("fixtures/case-result-tails-raw-floats-spark.jsonl");

fn references(source: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, count);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing input")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), count);
    Ok(records)
}

async fn compare_references(source: &str, count: usize) -> TestResult {
    let records = references(source, count)?;
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
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            comparison["id"] = case["id"].clone();
            comparison["input"] = case["input"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("CASE_RESULT_TAILS_1066_OBSERVATION {comparison}");
            if comparison["matches"] != true || comparison["optimized_matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn case_result_tails_match_spark() -> TestResult {
    compare_references(REFERENCE, 44).await
}

#[tokio::test]
async fn case_volatile_subquery_tails_match_spark() -> TestResult {
    compare_references(VOLATILE_SUBQUERIES, 4).await
}

#[tokio::test]
async fn case_deterministic_range_subquery_tails_match_spark() -> TestResult {
    compare_references(RANGE_SUBQUERY, 2).await
}

#[tokio::test]
async fn case_volatile_float_tails_match_spark() -> TestResult {
    compare_references(VOLATILE_FLOATS, 4).await
}

#[tokio::test]
async fn case_raw_float_tails_match_spark() -> TestResult {
    compare_references(RAW_FLOATS, 4).await
}

#[test]
fn case_tail_algebraic_boundaries_preserve_required_work() -> TestResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]).to_dfschema_ref()?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build());
    let guard = (lit(1_i32) / col("id")).eq(lit(0_i32));
    let result = col("id");
    let branch = |guard, result| (Box::new(guard), Box::new(result));
    let marked = |branches, fallback| Expr::Case(Case::new_spark_searched(branches, fallback));
    let equal = marked(
        vec![branch(guard.clone(), result.clone())],
        Some(Box::new(result.clone())),
    );
    assert_eq!(simplifier.simplify_algebraic_once(equal)?.data, result);
    let null = lit(ScalarValue::Int32(None));
    let implicit = marked(vec![branch(guard.clone(), null.clone())], None);
    assert_eq!(simplifier.simplify_algebraic_once(implicit)?.data, null);

    let volatile = datafusion::functions::expr_fn::uuid();
    let subquery = scalar_subquery(Arc::new(
        LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i32)])?
            .build()?,
    ));
    let volatile_subquery = scalar_subquery(Arc::new(
        LogicalPlanBuilder::empty(true)
            .project(vec![volatile.clone()])?
            .build()?,
    ));
    let opaque_subquery = scalar_subquery(Arc::new(
        datafusion::logical_expr::LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(sail_logical_plan::monotonic_id::MonotonicIdNode::try_new(
                Arc::new(LogicalPlanBuilder::empty(true).build()?),
                "sequence".to_owned(),
                sail_logical_plan::monotonic_id::MonotonicIdPurpose::SqlFunction,
            )?),
        }),
    ));
    for protected in [
        volatile.clone().eq(lit("")),
        volatile_subquery.clone().eq(lit("")),
        opaque_subquery.clone().eq(lit(0_i64)),
    ] {
        let prefix = vec![
            branch(guard.clone(), result.clone()),
            branch(protected, result.clone()),
        ];
        let mut branches = prefix.clone();
        branches.push(branch(guard.clone(), result.clone()));
        let expected = marked(prefix, Some(Box::new(result.clone())));
        let case = marked(branches, Some(Box::new(result.clone())));
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, expected);
    }
    for protected in [volatile, volatile_subquery, opaque_subquery] {
        let case = marked(
            vec![branch(guard.clone(), protected.clone())],
            Some(Box::new(protected)),
        );
        assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    }
    let deterministic_subquery_guard = marked(
        vec![branch(subquery.clone().eq(lit(1_i32)), result.clone())],
        Some(Box::new(result.clone())),
    );
    assert_eq!(
        simplifier
            .simplify_algebraic_once(deterministic_subquery_guard)?
            .data,
        result
    );
    let deterministic_subquery_results = marked(
        vec![branch(guard.clone(), subquery.clone())],
        Some(Box::new(subquery.clone())),
    );
    assert_eq!(
        simplifier
            .simplify_algebraic_once(deterministic_subquery_results)?
            .data,
        subquery
    );
    let distinct = marked(
        vec![branch(guard.clone(), lit(2_i32))],
        Some(Box::new(result.clone())),
    );
    assert_eq!(
        simplifier.simplify_algebraic_once(distinct.clone())?.data,
        distinct
    );
    let native = Expr::Case(Case::new(
        None,
        vec![branch(guard, result.clone())],
        Some(Box::new(result)),
    ));
    assert_eq!(
        simplifier.simplify_algebraic_once(native.clone())?.data,
        native
    );
    Ok(())
}

#[test]
fn case_tail_result_identity_uses_spark_floats_and_preserves_metadata() -> TestResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]).to_dfschema_ref()?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build());
    let nan = f64::from_bits(0x7ff8_0000_0000_0000);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    for (left, right, equal) in [
        (0.0, -0.0, true),
        (-0.0, 0.0, true),
        (nan, nan, true),
        (nan, other_nan, true),
        (1.0, 2.0, false),
    ] {
        let fallback = lit(right);
        let case = Expr::Case(Case::new_spark_searched(
            vec![(Box::new(col("id").eq(lit(0_i32))), Box::new(lit(left)))],
            Some(Box::new(fallback.clone())),
        ));
        let expected = if equal { fallback } else { case.clone() };
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, expected);
    }
    for (left, right) in [(0.0_f32, -0.0_f32), (f32::NAN, f32::from_bits(0x7fc0_0001))] {
        let fallback = lit(right);
        let case = Expr::Case(Case::new_spark_searched(
            vec![(Box::new(col("id").eq(lit(0_i32))), Box::new(lit(left)))],
            Some(Box::new(fallback.clone())),
        ));
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, fallback);
    }
    let case = Expr::Case(Case::new_spark_searched(
        vec![(
            Box::new(col("id").eq(lit(0_i32))),
            Box::new(col("id").alias("left")),
        )],
        Some(Box::new(col("id").alias("right"))),
    ));
    assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    let annotated = |source: &str| {
        col("id").alias_with_metadata(
            "value",
            Some(FieldMetadata::from(std::collections::HashMap::from([(
                "source".to_owned(),
                source.to_owned(),
            )]))),
        )
    };
    let case = Expr::Case(Case::new_spark_searched(
        vec![(
            Box::new(col("id").eq(lit(0_i32))),
            Box::new(annotated("left")),
        )],
        Some(Box::new(annotated("right"))),
    ));
    assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    Ok(())
}

#[tokio::test]
async fn spark_case_tail_rules_preserve_native_case_and_if() -> TestResult {
    let records = references(REFERENCE, 44)?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(
                &configured,
                "SELECT CASE WHEN id=0 THEN required ELSE required END AS r FROM case_parent_mixed",
                &SparkSqlOptions::default(),
            )
            .await?;
            collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
        }
        for sql in [
            "SELECT CASE WHEN 1/id=0 THEN required ELSE required END AS r FROM case_parent_mixed",
            "SELECT CASE WHEN id=0 THEN CAST(NULL AS INT) END AS r FROM case_parent_mixed",
            "SELECT CASE id WHEN 0 THEN v ELSE v END AS r FROM case_parent_mixed",
            "SELECT if(id=0,v,v) AS r FROM case_parent_mixed",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}
