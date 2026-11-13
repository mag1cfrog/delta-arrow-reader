//! Shared and captured EXISTS bindings match independent Spark rows and executed types.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::JoinType;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, encode_value, remove_nullability_and_metadata,
};

fn result_types(schema: &Value) -> TestResult<Value> {
    let mut types = json!(
        schema["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .map(|field| field["type"].clone())
            .collect::<Vec<_>>()
    );
    remove_nullability_and_metadata(&mut types);
    Ok(types)
}

fn identity(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn load_references() -> TestResult<(Vec<Value>, Value)> {
    let mut cases = Vec::new();
    let mut inputs = BTreeMap::<String, Value>::new();
    for fixture in [
        include_str!("fixtures/retained-exists-bindings/original-spark-results.jsonl"),
        include_str!("fixtures/retained-exists-bindings/spark-results.jsonl"),
        include_str!("fixtures/retained-exists-bindings/outer-key-spark-results.jsonl"),
        include_str!("fixtures/retained-exists-bindings/outer-payload-spark-results.jsonl"),
        include_str!("fixtures/retained-exists-bindings/supplement-spark-results.jsonl"),
    ] {
        let records = fixture
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
        let header = &records[0];
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(
            records.len() - 1,
            header["expected_record_count"]
                .as_u64()
                .ok_or("missing capture count")? as usize
        );
        for corpus in header["corpora"].as_array().ok_or("missing corpora")? {
            for input in corpus["metadata"]["inputs"]
                .as_array()
                .ok_or("missing inputs")?
            {
                let name = input["name"]
                    .as_str()
                    .ok_or("missing input name")?
                    .to_owned();
                if let Some(previous) = inputs.insert(name, input.clone()) {
                    assert_eq!(previous, *input);
                }
            }
        }
        cases.extend(records.into_iter().skip(1));
    }
    let mut expected = BTreeSet::new();
    let mut definitions = BTreeMap::new();
    for source in [
        include_str!("fixtures/retained-exists-bindings/seed-requests.json"),
        include_str!("fixtures/retained-exists-bindings/requests.json"),
        include_str!("fixtures/retained-exists-bindings/supplement-requests.json"),
    ] {
        let request: Value = serde_json::from_str(source)?;
        for corpus in request["corpora"]
            .as_array()
            .ok_or("missing request corpora")?
        {
            for case in corpus["cases"]
                .as_array()
                .ok_or("missing requested cases")?
            {
                let key = identity(case)?;
                assert!(expected.insert(key.clone()));
                assert!(definitions.insert(key, case.clone()).is_none());
            }
        }
    }
    let mut observed = BTreeSet::new();
    for case in &cases {
        let key = identity(case)?;
        assert!(observed.insert(key.clone()));
        assert_eq!(case["status"], "ok");
        let definition = definitions.get(&key).ok_or("missing case definition")?;
        assert_eq!(definition["sql"], case["sql"]);
        assert_eq!(definition["ordered"], case["ordered"]);
    }
    assert_eq!(observed, expected);
    assert_eq!(cases.len(), 80);
    Ok((
        cases,
        json!({"inputs":inputs.into_values().collect::<Vec<_>>()}),
    ))
}

async fn assert_native_count(session: &SessionContext) -> TestResult {
    let batches = session
        .sql("SELECT count(*) FROM left_inputs")
        .await?
        .collect()
        .await?;
    assert_eq!(encode_value(batches[0].column(0).as_ref(), 0)?, json!("5"));
    Ok(())
}

fn check_native_joins(plan: &dyn ExecutionPlan, expected: &[JoinType], found: &mut bool) {
    assert!(
        plan.downcast_ref::<NestedLoopJoinExec>().is_none(),
        "avoidable Cartesian expansion: {}",
        displayable(plan).indent(true)
    );
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && expected.contains(join.join_type())
        && !join.on().is_empty()
    {
        *found = true;
    }
    if plan.children().is_empty() {
        assert!(
            plan.schema()
                .fields()
                .iter()
                .all(|field| matches!(field.name().as_str(), "k" | "n")),
            "unneeded source columns: {:?}",
            plan.schema()
        );
    }
    for child in plan.children() {
        check_native_joins(child.as_ref(), expected, found);
    }
}

async fn assert_native_keyed_path(session: &SessionContext, case: &Value) -> TestResult {
    let expected: &[JoinType] = match case["id"].as_str() {
        Some(
            "800_live_shared_computed"
            | "800_live_nested_exists_flag"
            | "review_outer_capture_flag"
            | "review_distinct_outer_payload_exists"
            | "computed_nonpositive_exists",
        ) => &[JoinType::LeftSemi, JoinType::RightSemi],
        Some(
            "computed_not_exists"
            | "nested_not_exists"
            | "review_distinct_outer_payload_not"
            | "computed_nonpositive_not",
        ) => &[JoinType::LeftAnti, JoinType::RightAnti],
        Some(
            "computed_projected_flag" | "nested_projected_flag" | "outer_payload_projected_flag",
        ) => &[JoinType::LeftMark, JoinType::RightMark],
        _ => return Ok(()),
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let mut found = false;
    check_native_joins(physical.as_ref(), expected, &mut found);
    assert!(
        found,
        "keyed native join missing: {}",
        displayable(physical.as_ref()).indent(true)
    );
    Ok(())
}

#[tokio::test]
async fn retained_exists_bindings_match_spark() -> TestResult {
    let (cases, metadata) = load_references()?;
    let mut differences = Vec::new();
    for (partitions, input_batch, target_partitions, output_batch) in
        [(1, 1, 1, 1), (3, 2, 4, 3), (2, 32, 2, 32)]
    {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_target_partitions(target_partitions)
                            .with_batch_size(output_batch),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &metadata,
            input_batch,
            partitions,
        )?;
        assert_native_count(&session).await?;
        let error = plan_query(&session, "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT l.k+1 AS outer_k,r.k AS inner_k FROM right_inputs r) q WHERE q.absent_payload=q.inner_k)", &SparkSqlOptions::default()).await.err().ok_or("missing capture unexpectedly resolved")?;
        let message = error.to_string();
        assert!(
            message.contains("cannot resolve attribute") && message.contains("absent_payload"),
            "wrong validation failure: {message}"
        );
        for case in &cases {
            let actual = capture_executed_query_result(&session, case).await;
            println!(
                "RETAINED_EXISTS_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"source_partitions":partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"actual":actual.as_ref().map_err(ToString::to_string)})
            );
            match actual {
                Ok(actual) if actual["rows"]==case["rows"] && result_types(&actual["schema"])?==result_types(&case["schema"])? => {},
                other => differences.push(format!("{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected {case}, got {other:?}",case["id"],case["ansi"])),
            }
            assert_native_keyed_path(&session, case).await?;
        }
        assert_native_count(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
