//! Live projected outer bindings retain EXISTS row presence and native keyed joins.

use std::collections::BTreeSet;

use datafusion::common::JoinType;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::joins::HashJoinExec;
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

async fn assert_native_count(session: &SessionContext) -> TestResult {
    let batches = session
        .sql("SELECT count(*) FROM left_inputs")
        .await?
        .collect()
        .await?;
    assert_eq!(encode_value(batches[0].column(0).as_ref(), 0)?, json!("5"));
    Ok(())
}

fn contains_keyed_join(plan: &dyn ExecutionPlan, types: &[JoinType]) -> bool {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        return !join.on().is_empty() && types.contains(join.join_type());
    }
    plan.children()
        .iter()
        .any(|child| contains_keyed_join(child.as_ref(), types))
}

async fn assert_equality_uses_keyed_join(session: &SessionContext, case: &Value) -> TestResult {
    let expected = match case["id"].as_str() {
        Some("800_real_exists_projected") => [JoinType::LeftSemi, JoinType::RightSemi],
        Some("800_real_not_exists_projected") => [JoinType::LeftAnti, JoinType::RightAnti],
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
    assert!(
        contains_keyed_join(physical.as_ref(), &expected),
        "equality binding lost its keyed semi/anti join: {}",
        displayable(physical.as_ref()).indent(true)
    );
    Ok(())
}

#[tokio::test]
async fn projected_outer_exists_matches_spark() -> TestResult {
    let records = include_str!("fixtures/projected-outer-exists/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 52);
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/projected-outer-exists/requests.json"
    ))?;
    let selection = &records[0]["focused_selection"];
    assert_eq!(selection["capture_record_count"], 56);
    assert_eq!(selection["compared_record_count"], 52);
    assert_eq!(selection["boundary_issue"], 1305);
    let identity = |case: &Value| -> TestResult<(String, bool)> {
        Ok((
            case["id"].as_str().ok_or("missing ID")?.to_string(),
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        ))
    };
    assert_eq!(
        selection["excluded_cases"].as_array().map(Vec::len),
        Some(4)
    );
    let excluded = selection["excluded_cases"]
        .as_array()
        .ok_or("missing excluded identities")?
        .iter()
        .map(identity)
        .collect::<TestResult<BTreeSet<_>>>()?;
    let expected_excluded = [
        "800_direct_global_aggregate_empty",
        "800_direct_global_aggregate_empty_not",
    ]
    .into_iter()
    .flat_map(|id| [false, true].map(move |ansi| (id.to_string(), ansi)))
    .collect::<BTreeSet<_>>();
    assert_eq!(excluded, expected_excluded);
    let definitions = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing requested cases")?;
    let mut expected = BTreeSet::new();
    for case in definitions {
        let key = identity(case)?;
        if !excluded.contains(&key) {
            assert!(expected.insert(key));
        }
    }
    let mut identities = BTreeSet::new();
    for case in records.iter().skip(1) {
        let key = identity(case)?;
        assert!(identities.insert(key));
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == case["id"] && definition["ansi"] == case["ansi"])
            .ok_or("unrequested case")?;
        assert_eq!(definition["sql"], case["sql"]);
        assert_eq!(definition["ordered"], case["ordered"]);
        assert_eq!(case["status"], "ok");
    }
    assert_eq!(identities, expected);
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
            &records[0]["corpora"][0]["metadata"],
            input_batch,
            partitions,
        )?;
        assert_native_count(&session).await?;
        for case in records.iter().skip(1) {
            let observation = capture_executed_query_result(&session, case).await;
            println!(
                "PROJECTED_OUTER_EXISTS_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"source_partitions":partitions,"input_batch":input_batch,"target_partitions":target_partitions,"output_batch":output_batch,"actual":observation.as_ref().map_err(ToString::to_string)})
            );
            match observation {
                Ok(actual)
                    if actual["rows"] == case["rows"]
                        && result_types(&actual["schema"])? == result_types(&case["schema"])? => {}
                other => differences.push(format!(
                    "{} ANSI={} layout=({partitions},{input_batch},{target_partitions},{output_batch}): expected {case}, got {other:?}",
                    case["id"], case["ansi"]
                )),
            }
            assert_equality_uses_keyed_join(&session, case).await?;
        }
        assert_native_count(&session).await?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
