//! Tuple NOT IN excludes UNKNOWN comparisons without changing projected IN policy.

use std::collections::BTreeSet;

use datafusion::common::JoinType;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "../support/spark_inputs.rs"]
mod spark_inputs;
#[path = "../support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_executed_query_result, remove_nullability_and_metadata};

fn case_key(case: &Value) -> TestResult<(&str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

fn sorted_rows(mut rows: Value) -> TestResult<Value> {
    rows.as_array_mut()
        .ok_or("missing rows")?
        .sort_by_key(Value::to_string);
    Ok(rows)
}

fn scalar_policy_rows(id: &str) -> TestResult<Option<Value>> {
    let matched = match id {
        "scalar_projected_in" => "true",
        "scalar_projected_not_in" => "false",
        _ => return Ok(None),
    };
    // #1305 retains SQL three-valued scalar IN results separately from raw Spark.
    sorted_rows(json!([
        [null, "10", null],
        ["1", null, matched],
        ["1", "10", matched],
        ["2", "20", matched],
        ["2", "20", matched],
        ["3", "30", null]
    ]))
    .map(Some)
}

fn contains_hash_anti_join(plan: &dyn ExecutionPlan, key_count: usize) -> bool {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        return matches!(join.join_type(), JoinType::LeftAnti | JoinType::RightAnti)
            && join.on().len() == key_count;
    }
    plan.children()
        .iter()
        .any(|child| contains_hash_anti_join(child.as_ref(), key_count))
}

#[tokio::test]
async fn tuple_not_in_matches_spark_and_retains_scalar_policy() -> TestResult {
    let request: Value =
        serde_json::from_str(include_str!("../fixtures/tuple-not-in-null/requests.json"))?;
    let records = include_str!("../fixtures/tuple-not-in-null/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["issue"], 1332);
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
        // A partial NULL tuple can rule out a match through its other member.
        let truth_table = match case["id"].as_str() {
            Some("mixed_not_in") => Some(json!([["3", "30"]])),
            Some("partial_rhs_not_in") => Some(json!([
                [null, "10"],
                ["1", "10"],
                ["2", "20"],
                ["2", "20"],
                ["3", "30"]
            ])),
            Some("null_rhs_not_in" | "empty_in") => Some(json!([])),
            Some("empty_not_in") => {
                Some(request["corpora"][0]["metadata"]["inputs"][0]["rows"].clone())
            }
            _ => None,
        };
        if let Some(rows) = truth_table {
            assert_eq!(sorted_rows(rows)?, case["expected"]["rows"]);
        }
    }

    for (partitions, input_batch, output_batch) in [(1, 1, 1), (3, 2, 3)] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_target_partitions(partitions)
                            .with_batch_size(output_batch),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &request["corpora"][0]["metadata"],
            input_batch,
            partitions,
        )?;
        for case in &records[1..] {
            let (id, ansi) = case_key(case)?;
            let key_count = match id {
                "required_not_in" => Some(2),
                "computed_nullable_required_input" => Some(1),
                _ => None,
            };
            if let Some(key_count) = key_count {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let plan = displayable(physical.as_ref()).indent(true).to_string();
                assert!(
                    contains_hash_anti_join(physical.as_ref(), key_count),
                    "{id} must retain {key_count} hash keys: {plan}"
                );
                println!(
                    "TUPLE_NOT_IN_HASH_PLAN {}",
                    json!({"id":id,"ansi":ansi,"partitions":partitions,"hash_keys":key_count,"physical_plan":plan})
                );
            }
            let actual = capture_executed_query_result(&session, case).await?;
            let rows = scalar_policy_rows(id)?.unwrap_or_else(|| case["expected"]["rows"].clone());
            assert_eq!(
                actual["rows"], rows,
                "{id}, ANSI={ansi}, partitions={partitions}, batch={output_batch}"
            );
            let mut actual_schema = actual["schema"].clone();
            let mut spark_schema = case["schema_stages"]["executed"].clone();
            remove_nullability_and_metadata(&mut actual_schema);
            remove_nullability_and_metadata(&mut spark_schema);
            assert_eq!(actual_schema, spark_schema, "{id}, ANSI={ansi}");
        }
    }
    Ok(())
}
