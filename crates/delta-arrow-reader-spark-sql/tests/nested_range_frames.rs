//! RANGE peers and frame members must use the same ordering as their nested keys.

use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn reference_cases() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/nested-range-frames-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

#[tokio::test]
async fn nested_range_frames_match_spark() -> TestResult {
    let records = reference_cases()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 600);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
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
            let actual = capture_spark_query_result(&session, case).await;
            let matches = actual.as_ref().ok() == Some(&case["expected"]);
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_range_and_groups_keep_peers_across_batches() -> TestResult {
    let records = reference_cases()?;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
            let table = input["name"].as_str().ok_or("missing table name")?;
            if !table.ends_with("_mixed") {
                continue;
            }
            for (direction_index, direction) in [
                "ASC NULLS FIRST",
                "ASC NULLS LAST",
                "DESC NULLS FIRST",
                "DESC NULLS LAST",
            ]
            .iter()
            .enumerate()
            {
                let reference_id = format!("{table}_global_{direction_index}");
                let case = records[1..]
                    .iter()
                    .find(|case| case["id"] == reference_id && case["ansi"] == true)
                    .ok_or("missing peer reference")?;
                let expected = case["expected"]["rows"]
                    .as_array()
                    .ok_or("missing rows")?
                    .iter()
                    .map(|row| json!([row[0], row[2]]))
                    .collect::<Vec<_>>();
                for units in ["RANGE", "GROUPS"] {
                    let sql = format!(
                        "SELECT id,COUNT(*) OVER (ORDER BY k {direction} {units} BETWEEN CURRENT ROW AND CURRENT ROW) AS peer_count FROM {table}"
                    );
                    let physical = session.sql(&sql).await?.create_physical_plan().await?;
                    let plan = displayable(physical.as_ref()).indent(false).to_string();
                    assert!(plan.contains("BoundedWindowAggExec"), "{sql}: {plan}");
                    assert_eq!(physical.schema().field(1).data_type(), &DataType::Int64);
                    assert!(!physical.schema().field(1).is_nullable());
                    let batches = collect(physical, session.task_ctx()).await?;
                    if batch_size == 1 {
                        assert!(batches.len() > 1, "{sql}");
                    }
                    let mut actual = Vec::new();
                    for batch in batches {
                        for row in 0..batch.num_rows() {
                            actual.push(json!([
                                array_value_to_string(batch.column(0), row)?,
                                array_value_to_string(batch.column(1), row)?
                            ]));
                        }
                    }
                    actual.sort_by_key(Value::to_string);
                    assert_eq!(actual, expected, "{sql}");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_range_keeps_offset_validation_and_temporal_arithmetic() -> TestResult {
    let records = reference_cases()?;
    let session = SessionContext::new();
    register_recorded_inputs(&session, &records[0], 1)?;
    for table in ["integer_struct_mixed", "string_struct_array_mixed"] {
        for bounds in ["1 PRECEDING AND CURRENT ROW", "CURRENT ROW AND 1 FOLLOWING"] {
            let sql =
                format!("SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN {bounds}) FROM {table}");
            let result = match session.sql(&sql).await {
                Ok(query) => query.create_physical_plan().await,
                Err(error) => Err(error),
            };
            assert!(
                matches!(&result, Err(error) if error.to_string().contains("Cannot run range queries on datatype")),
                "{sql}: {result:?}"
            );
        }
    }
    for key_type in ["DATE", "TIMESTAMP"] {
        let sql = format!(
            "SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS n FROM (VALUES ({key_type} '2020-01-01'),({key_type} '2020-01-02'),({key_type} '2020-01-04')) inputs(k) ORDER BY k"
        );
        let batches = session.sql(&sql).await?.collect().await?;
        let mut counts = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                counts.push(array_value_to_string(batch.column(0), row)?);
            }
        }
        assert_eq!(counts, ["1", "2", "1"], "{sql}");
    }
    Ok(())
}
