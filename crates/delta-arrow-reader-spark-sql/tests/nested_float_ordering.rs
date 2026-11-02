//! Float normalization affects comparison keys while projected payloads stay intact.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn original_float_bits(array: &dyn Array, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        DataType::Float32 => json!(format!(
            "{:08x}",
            array.as_primitive::<Float32Type>().value(row).to_bits()
        )),
        DataType::Float64 => json!(format!(
            "{:016x}",
            array.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        DataType::List(_) => {
            let values = array.as_list::<i32>().value(row);
            json!(
                (0..values.len())
                    .map(|index| original_float_bits(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        DataType::Struct(_) => json!(
            array
                .as_struct()
                .columns()
                .iter()
                .map(|column| original_float_bits(column.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()?
        ),
        data_type => return Err(format!("unexpected nested float type: {data_type}").into()),
    })
}

#[tokio::test]
async fn nested_float_order_and_peers_match_spark_without_changing_payloads() -> TestResult {
    let records = include_str!("fixtures/nested-float-ordering-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 64);
    let inputs = records[0]["inputs"].as_array().ok_or("missing inputs")?;
    assert_eq!(inputs.len(), 8);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].to_string(), case["ansi"].to_string())));
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
            if actual.as_ref().ok() != Some(&case["expected"]) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
            let table = case["sql"]
                .as_str()
                .ok_or("missing SQL")?
                .rsplit_once("FROM ")
                .ok_or("missing table")?
                .1;
            let input = inputs
                .iter()
                .find(|input| input["name"] == table)
                .ok_or("missing provider")?;
            let mut expected = case["expected"]["rows"]
                .as_array()
                .ok_or("missing rows")?
                .clone();
            expected.sort_by_key(|row| {
                row[2]
                    .as_str()
                    .and_then(|value| value.parse::<usize>().ok())
            });
            let expected_ids = expected
                .iter()
                .map(|row| {
                    row[0]
                        .as_str()
                        .ok_or("missing ID")
                        .and_then(|value| value.parse::<i32>().map_err(|_| "bad ID"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let direction = case["id"]
                .as_str()
                .ok_or("missing case ID")?
                .rsplit_once('_')
                .ok_or("missing direction")?
                .1
                .parse::<usize>()?;
            let direction = [
                "ASC NULLS FIRST",
                "ASC NULLS LAST",
                "DESC NULLS FIRST",
                "DESC NULLS LAST",
            ][direction];
            let sql = format!("SELECT k,id FROM {table} ORDER BY k {direction},id");
            for (sql, key_index, id_index) in [
                (sql.as_str(), 0, 1),
                (case["sql"].as_str().ok_or("missing SQL")?, 1, 0),
            ] {
                let query = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let mut actual_ids = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    for row in 0..batch.num_rows() {
                        let id = batch
                            .column(id_index)
                            .as_primitive::<Int32Type>()
                            .value(row);
                        actual_ids.push(id);
                        assert_eq!(
                            original_float_bits(batch.column(key_index).as_ref(), row)?,
                            input["rows"][id as usize][0],
                            "{sql}: id={id}"
                        );
                    }
                }
                if key_index == 0 {
                    assert_eq!(
                        actual_ids, expected_ids,
                        "{sql} ANSI={} batch={batch_size}",
                        case["ansi"]
                    );
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
