//! Window keys share Spark equality while projected and lagged values stay intact.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, ArrayRef, AsArray, DictionaryArray, Int32Array};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn reference_cases() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/floating-windows-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

#[tokio::test]
async fn floating_windows_match_spark() -> TestResult {
    let records = reference_cases()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 848);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
    }
    let mut differences = Vec::new();
    let mut observations = Vec::new();
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
            observations.push(json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                "matches":matches,"actual":actual.as_ref().map_or_else(|e|json!({"error":e.to_string()}),Clone::clone)}));
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    if let Ok(path) = std::env::var("DAR_WINDOW_OBSERVATIONS") {
        std::fs::write(path, serde_json::to_string_pretty(&observations)?)?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn floating_payload_bits(column: &dyn Array, row: usize) -> TestResult<Vec<u64>> {
    if column.is_null(row) {
        return Ok(Vec::new());
    }
    Ok(match column.data_type() {
        DataType::Float32 => vec![u64::from(
            column.as_primitive::<Float32Type>().value(row).to_bits(),
        )],
        DataType::Float64 => vec![column.as_primitive::<Float64Type>().value(row).to_bits()],
        DataType::List(_) => {
            let values = column.as_list::<i32>().value(row);
            let mut bits = Vec::new();
            for index in 0..values.len() {
                bits.extend(floating_payload_bits(values.as_ref(), index)?);
            }
            bits
        }
        DataType::Struct(_) => {
            let mut bits = Vec::new();
            for field in column.as_struct().columns() {
                bits.extend(floating_payload_bits(field.as_ref(), row)?);
            }
            bits
        }
        other => return Err(format!("unexpected floating payload type: {other}").into()),
    })
}

fn payloads_by_id(batches: &[RecordBatch]) -> TestResult<BTreeMap<i32, Vec<u64>>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                Ok((
                    batch.column(0).as_primitive::<Int32Type>().value(row),
                    floating_payload_bits(batch.column(1).as_ref(), row)?,
                ))
            })
        })
        .collect()
}

#[tokio::test]
async fn window_normalization_preserves_original_float_payloads() -> TestResult {
    let records = reference_cases()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(1)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    register_recorded_inputs(&session, &records[0], 1)?;
    for kind in ["float", "double"] {
        for shape in ["scalar", "array", "struct", "nested", "struct_array"] {
            let table = format!("{kind}_{shape}_mixed");
            let source = session
                .sql(&format!("SELECT id,k FROM {table} ORDER BY id"))
                .await?
                .collect()
                .await?;
            let original = payloads_by_id(&source)?;
            assert!(original.values().any(|bits| !bits.is_empty()));
            for ansi in [true, false] {
                for window in ["PARTITION BY k ORDER BY id", "ORDER BY k,id"] {
                    let sql = format!(
                        "SELECT id,k,LAG(id,1) OVER ({window}) AS previous_id, LAG(k,1) OVER ({window}) AS previous_value,\
                         COLLECT_LIST(NAMED_STRUCT('id',id,'payload',k)) OVER ({window} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS payloads FROM {table} ORDER BY id"
                    );
                    let query = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions {
                            ansi_mode: ansi,
                            ..Default::default()
                        },
                    )
                    .await?;
                    let plan = query.create_physical_plan(&session).await?;
                    let actual =
                        datafusion::physical_plan::collect(plan, session.task_ctx()).await?;
                    assert_eq!(payloads_by_id(&actual)?, original, "{sql}");
                    for batch in actual {
                        assert_eq!(batch.schema().field(1), source[0].schema().field(1));
                        let previous = batch.column(2).as_primitive::<Int32Type>();
                        for row in 0..batch.num_rows() {
                            let expected = if previous.is_null(row) {
                                Vec::new()
                            } else {
                                original[&previous.value(row)].clone()
                            };
                            assert_eq!(
                                floating_payload_bits(batch.column(3).as_ref(), row)?,
                                expected,
                                "{sql}, row {row}"
                            );
                            let payloads = batch.column(4).as_list::<i32>().value(row);
                            let payloads = payloads.as_struct();
                            let ids = payloads.column(0).as_primitive::<Int32Type>();
                            for (index, id) in ids.values().iter().enumerate() {
                                assert_eq!(
                                    floating_payload_bits(payloads.column(1).as_ref(), index)?,
                                    original[id],
                                    "{sql}, payload id {id}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn window_keys_are_typed_after_grouping_rebasing() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for ansi in [true, false] {
        for key in ["GROUPING_ID(k)", "ARRAY(CAST(GROUPING_ID(k) AS DOUBLE))"] {
            let sql = format!(
                "SELECT k,GROUPING_ID(k) AS g,COUNT(*) AS n,\
                 RANK() OVER (PARTITION BY {key} ORDER BY k) AS r,\
                 ROW_NUMBER() OVER (ORDER BY {key},k) AS position \
                 FROM VALUES (1),(1),(2) AS t(k) GROUP BY GROUPING SETS ((k),())"
            );
            let actual =
                capture_spark_query_result(&session, &json!({"ansi":ansi,"sql":sql})).await?;
            assert_eq!(
                actual["rows"],
                json!([
                    ["1", "0", "2", "1", "1"],
                    ["2", "0", "1", "2", "2"],
                    [null, "1", "3", "1", "3"]
                ]),
                "{sql} ANSI={ansi}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn dictionary_float_window_keys_match_decoded_values() -> TestResult {
    use std::sync::Arc;

    let records = reference_cases()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size)),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for kind in ["float", "double"] {
            let source = session
                .sql(&format!("SELECT k,id FROM {kind}_scalar_mixed ORDER BY id"))
                .await?
                .collect()
                .await?;
            let source = arrow::compute::concat_batches(&source[0].schema(), &source)?;
            let source_bits = (0..source.num_rows())
                .map(|row| {
                    Ok((
                        source.column(1).as_primitive::<Int32Type>().value(row),
                        floating_payload_bits(source.column(0).as_ref(), row)?,
                    ))
                })
                .collect::<TestResult<BTreeMap<_, _>>>()?;
            let key: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..source.num_rows() as i32),
                source.column(0).clone(),
            )?);
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", key.data_type().clone(), true)
                    .with_metadata(source.schema().field(0).metadata().clone()),
                source.schema().field(1).clone(),
            ]));
            let table = format!("dictionary_{kind}");
            session.register_batch(
                table.as_str(),
                RecordBatch::try_new(schema.clone(), vec![key, source.column(1).clone()])?,
            )?;
            for ansi_mode in [true, false] {
                for frame in [
                    "PARTITION BY k ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
                    "ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
                    "ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING",
                    "ORDER BY k RANGE BETWEEN CAST(1 AS BIGINT) PRECEDING AND CAST(1 AS BIGINT) FOLLOWING",
                    "ORDER BY k RANGE BETWEEN CAST(0.5 AS DECIMAL(4,1)) PRECEDING AND CAST(0.5 AS DECIMAL(4,1)) FOLLOWING",
                ] {
                    let select = format!(
                        "SELECT id,COUNT(*) OVER ({frame}) AS members,SORT_ARRAY(COLLECT_LIST(id) OVER ({frame})) AS member_ids FROM"
                    );
                    let decoded = capture_spark_query_result(
                        &session,
                        &json!({"ansi":ansi_mode,"sql":format!("{select} {kind}_scalar_mixed")}),
                    )
                    .await;
                    let encoded = capture_spark_query_result(
                        &session,
                        &json!({"ansi":ansi_mode,"sql":format!("{select} {table}")}),
                    )
                    .await;
                    if decoded.is_err() || encoded.as_ref().ok() != decoded.as_ref().ok() {
                        let detail = match (encoded, decoded) {
                            (Ok(encoded), Ok(decoded)) => {
                                let actual =
                                    encoded["rows"].as_array().ok_or("missing encoded rows")?;
                                let expected =
                                    decoded["rows"].as_array().ok_or("missing decoded rows")?;
                                let first = (0..actual.len().max(expected.len()))
                                    .find(|&row| actual.get(row) != expected.get(row));
                                json!({
                                    "schema_matches":encoded["schema"] == decoded["schema"],
                                    "encoded_row_count":actual.len(), "decoded_row_count":expected.len(),
                                    "first_difference":first.map(|row| json!({"index":row,
                                        "encoded":actual.get(row), "decoded":expected.get(row)}))
                                })
                            }
                            (encoded, decoded) => json!({
                                "encoded_error":encoded.err().map(|error| error.to_string()),
                                "decoded_error":decoded.err().map(|error| error.to_string())
                            }),
                        };
                        differences.push(
                            json!({"kind":kind,"frame":frame,"ansi":ansi_mode,
                            "batch":batch_size,"difference":detail})
                            .to_string(),
                        );
                    }
                }
                if ansi_mode {
                    for input in [format!("{kind}_scalar_mixed"), table.clone()] {
                        let sql = format!(
                            "SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN CAST('bad' AS DOUBLE) PRECEDING AND CURRENT ROW) AS members FROM {input}"
                        );
                        let planned = plan_query(
                            &session,
                            &sql,
                            &SparkSqlOptions {
                                ansi_mode,
                                ..Default::default()
                            },
                        )
                        .await;
                        assert!(
                            planned.as_ref().is_err_and(|error| error
                                .to_string()
                                .contains("[CAST_INVALID_INPUT]")),
                            "{sql}: {planned:?}"
                        );
                    }
                }
                let query = plan_query(
                    &session,
                    &format!(
                        "SELECT id,k,RANK() OVER (PARTITION BY k ORDER BY k) AS r FROM {table}"
                    ),
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let batches = datafusion::physical_plan::collect(
                    query.create_physical_plan(&session).await?,
                    session.task_ctx(),
                )
                .await?;
                for batch in batches {
                    assert_eq!(batch.schema().field(1), schema.field(0));
                    let decoded =
                        arrow::compute::cast(batch.column(1), source.column(0).data_type())?;
                    for row in 0..batch.num_rows() {
                        let id = batch.column(0).as_primitive::<Int32Type>().value(row);
                        assert_eq!(
                            floating_payload_bits(decoded.as_ref(), row)?,
                            source_bits[&id]
                        );
                    }
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_window_planning_preserves_native_sql() -> TestResult {
    let records = reference_cases()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 1)?;
    }
    for kind in ["float", "double"] {
        for shape in ["scalar", "array", "struct", "nested", "struct_array"] {
            let table = format!("{kind}_{shape}_mixed");
            let sql = format!(
                "SELECT id,COUNT(*) OVER (PARTITION BY k) AS members, RANK() OVER (ORDER BY k) AS rank, ROW_NUMBER() OVER (ORDER BY k,id) AS position FROM {table} ORDER BY id"
            );
            capture_spark_query_result(&configured, &json!({"ansi":true,"sql":sql})).await?;
            assert_eq!(
                configured.sql(&sql).await?.collect().await?,
                native.sql(&sql).await?.collect().await?,
                "{sql}"
            );
        }
    }
    Ok(())
}
