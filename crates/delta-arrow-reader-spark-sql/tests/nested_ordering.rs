//! Nested Spark keys preserve values while sharing ordering across query consumers.

use std::collections::{BTreeSet, HashMap};

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int32Type, Int64Type, Schema};
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{collect, collect_partitioned};
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
    Ok(include_str!("fixtures/nested-ordering-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

#[tokio::test]
async fn nested_query_and_window_order_match_spark() -> TestResult {
    let records = reference_cases()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 64);
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].clone().to_string(),
            case["ansi"].clone().to_string()
        )));
    }
    for batch_size in [1, 32] {
        let session = session(batch_size);
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case).await;
            if actual.as_ref().ok() != Some(&case["expected"]) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let (order, table) = sql
                .strip_prefix("SELECT id,k,ROW_NUMBER() OVER (ORDER BY ")
                .ok_or("unexpected ordering query")?
                .split_once(") AS position FROM ")
                .ok_or("missing order/table")?;
            let mut expected = case["expected"]["rows"]
                .as_array()
                .ok_or("missing rows")?
                .clone();
            expected.sort_by_key(|row| {
                row[2]
                    .as_str()
                    .and_then(|value| value.parse::<usize>().ok())
            });
            let mut rank = 0;
            let mut dense_rank = 0;
            let mut prior = None;
            let mut ranked = Vec::new();
            for (index, row) in expected.iter().enumerate() {
                if prior != Some(&row[1]) {
                    rank = index + 1;
                    dense_rank += 1;
                }
                prior = Some(&row[1]);
                ranked.push(json!([
                    row[0],
                    row[1],
                    rank.to_string(),
                    dense_rank.to_string()
                ]));
            }
            ranked.sort_by_key(Value::to_string);
            let mut fields = case["expected"]["schema"]["fields"]
                .as_array()
                .ok_or("missing fields")?[..2]
                .to_vec();
            for name in ["rank", "dense_rank"] {
                fields.push(json!({"name":name,"type":"integer","nullable":false,"metadata":{}}));
            }
            let key_order = order.strip_suffix(",id").ok_or("missing tiebreaker")?;
            let ranked_case = json!({"ansi":case["ansi"],"sql":format!(
                "SELECT id,k,RANK() OVER (ORDER BY {key_order}) AS rank,DENSE_RANK() OVER (ORDER BY {key_order}) AS dense_rank FROM {table}"
            )});
            let actual = capture_spark_query_result(&session, &ranked_case).await?;
            assert_eq!(
                actual,
                json!({"schema":{"type":"struct","fields":fields},"rows":ranked}),
                "{ranked_case}"
            );
            let expected_ids = expected
                .iter()
                .map(|row| {
                    row[0]
                        .as_str()
                        .ok_or("missing ID")
                        .and_then(|value| value.parse::<i32>().map_err(|_| "bad ID"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let source = session
                .sql(&format!("SELECT id,k FROM {table}"))
                .await?
                .collect()
                .await?;
            let original_schema = source.first().ok_or("missing source batch")?.schema();
            let mut original_keys = HashMap::new();
            for batch in source {
                for index in 0..batch.num_rows() {
                    original_keys.insert(
                        ScalarValue::try_from_array(batch.column(0), index)?.to_string(),
                        ScalarValue::try_from_array(batch.column(1), index)?,
                    );
                }
            }
            let ordered = format!("SELECT id,k FROM {table} ORDER BY {order}");
            for sql in [
                ordered.clone(),
                format!("SELECT id,k FROM ({ordered}) ordered"),
            ] {
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                assert_eq!(
                    physical.schema().as_ref(),
                    original_schema.as_ref(),
                    "{sql}"
                );
                let mut actual_ids = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    for index in 0..batch.num_rows() {
                        let id = ScalarValue::try_from_array(batch.column(0), index)?;
                        let key = ScalarValue::try_from_array(batch.column(1), index)?;
                        assert_eq!(Some(&key), original_keys.get(&id.to_string()), "{sql}");
                        actual_ids.push(id.to_string().parse::<i32>()?);
                    }
                }
                assert_eq!(
                    actual_ids, expected_ids,
                    "ANSI={} batch={batch_size}: {sql}",
                    case["ansi"]
                );
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn nested_computed_keys_preserve_evaluation_and_range_rejection() -> TestResult {
    let records = reference_cases()?;
    let session = session(1);
    register_recorded_inputs(&session, &records[0], 1)?;
    for window in [false, true] {
        let key =
            "NAMED_STRUCT('key',k,'checked',CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT))";
        let sql = if window {
            format!("SELECT id,ROW_NUMBER() OVER (ORDER BY {key}) AS n FROM integer_array_mixed")
        } else {
            format!("SELECT id FROM integer_array_mixed ORDER BY {key}")
        };
        assert!(
            capture_spark_query_result(&session, &json!({"ansi":true,"sql":sql}))
                .await
                .is_err()
        );
    }
    for table in [
        "integer_array_mixed",
        "integer_struct_mixed",
        "integer_nested_mixed",
        "integer_struct_array_mixed",
    ] {
        let sql = format!(
            "SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM {table}"
        );
        assert!(
            capture_spark_query_result(&session, &json!({"ansi":true,"sql":sql}))
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_order_preserves_metadata_and_native_ordering() -> TestResult {
    let records = reference_cases()?;
    let native = SessionContext::new();
    let configured = session(32);
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 32)?;
        let source = session
            .sql("SELECT * FROM integer_array_mixed")
            .await?
            .collect()
            .await?;
        let batch = source.first().ok_or("missing provider")?;
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata([("origin".into(), "nested ordering".into())].into())
            })
            .collect::<Vec<_>>();
        session.register_batch(
            "metadata_keys",
            arrow::record_batch::RecordBatch::try_new(
                std::sync::Arc::new(Schema::new(fields)),
                batch.columns().to_vec(),
            )?,
        )?;
    }
    let sql = "SELECT k,id FROM metadata_keys ORDER BY k ASC NULLS LAST,id";
    let expected = native.sql(sql).await?.collect().await?;
    let actual = configured.sql(sql).await?.collect().await?;
    assert_eq!(actual, expected);
    let query = plan_query(&configured, sql, &SparkSqlOptions::default()).await?;
    let physical = query.create_physical_plan(&configured).await?;
    for field in physical.schema().fields() {
        assert_eq!(
            field.metadata().get("origin").map(String::as_str),
            Some("nested ordering")
        );
        assert_ne!(field.data_type(), &DataType::Binary);
    }
    assert_eq!(
        query.logical_plan.schema().field(0).data_type(),
        physical.schema().field(0).data_type()
    );
    assert!(physical.schema().field(0).is_nullable());
    let batches = collect(physical, configured.task_ctx()).await?;
    assert!(
        batches
            .iter()
            .all(|batch| batch.column(0).data_type() == expected[0].column(0).data_type())
    );
    Ok(())
}

#[tokio::test]
async fn nested_sort_keys_are_typed_after_grouping_and_hidden_column_rebasing() -> TestResult {
    let records = reference_cases()?;
    let session = session(1);
    register_recorded_inputs(&session, &records[0], 1)?;
    let expected = vec![
        (None, 0, 1),
        (Some(vec![]), 0, 1),
        (Some(vec![None]), 0, 1),
        (Some(vec![None, None]), 0, 2),
        (Some(vec![Some(-3), None]), 0, 1),
        (Some(vec![Some(0), None]), 0, 2),
        (Some(vec![Some(2), None]), 0, 2),
        (None, 1, 10),
    ];
    for ansi_mode in [true, false] {
        for order in [
            "grouping_id(),k ASC NULLS FIRST",
            "NAMED_STRUCT('gid',grouping_id(),'value',k) ASC NULLS LAST",
        ] {
            let sql = format!(
                "SELECT k,grouping_id() AS gid,COUNT(*) AS n,ROW_NUMBER() OVER (ORDER BY {order}) AS position FROM integer_array_mixed GROUP BY ROLLUP(k) HAVING COUNT(*)>0 ORDER BY position"
            );
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let batches = collect(
                query.create_physical_plan(&session).await?,
                session.task_ctx(),
            )
            .await?;
            let mut actual = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let key = if batch.column(0).is_null(row) {
                        None
                    } else {
                        Some(
                            batch
                                .column(0)
                                .as_list::<i32>()
                                .value(row)
                                .as_primitive::<Int32Type>()
                                .iter()
                                .collect::<Vec<_>>(),
                        )
                    };
                    actual.push((
                        key,
                        batch.column(1).as_primitive::<Int64Type>().value(row),
                        batch.column(2).as_primitive::<Int64Type>().value(row),
                    ));
                    assert_eq!(
                        batch.column(3).as_primitive::<Int32Type>().value(row) as usize,
                        actual.len(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(actual, expected, "{sql}");
        }
        for clause in ["ORDER BY", "SORT BY"] {
            for order in [
                "grouping_id(),k ASC NULLS FIRST,n",
                "NAMED_STRUCT('gid',grouping_id(),'value',k) ASC NULLS LAST,n",
            ] {
                let sql = format!(
                    "SELECT k,grouping_id() AS gid,COUNT(*) AS n FROM integer_array_mixed GROUP BY ROLLUP(k) HAVING COUNT(*)>0 {clause} {order}"
                );
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let mut positions = Vec::new();
                for partition in collect_partitioned(physical, session.task_ctx()).await? {
                    let mut prior = None;
                    for batch in partition {
                        for row in 0..batch.num_rows() {
                            let key = if batch.column(0).is_null(row) {
                                None
                            } else {
                                Some(
                                    batch
                                        .column(0)
                                        .as_list::<i32>()
                                        .value(row)
                                        .as_primitive::<Int32Type>()
                                        .iter()
                                        .collect::<Vec<_>>(),
                                )
                            };
                            let actual = (
                                key,
                                batch.column(1).as_primitive::<Int64Type>().value(row),
                                batch.column(2).as_primitive::<Int64Type>().value(row),
                            );
                            let position = expected
                                .iter()
                                .position(|row| row == &actual)
                                .ok_or("unexpected grouping row")?;
                            assert!(prior.is_none_or(|prior| prior < position), "{sql}");
                            prior = Some(position);
                            positions.push(position);
                        }
                    }
                }
                if clause == "SORT BY" {
                    positions.sort();
                }
                assert_eq!(positions, (0..expected.len()).collect::<Vec<_>>(), "{sql}");
            }
            let reference = records
                .iter()
                .find(|case| {
                    case["id"] == "integer_array_mixed_order_1" && case["ansi"] == ansi_mode
                })
                .ok_or("missing hidden-key reference")?;
            let sql = format!("SELECT id FROM integer_array_mixed {clause} k ASC NULLS LAST,id");
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(physical.schema().fields().len(), 1);
            let mut positions = Vec::new();
            for partition in collect_partitioned(physical, session.task_ctx()).await? {
                let mut prior = None;
                for batch in partition {
                    for id in batch.column(0).as_primitive::<Int32Type>().values() {
                        let reference = reference["expected"]["rows"]
                            .as_array()
                            .ok_or("missing rows")?
                            .iter()
                            .find(|row| row[0] == id.to_string())
                            .ok_or("unexpected ID")?;
                        let position = reference[2]
                            .as_str()
                            .ok_or("missing position")?
                            .parse::<usize>()?;
                        assert!(prior.is_none_or(|prior| prior < position), "{sql}");
                        prior = Some(position);
                        positions.push(position);
                    }
                }
            }
            if clause == "SORT BY" {
                positions.sort();
            }
            assert_eq!(positions, (1..=10).collect::<Vec<_>>(), "{sql}");
        }
    }
    Ok(())
}
