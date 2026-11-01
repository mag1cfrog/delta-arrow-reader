//! Exact mixed-unit comparisons for Arrow timestamps that exceed Spark's precision.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Int32Type, Int64Type, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde::Deserialize;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
struct TimestampComparisonCase {
    id: String,
    timezone: String,
    left_type: (String, Option<String>),
    right_type: (String, Option<String>),
    left: Vec<Option<i64>>,
    right: Vec<Option<i64>>,
    order: Vec<Option<i8>>,
    joins: Vec<[i32; 2]>,
}

fn fixture_temporal_type((unit, timezone): &(String, Option<String>)) -> TestResult<DataType> {
    let unit = match unit.as_str() {
        "date32" => return Ok(DataType::Date32),
        "date64" => return Ok(DataType::Date64),
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        "ns" => TimeUnit::Nanosecond,
        other => return Err(format!("unknown fixture unit: {other}").into()),
    };
    Ok(DataType::Timestamp(
        unit,
        timezone.as_deref().map(Arc::from),
    ))
}

fn temporal_column(data_type: &DataType, values: &[Option<i64>]) -> TestResult<ArrayRef> {
    // Keep a nonzero offset so comparison and NULLIF also exercise sliced validity.
    let ticks = Int64Array::from_iter([Some(0)].into_iter().chain(values.iter().copied()));
    Ok(arrow::compute::cast(&ticks, data_type)?.slice(1, values.len()))
}

fn create_session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

async fn execute_query(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> TestResult<Vec<RecordBatch>> {
    let query = plan_query(session, sql, options).await?;
    let physical = query.create_physical_plan(session).await?;
    for (logical, physical) in query
        .logical_plan
        .schema()
        .fields()
        .iter()
        .zip(physical.schema().fields())
    {
        assert_eq!(logical.data_type(), physical.data_type());
    }
    Ok(collect(physical, session.task_ctx()).await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_timestamps_match_independent_precision_and_timezone_reference() -> TestResult {
    let mut lines = include_str!("fixtures/timestamp-precision.jsonl").lines();
    let metadata: serde_json::Value =
        serde_json::from_str(lines.next().ok_or("missing metadata")?)?;
    assert_eq!(metadata["spark_version"], "4.2.0");
    assert_eq!(metadata["timezone_checks"].as_array().unwrap().len(), 18);
    let cases = lines
        .map(serde_json::from_str::<TimestampComparisonCase>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(cases.len(), 58);
    for batch_size in [1, 32] {
        let session = create_session(batch_size);
        for ansi_mode in [false, true] {
            for case in &cases {
                let options = SparkSqlOptions {
                    ansi_mode,
                    session_timezone: case.timezone.clone().into(),
                    ..Default::default()
                };
                for reverse in [false, true] {
                    for table in ["samples", "left_input", "right_input"] {
                        session.deregister_table(table)?;
                    }
                    let (left_type, right_type, left, right) = if reverse {
                        (&case.right_type, &case.left_type, &case.right, &case.left)
                    } else {
                        (&case.left_type, &case.right_type, &case.left, &case.right)
                    };
                    let left_type = fixture_temporal_type(left_type)?;
                    let right_type = fixture_temporal_type(right_type)?;
                    let a = temporal_column(&left_type, left)?;
                    let b = temporal_column(&right_type, right)?;
                    let keys: ArrayRef =
                        Arc::new(Int32Array::from_iter_values(0..left.len() as i32));
                    session.register_batch(
                        "samples",
                        RecordBatch::try_from_iter([
                            ("k", keys.clone()),
                            ("a", a.clone()),
                            ("b", b.clone()),
                        ])?,
                    )?;
                    let sql = "SELECT a=b,a<>b,a<b,a<=b,a>b,a>=b,a<=>b,a IS DISTINCT FROM b,NULLIF(a,b),NULLIF(b,a) FROM samples ORDER BY k";
                    let batches =
                        execute_query(&session, sql, &options)
                            .await
                            .map_err(|error| {
                                format!("{} reverse={reverse}, ANSI={ansi_mode}: {error}", case.id)
                            })?;
                    let mut observed = vec![Vec::new(); 8];
                    let mut original_left = Vec::new();
                    let mut original_right = Vec::new();
                    for batch in batches {
                        for (column, values) in observed.iter_mut().enumerate() {
                            values.extend(batch.column(column).as_boolean().iter());
                        }
                        for (column, expected_type, values) in [
                            (8, &left_type, &mut original_left),
                            (9, &right_type, &mut original_right),
                        ] {
                            assert_eq!(batch.column(column).data_type(), expected_type);
                            let ticks =
                                arrow::compute::cast(batch.column(column), &DataType::Int64)?;
                            values.extend(ticks.as_primitive::<Int64Type>().iter());
                        }
                    }
                    let order: Vec<_> = case
                        .order
                        .iter()
                        .map(|order| order.map(|order| if reverse { -order } else { order }))
                        .collect();
                    let predicates: [fn(i8) -> bool; 6] = [
                        |v| v == 0,
                        |v| v != 0,
                        |v| v < 0,
                        |v| v <= 0,
                        |v| v > 0,
                        |v| v >= 0,
                    ];
                    for (column, predicate) in predicates.iter().enumerate() {
                        assert_eq!(
                            observed[column],
                            order.iter().map(|v| v.map(predicate)).collect::<Vec<_>>(),
                            "{} reverse={reverse} operator={column}",
                            case.id
                        );
                    }
                    let equal: Vec<_> = order
                        .iter()
                        .enumerate()
                        .map(|(i, order)| {
                            Some(order.map_or(left[i].is_none() && right[i].is_none(), |v| v == 0))
                        })
                        .collect();
                    assert_eq!(observed[6], equal, "{} null-safe equality", case.id);
                    assert_eq!(
                        observed[7],
                        equal.iter().map(|v| v.map(|v| !v)).collect::<Vec<_>>()
                    );
                    for (actual, original) in [(original_left, left), (original_right, right)] {
                        assert_eq!(
                            actual,
                            original
                                .iter()
                                .zip(&order)
                                .map(|(value, order)| if *order == Some(0) { None } else { *value })
                                .collect::<Vec<_>>(),
                            "{} NULLIF",
                            case.id
                        );
                    }
                    let empty =
                        execute_query(&session, &format!("{sql} LIMIT 0"), &options).await?;
                    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
                    let between = execute_query(
                        &session,
                        "SELECT k FROM samples WHERE a BETWEEN b AND b ORDER BY k",
                        &options,
                    )
                    .await?;
                    let between: Vec<_> = between
                        .iter()
                        .flat_map(|b| {
                            b.column(0)
                                .as_primitive::<Int32Type>()
                                .values()
                                .iter()
                                .copied()
                        })
                        .collect();
                    assert_eq!(
                        between,
                        order
                            .iter()
                            .enumerate()
                            .filter_map(|(i, v)| (*v == Some(0)).then_some(i as i32))
                            .collect::<Vec<_>>()
                    );
                    session.register_batch(
                        "left_input",
                        RecordBatch::try_from_iter([("k", keys.clone()), ("v", a)])?,
                    )?;
                    session.register_batch(
                        "right_input",
                        RecordBatch::try_from_iter([("k", keys), ("v", b)])?,
                    )?;
                    let mut expected = case.joins.clone();
                    if reverse {
                        for pair in &mut expected {
                            pair.swap(0, 1);
                        }
                        expected.sort();
                    }
                    for condition in ["ON l.v=r.v", "USING(v)"] {
                        let sql = format!(
                            "SELECT l.k,r.k FROM left_input l JOIN right_input r {condition} ORDER BY l.k,r.k"
                        );
                        let output = execute_query(&session, &sql, &options).await?;
                        let actual: Vec<_> = output
                            .iter()
                            .flat_map(|batch| {
                                let left = batch.column(0).as_primitive::<Int32Type>();
                                let right = batch.column(1).as_primitive::<Int32Type>();
                                (0..batch.num_rows())
                                    .map(move |row| [left.value(row), right.value(row)])
                            })
                            .collect();
                        assert_eq!(
                            actual, expected,
                            "{} reverse={reverse} {condition}",
                            case.id
                        );
                    }
                }
            }
        }
    }
    // Opting into Spark support must not change SQL planned by the host frontend.
    let native = create_session(32);
    native.register_batch(
        "native_timestamps",
        RecordBatch::try_from_iter([
            (
                "a",
                temporal_column(
                    &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    &[Some(1)],
                )?,
            ),
            (
                "b",
                temporal_column(
                    &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    &[Some(0)],
                )?,
            ),
        ])?,
    )?;
    let output = native
        .sql("SELECT a=b FROM native_timestamps")
        .await?
        .collect()
        .await?;
    assert_eq!(
        output[0].column(0).as_boolean().iter().collect::<Vec<_>>(),
        [Some(true)]
    );
    Ok(())
}

#[tokio::test]
async fn timezone_range_errors_do_not_turn_values_into_null() -> TestResult {
    let session = create_session(32);
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            session_timezone: "America/New_York".into(),
            ..Default::default()
        };
        for data_type in [
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
        ] {
            session.deregister_table("samples")?;
            let value = if data_type == DataType::Date32 {
                i64::from(i32::MAX)
            } else {
                i64::MAX
            };
            let other_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            session.register_batch(
                "samples",
                RecordBatch::try_from_iter([
                    ("a", temporal_column(&data_type, &[Some(value)])?),
                    ("b", temporal_column(&other_type, &[Some(0)])?),
                ])?,
            )?;
            for expression in ["a=b", "b=a", "NULLIF(a,b)", "NULLIF(b,a)"] {
                let result = execute_query(
                    &session,
                    &format!("SELECT {expression} FROM samples"),
                    &options,
                )
                .await;
                assert!(
                    result.is_err_and(|error| error.to_string().contains(
                        "timestamp comparison exceeds the session timezone converter range"
                    )),
                    "{data_type} {expression}"
                );
            }
            // A known NULL right operand never needs timezone conversion.
            let batches = execute_query(
                &session,
                "SELECT NULLIF(a,CAST(NULL AS TIMESTAMP)) FROM samples",
                &options,
            )
            .await?;
            assert_eq!(batches[0].column(0).data_type(), &data_type);
            let ticks = arrow::compute::cast(batches[0].column(0), &DataType::Int64)?;
            assert_eq!(ticks.as_primitive::<Int64Type>().value(0), value);
        }
    }
    Ok(())
}
