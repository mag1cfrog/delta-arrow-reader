//! Current-time filters and repeated calls share one query clock.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Date32Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Date32Type, Field, Schema, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use sail_plan::like_errors::set_spark_query_start_time;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn temporal_input_session(batch_size: usize) -> TestResult<SessionContext> {
    let config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(1);
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config),
        )
        .build(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("d", DataType::Date32, true),
        Field::new("f", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(946_684_800_000_000),
                    Some(4_102_444_800_000_000),
                    None,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(Date32Array::from(vec![Some(10_957), Some(47_482), None])),
            Arc::new(StringArray::from(vec![
                Some("yyyy"),
                Some("yyyy-MM-dd"),
                None,
            ])),
        ],
    )?;
    session.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
    Ok(session)
}

#[tokio::test]
async fn current_time_values_and_executed_types_match_spark() -> TestResult {
    let records = include_str!("fixtures/current-time-expressions/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 84);
    for batch_size in [1, 32] {
        for zone in ["UTC", "America/Los_Angeles"] {
            let session = temporal_input_session(batch_size)?;
            session.register_udf(ScalarUDF::from(TimestampNow::new(
                zone.into(),
                TimeUnit::Microsecond,
            )));
            for case in records
                .iter()
                .skip(1)
                .filter(|case| case["timezone"] == zone)
            {
                let options = SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: zone.into(),
                    ..SparkSqlOptions::default()
                };
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &options,
                )
                .await?;
                let physical = if let Some(clock) = case["clock"].as_str() {
                    let mut state = session.state();
                    state.execution_props_mut().query_execution_start_time = Some(clock.parse()?);
                    set_spark_query_start_time(&mut state);
                    state
                        .config_mut()
                        .options_mut()
                        .extensions
                        .insert(SparkSqlExpressionSemantics::default());
                    DataFrame::new(state, query.logical_plan)
                        .create_physical_plan()
                        .await
                } else {
                    query.create_physical_plan(&session).await
                }
                .map_err(|error| format!("{}: {error}", case["id"]))?;
                let types = physical
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| match field.data_type() {
                        DataType::Boolean => Ok("boolean"),
                        DataType::Int32 => Ok("integer"),
                        DataType::Int64 => Ok("long"),
                        DataType::Date32 => Ok("date"),
                        DataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                            if tz.as_ref() == zone =>
                        {
                            Ok("timestamp")
                        }
                        other => Err(format!("unexpected executed type {other}")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                assert_eq!(json!(types), case["types"], "{case}");
                let mut rows = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    for row in 0..batch.num_rows() {
                        rows.push(
                            batch
                                .columns()
                                .iter()
                                .map(|column| {
                                    if column.is_null(row) {
                                        Ok(Value::Null)
                                    } else {
                                        let value = match column.data_type() {
                                            DataType::Date32 => column
                                                .as_primitive::<Date32Type>()
                                                .value(row)
                                                .to_string(),
                                            DataType::Timestamp(TimeUnit::Microsecond, _) => column
                                                .as_primitive::<TimestampMicrosecondType>()
                                                .value(row)
                                                .to_string(),
                                            _ => array_value_to_string(column, row)?,
                                        };
                                        Ok(Value::String(value))
                                    }
                                })
                                .collect::<TestResult<Vec<_>>>()?,
                        );
                    }
                }
                assert_eq!(json!(rows), case["rows"], "{case}");
            }
            let native = session
                .sql("SELECT now() = now() AS r, current_date() = current_date() AS d, timestamp_now() > TIMESTAMP '2000-01-01 00:00:00' AS t")
                .await?
                .collect()
                .await?;
            assert_eq!(native.len(), 1);
            assert_eq!(native[0].num_rows(), 1);
            for column in native[0].columns() {
                assert_eq!(column.data_type(), &DataType::Boolean);
                assert_eq!(array_value_to_string(column, 0)?, "true");
            }
        }
    }
    Ok(())
}
