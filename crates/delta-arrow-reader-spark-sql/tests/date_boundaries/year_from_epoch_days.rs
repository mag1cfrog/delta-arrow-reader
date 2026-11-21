//! YEAR agrees with the previous Arrow conversion at Date32 boundaries.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array};
use arrow::buffer::NullBuffer;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Date32Type};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

#[tokio::test]
async fn year_on_date_columns_matches_the_previous_converter() -> Result<()> {
    let min_day = Date32Type::from_naive_date(NaiveDate::MIN);
    let max_day = Date32Type::from_naive_date(NaiveDate::MAX);
    let days = Date32Array::new(
        vec![
            min_day,
            min_day + 1,
            -719_163,
            -719_162,
            -141_427,
            -1,
            0,
            1,
            11_016,
            max_day - 1,
            max_day,
            i32::MAX,
        ]
        .into(),
        Some(NullBuffer::from(vec![
            true, true, true, true, true, true, true, true, true, true, true, false,
        ])),
    );
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let inputs = [
        ("year_dates", days.clone()),
        ("sliced_year_dates", days.slice(1, days.len() - 1)),
        ("empty_year_dates", days.slice(0, 0)),
    ];
    for (name, values) in &inputs {
        session.register_batch(
            *name,
            RecordBatch::try_from_iter([("d", Arc::new(values.clone()) as ArrayRef)])?,
        )?;
    }
    let invalid = [
        ("before_min_year_date", min_day - 1),
        ("after_max_year_date", max_day + 1),
        ("min_date32", i32::MIN),
        ("max_date32", i32::MAX),
    ];
    for (name, day) in invalid {
        session.register_batch(
            name,
            RecordBatch::try_from_iter([(
                "d",
                Arc::new(Date32Array::from(vec![day])) as ArrayRef,
            )])?,
        )?;
    }

    for ansi_mode in [false, true] {
        for timezone in ["UTC", "America/New_York"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: timezone.to_owned(),
                ..Default::default()
            };
            for (name, values) in &inputs {
                let expected: Int32Array = values.try_unary(|day| {
                    Date32Type::to_naive_date_opt(day)
                        .map(|date| date.year())
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!("invalid control day {day}")
                        })
                })?;
                let sql = format!("SELECT year(d) AS y FROM {name}");
                let plan = plan_query(&session, &sql, &options)
                    .await?
                    .create_physical_plan(&session)
                    .await?;
                let schema = plan.schema();
                assert_eq!(schema.field(0).data_type(), &DataType::Int32);
                let batches = collect(plan, session.task_ctx()).await?;
                let actual = concat_batches(&schema, &batches)?;
                assert_eq!(actual.column(0).as_ref(), &expected as &dyn Array);
            }
            for (name, day) in invalid {
                assert!(Date32Type::to_naive_date_opt(day).is_none());
                let sql = format!("SELECT year(d) AS y FROM {name}");
                let plan = plan_query(&session, &sql, &options)
                    .await?
                    .create_physical_plan(&session)
                    .await?;
                let result = collect(plan, session.task_ctx()).await;
                assert!(matches!(result, Err(error) if error.to_string().contains(
                    &format!("Spark `year`: unable to convert days {day} to date")
                )));
            }
        }
    }
    Ok(())
}
