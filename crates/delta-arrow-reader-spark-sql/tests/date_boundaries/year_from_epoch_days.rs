//! YEAR agrees with the previous Arrow conversion at Date32 boundaries.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array};
use arrow::buffer::NullBuffer;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::datetime::spark_year::SparkYear;

#[tokio::test]
async fn year_on_date_columns_matches_the_previous_converter() -> Result<()> {
    // Chrono spans -262143-01-01 through +262142-12-31.
    let min_day = -96_465_292;
    let max_day = 95_026_236;
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
    let original_year = SparkYear::new();
    // Scalar YEAR still calls the converter formerly used by every array value.
    let previous_year = |day: Option<i32>| {
        let result = original_year.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Date32(day))],
            arg_fields: vec![Arc::new(Field::new("d", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("y", DataType::Int32, true)),
            config_options: Arc::default(),
        })?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(year)) => Ok(year),
            other => datafusion::common::exec_err!("unexpected YEAR scalar result: {other:?}"),
        }
    };
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
                session_timezone: timezone.into(),
                ..Default::default()
            };
            for (name, values) in &inputs {
                let expected = Int32Array::from(
                    values
                        .iter()
                        .map(&previous_year)
                        .collect::<Result<Vec<_>>>()?,
                );
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
                let expected_error = previous_year(Some(day));
                assert!(
                    matches!(expected_error, Err(ref error) if error.to_string().contains(
                        &format!("Spark `year`: unable to convert days {day} to date")
                    ))
                );
                let sql = format!("SELECT year(d) AS y FROM {name}");
                let plan = plan_query(&session, &sql, &options)
                    .await?
                    .create_physical_plan(&session)
                    .await?;
                let result = collect(plan, session.task_ctx()).await;
                assert!(
                    matches!((result, expected_error), (Err(actual), Err(expected))
                    if actual.find_root().to_string() == expected.find_root().to_string())
                );
            }
        }
    }
    Ok(())
}
