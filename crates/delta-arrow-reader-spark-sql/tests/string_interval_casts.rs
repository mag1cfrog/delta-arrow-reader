//! Runtime interval casts preserve values and types across numeric, quoted and signed inputs.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::IntervalMonthDayNano;
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

#[tokio::test]
async fn string_interval_casts_preserve_column_results_and_failures()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const HOUR: i64 = 3_600_000_000;
    const DAY: i64 = 24 * HOUR;
    let calendar = |months, days, nanoseconds| {
        ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        }))
    };
    for (target, input, expected) in [
        (
            "INTERVAL YEAR TO MONTH",
            [Some("2 months"), None, Some("'3 months'"), Some("-1 month")],
            [Some(2), None, Some(3), Some(-1)].map(ScalarValue::IntervalYearMonth),
        ),
        (
            "INTERVAL DAY TO SECOND",
            [
                Some("2 days 2 hours"),
                None,
                Some("'1 day 2 hours'"),
                Some("-1 day"),
            ],
            [
                Some(2 * DAY + 2 * HOUR),
                None,
                Some(DAY + 2 * HOUR),
                Some(-DAY),
            ]
            .map(ScalarValue::DurationMicrosecond),
        ),
        (
            "INTERVAL",
            [
                Some("2 months 1 day 2 seconds"),
                None,
                Some("'3 months 2 days 4 seconds'"),
                Some("-1 month 1 day 2 seconds"),
            ],
            [
                calendar(2, 1, 2_000_000_000),
                ScalarValue::IntervalMonthDayNano(None),
                calendar(3, 2, 4_000_000_000),
                calendar(-1, 1, 2_000_000_000),
            ],
        ),
    ] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_target_partitions(1)),
            )
            .build(),
        );
        let input: ArrayRef = Arc::new(StringArray::from(input.to_vec()));
        session.register_batch("input", RecordBatch::try_from_iter([("s", input)])?)?;
        let invalid: ArrayRef = Arc::new(StringArray::from(vec!["1 nonsense"]));
        session.register_batch(
            "invalid_input",
            RecordBatch::try_from_iter([("s", invalid)])?,
        )?;
        for ansi_mode in [false, true] {
            for cast in ["CAST", "TRY_CAST"] {
                let planned = plan_query(
                    &session,
                    &format!("SELECT {cast}(s AS {target}) FROM input"),
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = planned.create_physical_plan(&session).await?;
                assert_eq!(
                    physical.schema().field(0).data_type(),
                    &expected[0].data_type()
                );
                let mut actual = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    for row in 0..batch.num_rows() {
                        actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                    }
                }
                assert_eq!(actual, expected, "{cast} {target}, ANSI={ansi_mode}");

                let invalid = plan_query(
                    &session,
                    &format!("SELECT {cast}(s AS {target}) FROM invalid_input"),
                    &SparkSqlOptions {
                        ansi_mode,
                        ..Default::default()
                    },
                )
                .await?;
                let invalid = invalid.create_physical_plan(&session).await?;
                assert!(
                    collect(invalid, session.task_ctx()).await.is_err(),
                    "{cast} {target}, ANSI={ansi_mode}"
                );
            }
        }
    }
    Ok(())
}
