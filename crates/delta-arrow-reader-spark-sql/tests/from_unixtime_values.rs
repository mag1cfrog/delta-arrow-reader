//! Compare the default FROM_UNIXTIME format with row-supplied formatting.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_string_array;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

const SECONDS: &[Option<i64>] = &[
    None,
    Some(-62_167_219_200), // Year zero.
    Some(-1),
    Some(0),
    Some(1_710_064_799), // One second before the Los Angeles spring transition.
    Some(1_710_064_800),
    Some(1_730_624_399), // One second before the Los Angeles fall transition.
    Some(1_730_624_400),
    Some(253_402_300_800), // Year 10000 in UTC.
    Some(i64::MIN),
    Some(i64::MAX),
];

async fn timestamp_strings(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<Vec<Option<String>>> {
    let query = plan_query(session, sql, options).await?;
    let execution = query.create_physical_plan(session).await?;
    assert_eq!(execution.schema().field(0).data_type(), &DataType::Utf8);
    let mut values = Vec::new();
    for batch in collect(execution, session.task_ctx()).await? {
        values.extend(
            as_string_array(batch.column(0))?
                .iter()
                .map(|s| s.map(str::to_owned)),
        );
    }
    Ok(values)
}

#[tokio::test]
async fn default_unix_time_format_preserves_wall_times_and_errors() -> Result<()> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let columns: [(&str, ArrayRef); 3] = [
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..SECONDS.len() as i64)),
        ),
        ("seconds", Arc::new(Int64Array::from(SECONDS.to_vec()))),
        (
            "pattern",
            Arc::new(StringArray::from(vec![
                "yyyy-MM-dd HH:mm:ss";
                SECONDS.len()
            ])),
        ),
    ];
    session.register_batch("samples", RecordBatch::try_from_iter(columns)?)?;
    for ansi_mode in [false, true] {
        for (zone, epoch) in [
            ("UTC", "1970-01-01 00:00:00"),
            ("America/Los_Angeles", "1969-12-31 16:00:00"),
            ("+05:45", "1970-01-01 05:45:00"),
        ] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: zone.into(),
                ..Default::default()
            };
            // The last ID selects an empty input.
            for id in 0..=SECONDS.len() {
                let statement = |pattern| {
                    format!("SELECT from_unixtime(seconds{pattern}) FROM samples WHERE id = {id}")
                };
                let default = timestamp_strings(&session, &statement(""), &options).await;
                let row_pattern =
                    timestamp_strings(&session, &statement(",pattern"), &options).await;
                match SECONDS.get(id) {
                    Some(Some(0)) => {
                        assert_eq!(default.as_ref().ok(), Some(&vec![Some(epoch.to_owned())]))
                    }
                    Some(Some(seconds)) if *seconds == i64::MIN || *seconds == i64::MAX => {
                        assert!(default.is_err());
                    }
                    _ => {}
                }
                assert_eq!(
                    default.map_err(|e| e.to_string()),
                    row_pattern.map_err(|e| e.to_string()),
                    "{zone}, ANSI {ansi_mode}, row {id}"
                );
            }
        }
    }
    Ok(())
}
