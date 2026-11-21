//! Compare the omitted UTC pattern with the original explicit-format path.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_int64_array;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

const INPUTS: &[Option<&str>] = &[
    None,
    Some("0000-01-01 00:00:00"),
    Some("0001-01-01 00:00:00"),
    Some("1582-10-10 12:34:56"),
    Some("1969-12-31 23:59:59"),
    Some("1970-01-01 00:00:00"),
    Some("1900-02-29 12:34:56"),
    Some("2000-02-29 12:34:56"),
    Some("9999-12-31 23:59:59"),
    Some("10000-01-01 00:00:00"),
    Some("2020-01-01 24:00:00"),
    Some("2016-12-31 23:59:60"),
    Some("2020-02-30 12:34:56"),
    Some("2020-12-31 23:59:59.5"),
    Some("2020-12-31 23:59:59+05:45"),
    Some("2020-01-01T12:34:56"),
    Some("2020-1-1 12:34:56"),
    Some(" 2020-01-01 12:34:56"),
    Some("2024-03-10 02:30:00"),
    Some("2024-11-03 01:30:00"),
    Some(""),
    Some("not a timestamp"),
];

async fn epoch_seconds(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<Vec<Option<i64>>> {
    let query = plan_query(session, sql, options).await?;
    let execution = query.create_physical_plan(session).await?;
    assert_eq!(execution.schema().field(0).data_type(), &DataType::Int64);
    let mut seconds = Vec::new();
    for batch in collect(execution, session.task_ctx()).await? {
        seconds.extend(as_int64_array(batch.column(0))?.iter());
    }
    Ok(seconds)
}

#[tokio::test]
async fn omitted_unix_timestamp_format_preserves_values_and_errors() -> Result<()> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1)),
        )
        .build(),
    );
    let columns: [(&str, ArrayRef); 4] = [
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..INPUTS.len() as i64)),
        ),
        ("v", Arc::new(StringArray::from(INPUTS.to_vec()))),
        ("wide", Arc::new(LargeStringArray::from(INPUTS.to_vec()))),
        ("view", Arc::new(StringViewArray::from(INPUTS.to_vec()))),
    ];
    session.register_batch("samples", RecordBatch::try_from_iter(columns)?)?;
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..Default::default()
            };
            for function in ["unix_timestamp", "to_unix_timestamp"] {
                for column in ["v", "wide", "view"] {
                    // The last ID selects an empty input.
                    for id in 0..=INPUTS.len() {
                        let statement = |format| {
                            format!(
                                "SELECT {function}({column}{format}) FROM samples WHERE id = {id}"
                            )
                        };
                        let implicit = epoch_seconds(&session, &statement(""), &options).await;
                        let explicit =
                            epoch_seconds(&session, &statement(",'yyyy-MM-dd HH:mm:ss'"), &options)
                                .await;
                        if INPUTS.get(id) == Some(&Some("1969-12-31 23:59:59"))
                            && session_zone == "UTC"
                        {
                            assert_eq!(
                                implicit.as_ref().ok().map(Vec::as_slice),
                                Some(&[Some(-1)][..])
                            );
                        }
                        if INPUTS.get(id) == Some(&Some("not a timestamp")) && ansi_mode {
                            assert!(implicit.is_err());
                        }
                        assert_eq!(
                            implicit.map_err(|e| e.to_string()),
                            explicit.map_err(|e| e.to_string()),
                            "{function} {column} {session_zone} ANSI {ansi_mode} row {id}"
                        );
                    }
                }
                let sql = format!("SELECT {function}(v,CAST(NULL AS STRING)) FROM samples");
                assert_eq!(
                    epoch_seconds(&session, &sql, &options).await?,
                    vec![None; INPUTS.len()]
                );
            }
        }
    }
    Ok(())
}
