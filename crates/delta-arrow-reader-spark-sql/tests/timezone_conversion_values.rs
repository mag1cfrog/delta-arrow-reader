//! Preserve timestamp values and argument short-circuiting across zone conversions.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

const QUERIES: &[(&str, &str, &str)] = &[
    ("utc", "convert_timezone('UTC','UTC',ntz)", ""),
    (
        "named",
        "convert_timezone('UTC','America/Los_Angeles',ntz)",
        "",
    ),
    (
        "dst_local",
        "convert_timezone('America/Los_Angeles','UTC',ntz)",
        "",
    ),
    ("offset", "convert_timezone('UTC','+05:45',ntz)", ""),
    (
        "offset_limits",
        "convert_timezone('+18:00','-18:00',ntz)",
        "",
    ),
    ("implicit_source", "convert_timezone('+05:45',ntz)", ""),
    ("legacy_alias", "convert_timezone('PST','UTC',ntz)", ""),
    ("dynamic", "convert_timezone(zone_name,'UTC',ntz)", ""),
    (
        "null_source",
        "convert_timezone(CAST(NULL AS STRING),'Bad/Zone',ntz)",
        "",
    ),
    (
        "null_target",
        "convert_timezone('Bad/Zone',CAST(NULL AS STRING),ntz)",
        "",
    ),
    (
        "null_timestamp",
        "convert_timezone('Bad/Zone','UTC',ntz)",
        " WHERE id = 0",
    ),
    (
        "invalid_source",
        "convert_timezone('Bad/Zone','UTC',ntz)",
        " WHERE id = 1",
    ),
    (
        "invalid_target",
        "convert_timezone('UTC','Bad/Zone',ntz)",
        " WHERE id = 1",
    ),
    (
        "invalid_timestamp",
        "convert_timezone('UTC','UTC',CAST('not a timestamp' AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "null_source_skips_later_expressions",
        "convert_timezone(CAST(NULL AS STRING),CAST(CAST(concat('bad',CAST(id AS STRING)) AS INT) AS STRING),CAST(concat('bad',CAST(id AS STRING)) AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "null_target_skips_timestamp",
        "convert_timezone('UTC',CAST(NULL AS STRING),CAST(concat('bad',CAST(id AS STRING)) AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "invalid_source_precedes_invalid_target",
        "convert_timezone('Bad/Source','Bad/Target',ntz)",
        " WHERE id = 1",
    ),
    (
        "empty_with_invalid_timestamp",
        "convert_timezone('UTC','UTC',CAST('not a timestamp' AS TIMESTAMP_NTZ))",
        " WHERE false",
    ),
    ("empty", "convert_timezone('UTC','UTC',ntz)", " WHERE false"),
    (
        "from_utc_named",
        "from_utc_timestamp(ltz,'America/Los_Angeles')",
        "",
    ),
    (
        "to_utc_named",
        "to_utc_timestamp(ltz,'America/Los_Angeles')",
        "",
    ),
];

fn conversion_session() -> Result<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(3)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    );
    let timestamps = TimestampMicrosecondArray::from(vec![
        None,
        Some(0),
        Some(1_710_037_800_000_000), // Wall clock in the Los Angeles spring gap.
        Some(1_730_597_400_000_000), // Wall clock in the fall overlap.
        Some(1_710_066_600_000_000),
        Some(1_730_626_200_000_000),
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
    ]);
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..timestamps.len() as i64));
    let ltz: ArrayRef = Arc::new(timestamps.clone().with_timezone("UTC"));
    let ntz: ArrayRef = Arc::new(timestamps);
    let zones: ArrayRef = Arc::new(StringArray::from(vec![
        Some("Bad/Zone"),
        None,
        Some("America/Los_Angeles"),
        Some("America/Los_Angeles"),
        Some("+05:45"),
        Some("UTC"),
        Some("UTC"),
        Some("UTC"),
        Some("-18:00"),
    ]));
    session.register_batch(
        "observations",
        RecordBatch::try_from_iter([
            ("id", ids),
            ("ntz", ntz),
            ("ltz", ltz),
            ("zone_name", zones),
        ])?,
    )?;
    Ok(session)
}

async fn execute_conversion(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<Value> {
    let query = plan_query(session, sql, options).await?;
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    let types: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| format!("{:?}", field.data_type()))
        .collect();
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        assert_eq!(batch.schema().fields(), schema.fields(), "{sql}");
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|col| match ScalarValue::try_from_array(col, row)? {
                    ScalarValue::Int64(value) | ScalarValue::TimestampMicrosecond(value, _) => {
                        Ok(json!(value))
                    }
                    other => Err(DataFusionError::Internal(format!(
                        "Unexpected conversion result type {}",
                        other.data_type()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(values);
        }
    }
    Ok(json!({"types":types,"rows":rows}))
}

fn check_staging(observations: Vec<Value>) -> Result<()> {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/timezone-conversion-values/staging.json"
    ))
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn timezone_conversions_match_staging_in_both_ansi_modes() -> Result<()> {
    let session = conversion_session()?;
    let mut observations = Vec::new();
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..SparkSqlOptions::default()
            };
            for (name, expression, filter) in QUERIES {
                let sql = format!(
                    "SELECT id,{expression} AS converted FROM observations{filter} ORDER BY id"
                );
                let result = execute_conversion(&session, &sql, &options)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                observations.push(json!({"name":name,"ansi_mode":ansi_mode,"session_zone":session_zone,"result":result}));
            }
        }
    }
    check_staging(observations)
}

fn check_fixed_zone_staging(observations: Vec<Value>) -> Result<()> {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/timezone-conversion-values/fixed-zones-staging.json"
    ))
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn fixed_zone_conversions_preserve_timestamps_and_labels() -> Result<()> {
    let session = conversion_session()?;
    let mut observations = Vec::new();
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..SparkSqlOptions::default()
            };
            for (expression, filter) in [
                ("from_utc_timestamp(ltz,'UTC')", ""),
                ("to_utc_timestamp(ltz,'UTC')", ""),
                ("from_utc_timestamp(ltz,'UTC')", " WHERE id <= 5"),
                ("to_utc_timestamp(ltz,'UTC')", " WHERE id <= 5"),
                ("from_utc_timestamp(ltz,'+05:45')", ""),
                ("to_utc_timestamp(ltz,'+05:45')", ""),
                ("from_utc_timestamp(ltz,'-18:00')", ""),
                ("to_utc_timestamp(ltz,'+18:00')", ""),
                ("convert_timezone('UTC','Z',ntz)", ""),
                ("convert_timezone('Z','Z',ntz)", ""),
            ] {
                let sql = format!(
                    "SELECT id,{expression} AS converted FROM observations{filter} ORDER BY id"
                );
                let result = execute_conversion(&session, &sql, &options)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                observations.push(json!({
                    "expression":expression,"filter":filter,"ansi_mode":ansi_mode,
                    "session_zone":session_zone,"result":result
                }));
            }
        }
    }
    check_fixed_zone_staging(observations)
}
