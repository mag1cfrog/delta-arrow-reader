//! Integer-day DATE arithmetic preserves the entire Date32 representation.

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

pub(crate) async fn capture_date_result(
    session: &SessionContext,
    case: &Value,
) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    assert_eq!(query.column_names, ["r"]);
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    if field.data_type() != &DataType::Date32 {
        return capture_spark_query_result(session, case).await;
    }
    let logical = query.logical_plan.schema().field(0);
    assert_eq!(field.name(), &query.column_names[0]);
    if logical.data_type() != field.data_type()
        || logical.is_nullable() != field.is_nullable()
        || logical.metadata() != field.metadata()
    {
        return Err(format!("logical field {logical:?} differs from DATE field {field:?}").into());
    }
    let schema = json!({"type":"struct","fields":[{"name":field.name(),"type":"date",
        "nullable":field.is_nullable(),"metadata":field.metadata()}]});
    // UNIX_DATE observes Date32 extrema without converting them to a calendar date.
    let mut result = capture_spark_query_result(
        session,
        &json!({"ansi":ansi_mode,"sql":format!("SELECT unix_date(r) AS r FROM ({sql}) observed")}),
    )
    .await?;
    result["schema"] = schema;
    Ok(result)
}

#[tokio::test]
async fn date_day_arithmetic_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    for (capture, count) in [
        (
            include_str!("fixtures/date-day-arithmetic-spark.jsonl"),
            296,
        ),
        (
            include_str!("fixtures/date-day-admission-probes-spark.jsonl"),
            68,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records[0]["timezone"], "UTC");
        assert_eq!(records[0]["date_encoding"], "unix_date");
        assert_eq!(records.len() - 1, count);
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
                let actual = capture_date_result(&session, case).await;
                let expected = &case["expected"];
                let matches = match expected["condition"].as_str() {
                    Some(condition) => actual
                        .as_ref()
                        .is_err_and(|error| error.to_string().contains(&format!("[{condition}]"))),
                    None => actual.as_ref().ok() == Some(expected),
                };
                if !matches {
                    differences.push(format!(
                        "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_date_functions_preserve_native_calendar_arithmetic() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT DATE '2020-01-31' + INTERVAL '1 month' AS r",
        "SELECT DATE '2020-02-28' + INTERVAL '1 day' AS r",
        "SELECT DATE '2020-03-01' - INTERVAL '1 day' AS r",
        "SELECT TIMESTAMP '2020-02-28 23:59:59' + INTERVAL '1 day' AS r",
    ] {
        assert_eq!(
            configured.sql(sql).await?.collect().await?,
            native.sql(sql).await?.collect().await?,
            "{sql}"
        );
    }
    Ok(())
}
