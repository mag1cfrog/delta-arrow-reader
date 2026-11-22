//! Observe panics from real Spark SQL execution in an isolated process.
use std::{error::Error, sync::Arc};
use arrow::array::{ArrayRef, Date32Array, Time64MicrosecondArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = observe().await {
        println!("error {error}");
        std::process::exit(2);
    }
}

async fn observe() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [micros, zone, ansi, expression] = &args[..] else { return Err("bad arguments".into()) };
    let micros: i64 = micros.parse()?;
    let ansi_mode = ansi.parse()?;
    let session = SessionContext::new_with_state(configure_spark_sql(
        SessionStateBuilder::new().with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(1)),
    ).build());
    let ntz = TimestampMicrosecondArray::from(vec![micros]);
    let ltz: ArrayRef = Arc::new(ntz.clone().with_timezone("UTC"));
    let date: ArrayRef = Arc::new(Date32Array::from(vec![i32::try_from(micros.div_euclid(86_400_000_000))?]));
    let time: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![micros.rem_euclid(86_400_000_000)]));
    session.register_batch("probe", RecordBatch::try_from_iter([
        ("ntz", Arc::new(ntz) as ArrayRef), ("ltz", ltz), ("d", date), ("tod", time),
    ])?)?;
    let options = SparkSqlOptions { ansi_mode, session_timezone: zone.clone().into(), ..SparkSqlOptions::default() };
    println!("planning");
    let plan = plan_query(&session, &format!("SELECT {expression} AS result FROM probe"), &options).await?;
    println!("physical");
    let physical = plan.create_physical_plan(&session).await?;
    println!("collect");
    for batch in collect(physical, session.task_ctx()).await? {
        assert_eq!(batch.num_rows(), 1);
        let result = ScalarValue::try_from_array(batch.column(0), 0)?;
        let ScalarValue::TimestampMicrosecond(ticks, zone) = result else {
            return Err(format!("unexpected type {}", result.data_type()).into());
        };
        println!("value {ticks:?} {zone:?}");
    }
    Ok(())
}
