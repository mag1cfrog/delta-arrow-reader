//! Compare planning and execution cost of NULLIF over the same registered inputs.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::json;

const ROWS: usize = 262_144;

fn median(samples: &[u128]) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    for nullable in [false, true] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_target_partitions(2)),
            )
            .build(),
        );
        let left = Float64Array::from_iter(
            (0..ROWS).map(|i| (!nullable || i % 11 != 0).then_some((i % 17) as f64)),
        );
        let right = Float64Array::from_iter((0..ROWS).map(|i| {
            (!nullable || i % 13 != 0).then_some(if i % 3 == 0 { (i % 17) as f64 } else { 0.0 })
        }));
        session.register_batch(
            "inputs",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Float64, nullable),
                    Field::new("b", DataType::Float64, nullable),
                ])),
                vec![Arc::new(left), Arc::new(right)],
            )?,
        )?;
        for (name, expression) in [
            ("column_zero", "NULLIF(a,0D)"),
            ("column_nonzero", "NULLIF(a,1D)"),
            ("columns", "NULLIF(a,b)"),
            ("scalar_column", "NULLIF(1D,b)"),
        ] {
            let sql = format!("SELECT {expression} AS r FROM inputs");
            let (mut planning_ns, mut execution_ns) = (Vec::new(), Vec::new());
            let mut counts = None;
            for sample in 0..13 {
                let started = Instant::now();
                let query = plan_query(&session, &sql, &SparkSqlOptions::default()).await?;
                let plan = query.create_physical_plan(&session).await?;
                let planning = started.elapsed().as_nanos();
                let started = Instant::now();
                let batches = collect(plan, session.task_ctx()).await?;
                let execution = started.elapsed().as_nanos();
                let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                let nulls = batches
                    .iter()
                    .map(|b| b.column(0).null_count())
                    .sum::<usize>();
                assert_eq!(rows, ROWS);
                if let Some(previous) = counts {
                    assert_eq!(previous, (rows, nulls));
                }
                counts = Some((rows, nulls));
                if sample >= 2 {
                    planning_ns.push(planning);
                    execution_ns.push(execution);
                }
            }
            println!(
                "{}",
                json!({"case":name,"nullable":nullable,"rows":ROWS,"nulls":counts.map(|c|c.1),
                "planning_median_ns":median(&planning_ns),"execution_median_ns":median(&execution_ns),
                "planning_samples_ns":planning_ns,"execution_samples_ns":execution_ns})
            );
        }
    }
    Ok(())
}
