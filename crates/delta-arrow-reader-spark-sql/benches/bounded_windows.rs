//! Measure bounded-frame work separately from SQL planning and input construction.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::json;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    for rows in [4096, 8192] {
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(1024),
        );
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows as i64).map(|i| i % 31),
                    )),
                ),
                (
                    "keep",
                    Arc::new(BooleanArray::from_iter((0..rows).map(|i| Some(i % 3 != 0)))),
                ),
            ])?,
        )?;
        for expression in [
            "SUM(value)",
            "APPROX_DISTINCT(value)",
            "ARRAY_AGG(DISTINCT value)",
        ] {
            'frames: for (name, frame) in [
                ("rows_4", "3 PRECEDING AND CURRENT ROW"),
                ("rows_128", "127 PRECEDING AND CURRENT ROW"),
                ("prefix", "UNBOUNDED PRECEDING AND CURRENT ROW"),
                ("suffix", "CURRENT ROW AND UNBOUNDED FOLLOWING"),
            ] {
                let sql = format!(
                    "SELECT {expression} FILTER (WHERE keep) OVER (ORDER BY id ROWS BETWEEN {frame}) FROM inputs"
                );
                let planned = session.sql(&sql).await?.create_physical_plan().await;
                let plan = match planned {
                    Ok(plan) => plan,
                    Err(error) => {
                        println!(
                            "{}",
                            json!({"expression":expression,"frame":name,"rows":rows,"error":error.to_string()})
                        );
                        continue;
                    }
                };
                let mut samples = Vec::new();
                for repeat in 0..7 {
                    let started = Instant::now();
                    let batches = match collect(Arc::clone(&plan), session.task_ctx()).await {
                        Ok(batches) => batches,
                        Err(error) => {
                            println!(
                                "{}",
                                json!({"expression":expression,"frame":name,"rows":rows,"error":error.to_string()})
                            );
                            continue 'frames;
                        }
                    };
                    let elapsed = started.elapsed().as_micros();
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows
                    );
                    if repeat >= 2 {
                        samples.push(elapsed);
                    }
                }
                let mut sorted = samples.clone();
                sorted.sort_unstable();
                println!(
                    "{}",
                    json!({"expression":expression,"frame":name,"rows":rows,
                    "median_us":sorted[sorted.len()/2],"samples_us":samples})
                );
            }
        }
    }
    Ok(())
}
