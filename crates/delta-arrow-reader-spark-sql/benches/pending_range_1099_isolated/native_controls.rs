//! Check ordinary native RANGE workloads alongside the pending-frame probe.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext, col};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn bounded_windows(plan: &dyn ExecutionPlan) -> usize {
    assert!(
        !matches!(
            plan.name(),
            "SortExec" | "WindowAggExec" | "RepartitionExec"
        ),
        "native control changed execution shape: {}",
        plan.name()
    );
    usize::from(plan.downcast_ref::<BoundedWindowAggExec>().is_some())
        + plan
            .children()
            .iter()
            .map(|child| bounded_windows(child.as_ref()))
            .sum::<usize>()
}

fn check_rows(batches: &[RecordBatch], case: &str, rows: usize, partition_rows: usize) {
    let width = if case == "native_partitioned_range" {
        partition_rows
    } else {
        rows
    };
    let mut expected_id = 0;
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("INT id");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT count");
        assert_eq!(ids.null_count() + counts.null_count(), 0);
        for (id, count) in ids.values().iter().zip(counts.values()) {
            assert_eq!(*id as usize, expected_id);
            let expected = if case == "native_cumulative_range" {
                expected_id + 1
            } else {
                let position = expected_id % width;
                1 + usize::from(position > 0) + usize::from(position + 1 < width)
            };
            assert_eq!(*count as usize, expected);
            expected_id += 1;
        }
    }
    assert_eq!(expected_id, rows);
}

pub async fn run(manifest: &Path, measure: bool, block: usize) -> Result {
    let manifest: Value = serde_json::from_slice(&std::fs::read(manifest)?)?;
    let rows = manifest["rows"].as_u64().ok_or("missing row count")? as usize;
    let batch_rows = manifest["batch_rows"]
        .as_u64()
        .ok_or("missing batch size")? as usize;
    let partition_rows = manifest["partition_rows"]
        .as_u64()
        .ok_or("missing partition size")? as usize;
    assert_eq!(
        (rows, batch_rows, partition_rows),
        (1_000_000, 8_192, 1_000)
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("k", DataType::Int32, false),
        Field::new("p", DataType::Int32, false),
    ]));
    let batches = (0..rows)
        .step_by(batch_rows)
        .map(|start| {
            let end = (start + batch_rows).min(rows);
            let values = Int32Array::from_iter_values(start as i32..end as i32);
            let partitions =
                Int32Array::from_iter_values((start..end).map(|id| (id / partition_rows) as i32));
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(values.clone()),
                    Arc::new(values),
                    Arc::new(partitions),
                ],
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let shapes = manifest["shapes"]
        .as_array()
        .ok_or("missing native controls")?;
    assert_eq!(shapes.len(), 3);
    for offset in 0..shapes.len() {
        let shape = &shapes[(block + offset) % shapes.len()];
        let name = shape["id"].as_str().ok_or("missing control id")?;
        let sql = shape["sql"].as_str().ok_or("missing control SQL")?;
        let sorting = if name == "native_partitioned_range" {
            vec![col("p").sort(true, true), col("k").sort(true, true)]
        } else {
            vec![col("k").sort(true, true)]
        };
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(batch_rows),
        );
        let table = MemTable::try_new(Arc::clone(&schema), vec![batches.clone()])?
            .with_sort_order(vec![sorting]);
        session.register_table("ordinary_ranges", Arc::new(table))?;
        let plan = session.sql(sql).await?.create_physical_plan().await?;
        assert_eq!(bounded_windows(plan.as_ref()), 1);
        let schema = plan.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        let mut samples = Vec::new();
        for repetition in 0..if measure { 9 } else { 1 } {
            let started = (measure && repetition >= 2).then(Instant::now);
            let results = collect(Arc::clone(&plan), session.task_ctx()).await?;
            if let Some(started) = started {
                let elapsed = started.elapsed().as_nanos() as u64;
                super::record_sample(name, repetition - 1, elapsed)?;
                samples.push(elapsed);
            }
            assert!(results.iter().all(|batch| batch.schema() == schema));
            check_rows(&results, name, rows, partition_rows);
        }
        println!(
            "{}",
            json!({
                "id": name, "rows": rows, "batch": batch_rows, "configuration": "native",
                "samples_ns": samples, "fields": format!("{:?}", schema.fields()),
                "plan": displayable(plan.as_ref()).indent(true).to_string(),
            })
        );
    }
    Ok(())
}
