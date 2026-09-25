//! Compact DV loading must preserve results across scan backends and coordinate modes.

use std::error::Error;

use arrow::{array::Int32Array, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaTableBuilder,
    ParquetReaderBackend, WarmupMode,
};
use futures_util::TryStreamExt;

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const ROWS: u64 = 6 * 16_384;

fn error_chain(error: &dyn Error) -> String {
    std::iter::successors(Some(error), |&error| error.source())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn fixtures() -> TestResult<Vec<(RealParquetDeltaTable, Vec<u64>)>> {
    [
        ("empty", Vec::new()),
        ("sparse", (0..ROWS).step_by(997).collect()),
        ("dense", (0..ROWS / 2).collect()),
        ("alternating", (0..ROWS).step_by(2).collect()),
        (
            "clustered",
            (0..ROWS).filter(|row| row % 1000 < 600).collect(),
        ),
    ]
    .into_iter()
    .map(|(name, deleted)| {
        Ok((
            RealParquetDeltaTable::new_with_row_groups_and_deletion_vector(
                &format!("compact-{name}"),
                6,
                16_384,
                &deleted,
            )?,
            deleted,
        ))
    })
    .collect()
}

fn assert_ids(batches: &[RecordBatch], deleted: &[u64], lower: i32) -> TestResult {
    let mut actual = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("id must be Int32")?;
        actual.extend(ids.values().iter().copied());
    }
    actual.sort_unstable();
    let expected: Vec<_> = (1..=ROWS as i32)
        .filter(|id| *id > lower && deleted.binary_search(&((*id - 1) as u64)).is_err())
        .collect();
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn streaming_compact_vectors_match_row_membership() -> TestResult {
    for (fixture, deleted) in fixtures()? {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .with_warmup(warmup)
                .load_table()
                .await?;
            for backend in [
                ParquetReaderBackend::Direct,
                ParquetReaderBackend::DeltaKernel,
            ] {
                for lower in [0, 32_775] {
                    let scan = table
                        .scan()
                        .with_projection(["id"])
                        .with_execution_options(
                            DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                        )
                        .with_predicate(DeltaPredicate::Compare {
                            column: "id".to_owned(),
                            op: DeltaComparison::Gt,
                            value: DeltaScalar::Int32(lower),
                        })
                        .build()
                        .await?;
                    let stream = scan.into_stream();
                    let metrics = stream.metrics();
                    let batches = stream.try_collect::<Vec<_>>().await.map_err(|error| {
                        format!(
                            "{}: {warmup:?}, {backend:?}, id > {lower}: {}",
                            fixture.path().display(),
                            error_chain(&error)
                        )
                    })?;
                    assert_ids(&batches, &deleted, lower)?;
                    assert_eq!(metrics.snapshot().deletion_vector_failures, 0);
                    assert_eq!(metrics.snapshot().deletion_vector_coordinate_rejections, 0);
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn datafusion_compact_vectors_match_row_membership_after_repartitioning() -> TestResult {
    use datafusion::{
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use delta_arrow_reader::datafusion::{
        DeltaTableProvider, IntraFileRepartitioning, ScanOptions, collect_scan_metrics,
    };
    use std::sync::Arc;

    for (fixture, deleted) in fixtures()? {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .with_warmup(warmup)
                .load_table()
                .await?;
            for backend in [
                ParquetReaderBackend::Direct,
                ParquetReaderBackend::DeltaKernel,
            ] {
                let provider = DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        execution_options: DeltaScanExecutionOptions::new()
                            .with_parquet_backend(backend),
                        target_partitions: Some(8),
                        intra_file_repartitioning: IntraFileRepartitioning::Always,
                        ..Default::default()
                    },
                )?;
                let context = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_batch_size(127)
                        .with_target_partitions(8)
                        .with_repartition_file_min_size(1),
                );
                context.register_table("orders", Arc::new(provider))?;
                for lower in [0, 32_775] {
                    let plan = context
                        .sql(&format!("SELECT id FROM orders WHERE id > {lower}"))
                        .await?
                        .create_physical_plan()
                        .await?;
                    let metrics = collect_scan_metrics(plan.as_ref());
                    assert_eq!(metrics.len(), 1);
                    let batches = collect(plan, context.task_ctx()).await.map_err(|error| {
                        format!(
                            "{}: {warmup:?}, {backend:?}, id > {lower}: {}",
                            fixture.path().display(),
                            error_chain(&error)
                        )
                    })?;
                    assert_ids(&batches, &deleted, lower)?;
                    let snapshot = metrics[0].snapshot().reader_metrics;
                    if backend == ParquetReaderBackend::Direct {
                        assert!(snapshot.file_tasks_started > 1);
                    } else {
                        assert_eq!(snapshot.file_tasks_started, 1);
                    }
                    assert_eq!(snapshot.deletion_vector_failures, 0);
                    assert_eq!(snapshot.deletion_vector_coordinate_rejections, 0);
                }
            }
        }
    }
    Ok(())
}
