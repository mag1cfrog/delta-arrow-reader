//! Execution capacities must be valid before they reach Tokio primitives.

use std::{error::Error, time::Duration};

use arrow::{array::Int32Array, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaReaderPhase, DeltaScanExecutionOptions, DeltaTableBuilder, ParquetReaderBackend,
};
use futures_util::TryStreamExt;
use tokio::{sync::Semaphore, time::timeout};

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn invalid_capacities_return_configuration_errors_at_the_setter() {
    for value in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
        let options = DeltaScanExecutionOptions::new();
        for (name, result) in [
            (
                "max_concurrent_file_reads_per_scan",
                options.with_max_concurrent_file_reads_per_scan(Some(value)),
            ),
            (
                "max_concurrent_file_reads_per_partition",
                options.with_max_concurrent_file_reads_per_partition(value),
            ),
            (
                "output_buffer_batches_per_partition",
                options.with_output_buffer_batches_per_partition(value),
            ),
        ] {
            let error = result.expect_err("unsupported capacity must fail at configuration");
            assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
            assert_eq!(error.code(), "invalid_configuration");
            assert!(error.source().is_none());
            let suffix = if value == 0 {
                "must_be_positive"
            } else {
                "exceeds_tokio_max_permits"
            };
            assert_eq!(
                error.to_string(),
                format!(
                    "delta reader error: phase=configuration code=invalid_configuration reason={name}_{suffix}"
                ),
            );
        }
    }
}

#[test]
fn capacity_bounds_do_not_restrict_unrelated_options() -> TestResult {
    for value in [1, Semaphore::MAX_PERMITS] {
        let options = DeltaScanExecutionOptions::new()
            .with_max_concurrent_file_reads_per_scan(Some(value))?
            .with_max_concurrent_file_reads_per_partition(value)?
            .with_output_buffer_batches_per_partition(value)?;
        assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(value));
        assert_eq!(options.max_concurrent_file_reads_per_partition(), value);
        assert_eq!(options.output_buffer_batches_per_partition(), value);
        assert_eq!(
            options
                .with_max_concurrent_file_reads_per_scan(None)?
                .max_concurrent_file_reads_per_scan(),
            None,
        );
    }
    for value in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
        assert_eq!(
            DeltaScanExecutionOptions::new()
                .with_prefetch_files_per_partition(value)
                .prefetch_files_per_partition(),
            value,
        );
    }
    for value in [
        None,
        Some(1),
        Some(Semaphore::MAX_PERMITS + 1),
        Some(usize::MAX),
    ] {
        let options = DeltaScanExecutionOptions::new()
            .with_parquet_metadata_size_hint_bytes(value)?
            .with_parquet_full_file_read_threshold_bytes(value)?;
        assert_eq!(options.parquet_metadata_size_hint_bytes(), value);
        assert_eq!(options.parquet_full_file_read_threshold_bytes(), value);
    }
    Ok(())
}

fn capacity_cases() -> TestResult<Vec<(&'static str, usize, DeltaScanExecutionOptions)>> {
    let defaults = DeltaScanExecutionOptions::new();
    let max = Semaphore::MAX_PERMITS;
    let max_partition = defaults.with_max_concurrent_file_reads_per_partition(max)?;
    let all_max = max_partition
        .with_max_concurrent_file_reads_per_scan(Some(max))?
        .with_output_buffer_batches_per_partition(max)?;
    Ok(vec![
        ("defaults", 2, defaults),
        ("derived_below_limit", max / 3, defaults),
        ("derived_above_limit", max / 3 + 1, defaults),
        ("target_at_limit", max, defaults),
        ("target_above_limit", max + 1, defaults),
        ("target_at_usize_max", usize::MAX, defaults),
        ("partition_at_limit", 1, max_partition),
        ("derived_product_above_limit", 2, max_partition),
        ("derived_product_overflow", usize::MAX, max_partition),
        (
            "scan_at_limit",
            2,
            defaults.with_max_concurrent_file_reads_per_scan(Some(max))?,
        ),
        (
            "output_at_limit",
            2,
            defaults.with_output_buffer_batches_per_partition(max)?,
        ),
        (
            "small_scan_with_large_partition_and_prefetch",
            usize::MAX,
            all_max
                .with_max_concurrent_file_reads_per_scan(Some(1))?
                .with_prefetch_files_per_partition(usize::MAX),
        ),
        (
            "all_limits_with_no_prefetch",
            usize::MAX,
            all_max.with_prefetch_files_per_partition(0),
        ),
        (
            "cleared_scan_limit",
            usize::MAX,
            all_max.with_max_concurrent_file_reads_per_scan(None)?,
        ),
    ])
}

fn assert_rows(batches: &[RecordBatch], case: &str) -> TestResult {
    let mut ids = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 1, "{case}");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("id column must be Int32")?;
        ids.extend(column.iter());
    }
    ids.sort_unstable();
    assert_eq!(ids, [Some(1), Some(2), Some(3), Some(4)], "{case}");
    Ok(())
}

#[tokio::test]
async fn streaming_reads_accept_supported_capacities_and_large_targets() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_files("stream-capacities")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (case, target, options) in capacity_cases()? {
            let scan = table
                .scan()
                .with_execution_options(options.with_parquet_backend(backend))
                .with_target_partitions(target)?
                .with_projection(["id"])
                .build()
                .await?;
            assert_eq!(scan.partition_count(), target.min(2), "{case}");
            let stream = scan.into_stream();
            let metrics = stream.metrics();
            let batches =
                timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>()).await??;
            assert_rows(&batches, case)?;
            assert_eq!(metrics.snapshot().file_tasks_completed, 2, "{case}");
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn datafusion_reads_accept_supported_capacities_and_large_targets() -> TestResult {
    use datafusion::{
        datasource::TableProvider,
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics};

    let fixture = RealParquetDeltaTable::new_with_two_files("datafusion-capacities")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(2),
    );
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (case, target, options) in capacity_cases()? {
            let provider = DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    execution_options: options.with_parquet_backend(backend),
                    target_partitions: Some(target),
                    ..Default::default()
                },
            )?;
            let projection = vec![0];
            let plan = provider
                .scan(&context.state(), Some(&projection), &[], None)
                .await?;
            assert_eq!(
                plan.properties().output_partitioning().partition_count(),
                target.min(2)
            );
            let metrics = collect_scan_metrics(plan.as_ref());
            let batches =
                timeout(Duration::from_secs(10), collect(plan, context.task_ctx())).await??;
            assert_rows(&batches, case)?;
            assert_eq!(
                metrics[0].snapshot().reader_metrics.file_tasks_completed,
                2,
                "{case}"
            );
        }
    }
    Ok(())
}
