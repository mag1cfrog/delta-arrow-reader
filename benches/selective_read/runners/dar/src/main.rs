#[path = "../../common.rs"]
mod common;

use datafusion::prelude::SessionContext;
use delta_arrow_reader::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaSnapshotSelection, DeltaTableBuilder,
    ParquetReaderBackend, WarmupMode,
    datafusion::{IntraFileRepartitioning, ScanOptions, register_table},
};
use serde_json::{Value, json};

const READER: &str = "delta-arrow-reader";

fn options() -> common::Result<ScanOptions> {
    Ok(ScanOptions {
        execution_options: DeltaScanExecutionOptions::new()
            .with_parquet_backend(ParquetReaderBackend::Direct)
            .with_max_concurrent_file_reads_per_scan(Some(24))?
            .with_max_concurrent_file_reads_per_partition(3)?
            .with_output_buffer_batches_per_partition(1)?
            .with_prefetch_files_per_partition(2)
            .with_parquet_metadata_size_hint_bytes(Some(65_536))?
            .with_parquet_full_file_read_threshold_bytes(None)?,
        target_partitions: Some(8),
        intra_file_repartitioning: IntraFileRepartitioning::WhenBelowTarget,
        use_arrow_view_types: true,
    })
}

fn provider_settings(_: &SessionContext, reuse: bool) -> common::Result<Value> {
    Ok(json!({
        "entry_point": "DeltaTableBuilder -> datafusion::register_table",
        "scan_options": format!("{:?}", options()?),
        "warmup": if reuse { "QueryPlanning" } else { "None" },
        "generic_parquet_options": "Direct backend uses its own pruning and decoding path; inspect diagnostic plan",
    }))
}

async fn register(context: &SessionContext, request: &common::Request) -> common::Result<()> {
    let scan = options()?;
    let table = DeltaTableBuilder::new(&request.table_uri)
        .with_snapshot_selection(DeltaSnapshotSelection::Version(request.snapshot_version))
        .with_execution_options(scan.execution_options)
        .with_warmup(if request.reuse() {
            WarmupMode::QueryPlanning
        } else {
            WarmupMode::None
        })
        .load_table()
        .await?;
    if table.version() != request.snapshot_version {
        return Err("loaded snapshot differs from requested version".into());
    }
    register_table(context, common::TABLE, table, scan)?;
    Ok(())
}

async fn provider_evidence(context: &SessionContext, reuse: bool) -> common::Result<Value> {
    let provider = context.table_provider(common::TABLE).await?;
    Ok(
        json!({"schema": format!("{:?}", provider.schema()), "settings": provider_settings(context, reuse)?}),
    )
}

fn unsupported(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<DeltaReaderError>(),
        Some(
            DeltaReaderError::UnsupportedProtocol { .. }
                | DeltaReaderError::UnsupportedPredicate { .. }
        )
    )
}

fn main() {
    common::main();
}
