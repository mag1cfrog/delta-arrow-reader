#[path = "../../common.rs"]
mod common;

use datafusion::prelude::SessionContext;
use deltalake::{
    DeltaTableBuilder, DeltaTableError,
    delta_datafusion::{DeltaScanConfig, DeltaScanNext},
    kernel::EagerSnapshot,
};
use serde_json::{Value, json};
use std::sync::Arc;

const READER: &str = "delta-rs";

fn provider_settings(context: &SessionContext, reuse: bool) -> common::Result<Value> {
    let config = DeltaScanConfig::new_from_session(&context.state());
    if !config.enable_parquet_pushdown || !config.schema_force_view_types {
        return Err("delta-rs pushdown and view types must be enabled".into());
    }
    Ok(json!({
        "entry_point": "DeltaScanNext::builder -> TableProviderBuilder",
        "scan_config": serde_json::to_value(config)?,
        "snapshot": if reuse { "EagerSnapshot" } else { "Snapshot" },
        "file_selection": null,
    }))
}

async fn register(context: &SessionContext, request: &common::Request) -> common::Result<()> {
    let store =
        DeltaTableBuilder::from_url(url::Url::parse(&request.table_uri)?)?.build_storage()?;
    context
        .runtime_env()
        .register_object_store(store.object_store_url().as_ref(), store.object_store(None));
    let mut builder = DeltaScanNext::builder()
        .with_log_store(Arc::clone(&store))
        .with_table_version(request.snapshot_version)
        .with_session(Arc::new(context.state()));
    if request.reuse() {
        builder = builder.with_eager_snapshot(
            EagerSnapshot::try_new(store.as_ref(), Some(request.snapshot_version)).await?,
        );
    }
    context.register_table(common::TABLE, Arc::new(builder.build().await?))?;
    Ok(())
}

async fn provider_evidence(context: &SessionContext, _: bool) -> common::Result<Value> {
    let provider = context.table_provider(common::TABLE).await?;
    let provider = provider
        .downcast_ref::<DeltaScanNext>()
        .ok_or("unexpected delta-rs provider")?;
    // Untimed validation/diagnostics only: inspect the resulting provider, not just intended options.
    let serialized = serde_json::to_value(provider)?;
    if serialized["config"]["enable_parquet_pushdown"] != true
        || serialized["config"]["schema_force_view_types"] != true
    {
        return Err("built delta-rs provider lost pushdown/view settings".into());
    }
    Ok(json!({"scan_config": serialized["config"], "schema": serialized["full_schema"]}))
}

fn unsupported(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<DeltaTableError>(),
        Some(
            DeltaTableError::MissingFeature { .. }
                | DeltaTableError::UnsupportedColumnMapping { .. }
        )
    ) || matches!(
        error.downcast_ref::<deltalake::kernel::transaction::TransactionError>(),
        Some(deltalake::kernel::transaction::TransactionError::UnsupportedTableFeatures(_))
    )
}

fn main() {
    common::main();
}
