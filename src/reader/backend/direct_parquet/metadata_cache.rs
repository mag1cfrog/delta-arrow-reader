//! Shared parsed Parquet metadata cache.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::OnceCell;

type ParquetMetadataCell = OnceCell<Arc<ParquetMetaData>>;

/// Parsed Parquet metadata shared by file tasks.
///
/// An empty cache deduplicates lazy metadata loads for ranged tasks within one physical plan. A
/// cache populated during table loading can instead be retained and reused by later scans. This
/// type provides only keyed storage and single-flight loading; its owner defines the lifetime.
#[derive(Default)]
pub(crate) struct ParquetMetadataCache {
    entries: Mutex<HashMap<(Path, u64), Arc<ParquetMetadataCell>>>,
}

impl ParquetMetadataCache {
    pub(super) fn entry(&self, path: &Path, file_size: u64) -> Arc<ParquetMetadataCell> {
        Arc::clone(
            self.entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry((path.clone(), file_size))
                .or_insert_with(|| Arc::new(OnceCell::new())),
        )
    }
}
