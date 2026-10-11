//! Shared parsed Parquet metadata cache.

use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use object_store::path::Path;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataFetch, ParquetObjectReader},
    },
    errors::Result,
    file::{
        FOOTER_SIZE,
        metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader},
    },
};
use tokio::sync::OnceCell;

use super::nan_counts::NanCounts;

type ParquetMetadataCell = OnceCell<Arc<CachedParquetMetadata>>;

/// Footer statistics and the native metadata must share the same load and cache entry.
pub(super) struct CachedParquetMetadata {
    pub(super) parquet: Arc<ParquetMetaData>,
    pub(super) nan_counts: NanCounts,
}

impl CachedParquetMetadata {
    pub(super) async fn load(
        reader: &mut ParquetObjectReader,
        file_size: u64,
        size_hint: Option<usize>,
        options: &ArrowReaderOptions,
    ) -> Result<Arc<Self>> {
        let mut fetch = FooterCapture {
            reader,
            file_size,
            metadata_range: None,
            footer: None,
        };
        let parquet = ParquetMetaDataReader::new()
            .with_metadata_options(Some(options.metadata_options().clone()))
            .with_column_index_policy(options.column_index_policy())
            .with_offset_index_policy(options.offset_index_policy())
            .with_prefetch_hint(size_hint)
            .load_and_finish(&mut fetch, file_size)
            .await?;
        let nan_counts = fetch
            .footer
            .as_deref()
            .map(|footer| NanCounts::decode(footer, &parquet))
            .unwrap_or_default();
        Ok(Arc::new(Self {
            parquet: Arc::new(parquet),
            nan_counts,
        }))
    }
}

/// Observes the existing metadata requests, without making another request or
/// retaining raw footer bytes after decoding the counts. parquet-rs still owns
/// footer validation, range planning, and optional page-index loading.
struct FooterCapture<'a> {
    reader: &'a mut ParquetObjectReader,
    file_size: u64,
    metadata_range: Option<Range<u64>>,
    footer: Option<Bytes>,
}

impl FooterCapture<'_> {
    fn capture(&mut self, range: Range<u64>, bytes: &Bytes) -> Option<()> {
        if self.footer.is_some() || bytes.len() as u64 != range.end.checked_sub(range.start)? {
            return None;
        }
        if range.end == self.file_size {
            let tail =
                FooterTail::try_from(bytes.get(bytes.len().checked_sub(FOOTER_SIZE)?..)?).ok()?;
            if tail.is_encrypted_footer() {
                return None;
            }
            let end = self.file_size.checked_sub(FOOTER_SIZE as u64)?;
            self.metadata_range = Some(end.checked_sub(tail.metadata_length() as u64)?..end);
        }
        let metadata = self.metadata_range.as_ref()?;
        if range.start <= metadata.start && range.end >= metadata.end {
            let start = usize::try_from(metadata.start - range.start).ok()?;
            let end = usize::try_from(metadata.end - range.start).ok()?;
            self.footer = Some(bytes.slice(start..end));
        }
        Some(())
    }
}

impl MetadataFetch for &mut FooterCapture<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            let bytes = self.reader.get_bytes(range.clone()).await?;
            self.capture(range, &bytes);
            Ok(bytes)
        })
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_are_reused_only_for_the_same_path_and_size() {
        let cache = ParquetMetadataCache::default();
        let path = Path::from("part.parquet");
        let entry = cache.entry(&path, 100);

        assert!(Arc::ptr_eq(&entry, &cache.entry(&path, 100)));
        assert!(!Arc::ptr_eq(&entry, &cache.entry(&path, 101)));
        assert!(!Arc::ptr_eq(
            &entry,
            &cache.entry(&Path::from("other.parquet"), 100)
        ));
    }
}
