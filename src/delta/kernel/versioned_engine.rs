//! Bound snapshot discovery by existing log entries, not numeric version gaps.

use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::{
    DeltaResult, Engine, EvaluationHandler, FileMeta, FileSlice, JsonHandler, ParquetHandler,
    StorageHandler,
    path::{LogPathFileType, ParsedLogPath},
};
use url::Url;

/// A temporary view of the engine used only to load an explicit snapshot version.
#[derive(Clone)]
pub(super) struct VersionedEngine {
    engine: Arc<dyn Engine + Send + Sync>,
    version: u64,
}

impl VersionedEngine {
    pub(super) fn new(engine: Arc<dyn Engine + Send + Sync>, version: u64) -> Self {
        Self { engine, version }
    }
}

impl Engine for VersionedEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.engine.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        Arc::new(self.clone())
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.engine.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.engine.parquet_handler()
    }
}

impl StorageHandler for VersionedEngine {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let log_file_depth = path.path_segments().map(|segments| segments.count());
        let version = self.version;
        let files = self.engine.storage_handler().list_from(path)?;
        let files = files
            // The listing is recursive; only direct log entries belong to this snapshot.
            // Comparing depth also avoids differing percent encoding in listed URLs.
            .filter(move |file| {
                file.as_ref().map_or(true, |file| {
                    file.location.path_segments().map(|segments| segments.count()) == log_file_depth
                })
            })
            .filter_map(|file| match file.and_then(ParsedLogPath::try_from) {
                Ok(Some(path)) => Some(Ok(path)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .take_while(move |path| path.as_ref().map_or(true, |path| path.version <= version))
            .filter(move |path| {
                path.as_ref().map_or(true, |path| {
                    !matches!(path.file_type, LogPathFileType::CompactedCommit { hi } if hi > version)
                })
            })
            .map(|path| path.map(|path| path.location));
        Ok(Box::new(files))
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        if matches!(files.as_slice(), [(path, None)] if path.path().ends_with("/_last_checkpoint"))
        {
            // ponytail: one forward listing of retained log entries for time travel.
            // Ignore the latest-only hint; Kernel discovers complete checkpoints and
            // reads their schemas from the footers. Remove when Kernel bounds at_version.
            return Ok(Box::new(std::iter::empty()));
        }
        self.engine.storage_handler().read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        self.engine.storage_handler().copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        self.engine.storage_handler().put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        self.engine.storage_handler().head(path)
    }
}
