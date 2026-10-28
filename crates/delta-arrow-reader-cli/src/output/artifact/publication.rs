//! Finalize optional provenance and publish completed files without replacing destinations.

use std::{
    io::{self, Write},
    path::PathBuf,
};

use futures_util::future::AbortHandle;
use serde_json::{Value, json};
use sha2::Digest;
use snafu::ResultExt;
use tempfile::NamedTempFile;

use super::{MAX_MANIFEST_BYTES, StagedResult, encode_bounded_json};
use crate::diagnostics::{ArtifactIoSnafu, Error};

/// The result and optional manifest are complete but have no final names yet.
pub(crate) struct PendingArtifactPublication {
    result: NamedTempFile,
    manifest: Option<(NamedTempFile, PathBuf)>,
    result_path: PathBuf,
    receipt: Vec<u8>,
}

impl StagedResult {
    pub(crate) fn prepare_publication(
        self,
        completion: &Value,
    ) -> Result<PendingArtifactPublication, Error> {
        let manifest = if let Some(mut manifest) = self.manifest {
            manifest.replay["inputs"] = manifest.inputs.into();
            if manifest.replay["options"]
                .get("table")
                .is_some_and(|value| value.is_string())
            {
                manifest.replay["options"]["table-version"] =
                    completion["bindings"][0]["table_version"].clone();
            }
            if manifest.replay["options"]["table-version"].is_null()
                && let Some(options) = manifest.replay["options"].as_object_mut()
            {
                options.remove("table-version");
            }
            let document = json!({
                "format_version": 1, "kind": "result_manifest", "completion": "complete",
                "command": completion["command"], "scope": completion["scope"],
                "stop_reason": completion["stop_reason"], "has_more": completion["has_more"],
                "bindings": completion["bindings"], "rows": completion["rows"],
                "batches": completion["batches"], "output_bytes": completion["output_bytes"],
                "result": {"path": self.result_path, "format": self.format,
                    "sha256": format!("{:x}", manifest.hasher.finalize())},
                "build": {"cli": env!("CARGO_PKG_VERSION"), "core": delta_arrow_reader::VERSION, "datafusion": datafusion::DATAFUSION_VERSION},
                "replay": manifest.replay,
            });
            let bytes =
                encode_bounded_json(&document, MAX_MANIFEST_BYTES, "artifact_manifest_too_large")?;
            let parent = self.result_path.parent().ok_or(Error::ArtifactIo {
                source: io::ErrorKind::InvalidInput.into(),
                reason: "artifact_path_invalid",
            })?;
            let mut file = tempfile::Builder::new()
                .prefix(".dar-manifest-")
                .tempfile_in(parent)
                .context(ArtifactIoSnafu {
                    reason: "artifact_staging_failed",
                })?;
            file.write_all(&bytes)
                .and_then(|()| file.as_file().sync_all())
                .context(ArtifactIoSnafu {
                    reason: "artifact_write_failed",
                })?;
            Some((file, manifest.path))
        } else {
            None
        };
        self.file.as_file().sync_all().context(ArtifactIoSnafu {
            reason: "artifact_write_failed",
        })?;
        Ok(PendingArtifactPublication {
            result: self.file,
            manifest,
            result_path: self.result_path,
            receipt: self.receipt,
        })
    }
}

impl PendingArtifactPublication {
    /// Call after command cleanup; a requested manifest is published after the result.
    pub(crate) fn publish(self, cancellation: &AbortHandle) -> Result<Vec<u8>, Error> {
        if cancellation.is_aborted() {
            return Err(Error::Cancelled { status: 1 });
        }
        self.result
            .persist_noclobber(&self.result_path)
            .map_err(|error| Error::ArtifactIo {
                source: error.error,
                reason: "artifact_result_publish_failed",
            })?;
        if let Some((manifest, path)) = self.manifest {
            if cancellation.is_aborted() {
                return Err(Error::Cancelled { status: 1 });
            }
            manifest
                .persist_noclobber(path)
                .map_err(|error| Error::ArtifactIo {
                    source: error.error,
                    reason: "artifact_manifest_publish_failed",
                })?;
        }
        Ok(self.receipt)
    }
}
