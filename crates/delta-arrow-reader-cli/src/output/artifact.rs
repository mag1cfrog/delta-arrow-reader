//! Saved scan/query results and the manifests that commit them.
//!
//! `--output rows.arrow` publishes `rows.arrow` before `rows.arrow.manifest.json`.
//! A result without a matching completion manifest is uncommitted. Consumers check
//! the manifest's hash, framing and counts before accepting or replaying a result.

use std::{
    fs,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::{Stream, future::AbortHandle};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use tempfile::NamedTempFile;

use super::{OutputLimits, RowOutputFormat};
use crate::{ArtifactIoSnafu, Error, OutputStats};

pub(crate) const MAX_RECEIPT_BYTES: usize = 8 * 1024;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

/// Owns unpublished output; dropping it removes the temporary result.
pub(crate) struct StagedResult {
    file: NamedTempFile,
    hasher: Sha256,
    result_path: PathBuf,
    manifest_path: PathBuf,
    receipt: Vec<u8>,
    format: RowOutputFormat,
    replay: Value,
    inputs: Vec<Value>,
    cancellation: AbortHandle,
}

/// Both files are complete, but neither final name has been published.
pub(crate) struct PendingArtifactPublication {
    result: NamedTempFile,
    manifest: NamedTempFile,
    result_path: PathBuf,
    manifest_path: PathBuf,
    receipt: Vec<u8>,
}

impl StagedResult {
    pub(crate) fn new(
        path: &Path,
        format: RowOutputFormat,
        mut options: Value,
        cancellation: AbortHandle,
    ) -> Result<Self, Error> {
        if let Some(fields) = options.as_object_mut() {
            for value in fields.values_mut() {
                if value.is_number() {
                    *value = json!(value.to_string());
                }
            }
        }
        let invalid = || Error::ArtifactIo {
            source: io::ErrorKind::InvalidInput.into(),
            reason: "artifact_path_invalid",
        };
        let filename = path.file_name().ok_or_else(invalid)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = fs::canonicalize(parent).context(ArtifactIoSnafu {
            reason: "artifact_directory_unavailable",
        })?;
        let result_path = parent.join(filename);
        let result_text = result_path
            .to_str()
            .filter(|p| !p.chars().any(char::is_control))
            .ok_or_else(invalid)?;
        let manifest_path = PathBuf::from(format!("{result_text}.manifest.json"));
        let receipt = encode_bounded_json(
            &json!({
                "format_version": 1, "kind": "result_artifact",
                "result": result_path, "manifest": manifest_path,
            }),
            MAX_RECEIPT_BYTES,
            "artifact_receipt_too_large",
        )?;
        for destination in [&result_path, &manifest_path] {
            match fs::symlink_metadata(destination) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Ok(_) => {
                    return Err(Error::ArtifactIo {
                        source: io::ErrorKind::AlreadyExists.into(),
                        reason: "artifact_destination_exists",
                    });
                }
                Err(source) => {
                    return Err(Error::ArtifactIo {
                        source,
                        reason: "artifact_destination_unavailable",
                    });
                }
            }
        }
        let file = tempfile::Builder::new()
            .prefix(".dar-result-")
            .tempfile_in(&parent)
            .context(ArtifactIoSnafu {
                reason: "artifact_staging_failed",
            })?;
        let directory = std::env::current_dir().context(ArtifactIoSnafu {
            reason: "artifact_directory_unavailable",
        })?;
        let directory = directory.to_str().ok_or_else(invalid)?;
        Ok(Self {
            file,
            hasher: Sha256::new(),
            result_path,
            manifest_path,
            receipt,
            format,
            replay: json!({"working_directory": directory, "options": options}),
            inputs: Vec::new(),
            cancellation,
        })
    }

    /// Only query, table-declaration, predicate and execution inputs belong here.
    /// Credential files are references in options, never digests or copied contents.
    pub(crate) fn record_input_digest(&mut self, argument: &str, path: &str, digest: String) {
        self.inputs.push(json!({
            "argument": argument, "path": path, "sha256": digest,
        }));
    }

    pub(crate) async fn write_rows(
        self,
        schema: SchemaRef,
        stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
        bindings: Vec<Value>,
        limits: Option<OutputLimits>,
    ) -> Result<(Self, OutputStats), Error> {
        let format = self.format;
        let output = BufWriter::new(self);
        let (mut output, stats) = match format {
            RowOutputFormat::Arrow => {
                super::arrow::write_arrow_stream(output, schema, stream, limits, false).await
            }
            RowOutputFormat::Json => {
                super::json::write_query(
                    output,
                    schema,
                    stream,
                    &bindings,
                    limits.unwrap_or(OutputLimits {
                        max_rows: u64::MAX,
                        max_bytes: u64::MAX,
                    }),
                )
                .await
            }
        }
        .map_err(map_file_write_error)?;
        output.flush().context(ArtifactIoSnafu {
            reason: "artifact_write_failed",
        })?;
        let output = output.into_inner().map_err(|error| Error::ArtifactIo {
            source: error.into_error(),
            reason: "artifact_write_failed",
        })?;
        Ok((output, stats))
    }

    pub(crate) fn stage_manifest(
        mut self,
        completion: &Value,
    ) -> Result<PendingArtifactPublication, Error> {
        self.replay["inputs"] = self.inputs.into();
        if self.replay["options"]
            .get("table")
            .is_some_and(|value| value.is_string())
        {
            self.replay["options"]["table-version"] =
                completion["bindings"][0]["table_version"].clone();
        }
        if self.replay["options"]["table-version"].is_null()
            && let Some(options) = self.replay["options"].as_object_mut()
        {
            options.remove("table-version");
        }
        let manifest = json!({
            "format_version": 1, "kind": "result_manifest", "completion": "complete",
            "command": completion["command"], "scope": completion["scope"],
            "stop_reason": completion["stop_reason"], "has_more": completion["has_more"],
            "bindings": completion["bindings"], "rows": completion["rows"],
            "batches": completion["batches"], "output_bytes": completion["output_bytes"],
            "result": {"path": self.result_path, "format": self.format,
                "sha256": format!("{:x}", self.hasher.finalize())},
            "build": {"cli": env!("CARGO_PKG_VERSION"), "core": delta_arrow_reader::VERSION, "datafusion": datafusion::DATAFUSION_VERSION},
            "replay": self.replay,
        });
        let bytes =
            encode_bounded_json(&manifest, MAX_MANIFEST_BYTES, "artifact_manifest_too_large")?;
        let parent = self.result_path.parent().ok_or(Error::ArtifactIo {
            source: io::ErrorKind::InvalidInput.into(),
            reason: "artifact_path_invalid",
        })?;
        let mut manifest = tempfile::Builder::new()
            .prefix(".dar-manifest-")
            .tempfile_in(parent)
            .context(ArtifactIoSnafu {
                reason: "artifact_staging_failed",
            })?;
        manifest
            .write_all(&bytes)
            .and_then(|()| manifest.as_file().sync_all())
            .context(ArtifactIoSnafu {
                reason: "artifact_write_failed",
            })?;
        self.file.as_file().sync_all().context(ArtifactIoSnafu {
            reason: "artifact_write_failed",
        })?;
        Ok(PendingArtifactPublication {
            result: self.file,
            manifest,
            result_path: self.result_path,
            manifest_path: self.manifest_path,
            receipt: self.receipt,
        })
    }
}

impl Write for StagedResult {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // A large batch can serialize without yielding to Abortable's next poll.
        if self.cancellation.is_aborted() {
            return Err(io::Error::other("artifact write cancelled"));
        }
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl PendingArtifactPublication {
    /// Call after command cleanup. Cancellation between publications leaves an uncommitted result.
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
        if cancellation.is_aborted() {
            return Err(Error::Cancelled { status: 1 });
        }
        self.manifest
            .persist_noclobber(&self.manifest_path)
            .map_err(|error| Error::ArtifactIo {
                source: error.error,
                reason: "artifact_manifest_publish_failed",
            })?;
        Ok(self.receipt)
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn map_file_write_error(error: Error) -> Error {
    match error {
        Error::Output { source } => Error::ArtifactIo {
            source,
            reason: "artifact_write_failed",
        },
        error => error,
    }
}

fn encode_bounded_json(
    value: &Value,
    limit: usize,
    reason: &'static str,
) -> Result<Vec<u8>, Error> {
    let mut output = super::json::BoundedJsonBuffer {
        bytes: Vec::new(),
        max_bytes: limit as u64,
    };
    serde_json::to_writer(&mut output, value).map_err(|source| {
        if source.is_io() {
            Error::OutputLimit { reason }
        } else {
            Error::JsonOutput { source }
        }
    })?;
    output
        .write_all(b"\n")
        .map_err(|_| Error::OutputLimit { reason })?;
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use futures_util::stream;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn stage_fixture(directory: &Path) -> Result<PendingArtifactPublication, Error> {
        let mut staged = StagedResult::new(
            &directory.join("rows"),
            RowOutputFormat::Arrow,
            json!({}),
            AbortHandle::new_pair().0,
        )?;
        staged
            .write_all(b"result")
            .map_err(|source| Error::ArtifactIo {
                source,
                reason: "artifact_write_failed",
            })?;
        staged.stage_manifest(&crate::build_completion_record(
            "scan",
            Some(42),
            OutputStats {
                rows_written: 2,
                batches_written: 1,
                bytes_written: 6,
            },
            None,
        ))
    }

    #[test]
    fn publication_never_replaces_existing_or_racing_destinations() -> TestResult {
        for name in ["rows", "rows.manifest.json"] {
            let directory = tempfile::tempdir()?;
            let existing = directory.path().join(name);
            fs::write(&existing, b"existing data")?;
            assert!(
                StagedResult::new(
                    &directory.path().join("rows"),
                    RowOutputFormat::Arrow,
                    json!({}),
                    AbortHandle::new_pair().0,
                )
                .is_err()
            );
            assert_eq!(fs::read(&existing)?, b"existing data");
            fs::remove_file(&existing)?;
            let complete = stage_fixture(directory.path())?;
            fs::write(&existing, b"racing writer")?;
            let (cancel, _) = AbortHandle::new_pair();
            let error = complete
                .publish(&cancel)
                .err()
                .ok_or("expected publication failure")?;
            assert!(
                matches!(error, Error::ArtifactIo { source, .. } if source.kind() == io::ErrorKind::AlreadyExists)
            );
            assert_eq!(fs::read(&existing)?, b"racing writer");
            if name == "rows" {
                assert!(!directory.path().join("rows.manifest.json").exists());
            } else {
                assert_eq!(fs::read(directory.path().join("rows"))?, b"result");
            }
            assert!(fs::read_dir(directory.path())?.all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".dar-")
            }));
        }
        Ok(())
    }

    #[test]
    fn cancelled_publication_leaves_no_final_or_temporary_files() -> TestResult {
        let directory = tempfile::tempdir()?;
        let complete = stage_fixture(directory.path())?;
        let (cancel, _) = AbortHandle::new_pair();
        cancel.abort();
        assert!(matches!(
            complete.publish(&cancel),
            Err(Error::Cancelled { .. })
        ));
        assert_eq!(fs::read_dir(directory.path())?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn late_stream_errors_discard_both_output_formats() -> TestResult {
        for format in [RowOutputFormat::Arrow, RowOutputFormat::Json] {
            let directory = tempfile::tempdir()?;
            let staged = StagedResult::new(
                &directory.path().join("rows"),
                format,
                json!({}),
                AbortHandle::new_pair().0,
            )?;
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef,
            )])?;
            let result = staged
                .write_rows(
                    batch.schema(),
                    stream::iter([Ok(batch), Err(Error::Cancelled { status: 130 })]),
                    vec![json!({"alias": "t", "table_version": "0"})],
                    None,
                )
                .await;
            assert!(matches!(result, Err(Error::Cancelled { status: 130 })));
            assert_eq!(fs::read_dir(directory.path())?.count(), 0);
        }
        Ok(())
    }

    #[test]
    fn artifact_json_limits_include_escaping_utf8_and_newline() -> TestResult {
        for (limit, reason) in [
            (MAX_RECEIPT_BYTES, "artifact_receipt_too_large"),
            (MAX_MANIFEST_BYTES, "artifact_manifest_too_large"),
        ] {
            let value = json!("x".repeat(limit - 3));
            assert_eq!(encode_bounded_json(&value, limit, reason)?.len(), limit);
            assert!(matches!(
                encode_bounded_json(&value, limit - 1, reason),
                Err(Error::OutputLimit { .. })
            ));
        }
        assert_eq!(encode_bounded_json(&json!("\u{e9}"), 5, "limit")?.len(), 5);
        assert!(encode_bounded_json(&json!("\u{e9}"), 4, "limit").is_err());
        Ok(())
    }
}
