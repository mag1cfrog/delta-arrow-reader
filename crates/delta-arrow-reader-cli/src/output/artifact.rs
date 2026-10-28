//! Complete scan/query results published without replacing existing files.
//!
//! `--output rows.arrow` publishes one completed file. With `--manifest`, a
//! separate `rows.arrow.manifest.json` records provenance after that publication.
//! Only the explicitly requested pair needs a manifest to establish completion.

mod publication;

pub(crate) use publication::PendingArtifactPublication;

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

use super::{OutputLimits, OutputStats, RowOutputFormat};
use crate::diagnostics::{ArtifactIoSnafu, Error};

pub(crate) const MAX_RECEIPT_BYTES: usize = 8 * 1024;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

/// Owns unpublished output; dropping it removes the temporary result.
pub(crate) struct StagedResult {
    file: NamedTempFile,
    result_path: PathBuf,
    receipt: Vec<u8>,
    format: RowOutputFormat,
    manifest: Option<ResultManifestBuilder>,
    cancellation: AbortHandle,
}

/// Provenance and digest collection exist only for an explicitly requested manifest.
struct ResultManifestBuilder {
    path: PathBuf,
    hasher: Sha256,
    replay: Value,
    inputs: Vec<Value>,
}

impl StagedResult {
    pub(crate) fn new(
        path: &Path,
        format: RowOutputFormat,
        manifest_options: Option<Value>,
        cancellation: AbortHandle,
    ) -> Result<Self, Error> {
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
        let manifest = if let Some(mut options) = manifest_options {
            if let Some(fields) = options.as_object_mut() {
                for value in fields.values_mut() {
                    if value.is_number() {
                        *value = json!(value.to_string());
                    }
                }
            }
            let directory = std::env::current_dir().context(ArtifactIoSnafu {
                reason: "artifact_directory_unavailable",
            })?;
            let directory = directory.to_str().ok_or_else(invalid)?;
            Some(ResultManifestBuilder {
                path: PathBuf::from(format!("{result_text}.manifest.json")),
                hasher: Sha256::new(),
                replay: json!({"working_directory": directory, "options": options}),
                inputs: Vec::new(),
            })
        } else {
            None
        };
        let mut receipt = json!({
            "format_version": 2, "kind": "result_artifact", "result": result_path,
        });
        if let Some(manifest) = &manifest {
            receipt["format_version"] = json!(1);
            receipt["manifest"] = json!(manifest.path);
        }
        let receipt =
            encode_bounded_json(&receipt, MAX_RECEIPT_BYTES, "artifact_receipt_too_large")?;
        for (destination, exists_reason) in
            std::iter::once((&result_path, "artifact_destination_exists")).chain(
                manifest
                    .as_ref()
                    .map(|manifest| (&manifest.path, "artifact_manifest_destination_exists")),
            )
        {
            match fs::symlink_metadata(destination) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Ok(_) => {
                    return Err(Error::ArtifactIo {
                        source: io::ErrorKind::AlreadyExists.into(),
                        reason: exists_reason,
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
        Ok(Self {
            file,
            result_path,
            receipt,
            format,
            manifest,
            cancellation,
        })
    }

    /// Only query, table-declaration, predicate and execution inputs belong here.
    /// Credential files are references in options, never digests or copied contents.
    pub(crate) fn record_replay_input(&mut self, argument: &str, path: &str, bytes: &[u8]) {
        if let Some(manifest) = &mut self.manifest {
            manifest.inputs.push(json!({
                "argument": argument, "path": path,
                "sha256": format!("{:x}", Sha256::digest(bytes)),
            }));
        }
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
}

impl Write for StagedResult {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // A large batch can serialize without yielding to Abortable's next poll.
        if self.cancellation.is_aborted() {
            return Err(io::Error::other("artifact write cancelled"));
        }
        let written = self.file.write(bytes)?;
        if let Some(manifest) = &mut self.manifest {
            manifest.hasher.update(&bytes[..written]);
        }
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
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
mod tests;
