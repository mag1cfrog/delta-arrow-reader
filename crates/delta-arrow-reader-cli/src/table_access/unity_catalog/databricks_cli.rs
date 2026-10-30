//! Databricks CLI requests with bounded output, deadlines, and child cleanup.

mod error_details;

use std::{
    io,
    process::{Output, Stdio},
    sync::LazyLock,
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::watch,
};

use super::Error;
pub(super) use error_details::DatabricksErrorDetails;

pub(crate) const MAX_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;
pub(crate) const DATABRICKS_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

// One CLI command owns every initial and renewed grant request in this process.
static REQUEST_CANCELLATION_SIGNAL: LazyLock<watch::Sender<bool>> =
    LazyLock::new(|| watch::channel(false).0);

/// Requests cooperative cancellation of initial and renewed grants before the runtime exits.
/// Waits until runners have reaped their children and released their subscriptions.
pub(crate) async fn cancel_pending_requests() {
    REQUEST_CANCELLATION_SIGNAL.send_replace(true);
    REQUEST_CANCELLATION_SIGNAL.closed().await;
}

/// Fetches metadata including the table-kind manifest used to check direct-read support.
pub(super) async fn get_table_metadata(
    profile: &str,
    full_table_name: &str,
) -> Result<Vec<u8>, Error> {
    let profile_arg = format!("--profile={profile}");
    run_databricks_cli(
        &[
            "tables",
            "get",
            "--include-manifest-capabilities",
            &profile_arg,
            "--output",
            "json",
            "--",
            full_table_name,
        ],
        profile,
        |details| Error::TableMetadataRequestFailed { details },
    )
    .await
}

/// Fetches a catalog API response using the same profile and subprocess limits as table lookup.
pub(super) async fn get_catalog_api_response(profile: &str, path: &str) -> Result<Vec<u8>, Error> {
    run_databricks_cli(
        &[
            "api",
            "get",
            path,
            &format!("--profile={profile}"),
            "--output",
            "json",
        ],
        profile,
        |details| Error::TableMetadataRequestFailed { details },
    )
    .await
}

/// The catalog identity or external storage path used for initial and renewed read grants.
pub(super) enum ReadCredentialTarget {
    TableId(String),
    ExternalPath(url::Url),
}

/// Requests read-only credentials using the target selected from validated table metadata.
pub(super) async fn generate_read_credentials(
    profile: &str,
    target: &ReadCredentialTarget,
) -> Result<Vec<u8>, Error> {
    let profile_arg = format!("--profile={profile}");
    let args = match target {
        ReadCredentialTarget::TableId(table_id) => vec![
            "temporary-table-credentials",
            "generate-temporary-table-credentials",
            "--table-id",
            table_id,
            "--operation",
            "READ",
            &profile_arg,
            "--output",
            "json",
        ],
        ReadCredentialTarget::ExternalPath(path) => vec![
            "temporary-path-credentials",
            "generate-temporary-path-credentials",
            &profile_arg,
            "--output",
            "json",
            "--",
            path.as_str(),
            "PATH_READ",
        ],
    };
    run_databricks_cli(&args, profile, |details| {
        Error::ReadCredentialsRequestFailed { details }
    })
    .await
}

/// Caps each captured stream; the extra byte distinguishes an oversized response from EOF.
async fn read_bounded_output(
    reader: impl AsyncRead + Unpin,
    operation: &'static str,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_COMMAND_OUTPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| Error::DatabricksCommandFailed {
            details: DatabricksErrorDetails::from_io(operation, &source),
        })?;
    if bytes.len() as u64 > MAX_COMMAND_OUTPUT_BYTES {
        return Err(Error::InvalidDatabricksResponse);
    }
    Ok(bytes)
}

async fn run_databricks_cli(
    args: &[&str],
    profile: &str,
    request_failure: fn(DatabricksErrorDetails) -> Error,
) -> Result<Vec<u8>, Error> {
    let mut command = Command::new("databricks");
    command.args(args);
    // Keep the child owner alive after a read future drops, until command cleanup reaps it.
    let output = tokio::spawn(async move {
        run_command_with_timeout(&mut command, DATABRICKS_COMMAND_TIMEOUT).await
    })
    .await
    .map_err(|source| {
        request_failure(DatabricksErrorDetails {
            operation: Some("worker"),
            source_kind: Some(delta_arrow_reader::error_source_kind(&source)),
            ..Default::default()
        })
    })?
    .map_err(|error| match error {
        Error::DatabricksCommandFailed { details } => request_failure(details),
        error => error,
    })?;
    if !output.status.success() {
        return Err(request_failure(DatabricksErrorDetails::from_output(
            &output, profile,
        )));
    }
    Ok(output.stdout)
}

/// Captures both streams and stops the child on timeout or read failure.
/// Only the request wrapper may turn captured stderr into diagnostic details.
async fn run_command_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> Result<Output, Error> {
    let mut cancellation = REQUEST_CANCELLATION_SIGNAL.subscribe();
    if *cancellation.borrow() {
        drop(cancellation);
        return std::future::pending().await;
    }
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Fallback when a caller drops the runner before cooperative cleanup.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::DatabricksCliNotFound
            } else {
                Error::DatabricksCommandFailed {
                    details: DatabricksErrorDetails::from_io("spawn", &error),
                }
            }
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::DatabricksCommandFailed {
            details: Default::default(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::DatabricksCommandFailed {
            details: Default::default(),
        })?;
    let read_response = tokio::time::timeout(timeout, async {
        // Drain both pipes while waiting so a full stderr pipe cannot block the child.
        let (stdout, stderr, status) = tokio::try_join!(
            read_bounded_output(stdout, "read_stdout"),
            read_bounded_output(stderr, "read_stderr"),
            async {
                child
                    .wait()
                    .await
                    .map_err(|source| Error::DatabricksCommandFailed {
                        details: DatabricksErrorDetails::from_io("wait", &source),
                    })
            }
        )?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    });
    let result = tokio::select! {
        biased;
        _ = cancellation.wait_for(|cancelled| *cancelled) => Err(Error::Cancelled { status: 1 }),
        result = read_response => result.unwrap_or(Err(Error::DatabricksCommandTimedOut)),
    };
    if result.is_err() {
        let _ = child.kill().await;
    }
    if matches!(result, Err(Error::Cancelled { .. })) {
        drop(cancellation);
        // The supervisor owns cancellation reporting; do not turn cleanup into a Core read failure.
        return std::future::pending().await;
    }
    result
}

#[cfg(all(test, unix))]
mod tests;
