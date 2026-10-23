//! Databricks CLI requests with bounded output, deadlines, and child cleanup.

use std::{io, process::Stdio, sync::LazyLock, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::watch,
};

use super::Error;

pub(crate) const MAX_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;
pub(crate) const DATABRICKS_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

// One CLI command owns every initial and renewed grant request in this process.
static REQUEST_CANCELLATION_SIGNAL: LazyLock<watch::Sender<bool>> =
    LazyLock::new(|| watch::channel(false).0);

/// Requests cooperative cancellation of initial and renewed grants before the runtime exits.
/// Returns whether a runner still owns a child or is about to spawn one.
pub(crate) fn cancel_pending_requests() -> bool {
    REQUEST_CANCELLATION_SIGNAL.send_replace(true);
    REQUEST_CANCELLATION_SIGNAL.receiver_count() > 0
}

/// Fetches metadata including the table-kind manifest used to check direct-read support.
pub(super) async fn get_table_metadata(
    profile: &str,
    full_table_name: &str,
) -> Result<Vec<u8>, Error> {
    let profile_arg = format!("--profile={profile}");
    run_databricks_cli(&[
        "tables",
        "get",
        "--include-manifest-capabilities",
        &profile_arg,
        "--output",
        "json",
        "--",
        full_table_name,
    ])
    .await
    .map_err(|error| match error {
        Error::DatabricksCommandFailed => Error::TableMetadataRequestFailed,
        error => error,
    })
}

/// Requests READ credentials scoped to the table ID returned by the metadata request.
pub(super) async fn generate_read_credentials(
    profile: &str,
    table_id: &str,
) -> Result<Vec<u8>, Error> {
    let profile_arg = format!("--profile={profile}");
    run_databricks_cli(&[
        "temporary-table-credentials",
        "generate-temporary-table-credentials",
        "--table-id",
        table_id,
        "--operation",
        "READ",
        &profile_arg,
        "--output",
        "json",
    ])
    .await
    .map_err(|error| match error {
        Error::DatabricksCommandFailed => Error::ReadCredentialsRequestFailed,
        error => error,
    })
}

/// Caps each captured stream; the extra byte distinguishes an oversized response from EOF.
async fn read_bounded_output(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_COMMAND_OUTPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| Error::DatabricksCommandFailed)?;
    if bytes.len() as u64 > MAX_COMMAND_OUTPUT_BYTES {
        return Err(Error::InvalidDatabricksResponse);
    }
    Ok(bytes)
}

async fn run_databricks_cli(args: &[&str]) -> Result<Vec<u8>, Error> {
    run_command_with_timeout(
        Command::new("databricks").args(args),
        DATABRICKS_COMMAND_TIMEOUT,
    )
    .await
}

/// Captures stdout, drains stderr, and stops the child on timeout or read failure.
///
/// stderr can contain secrets, so callers receive only a redacted error on failure.
async fn run_command_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> Result<Vec<u8>, Error> {
    let mut cancellation = REQUEST_CANCELLATION_SIGNAL.subscribe();
    if *cancellation.borrow() {
        return Err(Error::Cancelled { status: 1 });
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
                Error::DatabricksCommandFailed
            }
        })?;
    let stdout = child.stdout.take().ok_or(Error::DatabricksCommandFailed)?;
    let stderr = child.stderr.take().ok_or(Error::DatabricksCommandFailed)?;
    let read_response = tokio::time::timeout(timeout, async {
        // Drain both pipes while waiting so a full stderr pipe cannot block the child.
        let (stdout, _, status) = tokio::try_join!(
            read_bounded_output(stdout),
            read_bounded_output(stderr),
            async {
                child
                    .wait()
                    .await
                    .map_err(|_| Error::DatabricksCommandFailed)
            }
        )?;
        if !status.success() {
            return Err(Error::DatabricksCommandFailed);
        }
        Ok(stdout)
    });
    let result = tokio::select! {
        biased;
        _ = cancellation.wait_for(|cancelled| *cancelled) => Err(Error::Cancelled { status: 1 }),
        result = read_response => result.unwrap_or(Err(Error::DatabricksCommandTimedOut)),
    };
    if result.is_err() {
        let _ = child.kill().await;
    }
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subprocess_output_is_bounded_and_timeout_reaps_child() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pid");
        let result = run_command_with_timeout(
            Command::new("/bin/sh")
                .args(["-c", "printf '%s' $$ > \"$1\"; exec /bin/sleep 60", "sh"])
                .arg(&pid_file),
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(result, Err(Error::DatabricksCommandTimedOut));
        let pid = std::fs::read_to_string(pid_file).unwrap();
        let status = std::process::Command::new("/bin/kill")
            .args(["-0", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "timed-out child was not reaped");
        let bytes = vec![b'x'; MAX_COMMAND_OUTPUT_BYTES as usize + 1];
        assert_eq!(
            read_bounded_output(bytes.as_slice()).await,
            Err(Error::InvalidDatabricksResponse)
        );
        assert_eq!(
            run_command_with_timeout(
                Command::new("/bin/sh").args(["-c", "printf secret-sentinel >&2; exit 1"]),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::DatabricksCommandFailed)
        );
    }
}
