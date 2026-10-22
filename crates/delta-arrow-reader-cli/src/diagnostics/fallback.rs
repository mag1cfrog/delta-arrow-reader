//! Best-effort failure reporting when the diagnostic thread cannot start.

use std::{
    fs::File,
    io::{self, Write},
    sync::TryLockError,
};

#[cfg(unix)]
use std::os::fd::AsFd;
#[cfg(windows)]
use std::os::windows::io::AsHandle;

use super::{DiagnosticFormat, TERMINAL_RECORD_WRITTEN, log_targets, stderr_capacity, subscriber};

impl DiagnosticFormat {
    pub(super) fn report_failure_without_thread(self, error: &crate::Error) {
        // A cloned handle bypasses Rust's stderr lock, which another worker may be holding.
        #[cfg(unix)]
        let handle = io::stderr().as_fd().try_clone_to_owned();
        #[cfg(windows)]
        let handle = io::stderr().as_handle().try_clone_to_owned();
        let Ok(handle) = handle else { return };
        let mut writer = FallbackStderrWriter(File::from(handle));
        match self {
            Self::Text => {
                let (targets, _) = log_targets();
                tracing::subscriber::with_default(
                    subscriber(targets, std::sync::Mutex::new(writer)),
                    || error.log_failure(),
                );
            }
            Self::Json => {
                let mut written = match TERMINAL_RECORD_WRITTEN.try_lock() {
                    Ok(guard) => guard,
                    Err(TryLockError::Poisoned(error)) => error.into_inner(),
                    Err(TryLockError::WouldBlock) => return,
                };
                if !*written {
                    let line = format!("{}\n", error.to_json_diagnostic());
                    let _ = writer.write_all(line.as_bytes());
                    *written = true;
                }
            }
        }
    }
}

/// Writes only while stderr has capacity, in chunks within POSIX's atomic pipe-write minimum.
struct FallbackStderrWriter(File);

impl Write for FallbackStderrWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let bytes = &bytes[..bytes.len().min(512)];
        if !stderr_capacity::can_write(bytes.len())? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.0.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
