//! Detect a closed stdout pipe while a command is waiting for credentials or storage.

use std::io;

/// Leaves stdout's blocking mode and contents unchanged; the Arrow writer owns all writes.
pub(super) async fn wait_for_disconnect() -> io::Error {
    #[cfg(unix)]
    {
        use std::time::Duration;

        let stdout = io::stdout();
        loop {
            // A full pipe is still connected; only errors stop the command here.
            if let Err(error) = crate::output::capacity::can_write(&stdout, 1) {
                return error;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    #[cfg(not(unix))]
    {
        // shortcut: non-Unix detects disconnects on writes; add native monitoring for prompt cancellation there.
        std::future::pending().await
    }
}
