//! Detect a closed stdout pipe while a UC scan is waiting for credentials or storage.

use std::io;

/// Leaves stdout's blocking mode and contents unchanged; the Arrow writer owns all writes.
pub(super) async fn wait_for_disconnect() -> io::Error {
    #[cfg(unix)]
    {
        use std::{os::fd::AsRawFd, time::Duration};

        let stdout = io::stdout();
        loop {
            if let Err(error) = check_for_disconnect(stdout.as_raw_fd()) {
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

#[cfg(unix)]
fn check_for_disconnect(fd: std::os::fd::RawFd) -> io::Result<()> {
    let mut descriptor = libc::pollfd {
        fd,
        // poll reports errors and hangups even without requesting readiness events.
        events: 0,
        revents: 0,
    };
    // SAFETY: poll borrows one initialized descriptor and never owns or changes the file handle.
    if unsafe { libc::poll(&mut descriptor, 1, 0) } < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(())
        } else {
            Err(error)
        };
    }
    if descriptor.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    if descriptor.revents & (libc::POLLERR | libc::POLLHUP) != 0 {
        return Err(io::Error::from(io::ErrorKind::BrokenPipe));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        os::fd::AsRawFd,
    };

    #[test]
    fn monitoring_preserves_pipe_contents_and_accepts_regular_files() {
        let (mut reader, mut writer) = io::pipe().unwrap();
        check_for_disconnect(writer.as_raw_fd()).unwrap();
        writer.write_all(b"arrow").unwrap();
        check_for_disconnect(writer.as_raw_fd()).unwrap();
        let mut bytes = [0; 5];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"arrow");
        drop(reader);
        assert_eq!(
            check_for_disconnect(writer.as_raw_fd()).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        check_for_disconnect(tempfile::tempfile().unwrap().as_raw_fd()).unwrap();
    }
}
