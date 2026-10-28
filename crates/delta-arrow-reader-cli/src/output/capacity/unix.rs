use std::io;

pub(crate) fn can_write(output: &impl std::os::fd::AsRawFd, _bytes: usize) -> io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: output.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: poll borrows one initialized descriptor without changing its flags.
    let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
    #[cfg(target_os = "macos")]
    let result = if result >= 0
        && descriptor.revents & libc::POLLNVAL != 0
        && (0..libc::FD_SETSIZE as i32).contains(&descriptor.fd)
    {
        // macOS poll rejects some character devices, including /dev/null; select supports them.
        // SAFETY: the descriptor fits fd_set, and all buffers are initialized and borrowed.
        let selected = unsafe {
            let mut writable = std::mem::zeroed::<libc::fd_set>();
            libc::FD_SET(descriptor.fd, &mut writable);
            let mut timeout = libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            };
            libc::select(
                descriptor.fd + 1,
                std::ptr::null_mut(),
                &mut writable,
                std::ptr::null_mut(),
                &mut timeout,
            )
        };
        descriptor.revents = if selected > 0 { libc::POLLOUT } else { 0 };
        selected
    } else {
        result
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        };
    }
    if descriptor.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    if descriptor.revents & (libc::POLLERR | libc::POLLHUP) != 0 {
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    Ok(descriptor.revents & libc::POLLOUT != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn readiness_checks_preserve_pipe_contents_and_detect_closed_readers() {
        let (mut reader, mut writer) = io::pipe().unwrap();
        assert!(can_write(&writer, 8).unwrap());
        writer.write_all(b"arrow").unwrap();
        can_write(&writer, 8).unwrap();
        let mut bytes = [0; 5];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"arrow");
        drop(reader);
        assert_eq!(
            can_write(&writer, 8).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            can_write(&i32::MAX, 8).unwrap_err().raw_os_error(),
            Some(libc::EBADF)
        );
        assert!(can_write(&tempfile::tempfile().unwrap(), 8).unwrap());
    }

    #[test]
    fn null_devices_and_terminals_accept_completion_markers() {
        use std::os::fd::{FromRawFd, OwnedFd};

        let null = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .unwrap();
        assert!(can_write(&null, 8).unwrap());

        let mut master_fd = -1;
        let mut slave_fd = -1;
        // SAFETY: both descriptors are writable; null options request the default terminal settings.
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        // SAFETY: successful openpty returns two distinct descriptors owned by this test.
        let (_master, slave) = unsafe {
            (
                OwnedFd::from_raw_fd(master_fd),
                OwnedFd::from_raw_fd(slave_fd),
            )
        };
        assert!(can_write(&slave, 8).unwrap());
    }
}
