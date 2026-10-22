//! Whether stderr can accept a small terminal record without waiting for its reader.
//!
//! A full pipe delays completion until the consumer drains it. Checking capacity
//! lets the command handle cancellation during that wait without queuing a success
//! record. A stderr pipe must have one command as its writer; terminal writes
//! within that process are serialized by the diagnostics module.
//! The caller limits records to 512 bytes, the POSIX minimum atomic pipe write.

use std::io;

#[cfg(unix)]
pub(super) fn can_write(_bytes: usize) -> io::Result<bool> {
    use std::os::fd::AsRawFd;

    let mut descriptor = libc::pollfd {
        fd: io::stderr().as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: poll borrows one initialized descriptor without changing its flags.
    if unsafe { libc::poll(&mut descriptor, 1, 0) } < 0 {
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

#[cfg(windows)]
pub(super) fn can_write(bytes: usize) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_PIPE_CONNECTED_STATE, FILE_PIPE_LOCAL_INFORMATION, FilePipeLocalInformation,
            NtQueryInformationFile,
        },
        Win32::{
            Foundation::RtlNtStatusToDosError,
            Storage::FileSystem::{FILE_TYPE_PIPE, GetFileType},
            System::IO::IO_STATUS_BLOCK,
        },
    };

    let handle = io::stderr().as_raw_handle();
    // SAFETY: stderr owns this live handle; the query does not change or close it.
    if unsafe { GetFileType(handle) } != FILE_TYPE_PIPE {
        return Ok(true);
    }
    let mut information = FILE_PIPE_LOCAL_INFORMATION::default();
    let mut status = IO_STATUS_BLOCK::default();
    // SAFETY: both output buffers are initialized and have the declared sizes.
    let result = unsafe {
        NtQueryInformationFile(
            handle,
            &mut status,
            (&mut information as *mut FILE_PIPE_LOCAL_INFORMATION).cast(),
            size_of::<FILE_PIPE_LOCAL_INFORMATION>() as u32,
            FilePipeLocalInformation,
        )
    };
    if result < 0 {
        // SAFETY: this function only translates the returned status code.
        return Err(io::Error::from_raw_os_error(unsafe {
            RtlNtStatusToDosError(result) as i32
        }));
    }
    if information.NamedPipeState != FILE_PIPE_CONNECTED_STATE {
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    Ok(information.WriteQuotaAvailable as usize >= bytes)
}
