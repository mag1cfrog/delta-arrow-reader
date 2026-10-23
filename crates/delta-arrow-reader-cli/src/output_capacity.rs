//! Whether an output can accept a small completion marker without waiting for its reader.
//!
//! A full pipe delays completion until the consumer drains it. Checking capacity
//! lets the command handle cancellation during that wait without queuing a success
//! record. Each pipe must have one writer; terminal writes within the process
//! are serialized by the diagnostics module.
//! The caller limits each write to 512 bytes, the POSIX minimum atomic pipe write.

use std::io;

#[cfg(unix)]
pub(crate) fn can_write(output: &impl std::os::fd::AsRawFd, _bytes: usize) -> io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: output.as_raw_fd(),
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
pub(crate) fn can_write(
    output: &impl std::os::windows::io::AsRawHandle,
    bytes: usize,
) -> io::Result<bool> {
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

    let handle = output.as_raw_handle();
    // SAFETY: output owns this live handle; the query does not change or close it.
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
