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
            FILE_PIPE_CLIENT_END, FILE_PIPE_COMPLETE_OPERATION, FILE_PIPE_CONNECTED_STATE,
            FILE_PIPE_INFORMATION, FILE_PIPE_LOCAL_INFORMATION, FILE_PIPE_QUEUE_OPERATION,
            FilePipeInformation, FilePipeLocalInformation, NtQueryInformationFile,
            NtSetInformationFile,
        },
        Win32::{
            Foundation::STATUS_PIPE_BUSY,
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
    check_nt_status(result)?;
    if information.NamedPipeState != FILE_PIPE_CONNECTED_STATE {
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    if information.WriteQuotaAvailable as usize >= bytes {
        return Ok(true);
    }

    // A pending read also consumes quota. A mode change distinguishes it from queued data.
    let mut original_mode = FILE_PIPE_INFORMATION::default();
    // SAFETY: the initialized output buffer has the size required by FilePipeInformation.
    check_nt_status(unsafe {
        NtQueryInformationFile(
            handle,
            &mut status,
            (&mut original_mode as *mut FILE_PIPE_INFORMATION).cast(),
            size_of::<FILE_PIPE_INFORMATION>() as u32,
            FilePipeInformation,
        )
    })?;
    let probe_mode = FILE_PIPE_INFORMATION {
        ReadMode: original_mode.ReadMode,
        CompletionMode: if original_mode.CompletionMode == FILE_PIPE_QUEUE_OPERATION {
            FILE_PIPE_COMPLETE_OPERATION
        } else {
            FILE_PIPE_QUEUE_OPERATION
        },
    };
    // SAFETY: the borrowed live handle has one writer, and the mode buffer has the declared size.
    let result = unsafe {
        NtSetInformationFile(
            handle,
            &mut status,
            (&probe_mode as *const FILE_PIPE_INFORMATION).cast(),
            size_of::<FILE_PIPE_INFORMATION>() as u32,
            FilePipeInformation,
        )
    };
    if result == STATUS_PIPE_BUSY {
        return Ok(false);
    }
    check_nt_status(result)?;
    // SAFETY: restore the exact mode read above before the caller can write to the pipe.
    check_nt_status(unsafe {
        NtSetInformationFile(
            handle,
            &mut status,
            (&original_mode as *const FILE_PIPE_INFORMATION).cast(),
            size_of::<FILE_PIPE_INFORMATION>() as u32,
            FilePipeInformation,
        )
    })?;
    let capacity = if information.NamedPipeEnd == FILE_PIPE_CLIENT_END {
        information.InboundQuota
    } else {
        information.OutboundQuota
    };
    Ok(capacity as usize >= bytes)
}

#[cfg(windows)]
fn check_nt_status(status: windows_sys::Win32::Foundation::NTSTATUS) -> io::Result<()> {
    if status < 0 {
        // SAFETY: this function only translates the returned status code.
        Err(io::Error::from_raw_os_error(unsafe {
            windows_sys::Win32::Foundation::RtlNtStatusToDosError(status) as i32
        }))
    } else {
        Ok(())
    }
}

#[cfg(all(test, windows))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        os::windows::io::AsRawHandle,
        thread,
        time::{Duration, Instant},
    };
    use windows_sys::{
        Wdk::Storage::FileSystem::{
            FILE_PIPE_COMPLETE_OPERATION, FILE_PIPE_INFORMATION, FILE_PIPE_LOCAL_INFORMATION,
            FILE_PIPE_QUEUE_OPERATION, FilePipeInformation, FilePipeLocalInformation,
            NtQueryInformationFile, NtSetInformationFile,
        },
        Win32::System::IO::IO_STATUS_BLOCK,
    };

    fn read_pipe_information(
        pipe: &impl AsRawHandle,
    ) -> (FILE_PIPE_LOCAL_INFORMATION, FILE_PIPE_INFORMATION) {
        let mut local = FILE_PIPE_LOCAL_INFORMATION::default();
        let mut mode = FILE_PIPE_INFORMATION::default();
        let mut status = IO_STATUS_BLOCK::default();
        // SAFETY: the pipe is live and each initialized buffer matches its information class.
        unsafe {
            check_nt_status(NtQueryInformationFile(
                pipe.as_raw_handle(),
                &mut status,
                (&mut local as *mut FILE_PIPE_LOCAL_INFORMATION).cast(),
                size_of::<FILE_PIPE_LOCAL_INFORMATION>() as u32,
                FilePipeLocalInformation,
            ))
            .unwrap();
            check_nt_status(NtQueryInformationFile(
                pipe.as_raw_handle(),
                &mut status,
                (&mut mode as *mut FILE_PIPE_INFORMATION).cast(),
                size_of::<FILE_PIPE_INFORMATION>() as u32,
                FilePipeInformation,
            ))
            .unwrap();
        }
        (local, mode)
    }

    #[test]
    fn pending_large_reads_allow_a_marker_without_changing_pipe_mode() {
        for completion_mode in [FILE_PIPE_QUEUE_OPERATION, FILE_PIPE_COMPLETE_OPERATION] {
            for remaining_quota in [0, 1] {
                let (mut reader, mut writer) = io::pipe().unwrap();
                let (local, mut mode) = read_pipe_information(&writer);
                let capacity = local.WriteQuotaAvailable as usize;
                assert!(capacity > 8);
                mode.CompletionMode = completion_mode;
                let mut status = IO_STATUS_BLOCK::default();
                // SAFETY: this new pipe has no other writer and the mode buffer matches its class.
                check_nt_status(unsafe {
                    NtSetInformationFile(
                        writer.as_raw_handle(),
                        &mut status,
                        (&mode as *const FILE_PIPE_INFORMATION).cast(),
                        size_of::<FILE_PIPE_INFORMATION>() as u32,
                        FilePipeInformation,
                    )
                })
                .unwrap();
                let read_size = if remaining_quota == 0 {
                    capacity * 2
                } else {
                    capacity - 1
                };
                let reading = thread::spawn(move || {
                    let mut buffer = vec![0; read_size];
                    let count = reader.read(&mut buffer).unwrap();
                    buffer.truncate(count);
                    buffer
                });
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let (local, _) = read_pipe_information(&writer);
                    if local.WriteQuotaAvailable < 8 {
                        assert_eq!(local.WriteQuotaAvailable, remaining_quota);
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "reader did not enter a pending read"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                assert!(can_write(&writer, 8).unwrap());
                let (_, after) = read_pipe_information(&writer);
                assert_eq!(after.ReadMode, mode.ReadMode);
                assert_eq!(after.CompletionMode, mode.CompletionMode);
                writer.write_all(b"complete").unwrap();
                drop(writer);
                assert_eq!(reading.join().unwrap(), b"complete");
            }
        }
    }

    #[test]
    fn full_and_closed_pipes_do_not_accept_a_completion_marker() {
        let (reader, mut writer) = io::pipe().unwrap();
        let (local, mode) = read_pipe_information(&writer);
        writer
            .write_all(&vec![0; local.WriteQuotaAvailable as usize])
            .unwrap();
        assert!(!can_write(&writer, 8).unwrap());
        let (_, after) = read_pipe_information(&writer);
        assert_eq!(after.ReadMode, mode.ReadMode);
        assert_eq!(after.CompletionMode, mode.CompletionMode);
        drop(reader);
        assert_eq!(
            can_write(&writer, 8).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(can_write(&tempfile::tempfile().unwrap(), 8).unwrap());
    }
}
