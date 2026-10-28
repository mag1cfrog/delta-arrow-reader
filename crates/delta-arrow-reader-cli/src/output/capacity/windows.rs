use std::io;

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
