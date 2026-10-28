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
