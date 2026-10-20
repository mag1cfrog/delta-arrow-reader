use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Arc};

use arrow_array::{Int32Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::scan::ipc::IpcTable;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn failed_output(rows: usize) -> TestResult<Output> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let mut writer = StreamWriter::try_new(Vec::new(), &schema)?;
    if rows > 0 {
        writer.write(&RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1; rows]))],
        )?)?;
    }
    writer.finish()?;
    let mut stdout = writer.into_inner()?;
    assert!(stdout.ends_with(&IPC_END_MARKER));
    stdout.truncate(stdout.len() - IPC_END_MARKER.len());
    Ok(Output {
        status: ExitStatus::from_raw(1 << 8),
        stdout,
        stderr: b"ERROR delta_arrow_reader: event=\"scan_execution.failed\" outcome=\"failed\" operation_id=3 error_phase=\"data_file_read\" error_code=\"data_file_read\" error_reason=\"read_failed\"\n".to_vec(),
    })
}

#[test]
fn decodable_partial_batches_still_require_a_failed_exit() -> TestResult {
    let path = Path::new("partial scan");
    let mut output = failed_output(2)?;
    check_read_failure_output(&output, path, 4)?;
    assert!(IpcTable::read_stream(&output.stdout, path).is_err());
    for status in [0, 2 << 8, 3 << 8, 9] {
        output.status = ExitStatus::from_raw(status);
        assert!(check_read_failure_output(&output, path, 4).is_err());
    }
    Ok(())
}

#[test]
fn read_failure_requires_complete_batches_without_a_stream_end_marker() -> TestResult {
    let path = Path::new("invalid partial scan");
    let mut output = failed_output(2)?;
    let partial = output.stdout.clone();
    for stdout in [
        Vec::new(),
        failed_output(0)?.stdout,
        failed_output(4)?.stdout,
        partial[..partial.len() - 1].to_vec(),
        [partial.as_slice(), &IPC_END_MARKER].concat(),
        [partial.as_slice(), &IPC_END_MARKER, b"trailing bytes"].concat(),
    ] {
        output.stdout = stdout;
        assert!(check_read_failure_output(&output, path, 4).is_err());
    }
    Ok(())
}
