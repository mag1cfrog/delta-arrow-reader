use std::{
    collections::HashSet, fs, io::Cursor, path::Path, process::Output, sync::Mutex, time::Duration,
};

use arrow_ipc::reader::StreamReader;
use hyper::{StatusCode, body::Bytes};
use snafu::{ResultExt, ensure};

use super::{ScanFixture, fixtures::create_repeated_table};
use crate::{
    command::output_on_single_cpu,
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, Result},
    http_fixture::{
        HttpTestServer,
        table::{HttpTable, response},
    },
    process::check_diagnostic,
    scan::ipc::IPC_END_MARKER,
};

impl ScanFixture<'_> {
    pub(super) fn check_later_read_failure(&self) -> Result<()> {
        let directory = self.directory.path();
        let table_path = directory.join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        let table = HttpTable::from_directory(&table_path)?;
        let data_paths = Mutex::new(HashSet::new());
        let server = HttpTestServer::start(move |request| {
            let fail = if request.uri().path().ends_with(".parquet") {
                let mut paths = data_paths.lock().map_err(|_| {
                    InvalidInputSnafu {
                        message: "read failure fixture's request lock was poisoned",
                    }
                    .build()
                })?;
                paths.insert(request.uri().path().to_owned());
                paths.len() > 4
            } else {
                false
            };
            if fail {
                Ok(Some(response(
                    StatusCode::NOT_FOUND,
                    Bytes::from_static(b"secret simulated data-file failure"),
                )))
            } else {
                table.respond(request).map(Some)
            }
        })?;
        let options = directory.join("storage.json");
        fs::write(&options, br#"{"allow_http":"true"}"#).context(FileSnafu { path: &options })?;
        let output = output_on_single_cpu(
            self.command()
                .arg("--storage-options-file")
                .arg(options)
                .arg(format!("{}/", server.origin)),
            Duration::from_secs(15),
        )?;
        let requests = server.take_requests()?;
        let requested_files: HashSet<_> = requests
            .iter()
            .map(|request| request.uri.path())
            .filter(|path| path.ends_with(".parquet"))
            .collect();
        ensure!(
            requested_files.len() > 4,
            InvalidInputSnafu {
                message: format!(
                    "scan never reached the injected read failure: {requested_files:?}"
                )
            }
        );
        check_read_failure_output(&output, &directory.join("stdout.arrow"), 32 * 120)
    }
}

fn check_read_failure_output(output: &Output, path: &Path, total_rows: usize) -> Result<()> {
    ensure!(
        output.status.code() == Some(1),
        InvalidInputSnafu {
            message: format!(
                "expected exit 1 after a data-file read failure, got {}; stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        }
    );
    check_diagnostic(&output.stderr, "data_file_read", "data_file_read")?;
    ensure!(
        !output.stdout.ends_with(&IPC_END_MARKER),
        InvalidInputSnafu {
            message: "failed scan wrote an IPC end marker",
        }
    );
    // EOF at a batch boundary decodes successfully, so the child status must still be checked.
    let mut cursor = Cursor::new(&output.stdout);
    let rows = StreamReader::try_new(&mut cursor, None)
        .context(ArrowSnafu { path })?
        .try_fold(0, |rows, batch| batch.map(|batch| rows + batch.num_rows()))
        .context(ArrowSnafu { path })?;
    ensure!(
        cursor.position() == output.stdout.len() as u64,
        InvalidInputSnafu {
            message: "failed scan has trailing bytes after an IPC end marker",
        }
    );
    ensure!(
        rows > 0 && rows < total_rows,
        InvalidInputSnafu {
            message: format!(
                "expected some rows but fewer than {total_rows} after a read failure, got {rows}"
            )
        }
    );
    let truncated = &output.stdout[..output.stdout.len() - 1];
    let decoded = StreamReader::try_new(truncated, None)
        .and_then(|mut reader| reader.try_for_each(|batch| batch.map(|_| ())));
    ensure!(
        decoded.is_err(),
        InvalidInputSnafu {
            message: "truncating the last IPC batch must fail decoding",
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
