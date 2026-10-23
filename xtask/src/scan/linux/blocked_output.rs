use std::{fs, io, time::Duration};

use rustix::process::{Pid, Signal, kill_process};
use snafu::{ResultExt, ensure};

use super::{ScanFixture, child::ScanProcess, fixtures::create_repeated_table};
use crate::{
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    http_fixture::{HttpTestServer, table::HttpTable},
    process::{check_diagnostic, check_no_child_processes},
};

impl ScanFixture<'_> {
    pub(super) fn check_closed_output_pipe(&self) -> Result<()> {
        let (server, mut scan) = self.start_blocked_scan()?;
        drop(scan.stdout.take());
        let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
        ensure!(
            status.code() == Some(3),
            InvalidInputSnafu {
                message: format!("expected exit 3 after closing stdout, got {status}"),
            }
        );
        check_diagnostic(&stderr, "execution", "output_write")?;
        server.take_requests()?;
        Ok(())
    }

    pub(super) fn check_signals_during_blocked_output(&self) -> Result<()> {
        for signal in [Signal::INT, Signal::TERM] {
            eprintln!("  signal={signal:?}");
            let (server, mut scan) = self.start_blocked_scan()?;
            kill_process(Pid::from_child(&scan.child), signal)
                .map_err(io::Error::from)
                .context(IoSnafu {
                    operation: "signal the CLI while stdout is blocked",
                })?;
            let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
            ensure!(
                status.code() == Some(128 + signal.as_raw()) && stderr.is_empty(),
                InvalidInputSnafu {
                    message: format!(
                        "expected exit {} after {signal:?} without stderr, got {status}; stderr={}",
                        128 + signal.as_raw(),
                        String::from_utf8_lossy(&stderr)
                    ),
                }
            );
            server.take_requests()?;
        }
        Ok(())
    }

    fn start_blocked_scan(&self) -> Result<(HttpTestServer, ScanProcess)> {
        let directory = self.directory.path();
        let table_path = directory.join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        let table = HttpTable::from_directory(&table_path)?;
        let server = HttpTestServer::start(move |request| table.respond(request).map(Some))?;
        let options = directory.join("storage.json");
        fs::write(&options, br#"{"allow_http":"true"}"#).context(FileSnafu { path: &options })?;
        let mut command = self.command();
        command
            .arg("--storage-options-file")
            .arg(options)
            .arg(format!("{}/", server.origin));
        let mut scan = ScanProcess::start(command)?;
        scan.wait_for_blocked_output(Duration::from_secs(15))?;
        check_no_child_processes(&scan.child)?;
        Ok((server, scan))
    }
}
