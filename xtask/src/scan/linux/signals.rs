use std::{
    fs, io,
    time::{Duration, Instant},
};

use rustix::process::{Pid, Signal, kill_process};
use snafu::{ResultExt, ensure};

use super::{ScanFixture, child::ScanProcess, fixtures::create_repeated_table};
use crate::{
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    http_fixture::{HttpTestServer, table::HttpTable},
    process::check_no_child_processes,
};

impl ScanFixture<'_> {
    pub(super) fn check_signals_during_held_data_requests(&self) -> Result<()> {
        let directory = self.directory.path();
        let table_path = directory.join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        let options = directory.join("storage.json");
        fs::write(&options, br#"{"allow_http":"true"}"#).context(FileSnafu { path: &options })?;
        for signal in [Signal::INT, Signal::TERM] {
            eprintln!("  signal={signal:?}");
            let table = HttpTable::from_directory(&table_path)?;
            let server = HttpTestServer::start(move |request| {
                if request.uri().path().ends_with(".parquet") {
                    Ok(None)
                } else {
                    table.respond(request).map(Some)
                }
            })?;
            let mut command = self.command();
            command
                .arg("--storage-options-file")
                .arg(&options)
                .arg(format!("{}/", server.origin));
            let mut scan = ScanProcess::start(command)?;
            let deadline = Instant::now() + Duration::from_secs(10);
            let request = loop {
                let request =
                    server.wait_for_request(deadline.saturating_duration_since(Instant::now()))?;
                if request.uri.path().ends_with(".parquet") {
                    break request;
                }
            };
            let schema = scan.read_schema(Duration::from_secs(15))?;
            let names: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            ensure!(
                names == ["id", "value", "label", "region"],
                InvalidInputSnafu {
                    message: format!("unexpected schema while data requests are held: {schema:?}"),
                }
            );
            check_no_child_processes(&scan.child)?;
            kill_process(Pid::from_child(&scan.child), signal)
                .map_err(io::Error::from)
                .context(IoSnafu {
                    operation: "signal the CLI during held data requests",
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
            server.wait_for_disconnect(request.connection_id, Duration::from_secs(5))?;
        }
        Ok(())
    }
}
