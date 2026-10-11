use std::{
    fs,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use arrow_ipc::reader::StreamReader;
use serde_json::json;
use snafu::{ResultExt, ensure};
use tokio::sync::watch;

use super::{
    ScanFixture,
    child::{PipeReaderWithDeadline, ScanProcess},
    fixtures::create_repeated_table,
    http::DataRequests,
};
use crate::{
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    http_fixture::{HttpTestServer, table::HttpTable},
    scan::ipc::IpcTable,
};

impl ScanFixture<'_> {
    pub(super) fn check_partition_read_limits(&self) -> Result<()> {
        let table_path = self.directory.path().join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 4)?;
        let options_path = self.directory.path().join("execution-options.json");
        let cases = [
            ("default target", None, 8, 1, 0, 1),
            ("two partitions", Some(2), 8, 1, 0, 2),
            ("more partitions than files", Some(usize::MAX), 8, 1, 0, 4),
            ("scan capacity", Some(2), 1, 3, 2, 1),
            ("partition capacity", Some(1), 8, 1, 2, 1),
        ];
        let mut expected: Option<IpcTable> = None;
        for backend in ["direct", "delta_kernel"] {
            for (name, target, scan_capacity, partition_capacity, prefetch, held_files) in cases {
                eprintln!("  backend={backend}, case={name}");
                let options = json!({
                    "parquet_backend": backend,
                    "max_concurrent_file_reads_per_scan": scan_capacity,
                    "max_concurrent_file_reads_per_partition": partition_capacity,
                    "output_buffer_batches_per_partition": 1,
                    "prefetch_files_per_partition": prefetch,
                });
                fs::write(&options_path, options.to_string()).context(FileSnafu {
                    path: &options_path,
                })?;
                let requests = Arc::new(DataRequests::default());
                let recorded = Arc::clone(&requests);
                let table = Arc::new(HttpTable::from_directory(&table_path)?);
                let (release, released) = watch::channel(false);
                let server = HttpTestServer::start_async(move |request| {
                    let recorded = Arc::clone(&recorded);
                    let table = Arc::clone(&table);
                    let mut released = released.clone();
                    async move {
                        recorded.record(request.uri().path())?;
                        if request.uri().path().ends_with(".parquet") {
                            released
                                .wait_for(|released| *released)
                                .await
                                .map_err(|error| {
                                    InvalidInputSnafu {
                                        message: format!(
                                            "data request release channel closed: {error}"
                                        ),
                                    }
                                    .build()
                                })?;
                        }
                        table.respond(&request).map(Some)
                    }
                })?;
                let mut command = self.http_command(&server.origin, r#"{"allow_http":"true"}"#)?;
                command.arg("--execution-options-file").arg(&options_path);
                if let Some(target) = target {
                    command.args(["--target-partitions", &target.to_string()]);
                }
                let mut scan = ScanProcess::start(command)?;
                let deadline = Instant::now() + Duration::from_secs(5);
                while requests.count()? < held_files {
                    server.take_requests()?;
                    ensure!(
                        Instant::now() < deadline,
                        InvalidInputSnafu {
                            message: format!(
                                "expected {held_files} held files, got {}",
                                requests.count()?
                            ),
                        }
                    );
                    thread::sleep(Duration::from_millis(10));
                }
                thread::sleep(Duration::from_millis(250));
                ensure!(
                    requests.count()? == held_files,
                    InvalidInputSnafu {
                        message: format!(
                            "expected {held_files} held files, got {}",
                            requests.count()?
                        ),
                    }
                );
                release.send_replace(true);
                let actual = self.check_completed_scan(&mut scan, 4 * 120)?;
                if let Some(expected) = &expected {
                    ensure!(
                        actual.schema == expected.schema,
                        InvalidInputSnafu {
                            message: "scan scheduling changed schema metadata",
                        }
                    );
                    actual.check_matches(expected, None)?;
                } else {
                    expected = Some(actual);
                }
                server.take_requests()?;
            }
        }
        Ok(())
    }

    pub(super) fn check_narrow_read_limits(&self) -> Result<()> {
        let table_path = self.directory.path().join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        for backend in ["direct", "delta_kernel"] {
            eprintln!("  backend={backend}");
            let options = json!({
                "parquet_backend": backend, "max_concurrent_file_reads_per_scan": 1,
                "max_concurrent_file_reads_per_partition": 1, "output_buffer_batches_per_partition": 1,
                "prefetch_files_per_partition": 0,
            });
            let path = self.directory.path().join("execution-options.json");
            fs::write(&path, options.to_string()).context(FileSnafu { path: &path })?;
            let (server, requests) = tracked_table(&table_path)?;
            let mut command = self.http_command(&server.origin, r#"{"allow_http":"true"}"#)?;
            command
                .arg("--execution-options-file")
                .arg(path)
                .args(["--target-partitions", "1"]);
            let mut scan = ScanProcess::start(command)?;
            scan.wait_for_blocked_output(Duration::from_secs(15))?;
            thread::sleep(Duration::from_millis(250));
            let count = requests.count()?;
            // One file writes stdout, one batch is queued, and one producer waits to send.
            ensure!(
                (1..=3).contains(&count),
                InvalidInputSnafu {
                    message: format!(
                        "expected at most 3 files while stdout is blocked, got {count}"
                    ),
                }
            );
            self.check_completed_scan(&mut scan, 32 * 120)?;
            ensure!(
                requests.count()? == 32,
                InvalidInputSnafu {
                    message: "scan did not request all 32 files"
                }
            );
            server.take_requests()?;
        }
        Ok(())
    }

    pub(super) fn check_incremental_output(&self) -> Result<()> {
        let table_path = self.directory.path().join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        let (server, requests) = tracked_table(&table_path)?;
        let mut scan =
            ScanProcess::start(self.http_command(&server.origin, r#"{"allow_http":"true"}"#)?)?;
        scan.wait_for_blocked_output(Duration::from_secs(15))?;
        let count = requests.count()?;
        ensure!(
            (1..32).contains(&count),
            InvalidInputSnafu {
                message: format!(
                    "expected some unread files while stdout is blocked, got {count} requests"
                ),
            }
        );
        let stdout = scan.stdout.as_mut().expect("a new scan has a stdout pipe");
        let mut reader = StreamReader::try_new(
            PipeReaderWithDeadline {
                pipe: stdout,
                deadline: Instant::now() + Duration::from_secs(15),
            },
            None,
        )
        .context(ArrowSnafu {
            path: "scan stdout",
        })?;
        reader.get_mut().deadline = Instant::now() + Duration::from_secs(15);
        let first = reader
            .next()
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: "scan ended before its first batch",
                }
                .build()
            })?
            .context(ArrowSnafu {
                path: "scan stdout",
            })?;
        ensure!(
            first.num_rows() > 0,
            InvalidInputSnafu {
                message: "scan returned an empty first batch"
            }
        );
        ensure!(
            scan.child
                .try_wait()
                .context(IoSnafu {
                    operation: "inspect the scan process status"
                })?
                .is_none(),
            InvalidInputSnafu {
                message: "scan finished before the first batch was consumed",
            }
        );
        reader.get_mut().deadline = Instant::now() + Duration::from_secs(15);
        let rows = reader
            .try_fold(first.num_rows(), |rows, batch| {
                batch.map(|batch| rows + batch.num_rows())
            })
            .context(ArrowSnafu {
                path: "scan stdout",
            })?;
        let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
        ensure!(
            rows == 32 * 120 && status.success() && stderr.is_empty(),
            InvalidInputSnafu {
                message: format!(
                    "incremental scan failed: rows={rows}, status={status}, stderr={}",
                    String::from_utf8_lossy(&stderr)
                ),
            }
        );
        ensure!(
            requests.count()? == 32,
            InvalidInputSnafu {
                message: "scan did not request all 32 files"
            }
        );
        server.take_requests()?;
        Ok(())
    }
}

fn tracked_table(path: &std::path::Path) -> Result<(HttpTestServer, Arc<DataRequests>)> {
    let table = HttpTable::from_directory(path)?;
    let requests = Arc::new(DataRequests::default());
    let recorded = Arc::clone(&requests);
    let server = HttpTestServer::start(move |request| {
        recorded.record(request.uri().path())?;
        table.respond(request).map(Some)
    })?;
    Ok((server, requests))
}
