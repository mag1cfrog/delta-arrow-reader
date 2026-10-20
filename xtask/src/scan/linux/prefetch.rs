use std::{fs, time::Duration};

use snafu::{ResultExt, ensure};

use super::{ScanFixture, child::ScanProcess};
use crate::{
    error::{FileSnafu, InvalidInputSnafu, Result},
    process::{check_diagnostic, copy_directory},
    scan::ipc::IPC_END_MARKER,
};

impl ScanFixture<'_> {
    pub(super) fn check_blocking_prefetch_exit(&self) -> Result<()> {
        let table = self.directory.path().join("prefetch-table");
        copy_directory(&self.corpus.join("partitioned/table"), &table)?;
        let mut files = Vec::new();
        let mut pending = vec![table.clone()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).context(FileSnafu { path: &directory })? {
                let path = entry.context(FileSnafu { path: &directory })?.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
                {
                    files.push((
                        fs::metadata(&path)
                            .context(FileSnafu { path: &path })?
                            .len(),
                        path,
                    ));
                }
            }
        }
        files.sort_by_key(|entry| std::cmp::Reverse(entry.0));
        ensure!(
            files.len() == 3,
            InvalidInputSnafu {
                message: "prefetch fixture must contain three data files"
            }
        );
        // The core orders files by size; hold the second open while the first waits for its marker.
        let first = &files[0].1;
        let blocked = &files[1].1;
        let original = fs::read(first).context(FileSnafu { path: first })?;
        for mode in ["limit", "reader_error", "output_error"] {
            eprintln!("  mode={mode}");
            let marker = self.directory.path().join(format!("open-started-{mode}"));
            fs::write(
                first,
                if mode == "reader_error" {
                    vec![b'x'; original.len()]
                } else {
                    original.clone()
                },
            )
            .context(FileSnafu { path: first })?;
            let mut command = self.command();
            command
                .arg(&table)
                .env("LD_PRELOAD", self.fault_library)
                .env("DAR_TEST_BLOCKED_OPEN", blocked)
                .env("DAR_TEST_WAITING_OPEN", first)
                .env("DAR_TEST_OPEN_STARTED", &marker);
            if mode == "limit" {
                command.args(["--limit", "1"]);
            }
            let mut scan = ScanProcess::start(command)?;
            match mode {
                "output_error" => {
                    scan.wait_for_blocked_output(Duration::from_secs(15))?;
                    drop(scan.stdout.take());
                    let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
                    ensure!(
                        status.code() == Some(3),
                        InvalidInputSnafu {
                            message: format!("expected output failure exit 3, got {status}")
                        }
                    );
                    check_diagnostic(&stderr, "execution", "output_write")?;
                }
                "reader_error" => {
                    let output = scan.wait_with_output(Duration::from_secs(15))?;
                    ensure!(
                        output.status.code() == Some(1),
                        InvalidInputSnafu {
                            message: format!("expected read failure exit 1, got {}", output.status)
                        }
                    );
                    check_diagnostic(&output.stderr, "data_file_read", "data_file_read")?;
                    ensure!(
                        !output.stdout.ends_with(&IPC_END_MARKER),
                        InvalidInputSnafu {
                            message: "failed scan wrote an IPC end marker"
                        }
                    );
                }
                _ => {
                    self.check_completed_scan(&mut scan, 1)?;
                }
            }
            ensure!(
                marker.try_exists().context(FileSnafu { path: &marker })?,
                InvalidInputSnafu {
                    message: "unused blocking prefetch never started"
                }
            );
        }
        Ok(())
    }
}
