use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::Duration,
};

use serde_json::json;
use snafu::{ResultExt, ensure};

use super::ipc::IpcTable;
use crate::{
    command::{output_on_single_cpu, output_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, Result},
    process::{check_diagnostic, compile_fault_library},
};

mod blocked_output;
mod child;
mod fixtures;
mod http;
mod https;
mod prefetch;
mod read_failure;
mod scheduling;
mod signals;
mod slow_consumer;

use fixtures::{FLOAT64_VALUES, create_float64_table, create_repeated_table};

struct ScanFixture<'a> {
    binary: &'a Path,
    corpus: PathBuf,
    fault_library: &'a Path,
    directory: tempfile::TempDir,
}

type ScanCheck<'a> = fn(&ScanFixture<'a>) -> Result<()>;

pub(super) fn run(binary: &Path, repository: &Path) -> Result<()> {
    let temporary = tempfile::tempdir().context(FileSnafu {
        path: std::env::temp_dir(),
    })?;
    let fault_library = temporary.path().join("process_faults.so");
    compile_fault_library(repository, &fault_library)?;
    let checks: &[(&str, ScanCheck<'_>)] = &[
        (
            "IPC, projection, predicates, and execution options",
            ScanFixture::check_corpus,
        ),
        ("empty table retains schema", ScanFixture::check_empty_table),
        (
            "Float64 predicates distinguish adjacent values",
            ScanFixture::check_float64_predicates,
        ),
        (
            "scan thread creation failures do not hang",
            ScanFixture::check_thread_start_failures,
        ),
        (
            "HTTPS trust for inspection and scan",
            ScanFixture::check_https_trust,
        ),
        (
            "later read failures leave incomplete IPC output",
            ScanFixture::check_later_read_failure,
        ),
        (
            "closing stdout stops a blocked scan",
            ScanFixture::check_closed_output_pipe,
        ),
        (
            "signals stop a scan blocked on stdout",
            ScanFixture::check_signals_during_blocked_output,
        ),
        (
            "signals stop held data requests",
            ScanFixture::check_signals_during_held_data_requests,
        ),
        (
            "partition targets and read capacities bound requests",
            ScanFixture::check_partition_read_limits,
        ),
        (
            "narrow execution options bound reads behind blocked stdout",
            ScanFixture::check_narrow_read_limits,
        ),
        (
            "blocked output resumes incrementally",
            ScanFixture::check_incremental_output,
        ),
        (
            "slow consumers do not time out completed responses",
            ScanFixture::check_slow_consumer,
        ),
        (
            "scan exit does not wait for blocking file prefetch",
            ScanFixture::check_blocking_prefetch_exit,
        ),
    ];
    for (name, check) in checks {
        eprintln!("Scan check: {name}");
        let directory = tempfile::Builder::new()
            .prefix("secret-dar-scan-")
            .tempdir()
            .context(FileSnafu {
                path: std::env::temp_dir(),
            })?;
        check(&ScanFixture {
            binary,
            corpus: repository
                .join("crates/delta-arrow-reader/tests/reader/fixtures/external_writer/corpus"),
            fault_library: &fault_library,
            directory,
        })?;
    }
    eprintln!("{} Rust scan checks passed", checks.len());
    Ok(())
}

impl ScanFixture<'_> {
    fn command(&self) -> Command {
        let mut command = Command::new(self.binary);
        command
            .arg("scan")
            .current_dir(self.directory.path())
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .env("RUST_LOG", "trace")
            .stdin(Stdio::null());
        command
    }

    fn scan(&self, command: &mut Command) -> Result<IpcTable> {
        let output = output_with_timeout(command, Duration::from_secs(15))?;
        self.check_successful_scan(&output)
    }

    fn check_successful_scan(&self, output: &Output) -> Result<IpcTable> {
        super::ipc::check_scan_output(output)
    }

    fn check_completed_scan(&self, scan: &mut child::ScanProcess, rows: usize) -> Result<IpcTable> {
        let actual =
            self.check_successful_scan(&scan.wait_with_output(Duration::from_secs(15))?)?;
        ensure!(
            actual.row_count() == rows,
            InvalidInputSnafu {
                message: format!("expected {rows} scan rows, got {}", actual.row_count()),
            }
        );
        Ok(actual)
    }

    fn check_corpus(&self) -> Result<()> {
        super::corpus::check_corpus(&self.corpus, self.directory.path(), &|| self.command())
    }

    fn check_empty_table(&self) -> Result<()> {
        let table = self.directory.path().join("empty-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table, 0)?;
        let expected_path = self.corpus.join("partitioned/expected.arrow");
        let mut expected = IpcTable::read_file(&expected_path)?;
        expected.batches.clear();
        self.scan(self.command().arg(&table))?
            .check_matches(&expected, None)?;
        let expected = expected.project(&[], &expected_path)?;
        self.scan(self.command().arg("--no-columns").arg(&table))?
            .check_matches(&expected, None)
    }

    fn check_float64_predicates(&self) -> Result<()> {
        let table = self.directory.path().join("float64-table");
        let data = create_float64_table(&table)?;
        let predicate = self.directory.path().join("predicate.json");
        for (op, rows) in [
            ("eq", 0..1),
            ("ne", 1..2),
            ("lt", 0..0),
            ("le", 0..1),
            ("gt", 1..2),
            ("ge", 0..2),
        ] {
            eprintln!("  comparison={op}");
            // The CLI's JSON parser previously rounded the first value to the second.
            let input = json!({
                "op": op, "column": "value", "value": {"type": "float64", "value": FLOAT64_VALUES[0]},
            });
            fs::write(&predicate, input.to_string()).context(FileSnafu { path: &predicate })?;
            let actual = self.scan(
                self.command()
                    .arg(&table)
                    .arg("--predicate-file")
                    .arg(&predicate),
            )?;
            let expected = IpcTable {
                schema: data.schema(),
                batches: vec![data.slice(rows.start, rows.len())],
            };
            actual.check_matches(&expected, None)?;
        }
        Ok(())
    }

    fn check_thread_start_failures(&self) -> Result<()> {
        let table = self.directory.path().join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table, 32)?;
        let mut saw_success = false;
        let mut saw_failure = false;
        for limit in 0..8 {
            eprintln!("  thread_limit={limit}");
            let output = output_on_single_cpu(
                self.command()
                    .arg(&table)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("RUST_BACKTRACE", "full")
                    .env("DAR_TEST_THREAD_LIMIT", limit.to_string()),
                Duration::from_secs(15),
            )?;
            if output.status.code() == Some(1) {
                check_diagnostic(&output.stderr, "execution", "runtime_initialization")?;
                saw_failure = true;
            } else {
                let actual = self.check_successful_scan(&output)?;
                ensure!(
                    actual.row_count() == 32 * 120,
                    InvalidInputSnafu {
                        message: format!(
                            "expected 3840 scan rows after thread creation succeeded, got {}",
                            actual.row_count()
                        ),
                    }
                );
                saw_success = true;
            }
        }
        ensure!(
            saw_success && saw_failure,
            InvalidInputSnafu {
                message: "thread creation fault checks must exercise both successful and failed scans",
            }
        );
        Ok(())
    }
}
