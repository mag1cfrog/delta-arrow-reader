use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::Arc,
    time::Duration,
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use snafu::{ResultExt, ensure};

use super::ipc::IpcTable;
use crate::{
    command::{output_on_single_cpu, output_with_timeout},
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, Result},
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
            "IPC matches Spark fixtures",
            ScanFixture::check_spark_fixtures,
        ),
        (
            "filter column omitted from output",
            ScanFixture::check_filter_column_omitted_from_output,
        ),
        (
            "execution options with filtering",
            ScanFixture::check_execution_options,
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
            corpus: repository.join("tests/reader/fixtures/external_writer/corpus"),
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
        ensure!(
            output.status.success() && output.stderr.is_empty(),
            InvalidInputSnafu {
                message: format!(
                    "scan failed: {}; stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                )
            }
        );
        IpcTable::read_stream(&output.stdout, &self.directory.path().join("stdout.arrow"))
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

    fn check_spark_fixtures(&self) -> Result<()> {
        for name in ["partitioned", "nested_mapping", "deletion_vectors"] {
            let fixture = self.corpus.join(name);
            let expected_path = fixture.join("expected.arrow");
            let expected = IpcTable::read_file(&expected_path)?;
            for columns in [None, Some(&["value", "id"][..]), Some(&[][..])] {
                let indices: Vec<_> = match columns {
                    None => (0..expected.schema.fields().len()).collect(),
                    Some(columns) => columns
                        .iter()
                        .map(|name| expected.schema.index_of(name))
                        .collect::<std::result::Result<_, _>>()
                        .context(ArrowSnafu {
                            path: &expected_path,
                        })?,
                };
                let projected = expected.project(&indices, &expected_path)?;
                for limit in [None, Some(0), Some(1), Some(1000)] {
                    eprintln!("  fixture={name}, columns={columns:?}, limit={limit:?}");
                    let mut command = self.command();
                    if let Some(columns) = columns {
                        if columns.is_empty() {
                            command.arg("--no-columns");
                        }
                        for column in columns {
                            command.args(["--column", column]);
                        }
                    }
                    if let Some(limit) = limit {
                        command.args(["--limit", &limit.to_string()]);
                    }
                    let actual = self.scan(command.arg(fixture.join("table")))?;
                    actual.check_matches(&projected, limit)?;
                }
            }
        }
        Ok(())
    }

    fn filtered_scan_command(&self) -> Result<Command> {
        let predicate = self.directory.path().join("predicate.json");
        let input = json!({"op": "eq", "column": "id", "value": {"type": "int32", "value": "10"}});
        fs::write(&predicate, input.to_string()).context(FileSnafu { path: &predicate })?;
        let mut command = self.command();
        command
            .arg(self.corpus.join("partitioned/table"))
            .arg("--predicate-file")
            .arg(predicate)
            .args(["--column", "value"]);
        Ok(command)
    }

    fn check_filter_column_omitted_from_output(&self) -> Result<()> {
        let actual = self.scan(&mut self.filtered_scan_command()?)?;
        check_filtered_value(&actual)
    }

    fn check_execution_options(&self) -> Result<()> {
        for backend in [Some("direct"), Some("delta_kernel"), None] {
            eprintln!("  backend={backend:?}");
            let mut command = self.filtered_scan_command()?;
            if let Some(backend) = backend {
                let options = json!({
                    "parquet_backend": backend, "max_concurrent_file_reads_per_scan": 1,
                    "max_concurrent_file_reads_per_partition": 1, "output_buffer_batches_per_partition": 1,
                    "prefetch_files_per_partition": 0, "parquet_metadata_size_hint_bytes": null,
                    "parquet_full_file_read_threshold_bytes": 1048576,
                });
                let path = self.directory.path().join("execution-options.json");
                fs::write(&path, options.to_string()).context(FileSnafu { path: &path })?;
                command
                    .arg("--execution-options-file")
                    .arg(path)
                    .args(["--target-partitions", "2"]);
            }
            let actual = self.scan(command.args(["--limit", "1"]))?;
            check_filtered_value(&actual)?;
        }
        Ok(())
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

fn check_filtered_value(actual: &IpcTable) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![30]))])
        .context(ArrowSnafu {
            path: "filtered fixture",
        })?;
    actual.check_matches(
        &IpcTable {
            schema,
            batches: vec![batch],
        },
        None,
    )
}
