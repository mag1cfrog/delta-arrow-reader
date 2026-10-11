use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};

use super::ipc::IpcTable;
use crate::{
    command::output_with_timeout,
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
};

struct ScanFixture<'a> {
    binary: &'a Path,
    corpus: PathBuf,
    directory: tempfile::TempDir,
}

type ScanCheck<'a> = fn(&ScanFixture<'a>) -> Result<()>;

pub(super) fn run(binary: &Path, repository: &Path) -> Result<()> {
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
        ensure!(
            output.status.success() && output.stderr.is_empty(),
            InvalidInputSnafu {
                message: format!(
                    "scan {:?} failed: {}; stderr={}",
                    command.get_args().collect::<Vec<_>>(),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                )
            }
        );
        IpcTable::read_stream(&output.stdout, &self.directory.path().join("stdout.arrow"))
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
        let source = self
            .corpus
            .join("partitioned/table/_delta_log/00000000000000000000.json");
        let actions = fs::read_to_string(&source).context(FileSnafu { path: &source })?;
        let mut metadata = String::new();
        for line in actions.lines() {
            let action: Value = serde_json::from_str(line).context(JsonSnafu {
                subject: "fixture log action",
            })?;
            if action.get("protocol").is_some() || action.get("metaData").is_some() {
                metadata.push_str(line);
                metadata.push('\n');
            }
        }
        let table = self.directory.path().join("empty-table");
        let log = table.join("_delta_log");
        fs::create_dir_all(&log).context(FileSnafu { path: &log })?;
        let log = log.join("00000000000000000000.json");
        fs::write(&log, metadata).context(FileSnafu { path: &log })?;
        let expected_path = self.corpus.join("partitioned/expected.arrow");
        let mut expected = IpcTable::read_file(&expected_path)?;
        expected.batches.clear();
        self.scan(self.command().arg(&table))?
            .check_matches(&expected, None)?;
        let expected = expected.project(&[], &expected_path)?;
        self.scan(self.command().arg("--no-columns").arg(&table))?
            .check_matches(&expected, None)
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
