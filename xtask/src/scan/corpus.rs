use std::{
    fs, os::unix::fs::PermissionsExt, path::Path, process::Command, sync::Arc, time::Duration,
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use snafu::ResultExt;

use super::ipc::{IpcTable, check_completed_scan};
use crate::{
    command::output_with_timeout,
    error::{ArrowSnafu, FileSnafu, Result},
};

pub(crate) fn check_corpus(
    corpus: &Path,
    directory: &Path,
    command: &dyn Fn() -> Command,
) -> Result<()> {
    let fixture = CorpusFixture {
        corpus,
        directory,
        command,
    };
    fixture.check_spark_fixtures()?;
    fixture.check_filter_column_omitted_from_output()?;
    fixture.check_execution_options()
}

struct CorpusFixture<'a> {
    corpus: &'a Path,
    directory: &'a Path,
    command: &'a dyn Fn() -> Command,
}

impl CorpusFixture<'_> {
    fn scan(
        &self,
        command: &mut Command,
        table_version: u64,
        limit: Option<usize>,
    ) -> Result<IpcTable> {
        command.args([
            "--diagnostics=json",
            "--table-version",
            &table_version.to_string(),
        ]);
        if let Some(limit) = limit {
            command.args(["--limit", &limit.to_string()]);
        }
        let output = output_with_timeout(command, Duration::from_secs(30))?;
        check_completed_scan(&output, table_version, limit)
    }

    fn check_spark_fixtures(&self) -> Result<()> {
        for (name, table_version) in [
            ("partitioned", 0),
            ("nested_mapping", 1),
            ("deletion_vectors", 1),
        ] {
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
                    let mut command = (self.command)();
                    if let Some(columns) = columns {
                        if columns.is_empty() {
                            command.arg("--no-columns");
                        }
                        for column in columns {
                            command.args(["--column", column]);
                        }
                    }
                    let actual =
                        self.scan(command.arg(fixture.join("table")), table_version, limit)?;
                    actual.check_matches(&projected, limit)?;
                }
            }
        }
        Ok(())
    }

    fn filtered_scan_command(&self) -> Result<Command> {
        let predicate = self.directory.join("predicate.json");
        let input = json!({"op": "eq", "column": "id", "value": {"type": "int32", "value": "10"}});
        fs::write(&predicate, input.to_string()).context(FileSnafu { path: &predicate })?;
        fs::set_permissions(&predicate, fs::Permissions::from_mode(0o644))
            .context(FileSnafu { path: &predicate })?;
        let mut command = (self.command)();
        command
            .arg(self.corpus.join("partitioned/table"))
            .arg("--predicate-file")
            .arg(predicate)
            .args(["--column", "value"]);
        Ok(command)
    }

    fn check_filter_column_omitted_from_output(&self) -> Result<()> {
        let actual = self.scan(&mut self.filtered_scan_command()?, 0, None)?;
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
                let path = self.directory.join("execution-options.json");
                fs::write(&path, options.to_string()).context(FileSnafu { path: &path })?;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
                    .context(FileSnafu { path: &path })?;
                command
                    .arg("--execution-options-file")
                    .arg(path)
                    .args(["--target-partitions", "2"]);
            }
            let actual = self.scan(&mut command, 0, Some(1))?;
            check_filtered_value(&actual)?;
        }
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
