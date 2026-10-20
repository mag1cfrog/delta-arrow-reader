use std::{
    ffi::OsString,
    fs,
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::Duration,
};

use serde_json::{Value, json};
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    command::{output_with_timeout, run_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
};

struct ProcessFixture<'a> {
    binary: &'a Path,
    version: &'a str,
    repository: &'a Path,
    fault_library: &'a Path,
    directory: tempfile::TempDir,
}

type ProcessCheck<'a> = fn(&ProcessFixture<'a>) -> Result<()>;

mod http;
mod output;
mod planning;
mod signals;
mod validation;

pub(crate) use signals::check_no_child_processes;

pub(super) fn run(binary: &Path, version: &str, repository: &Path) -> Result<()> {
    let temporary = tempfile::tempdir().context(FileSnafu {
        path: std::env::temp_dir(),
    })?;
    let fault_library = temporary.path().join("process_faults.so");
    compile_fault_library(repository, &fault_library)?;
    let checks: &[(&str, ProcessCheck<'_>)] = &[
        (
            "static help and version",
            ProcessFixture::check_static_help_and_version,
        ),
        (
            "inherited Tokio worker count",
            ProcessFixture::check_inherited_worker_count,
        ),
        (
            "thread start failures",
            ProcessFixture::check_thread_start_failures,
        ),
        (
            "relative paths, file URLs, and literal dash",
            ProcessFixture::check_table_locations,
        ),
        ("literal percent paths", ProcessFixture::check_percent_paths),
        (
            "historical snapshots without data files",
            ProcessFixture::check_historical_snapshots,
        ),
        (
            "schema metadata and unsupported scan protocol",
            ProcessFixture::check_schema_metadata,
        ),
        (
            "unrelated and newer log files",
            ProcessFixture::check_unrelated_log_files,
        ),
        (
            "sparse log versions",
            ProcessFixture::check_sparse_log_versions,
        ),
        (
            "reader diagnostics and redaction",
            ProcessFixture::check_reader_diagnostics,
        ),
        (
            "unused blocking prefetch",
            ProcessFixture::check_unused_blocking_prefetch,
        ),
        (
            "inspect input validation",
            ProcessFixture::check_inspect_input_validation,
        ),
        (
            "scan input validation and terminal output",
            ProcessFixture::check_scan_input_validation,
        ),
        (
            "execution options validation",
            ProcessFixture::check_execution_options_validation,
        ),
        (
            "execution options size and literal dash",
            ProcessFixture::check_execution_options_size_and_literal_dash,
        ),
        (
            "predicate JSON validation",
            ProcessFixture::check_predicate_json_validation,
        ),
        (
            "predicate size and literal dash",
            ProcessFixture::check_predicate_size_and_literal_dash,
        ),
        (
            "storage options byte limit",
            ProcessFixture::check_storage_options_size_limit,
        ),
        (
            "predicate planning",
            ProcessFixture::check_predicate_planning,
        ),
        (
            "duplicate columns during snapshot loading",
            ProcessFixture::check_duplicate_columns,
        ),
        (
            "projection planning",
            ProcessFixture::check_projection_planning,
        ),
        (
            "stdout write failures",
            ProcessFixture::check_stdout_failures,
        ),
        (
            "explicit versions stop before auxiliary listing pages",
            ProcessFixture::check_paginated_listing,
        ),
        (
            "HTTP inspection reads only metadata",
            ProcessFixture::check_http_metadata_only,
        ),
        (
            "signals terminate blocked loading without child processes",
            ProcessFixture::check_signals,
        ),
    ];
    for (name, check) in checks {
        eprintln!("Process check: {name}");
        let directory = tempfile::Builder::new()
            .prefix("secret-dar-")
            .tempdir()
            .context(FileSnafu {
                path: std::env::temp_dir(),
            })?;
        check(&ProcessFixture {
            binary,
            version,
            repository,
            fault_library: &fault_library,
            directory,
        })?;
    }
    eprintln!("{} Rust process checks passed", checks.len());
    Ok(())
}

impl ProcessFixture<'_> {
    fn command(&self) -> Command {
        let mut command = Command::new(self.binary);
        command
            .current_dir(self.directory.path())
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .env_remove("RUST_LOG")
            .stdin(Stdio::null());
        command
    }

    fn write_input_file(&self, content: &[u8]) -> Result<PathBuf> {
        let path = self.directory.path().join("secret input, file.json");
        write_fixture(&path, content)?;
        Ok(path)
    }

    fn create_metadata_table(&self, fields: Value, protocol: u32) -> Result<PathBuf> {
        let table = self.directory.path().join("-secret table, with spaces");
        let log = table.join("_delta_log");
        fs::create_dir_all(&log).context(FileSnafu { path: &log })?;
        let schema = json!({"type": "struct", "fields": fields});
        let actions = [
            json!({"protocol": {"minReaderVersion": protocol, "minWriterVersion": 2}}),
            json!({"metaData": {
                "id": "test", "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {},
            }}),
        ];
        let contents: String = actions.iter().map(|action| format!("{action}\n")).collect();
        write_fixture(&log.join("00000000000000000000.json"), contents)?;
        Ok(table)
    }

    fn check_static_help_and_version(&self) -> Result<()> {
        for args in [
            vec!["--help"],
            vec!["-h"],
            vec!["inspect", "--help"],
            vec!["inspect", "-h"],
            vec!["help", "inspect"],
            vec![
                "inspect",
                "--storage-options-file",
                "secret-missing",
                "--help",
            ],
            vec!["scan", "--help"],
            vec!["scan", "-h"],
            vec!["help", "scan"],
            vec!["scan", "--storage-options-file", "secret-missing", "--help"],
            vec!["scan", "--predicate-file", "secret-missing", "--help"],
            vec![
                "scan",
                "--execution-options-file",
                "secret-missing",
                "--help",
            ],
        ] {
            let output = run_cli(
                self.command()
                    .args(&args)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("DAR_TEST_REJECT_THREADS", "1"),
            )?;
            ensure!(
                output.status.success() && output.stderr.is_empty(),
                InvalidInputSnafu {
                    message: format!("help failed for {args:?}: {}", describe_output(&output))
                }
            );
            let text = String::from_utf8_lossy(&output.stdout);
            let mut expected = vec!["Usage:", "--help"];
            if args.contains(&"inspect") || args.contains(&"scan") {
                expected.extend(["--table-version", "--storage-options-file"]);
            }
            if args.contains(&"scan") {
                expected.extend([
                    "--column",
                    "--no-columns",
                    "--limit",
                    "--predicate-file",
                    "--execution-options-file",
                    "--target-partitions",
                ]);
            }
            ensure!(
                expected.iter().all(|flag| text.contains(flag)),
                InvalidInputSnafu {
                    message: format!("help for {args:?} lacks an expected option: {text}")
                }
            );
        }
        for flag in ["--version", "-V"] {
            let output = run_cli(
                self.command()
                    .arg(flag)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("DAR_TEST_REJECT_THREADS", "1"),
            )?;
            ensure!(
                output.status.success()
                    && output.stderr.is_empty()
                    && output.stdout == format!("dar {}\n", self.version).as_bytes(),
                InvalidInputSnafu {
                    message: format!("unexpected {flag} result: {}", describe_output(&output))
                }
            );
        }
        Ok(())
    }

    fn check_inherited_worker_count(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        for count in [
            OsString::from("0"),
            OsString::from("secret-invalid-count"),
            OsString::from("9999999999999999999999999"),
            OsString::from_vec(b"secret-\xff".to_vec()),
        ] {
            check_inspection(&run_cli(
                self.command()
                    .arg("inspect")
                    .arg(&table)
                    .env("TOKIO_WORKER_THREADS", count),
            )?)?;
        }
        Ok(())
    }

    fn check_thread_start_failures(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        // Fail the core blocking thread, Kernel background thread, and worker in turn.
        for limit in 0..=3 {
            let output = run_cli(
                self.command()
                    .arg("inspect")
                    .arg(&table)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("RUST_BACKTRACE", "full")
                    .env("DAR_TEST_THREAD_LIMIT", limit.to_string()),
            )?;
            if limit < 3 {
                check_error(&output, 1, "execution", "runtime_initialization")?;
            } else {
                check_inspection(&output)?;
            }
        }
        Ok(())
    }

    fn check_table_locations(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        let options = self.directory.path().join("-");
        write_fixture(&options, "{}")?;
        let expected = json!({"format_version": 1, "table_version": "0",
            "schema": {"fields": [], "metadata": {}}});
        for output in [
            run_cli(
                self.command()
                    .args(["inspect", "--"])
                    .arg(table.file_name().context(InvalidInputSnafu {
                        message: "table fixture path must have a name",
                    })?),
            )?,
            run_cli(self.command().arg("inspect").arg(file_url(&table)?))?,
            run_cli(
                self.command()
                    .args(["inspect", "--storage-options-file", "-"])
                    .arg(&table),
            )?,
        ] {
            let value = check_inspection(&output)?;
            ensure!(
                value == expected,
                InvalidInputSnafu {
                    message: format!("unexpected empty-table inspection: {value}")
                }
            );
        }
        Ok(())
    }

    fn check_percent_paths(&self) -> Result<()> {
        let original = self.create_metadata_table(json!([]), 1)?;
        let table = self.directory.path().join("secret%name");
        fs::rename(original, &table).context(FileSnafu { path: &table })?;
        for location in [
            table.as_os_str().to_owned(),
            OsString::from(file_url(&table)?),
        ] {
            let latest = check_inspection(&run_cli(self.command().arg("inspect").arg(&location))?)?;
            let historical = check_inspection(&run_cli(
                self.command()
                    .args(["inspect", "--table-version", "0"])
                    .arg(&location),
            )?)?;
            ensure!(
                historical == latest,
                InvalidInputSnafu {
                    message: format!("historical percent-path inspection differs: {historical}")
                }
            );
        }
        Ok(())
    }

    fn check_historical_snapshots(&self) -> Result<()> {
        let corpus = self
            .repository
            .join("crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus");
        for (name, latest) in [
            ("partitioned", "0"),
            ("nested_mapping", "1"),
            ("deletion_vectors", "1"),
        ] {
            let table = self.directory.path().join(name);
            copy_directory(
                &corpus.join(name).join("table/_delta_log"),
                &table.join("_delta_log"),
            )?;
            let current = check_inspection(&run_cli(self.command().arg("inspect").arg(&table))?)?;
            let original = check_inspection(&run_cli(
                self.command()
                    .args(["inspect", "--table-version", "0"])
                    .arg(&table),
            )?)?;
            ensure!(
                current["table_version"] == latest && original["table_version"] == "0",
                InvalidInputSnafu {
                    message: format!(
                        "wrong snapshot for {name}: current={current}, original={original}"
                    )
                }
            );
            if name == "nested_mapping" {
                let city = &current["schema"]["fields"][2]["data_type"]["Struct"][1];
                let old_city = &original["schema"]["fields"][2]["data_type"]["Struct"][1];
                ensure!(
                    city["name"] == "city"
                        && old_city["name"] == "old_city"
                        && city["metadata"]["delta.columnMapping.id"] == "5",
                    InvalidInputSnafu {
                        message: format!(
                            "column mapping changed: current={city}, original={old_city}"
                        )
                    }
                );
            }
        }
        Ok(())
    }

    fn check_schema_metadata(&self) -> Result<()> {
        let table = self.create_metadata_table(
            json!([{"name": "secret-name", "type": "string",
            "nullable": false, "metadata": {"comment": "secret requested metadata"}}]),
            4,
        )?;
        let value = check_inspection(&run_cli(self.command().arg("inspect").arg(&table))?)?;
        let field = &value["schema"]["fields"][0];
        ensure!(
            field["name"] == "secret-name"
                && field["nullable"] == false
                && field["metadata"]["comment"] == "secret requested metadata",
            InvalidInputSnafu {
                message: format!("requested schema metadata was not preserved: {value}")
            }
        );
        Ok(())
    }

    fn check_unrelated_log_files(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        let log = table.join("_delta_log");
        write_fixture(
            &log.join("00000000000000000001.json"),
            "secret invalid JSON\n",
        )?;
        write_fixture(
            &log.join("00000000000000000000.00000000000000000001.compacted.json"),
            "secret invalid compaction\n",
        )?;
        let value = check_inspection(&run_cli(
            self.command()
                .args(["inspect", "--table-version", "0"])
                .arg(&table),
        )?)?;
        ensure!(
            value["table_version"] == "0",
            InvalidInputSnafu {
                message: format!("wrong historical snapshot: {value}")
            }
        );
        let version = 1_000_000_000_000u64;
        write_fixture(&log.join(format!("{version:020}.crc")), "{}")?;
        let nested = log.join(format!("{version:020}.nested"));
        fs::create_dir(&nested).context(FileSnafu { path: &nested })?;
        write_fixture(&nested.join("00000000000000000000.json"), "{}")?;
        check_error(
            &run_cli(
                self.command()
                    .args(["inspect", "--table-version"])
                    .arg(version.to_string())
                    .arg(&table),
            )?,
            1,
            "snapshot",
            "snapshot_load",
        )?;
        Ok(())
    }

    fn check_sparse_log_versions(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        let log = table.join("_delta_log");
        let path = log.join("00000000000000000000.json");
        let commit = fs::read(&path).context(FileSnafu { path })?;
        let version = 1_000_000_000_000u64;
        for (suffix, content) in [
            (".checkpoint.parquet", b"".as_slice()),
            (
                ".checkpoint.0000000001.0000000002.parquet",
                b"secret incomplete checkpoint".as_slice(),
            ),
            (".json", commit.as_slice()),
        ] {
            let path = log.join(format!("{version:020}{suffix}"));
            write_fixture(&path, content)?;
            check_error(
                &run_cli(
                    self.command()
                        .args(["inspect", "--table-version"])
                        .arg(version.to_string())
                        .arg(&table),
                )?,
                1,
                "snapshot",
                "snapshot_load",
            )?;
            fs::remove_file(&path).context(FileSnafu { path: &path })?;
        }
        Ok(())
    }

    fn check_reader_diagnostics(&self) -> Result<()> {
        let table = self.create_metadata_table(json!([]), 1)?;
        for (location, phase, code) in [
            (
                self.directory
                    .path()
                    .join("secret-missing")
                    .into_os_string(),
                "table_location",
                "invalid_table_location",
            ),
            (
                self.directory.path().as_os_str().to_owned(),
                "snapshot",
                "snapshot_load",
            ),
            (
                OsString::from(
                    "unknown://secret-user:secret-password@host/table?token=secret-token",
                ),
                "storage",
                "storage_initialization",
            ),
        ] {
            let output = run_cli(self.command().arg("inspect").arg(location))?;
            let diagnostic = check_error(&output, 1, phase, code)?;
            ensure!(
                diagnostic.contains("event=\"table_load.failed\"")
                    && diagnostic.contains("operation_id="),
                InvalidInputSnafu {
                    message: format!("core operation identity was lost: {diagnostic}")
                }
            );
        }
        for version in [
            "999",
            "1000000000000",
            "18446744073709551614",
            "18446744073709551615",
        ] {
            check_error(
                &run_cli(
                    self.command()
                        .args(["inspect", "--table-version", version])
                        .arg(&table),
                )?,
                1,
                "snapshot",
                "snapshot_load",
            )?;
        }
        let options = self.directory.path().join("secret options, file.json");
        write_fixture(&options, br#"{"allow_http":"secret-invalid-value"}"#)?;
        check_error(
            &run_cli(
                self.command()
                    .args(["inspect", "--storage-options-file"])
                    .arg(options)
                    .arg("http://127.0.0.1:9/secret-table"),
            )?,
            1,
            "storage",
            "storage_initialization",
        )?;
        let table = self.create_metadata_table(invalid_schema_fields(), 1)?;
        check_error(
            &run_cli(self.command().arg("inspect").arg(table))?,
            1,
            "schema",
            "schema_conversion",
        )?;
        Ok(())
    }

    fn check_unused_blocking_prefetch(&self) -> Result<()> {
        for invalid in [false, true] {
            let table = self.create_metadata_table(
                if invalid {
                    invalid_schema_fields()
                } else {
                    json!([])
                },
                1,
            )?;
            let log = table.join("_delta_log");
            let old = log.join("00000000000000000000.json");
            let latest = log.join("00000000000000000001.json");
            fs::copy(&old, &latest).context(FileSnafu { path: &latest })?;
            write_fixture(&old, "unused invalid JSON\n")?;
            let started = self.directory.path().join(if invalid {
                "error-open-started"
            } else {
                "open-started"
            });
            let output = run_cli(
                self.command()
                    .arg("inspect")
                    .arg(&table)
                    .env("LD_PRELOAD", self.fault_library)
                    .env("DAR_TEST_BLOCKED_OPEN", &old)
                    .env("DAR_TEST_WAITING_OPEN", &latest)
                    .env("DAR_TEST_OPEN_STARTED", &started),
            )?;
            ensure!(
                started.exists(),
                InvalidInputSnafu {
                    message: "unused blocking prefetch never started"
                }
            );
            if invalid {
                check_error(&output, 1, "schema", "schema_conversion")?;
            } else {
                let value = check_inspection(&output)?;
                ensure!(
                    value["table_version"] == "1",
                    InvalidInputSnafu {
                        message: format!("wrong latest snapshot: {value}")
                    }
                );
            }
        }
        Ok(())
    }
}

fn run_cli(command: &mut Command) -> Result<Output> {
    output_with_timeout(command, Duration::from_secs(15))
}

pub(crate) fn compile_fault_library(repository: &Path, destination: &Path) -> Result<()> {
    run_with_timeout(
        Command::new("cc")
            .args(["-shared", "-fPIC"])
            .arg(repository.join("crates/delta-arrow-reader-cli/tests/process_faults.c"))
            .arg("-o")
            .arg(destination)
            .arg("-ldl")
            .stdin(Stdio::null()),
        Duration::from_secs(30),
    )
}

pub(crate) fn check_error<'a>(
    output: &'a Output,
    status: i32,
    phase: &str,
    code: &str,
) -> Result<&'a str> {
    ensure!(
        output.status.code() == Some(status) && output.stdout.is_empty(),
        InvalidInputSnafu {
            message: format!(
                "expected exit {status} with empty stdout: {}",
                describe_output(output)
            )
        }
    );
    check_diagnostic(&output.stderr, phase, code)
}

pub(crate) fn check_diagnostic<'a>(stderr: &'a [u8], phase: &str, code: &str) -> Result<&'a str> {
    let text = std::str::from_utf8(stderr).map_err(|_| {
        InvalidInputSnafu {
            message: "CLI diagnostic was not UTF-8",
        }
        .build()
    })?;
    ensure!(
        text.ends_with('\n')
            && text.lines().count() == 1
            && (text.starts_with("ERROR delta_arrow_reader: ")
                || text.starts_with("ERROR delta_arrow_reader_cli: "))
            && !text.contains("secret")
            && !text.contains('\x1b'),
        InvalidInputSnafu {
            message: format!("expected one redacted ERROR: {text:?}")
        }
    );
    for field in [
        format!("error_phase=\"{phase}\""),
        format!("error_code=\"{code}\""),
        "outcome=\"failed\"".into(),
    ] {
        ensure!(
            text.split_whitespace().any(|token| token == field),
            InvalidInputSnafu {
                message: format!("expected {field}: {text}")
            }
        );
    }
    ensure!(
        text.split_whitespace()
            .any(|token| token.starts_with("error_reason=\"")
                && token.ends_with('"')
                && token.len() > "error_reason=\"\"".len()),
        InvalidInputSnafu {
            message: format!("missing failure reason: {text}")
        }
    );
    Ok(text)
}

pub(crate) fn check_inspection(output: &Output) -> Result<Value> {
    ensure!(
        output.status.success()
            && output.stderr.is_empty()
            && output.stdout.ends_with(b"\n")
            && output.stdout.iter().filter(|byte| **byte == b'\n').count() == 1,
        InvalidInputSnafu {
            message: format!(
                "expected one inspection JSON line without stderr: {}",
                describe_output(output)
            )
        }
    );
    let value: Value = serde_json::from_slice(&output.stdout).context(JsonSnafu {
        subject: "CLI inspection",
    })?;
    ensure!(
        value
            .as_object()
            .is_some_and(|fields| fields.len() == 3 && fields.contains_key("schema"))
            && value["format_version"] == 1
            && value["table_version"].is_string(),
        InvalidInputSnafu {
            message: format!("unexpected inspection fields: {value}")
        }
    );
    Ok(value)
}

fn describe_output(output: &Output) -> String {
    format!(
        "status={}, stdout={:?}, stderr={:?}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

fn write_fixture(path: &Path, contents: impl AsRef<[u8]>) -> Result<()> {
    fs::write(path, contents).context(FileSnafu { path })
}

fn file_url(path: &Path) -> Result<String> {
    url::Url::from_file_path(path)
        .map(String::from)
        .map_err(|()| {
            InvalidInputSnafu {
                message: format!("cannot form a file URL from {}", path.display()),
            }
            .build()
        })
}

pub(crate) fn copy_directory(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination).context(FileSnafu { path: destination })?;
    for entry in fs::read_dir(source).context(FileSnafu { path: source })? {
        let entry = entry.context(FileSnafu { path: source })?;
        let destination = destination.join(entry.file_name());
        if entry
            .file_type()
            .context(FileSnafu { path: entry.path() })?
            .is_dir()
        {
            copy_directory(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), &destination).context(FileSnafu { path: &destination })?;
        }
    }
    Ok(())
}

fn invalid_schema_fields() -> Value {
    json!([{"name": "secret-array", "type": {"type": "array", "elementType": "string", "containsNull": true},
        "nullable": true, "metadata": {"delta.columnMapping.nested.ids": "secret-invalid-object"}}])
}

#[cfg(test)]
mod tests;
