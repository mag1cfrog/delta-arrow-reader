use std::{
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use arrow::ipc::reader::StreamReader;
use serde_json::Value;

use super::{run_with_stdout, run_with_timeout};

fn partitioned_table_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    )
}

pub(super) fn assert_single_terminal_record(stderr: &[u8], kind: &str) -> Value {
    let text = std::str::from_utf8(stderr).unwrap();
    assert!(text.ends_with('\n'), "{text}");
    assert!(!text.contains('\x1b'), "{text}");
    assert!(!text.contains("secret"), "{text}");
    let records: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1, "{text}");
    let record = records.into_iter().next().unwrap();
    assert_eq!(record["format_version"], 1);
    assert_eq!(record["kind"], kind);
    assert_eq!(record["terminal"], true);
    assert!(stderr.len() <= 8192);
    record
}

#[test]
fn argument_errors_include_safe_corrections() -> Result<(), Box<dyn std::error::Error>> {
    let limit_range = format!("unsigned decimal integer from 0 to {}", usize::MAX);
    let partition_range = format!("unsigned decimal integer from 1 to {}", usize::MAX);
    let version_range = format!("unsigned decimal integer from 0 to {}", u64::MAX);
    for (args, kind, argument, expected, hint) in [
        (
            vec!["scan", "secret-table", "--limit", "secret-value"],
            "ValueValidation",
            "--limit",
            limit_range.as_str(),
            "Supply a value",
        ),
        (
            vec!["scan", "secret-table", "--target-partitions", "0"],
            "ValueValidation",
            "--target-partitions",
            partition_range.as_str(),
            "Supply a value",
        ),
        (
            vec![
                "inspect",
                "secret-table",
                "--table-version",
                "18446744073709551616",
            ],
            "ValueValidation",
            "--table-version",
            version_range.as_str(),
            "Supply a value",
        ),
        (
            vec!["scan", "secret-table", "--limit"],
            "InvalidValue",
            "--limit",
            limit_range.as_str(),
            "Supply a value",
        ),
        (
            vec!["scan", "secret-table", "--limit", "1", "--limit", "2"],
            "ArgumentConflict",
            "--limit",
            limit_range.as_str(),
            "Remove",
        ),
        (
            vec!["inspect"],
            "MissingRequiredArgument",
            "table",
            "UTF-8 local path",
            "Supply the required argument",
        ),
        (
            vec!["--diagnostics=secret-format", "inspect", "secret-table"],
            "InvalidValue",
            "--diagnostics",
            "json (use --diagnostics=json)",
            "Supply a value",
        ),
        (
            vec!["--diagnostics", "json", "inspect", "secret-table"],
            "NoEquals",
            "--diagnostics",
            "json (use --diagnostics=json)",
            "Attach",
        ),
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(&args)
                .arg("--diagnostics=json")
                .env("RUST_LOG", "off"),
        )?;
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_cli_argument");
        assert_eq!(record["argument_kind"], kind, "{args:?}: {record}");
        assert_eq!(record["argument"], argument, "{args:?}: {record}");
        assert!(
            record["expected"].as_str().unwrap().starts_with(expected),
            "{record}"
        );
        assert!(
            record["hint"].as_str().unwrap().starts_with(hint),
            "{record}"
        );
        assert_eq!(record["recovery"], "fix_input");
        assert_eq!(record["retryable"], false);

        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(&args)
                .env("RUST_LOG", "warn"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        let text = std::str::from_utf8(&output.stderr)?;
        assert!(text.contains(&format!("argument=\"{argument}\"")), "{text}");
        assert!(text.contains(expected), "{text}");
        assert!(text.contains(hint), "{text}");
        assert!(!text.contains("secret"), "{text}");
    }
    Ok(())
}

#[test]
fn unknown_arguments_offer_help_without_echoing_names() -> Result<(), Box<dyn std::error::Error>> {
    for args in [
        vec!["--secret-token=secret-value"],
        vec!["secret-command"],
        vec!["scan", "secret-table", "--limit-secret=1"],
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(&args)
                .arg("--diagnostics=json")
                .env("RUST_LOG", "trace"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert!(record.get("argument").is_none(), "{record}");
        assert!(record.get("expected").is_none(), "{record}");
        assert_eq!(
            record["hint"],
            "Use --help to choose a supported command or option."
        );
    }
    Ok(())
}

#[test]
fn failures_stay_json_with_tracing_off_on_or_invalid() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let malformed = directory.path().join("secret-malformed.json");
    fs::write(&malformed, b"{\"secret-token\":")?;
    let duplicate = directory.path().join("secret-duplicate.json");
    fs::write(&duplicate, br#"{"secret-key":"one","secret-key":"two"}"#)?;
    let configuration = directory.path().join("secret-configuration.json");
    fs::write(
        &configuration,
        br#"{"max_concurrent_file_reads_per_partition":0}"#,
    )?;
    let missing = directory.path().join("secret-missing.json");
    let table = partitioned_table_path();
    let cases = [
        (vec![], 2, "configuration", "invalid_cli_argument"),
        (
            vec!["--secret-argument"],
            2,
            "configuration",
            "invalid_cli_argument",
        ),
        (
            vec!["--diagnostics=secret-format", "inspect", "unused"],
            2,
            "configuration",
            "invalid_cli_argument",
        ),
        (
            vec!["secret-command"],
            2,
            "configuration",
            "invalid_cli_argument",
        ),
        (
            vec!["scan", "unused", "--limit", "secret-limit"],
            2,
            "configuration",
            "invalid_cli_argument",
        ),
        (
            vec!["scan", "unused", "--limit"],
            2,
            "configuration",
            "invalid_cli_argument",
        ),
        (
            vec![
                "inspect",
                "unused",
                "--storage-options-file",
                malformed.to_str().unwrap(),
            ],
            2,
            "configuration",
            "invalid_input_json",
        ),
        (
            vec![
                "inspect",
                "unused",
                "--storage-options-file",
                duplicate.to_str().unwrap(),
            ],
            2,
            "configuration",
            "invalid_input_json",
        ),
        (
            vec![
                "inspect",
                "unused",
                "--storage-options-file",
                missing.to_str().unwrap(),
            ],
            2,
            "configuration",
            "input_file_io",
        ),
        (
            vec![
                "scan",
                "unused",
                "--execution-options-file",
                configuration.to_str().unwrap(),
            ],
            2,
            "configuration",
            "invalid_configuration",
        ),
        (
            vec![
                "inspect",
                "unknown://secret-user:secret-password@host/secret-table?token=secret",
            ],
            1,
            "storage",
            "storage_initialization",
        ),
        (
            vec!["scan", table.to_str().unwrap(), "--column", "secret-column"],
            1,
            "scan_planning",
            "invalid_projection",
        ),
    ];
    for filter in ["off", "trace", "secret-invalid-filter=not-a-level"] {
        for (args, status, phase, code) in &cases {
            for position in [0, args.len()] {
                let mut args = args.clone();
                args.insert(position, "--diagnostics=json");
                let output = run_with_timeout(
                    Command::new(env!("CARGO_BIN_EXE_dar"))
                        .args(&args)
                        .env("RUST_LOG", filter),
                )?;
                assert_eq!(output.status.code(), Some(*status), "{args:?}: {output:?}");
                assert!(output.stdout.is_empty());
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["phase"], *phase);
                assert_eq!(record["code"], *code);
                assert!(record["reason"].is_string());
                if *code == "invalid_input_json" {
                    assert!(record["line"].is_number());
                    assert!(record["column"].is_number());
                }
            }
        }
    }
    Ok(())
}

#[test]
fn success_preserves_output_and_reports_actual_counts() -> Result<(), Box<dyn std::error::Error>> {
    let table = partitioned_table_path();
    let inspected = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .arg("inspect")
            .arg(&table),
    )?;
    let inspected: Value = serde_json::from_slice(&inspected.stdout)?;
    for args in [
        vec!["--help"],
        vec!["inspect", "--help"],
        vec!["--version"],
        vec!["inspect", table.to_str().unwrap()],
        vec!["scan", table.to_str().unwrap(), "--limit", "0"],
        vec![
            "scan",
            table.to_str().unwrap(),
            "--limit",
            "2",
            "--no-columns",
        ],
        vec!["scan", table.to_str().unwrap(), "--limit", "1000"],
        vec!["scan", table.to_str().unwrap()],
    ] {
        let baseline = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args(&args))?;
        assert!(baseline.status.success(), "{baseline:?}");
        for (position, filter) in [(0, "off"), (1, "trace"), (args.len(), "off")] {
            let mut diagnostic_args = args.clone();
            diagnostic_args.insert(position, "--diagnostics=json");
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(&diagnostic_args)
                    .env("RUST_LOG", filter),
            )?;
            assert!(output.status.success(), "{diagnostic_args:?}: {output:?}");
            let record = assert_single_terminal_record(&output.stderr, "completion");
            assert_eq!(record["output_bytes"], output.stdout.len().to_string());
            assert_eq!(record["completion"], "complete");
            if args[0] == "scan" {
                let batches = StreamReader::try_new(Cursor::new(&output.stdout), None)?
                    .collect::<Result<Vec<_>, _>>()?;
                let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
                assert_eq!(record["rows"], rows.to_string());
                assert_eq!(record["batches"], batches.len().to_string());
                let baseline = StreamReader::try_new(Cursor::new(&baseline.stdout), None)?
                    .collect::<Result<Vec<_>, _>>()?;
                assert_eq!(batches, baseline);
                if let Some(index) = args.iter().position(|arg| *arg == "--limit") {
                    assert_eq!(record["scope"], "limited_scan");
                    if rows == args[index + 1].parse::<usize>()? {
                        assert_eq!(record["stop_reason"], "row_limit");
                        assert_eq!(record["has_more"], Value::Null);
                    } else {
                        assert_eq!(record["stop_reason"], "end_of_input");
                        assert_eq!(record["has_more"], false);
                    }
                } else {
                    assert_eq!(record["scope"], "scan");
                    assert_eq!(record["has_more"], false);
                }
            } else {
                assert_eq!(output.stdout, baseline.stdout);
            }
            if !args.contains(&"--help") && matches!(args[0], "inspect" | "scan") {
                assert_eq!(record["bindings"][0]["alias"], "t");
                assert_eq!(
                    record["bindings"][0]["table_version"],
                    inspected["table_version"]
                );
            } else {
                assert_eq!(record["bindings"], serde_json::json!([]));
            }
        }
    }
    Ok(())
}

#[test]
fn wheel_command_accepts_global_diagnostics() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let executable = directory.path().join(format!(
        "delta-arrow-reader{}",
        std::env::consts::EXE_SUFFIX
    ));
    #[cfg(unix)]
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dar"), &executable)?;
    #[cfg(not(unix))]
    fs::copy(env!("CARGO_BIN_EXE_dar"), &executable)?;
    let output =
        run_with_timeout(Command::new(&executable).args(["--version", "--diagnostics=json"]))?;
    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        format!("delta-arrow-reader {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
    );
    assert_single_terminal_record(&output.stderr, "completion");
    Ok(())
}

#[test]
fn values_and_arguments_after_double_dash_do_not_enable_json()
-> Result<(), Box<dyn std::error::Error>> {
    for args in [
        vec!["inspect", "--", "--diagnostics=json"],
        vec!["inspect", "unused", "--", "--diagnostics=json"],
        vec![
            "inspect",
            "unused",
            "--storage-options-file=--diagnostics=json",
        ],
        vec![
            "scan",
            "unused",
            "--column=--diagnostics=json",
            "--limit",
            "secret-limit",
        ],
        vec!["--diagnostics", "json", "inspect", "unused"],
        vec!["--diagnostics=secret-format", "inspect", "unused"],
        vec!["--diagnostics=text", "inspect", "unused"],
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(&args)
                .env("RUST_LOG", "off"),
        )?;
        assert!(!output.status.success(), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
        assert!(output.stderr.is_empty(), "{args:?}: {output:?}");
    }
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .args([
                "inspect",
                "--diagnostics=json",
                "--",
                "secret-missing-table",
            ])
            .env("RUST_LOG", "off"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert_single_terminal_record(&output.stderr, "diagnostic");
    Ok(())
}

#[cfg(unix)]
#[test]
fn non_utf8_arguments_and_filters_do_not_escape_json() -> Result<(), Box<dyn std::error::Error>> {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .arg("inspect")
            .arg(OsString::from_vec(b"secret-table-\xff".to_vec()))
            .arg("--diagnostics=json")
            .env(
                "RUST_LOG",
                OsString::from_vec(b"secret-filter-\xff".to_vec()),
            ),
    )?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["argument_kind"], "InvalidUtf8");
    assert_eq!(record["hint"], "Use UTF-8 text for command-line arguments.");
    Ok(())
}

#[test]
fn closed_stdout_has_one_failure_and_no_completion() -> Result<(), Box<dyn std::error::Error>> {
    for command in ["inspect", "scan"] {
        let (reader, writer) = io::pipe()?;
        drop(reader);
        let output = run_with_stdout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .arg("--diagnostics=json")
                .arg(command)
                .arg(partitioned_table_path())
                .env("RUST_LOG", "off"),
            Stdio::from(writer),
        )?;
        assert_eq!(output.status.code(), Some(3), "{output:?}");
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "output_write");
        assert_eq!(record["source_kind"], "broken_pipe");
    }
    Ok(())
}

#[test]
fn data_file_failure_never_emits_completion() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let log = Path::new("_delta_log/00000000000000000000.json");
    fs::create_dir(directory.path().join("_delta_log"))?;
    // Keep the snapshot but omit its data files to fail after writing the IPC schema.
    fs::copy(
        partitioned_table_path().join(log),
        directory.path().join(log),
    )?;
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["--diagnostics=json", "scan"])
            .arg(directory.path())
            .env("RUST_LOG", "off"),
    )?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["phase"], "data_file_read");
    assert_eq!(record["code"], "data_file_read");
    assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
    StreamReader::try_new(Cursor::new(&output.stdout), None)?;
    Ok(())
}
