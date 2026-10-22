//! CLI output, diagnostics, argument validation, and reader integration tests.

#![allow(clippy::panic, clippy::unwrap_used)]

mod capabilities;
mod diagnostics;
#[cfg(unix)]
mod direct_credentials;
mod inspection;
#[cfg(unix)]
mod mock_cloud_storage;
mod predicates;
mod query;
#[cfg(unix)]
mod unity_catalog;

use std::{
    fs::{self, File},
    io::{self, Cursor, Read},
    path::Path,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use arrow::{
    compute::{concat_batches, sort_to_indices, take_record_batch},
    datatypes::{DataType, Fields, Schema},
    ipc::reader::{FileReader, StreamReader},
    record_batch::RecordBatch,
};
use delta_arrow_reader::{DeltaSnapshotSelection, DeltaTableBuilder, TryStreamExt, WarmupMode};
use serde_json::Value;

#[test]
fn executable_names_preserve_help_version_and_redacted_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new()
        .prefix("secret cli directory ")
        .tempdir()?;
    for (filename, command_name) in [
        ("dar", "dar"),
        ("delta-arrow-reader", "delta-arrow-reader"),
        ("secret-command", "dar"),
        ("delta-arrow-reader.secret", "dar"),
    ] {
        let executable = directory
            .path()
            .join(format!("{filename}{}", std::env::consts::EXE_SUFFIX));
        // Concurrent Unix forks can inherit a copied executable's open write handle.
        #[cfg(unix)]
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dar"), &executable)?;
        #[cfg(not(unix))]
        fs::copy(env!("CARGO_BIN_EXE_dar"), &executable)?;
        for flag in ["--version", "-V"] {
            let output = run_with_timeout(
                Command::new(&executable)
                    .current_dir(directory.path())
                    .arg(flag),
            )?;
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            assert_eq!(
                output.stdout,
                format!("{command_name} {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
            );
        }
        for (args, subcommand) in [
            (vec!["--help"], ""),
            (vec!["-h"], ""),
            (vec!["capabilities", "--help"], " capabilities"),
            (vec!["inspect", "--help"], " inspect"),
            (vec!["help", "inspect"], " inspect"),
            (vec!["scan", "--help"], " scan"),
            (vec!["query", "--help"], " query"),
            (vec!["help", "query"], " query"),
            (
                vec!["scan", "--storage-options-file", "secret-missing", "--help"],
                " scan",
            ),
        ] {
            let output = run_with_timeout(
                Command::new(&executable)
                    .current_dir(directory.path())
                    .args(args),
            )?;
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            let help = String::from_utf8(output.stdout)?;
            assert!(help.contains(&format!("Usage: {command_name}{subcommand} ")));
            assert!(!help.contains("secret"), "{help}");
        }
        let discovery = run_with_timeout(Command::new(&executable).arg("capabilities"))?;
        assert!(discovery.status.success(), "{discovery:?}");
        assert!(discovery.stdout.len() <= 2048);
        let index: Value = serde_json::from_slice(&discovery.stdout)?;
        for command in index["commands"].as_array().unwrap() {
            assert_eq!(command["details"][0], command_name);
        }
        assert_eq!(index["schema_command"][0], command_name);
        assert_eq!(index["all_command"][0], command_name);
        assert!(!String::from_utf8_lossy(&discovery.stdout).contains("secret"));
        let output = run_with_timeout(
            Command::new(&executable)
                .current_dir(directory.path())
                .arg("--secret-argument"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let diagnostic = check_failure(&output.stderr, "configuration", "invalid_cli_argument");
        assert!(diagnostic.contains("argument_kind=UnknownArgument"));
        fs::remove_file(executable)?;
    }
    Ok(())
}

#[test]
fn argument_failures_keep_distinct_safe_kinds() -> Result<(), Box<dyn std::error::Error>> {
    for (arguments, kind) in [
        (vec!["--secret-option"], "UnknownArgument"),
        (vec!["inspect"], "MissingRequiredArgument"),
        (vec!["secret-command"], "InvalidSubcommand"),
        (
            vec!["scan", "secret-table", "--limit", "secret-value"],
            "ValueValidation",
        ),
        (
            vec![
                "scan",
                "secret-table",
                "--no-columns",
                "--column",
                "secret-column",
            ],
            "ArgumentConflict",
        ),
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args(arguments))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let diagnostic = check_failure(&output.stderr, "configuration", "invalid_cli_argument");
        assert!(
            diagnostic.contains(&format!("argument_kind={kind}")),
            "{diagnostic}"
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn json_input_failures_keep_distinct_safe_causes() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new().prefix("secret-inputs").tempdir()?;
    let path = directory.path().join("secret-input.json");
    let mut oversized = br#"{"secret-key":"secret-value"}"#.to_vec();
    oversized.resize(1024 * 1024 + 1, b' ');
    for option in [
        "--storage-options-file",
        "--predicate-file",
        "--execution-options-file",
    ] {
        for (contents, detail, hint) in [
            (
                b"!secret-value".as_slice(),
                "kind=Syntax",
                "Fix JSON syntax",
            ),
            (
                br#""secret-value""#.as_slice(),
                "kind=Data",
                "expected format",
            ),
            (b"{".as_slice(), "kind=Eof", "Complete the JSON document"),
            (
                oversized.as_slice(),
                "reason=input_file_too_large",
                "Keep the JSON input at or below 1 MiB.",
            ),
        ] {
            fs::write(&path, contents)?;
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(["scan", option])
                    .arg(&path)
                    .arg("secret-missing-table")
                    .env("RUST_LOG", "trace"),
            )?;
            assert_eq!(output.status.code(), Some(2), "{output:?}");
            assert!(output.stdout.is_empty());
            let diagnostic = check_failure(&output.stderr, "configuration", "invalid_input_json");
            let field = if detail.starts_with("kind=") {
                format!("json_{detail}")
            } else {
                format!("error_reason=\"{}\"", detail.trim_start_matches("reason="))
            };
            assert!(
                diagnostic.contains(&field),
                "{option}, expected {field}: {diagnostic}"
            );
            assert!(
                diagnostic.contains(&format!("argument=\"{option}\"")),
                "{diagnostic}"
            );
            assert!(diagnostic.contains(hint), "{diagnostic}");
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));

            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(["scan", option])
                    .arg(&path)
                    .args(["secret-missing-table", "--diagnostics=json"])
                    .env("RUST_LOG", "off"),
            )?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let record = diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["argument"], option);
            assert_eq!(record["code"], "invalid_input_json");
            assert_eq!(record["recovery"], "fix_input");
            assert_eq!(record["retryable"], false);
            assert!(record["expected"].is_string());
            assert!(record["hint"].as_str().unwrap().contains(hint), "{record}");
            if detail == "kind=Data" {
                assert_eq!(record["field_path"], "");
            } else {
                assert!(record.get("field_path").is_none(), "{record}");
            }
            if let Some(kind) = detail.strip_prefix("kind=") {
                assert_eq!(record["json_kind"], kind);
                assert!(record["line"].as_u64().unwrap() > 0);
                assert!(record["column"].as_u64().unwrap() > 0);
            } else {
                assert_eq!(record["reason"], "input_file_too_large");
                assert!(record.get("line").is_none());
            }
        }
    }
    Ok(())
}

#[test]
fn predicate_validation_reasons_survive_json_decoding() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("secret-predicate.json");
    let leaf = r#"{"op":"is_null","column":"secret-column"}"#;
    let oversized_float =
        r#"{"op":"eq","column":"secret-column","value":{"type":"float32","value":1e100}}"#;
    let wrong_binary_size = r#"{"op":"eq","column":"secret-column","value":{"type":"fixed_size_binary","size":2,"value":[1]}}"#;
    let nested = |predicate: &str| {
        format!(
            r#"{{"op":"and","args":[{leaf},{{"op":"not","arg":{{"op":"or","args":[{leaf},{predicate}]}}}}]}}"#
        )
    };
    for (predicate, reason, field_path, expected) in [
        (
            format!(
                "{}{leaf}{}",
                r#"{"op":"not","arg":"#.repeat(32),
                "}".repeat(32)
            ),
            "predicate_depth_limit_exceeded",
            "/arg".repeat(32),
            "32 levels",
        ),
        (
            format!(r#"{{"op":"and","args":[{}]}}"#, [leaf; 1024].join(",")),
            "predicate_node_limit_exceeded",
            "/args/1023".into(),
            "1024 predicate nodes",
        ),
        (
            oversized_float.into(),
            "predicate_non_finite_float",
            "/value/value".into(),
            "finite after conversion to float32",
        ),
        (
            wrong_binary_size.into(),
            "predicate_binary_size_mismatch",
            "/value/size".into(),
            "byte array length",
        ),
        (
            nested(oversized_float),
            "predicate_non_finite_float",
            "/args/1/arg/args/1/value/value".into(),
            "finite after conversion to float32",
        ),
        (
            nested(wrong_binary_size),
            "predicate_binary_size_mismatch",
            "/args/1/arg/args/1/value/size".into(),
            "byte array length",
        ),
    ] {
        let input: Value = serde_json::from_str(&predicate)?;
        assert!(input.pointer(&field_path).is_some(), "{field_path}");
        fs::write(&path, predicate)?;
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(["scan", "--predicate-file"])
                .arg(&path)
                .arg("secret-missing-table"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let diagnostic = check_failure(&output.stderr, "configuration", "invalid_input_json");
        assert!(
            diagnostic.contains(&format!("error_reason=\"{reason}\"")),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("argument=\"--predicate-file\""),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains(&format!("field_path=\"{field_path}\"")),
            "{diagnostic}"
        );
        assert!(diagnostic.contains(expected), "{diagnostic}");
        assert!(diagnostic.contains("hint="), "{diagnostic}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));

        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(["scan", "--predicate-file"])
                .arg(&path)
                .args(["secret-missing-table", "--diagnostics=json"])
                .env("RUST_LOG", "off"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_input_json");
        assert_eq!(record["reason"], reason);
        assert_eq!(record["argument"], "--predicate-file");
        assert_eq!(record["field_path"], field_path);
        assert!(
            record["expected"].as_str().unwrap().contains(expected),
            "{record}"
        );
        assert_eq!(record["recovery"], "fix_input");
        assert_eq!(record["retryable"], false);
        assert!(record.get("line").is_none());
    }
    Ok(())
}

#[test]
fn external_writer_schemas_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus");
    for (name, version) in [
        ("partitioned", "0"),
        ("nested_mapping", "1"),
        ("deletion_vectors", "1"),
    ] {
        let fixture = corpus.join(name);
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .arg("inspect")
                .arg(fixture.join("table")),
        )?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout)?;
        capabilities::assert_output_matches_schema("inspection", &value);
        assert_eq!(value["table_version"], version);
        let schema: Schema = serde_json::from_value(value["schema"].clone())?;
        let expected =
            FileReader::try_new(File::open(fixture.join("expected.arrow"))?, None)?.schema();
        // Spark's IPC export omits Delta column-mapping metadata. Check the
        // complete logical types against IPC and metadata against Spark's log.
        let mut delta_schema = Value::Null;
        for version in 0..=version.parse::<u64>()? {
            let log =
                fs::read_to_string(fixture.join(format!("table/_delta_log/{version:020}.json")))?;
            for line in log.lines() {
                let action: Value = serde_json::from_str(line)?;
                if let Some(schema) = action["metaData"]["schemaString"].as_str() {
                    delta_schema = serde_json::from_str(schema)?;
                }
            }
        }
        assert_fixture_schema_fields(
            schema.fields(),
            expected.fields(),
            delta_schema["fields"].as_array().unwrap(),
        );
        assert_eq!(schema.metadata(), expected.metadata());
    }
    Ok(())
}

#[test]
fn scans_match_core_for_projection_limits_and_versions() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        for name in ["partitioned", "nested_mapping", "deletion_vectors"] {
            let path = corpus.join(name).join("table");
            for version in [None, Some(0)] {
                let table = DeltaTableBuilder::new(path.to_str().unwrap())
                    .with_warmup(WarmupMode::None)
                    .with_snapshot_selection(version.map_or(
                        DeltaSnapshotSelection::Latest,
                        DeltaSnapshotSelection::Version,
                    ))
                    .load_table()
                    .await?;
                let projections: [Option<&[&str]>; 3] = [None, Some(&["value", "id"]), Some(&[])];
                for columns in projections {
                    let mut builder = table.scan();
                    if let Some(columns) = columns {
                        builder = builder.with_projection(columns.iter().copied());
                    }
                    let scan = builder.build().await?;
                    let schema = scan.schema();
                    let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
                    let expected = concat_batches(&schema, &batches)?;
                    for limit in [None, Some(0), Some(1), Some(1_000)] {
                        let context = format!(
                            "{name} version={version:?} columns={columns:?} limit={limit:?}"
                        );
                        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
                        command.arg("scan").arg(&path);
                        if let Some(version) = version {
                            command.args(["--table-version", &version.to_string()]);
                        }
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
                        let output = run_with_timeout(&mut command)?;
                        assert!(
                            output.status.success(),
                            "{context}: {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        assert!(output.stderr.is_empty(), "{context}");
                        assert!(
                            output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]),
                            "missing IPC end marker: {context}"
                        );
                        let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
                        assert_eq!(reader.schema(), schema, "{context}");
                        let actual =
                            concat_batches(&schema, &reader.collect::<Result<Vec<_>, _>>()?)?;
                        assert_eq!(
                            actual.num_rows(),
                            expected.num_rows().min(limit.unwrap_or(usize::MAX)),
                            "{context}"
                        );
                        if actual.num_columns() == 0 || actual.num_rows() == 0 {
                            continue;
                        }
                        // The core does not promise row order. A one-row limit must
                        // return a row from the full scan; otherwise compare all rows.
                        if limit == Some(1) {
                            assert!(
                                (0..expected.num_rows())
                                    .any(|row| actual == expected.slice(row, 1)),
                                "{context}"
                            );
                        } else {
                            assert_eq!(sort_by_id(&actual)?, sort_by_id(&expected)?, "{context}");
                        }
                    }
                }
            }
        }
        Ok(())
    });
    runtime.shutdown_background();
    result
}

fn sort_by_id(batch: &RecordBatch) -> Result<RecordBatch, arrow::error::ArrowError> {
    let indices = sort_to_indices(batch.column_by_name("id").unwrap(), None, None)?;
    take_record_batch(batch, &indices)
}

fn run_with_timeout(command: &mut Command) -> io::Result<Output> {
    run_with_stdout(command, Stdio::piped())
}

fn run_with_stdout(command: &mut Command, stdout: Stdio) -> io::Result<Output> {
    // Shell logging settings must not change a fixture's expected stderr.
    if !command.get_envs().any(|(key, _)| key == "RUST_LOG") {
        command.env_remove("RUST_LOG");
    }
    let mut child = command.stdout(stdout).stderr(Stdio::piped()).spawn()?;
    let stdout = child.stdout.take();
    let mut stderr = child.stderr.take().unwrap();
    // Drain both pipes while waiting so a full pipe cannot block the child.
    thread::scope(|scope| {
        let stdout = scope.spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut stdout) = stdout {
                stdout.read_to_end(&mut bytes)?;
            }
            Ok::<_, io::Error>(bytes)
        });
        let stderr = scope.spawn(move || {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Ok(status),
                Err(error) => break Err(error),
                Ok(None) if Instant::now() >= deadline => {
                    break Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "CLI fixture timed out",
                    ));
                }
                Ok(None) => thread::sleep(Duration::from_millis(10)),
            }
        };
        if status.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        Ok(Output {
            status: status?,
            stdout: stdout.join().unwrap()?,
            stderr: stderr.join().unwrap()?,
        })
    })
}

fn assert_fixture_schema_fields(actual: &Fields, oracle: &Fields, delta: &[Value]) {
    assert_eq!(actual.len(), oracle.len());
    assert_eq!(actual.len(), delta.len());
    for ((actual, expected), delta) in actual.iter().zip(oracle).zip(delta) {
        assert_eq!(actual.name(), expected.name());
        assert_eq!(actual.is_nullable(), expected.is_nullable());
        let metadata = delta["metadata"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string()),
                )
            })
            .collect();
        assert_eq!(actual.metadata(), &metadata);
        match (actual.data_type(), expected.data_type()) {
            (DataType::Struct(actual), DataType::Struct(expected)) => assert_fixture_schema_fields(
                actual,
                expected,
                delta["type"]["fields"].as_array().unwrap(),
            ),
            (actual, expected) => assert_eq!(actual, expected),
        }
    }
}

fn check_failure<'a>(stderr: &'a [u8], phase: &str, code: &str) -> &'a str {
    let text = std::str::from_utf8(stderr).unwrap();
    assert!(text.ends_with('\n'), "{text}");
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(text.starts_with("ERROR delta_arrow_reader"), "{text}");
    for field in [
        format!("error_phase=\"{phase}\""),
        format!("error_code=\"{code}\""),
        "outcome=\"failed\"".into(),
    ] {
        assert!(
            text.split_whitespace().any(|token| token == field),
            "missing {field}: {text}"
        );
    }
    assert!(text.contains("error_reason=\""), "{text}");
    assert!(!text.contains("secret"), "{text}");
    text
}

#[test]
fn verbosity_preserves_stdout_and_core_failure_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let table = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    );
    for command in ["inspect", "scan"] {
        let mut args = vec![command];
        if command == "scan" {
            args.extend(["--limit", "0"]);
        }
        let baseline = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(&args)
                .arg(&table)
                .env_remove("RUST_LOG"),
        )?;
        assert!(baseline.status.success(), "{baseline:?}");
        assert!(baseline.stderr.is_empty());
        for level in [
            "error",
            "warn",
            "info",
            "debug",
            "trace",
            "delta_arrow_reader=debug",
            "off",
        ] {
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(&args)
                    .arg(&table)
                    .env("RUST_LOG", level),
            )?;
            assert!(output.status.success(), "{output:?}");
            assert_eq!(output.stdout, baseline.stdout, "{command}, {level}");
            let text = String::from_utf8(output.stderr)?;
            assert!(!text.contains("secret"), "{text}");
            if matches!(level, "error" | "warn" | "off") {
                assert!(text.is_empty(), "{text}");
            } else {
                assert!(text.contains("table_load.completed"), "{text}");
                if command == "scan" {
                    assert!(text.contains("scan_execution.completed"), "{text}");
                }
                if level.contains("debug") || level == "trace" {
                    assert!(text.contains("DEBUG"), "{text}");
                }
            }
        }
    }
    for (args, event, code) in [
        (
            vec![
                "inspect",
                "unknown://secret-user:secret-password@host/secret-table?token=secret-token",
            ],
            "table_load.failed",
            "storage_initialization",
        ),
        (
            vec!["scan", table.to_str().unwrap(), "--column", "secret-column"],
            "scan_planning.failed",
            "invalid_projection",
        ),
    ] {
        for level in ["warn", "info", "debug", "trace"] {
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(&args)
                    .env("RUST_LOG", level),
            )?;
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert!(output.stdout.is_empty());
            let text = String::from_utf8(output.stderr)?;
            assert!(!text.contains("secret"), "{text}");
            let failures: Vec<_> = text
                .lines()
                .filter(|line| line.starts_with("ERROR ") && line.contains("outcome=\"failed\""))
                .collect();
            assert_eq!(failures.len(), 1, "{text}");
            let failure = failures[0];
            for field in [
                format!("event=\"{event}\""),
                format!("error_code=\"{code}\""),
                "operation_id=".into(),
                "error_reason=".into(),
            ] {
                assert!(failure.contains(&field), "{field}: {text}");
            }
            assert!(!failure.contains("cli.failed"), "{text}");
            let corrections = text
                .lines()
                .filter(|line| line.contains("event=\"cli.input_correction\""))
                .count();
            assert_eq!(
                corrections,
                usize::from(code == "invalid_projection"),
                "{text}"
            );
        }
    }
    Ok(())
}

#[test]
fn invalid_filter_is_redacted_and_falls_back_without_affecting_stdout()
-> Result<(), Box<dyn std::error::Error>> {
    for filter in [
        "delta_arrow_reader=secret-invalid-level",
        "trace=debug=secret",
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .arg("--version")
                .env("RUST_LOG", filter),
        )?;
        assert!(output.status.success());
        assert_eq!(
            output.stdout,
            format!("dar {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
        );
        let text = String::from_utf8(output.stderr)?;
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(text.trim_start().starts_with("WARN "), "{text}");
        assert!(text.contains("invalid_rust_log"), "{text}");
        assert!(!text.contains("secret"), "{text}");
    }
    Ok(())
}
