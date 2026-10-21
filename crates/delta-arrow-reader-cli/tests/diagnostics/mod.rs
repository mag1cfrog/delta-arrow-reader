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
    super::capabilities::assert_output_matches_schema(kind, &record);
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
fn input_schema_corrections_omit_unknown_keys_and_values() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("secret-input.json");
    for (argument, inputs) in [
        (
            "--storage-options-file",
            &[
                (
                    r#"{"secret-key":null}"#,
                    "",
                    "string storage option",
                    "expected format",
                ),
                (
                    r#"{"secret-key":"secret-value","secret-key":"secret-duplicate"}"#,
                    "",
                    "unique object keys",
                    "duplicate",
                ),
                (
                    r#"{"op":"secret-value","op":"secret-duplicate"}"#,
                    "",
                    "unique object keys",
                    "duplicate",
                ),
                (
                    r#"{"secret-key":"secret-value","op":{"secret-nested-key":false}}"#,
                    "",
                    "string storage option",
                    "expected format",
                ),
            ][..],
        ),
        (
            "--predicate-file",
            &[
                (
                    r#"{"op":"secret-operator","secret-key":"secret-value"}"#,
                    "/op",
                    "one of: constant, eq",
                    "listed values",
                ),
                (
                    r#"{"op":["secret-operator"]}"#,
                    "/op",
                    "one of: constant, eq",
                    "listed values",
                ),
                (
                    r#"{"value":true}"#,
                    "/op",
                    "one of: constant, eq",
                    "missing field",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"secret-type","value":"secret-value"}}"#,
                    "/value/type",
                    "one of: boolean, int8",
                    "listed values",
                ),
                (
                    r#"{"op":"constant","value":true,"secret-key":"secret-value"}"#,
                    "",
                    "object fields: op, value",
                    "unsupported fields",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"int32","value":"secret-value"}}"#,
                    "/value/value",
                    "canonical decimal integer string",
                    "inspected column",
                ),
                (
                    r#"{"op":"constant","value":true,"value":false}"#,
                    "/value",
                    "one occurrence",
                    "duplicate field",
                ),
                (
                    r#"{"op":"constant","value":true,"\u0076alue":false}"#,
                    "/value",
                    "one occurrence",
                    "duplicate field",
                ),
                (
                    r#"{"op":"constant","op":"constant","value":true}"#,
                    "/op",
                    "one occurrence",
                    "duplicate field",
                ),
                (
                    r#"{"op":"constant"}"#,
                    "/value",
                    "required field",
                    "missing field",
                ),
                (
                    r#"{"op":"not","arg":{"op":"and","args":[{"op":"constant","value":true},{"op":"eq","column":"secret-column","value":{"type":"secret-type","value":"secret-value"}}]}}"#,
                    "/arg/args/1/value/type",
                    "one of: boolean",
                    "listed values",
                ),
                (
                    r#"{"op":"not","arg":{"op":"constant","value":true,"secret-key":"secret-value"}}"#,
                    "/arg",
                    "object fields: op, value",
                    "unsupported fields",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"binary","value":[0,256]}}"#,
                    "/value/value/1",
                    "from 0 to 255",
                    "expected format",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"timestamp_us","value":"0","timezone":""}}"#,
                    "/value/timezone",
                    "nonempty JSON string",
                    "timezone string",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"decimal128","value":"1","precision":2.0,"scale":0}}"#,
                    "/value/precision",
                    "from 0 to 255",
                    "expected format",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"decimal128","value":"1","precision":2,"scale":128}}"#,
                    "/value/scale",
                    "from -128 to 127",
                    "expected format",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"fixed_size_binary","value":[0],"size":2147483648}}"#,
                    "/value/size",
                    "fitting i32",
                    "expected format",
                ),
                (
                    r#"{"op":"not","arg":{"op":"eq","column":"secret-column","value":{"type":"binary","value":[0,255,"secret-byte"]}}}"#,
                    "/arg/value/value/2",
                    "from 0 to 255",
                    "expected format",
                ),
                (
                    r#"{"op":"eq","column":"secret-column","value":{"type":"int8","value":"1","secret/key~\n":false}}"#,
                    "/value",
                    "object fields: type, value",
                    "unsupported fields",
                ),
                (
                    r#"{"op":"not","arg":{"op":"eq","column":"secret-column","value":{"type":"int8","value":"1","\u0076alue":"secret-value"}}}"#,
                    "/arg/value/value",
                    "one occurrence",
                    "duplicate field",
                ),
            ][..],
        ),
        (
            "--execution-options-file",
            &[
                (
                    r#"{"secret-key":"secret-value"}"#,
                    "",
                    "object fields: parquet_backend",
                    "unsupported fields",
                ),
                (
                    r#"{"parquet_backend":"secret-value"}"#,
                    "/parquet_backend",
                    "direct, delta_kernel",
                    "listed values",
                ),
                (
                    r#"{"parquet_backend":"direct","parquet_backend":"secret-value"}"#,
                    "/parquet_backend",
                    "one occurrence",
                    "duplicate field",
                ),
                (
                    r#"{"prefetch_files_per_partition":1.0}"#,
                    "/prefetch_files_per_partition",
                    "JSON integer from 0",
                    "expected format",
                ),
                (
                    r#"{"max_concurrent_file_reads_per_scan":"1"}"#,
                    "/max_concurrent_file_reads_per_scan",
                    "JSON integer from 1",
                    "expected format",
                ),
                (
                    r#"{"parquet_metadata_size_hint_bytes":-0}"#,
                    "/parquet_metadata_size_hint_bytes",
                    "JSON integer from 1",
                    "expected format",
                ),
                (
                    r#"{"prefetch_files_per_partition":1,"secret/key~\n":{"secret-nested-key":false}}"#,
                    "",
                    "object fields: parquet_backend",
                    "unsupported fields",
                ),
                (
                    r#"{"prefetch_files_per_partition":1,"\u0070refetch_files_per_partition":"secret-value"}"#,
                    "/prefetch_files_per_partition",
                    "one occurrence",
                    "duplicate field",
                ),
            ][..],
        ),
    ] {
        for (input, field_path, expected, hint) in inputs {
            fs::write(&path, input)?;
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(["scan", "secret-table", argument])
                    .arg(&path)
                    .arg("--diagnostics=json")
                    .env("RUST_LOG", "trace"),
            )?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["argument"], argument);
            assert_eq!(record["json_kind"], "Data");
            assert_eq!(record["code"], "invalid_input_json");
            assert_eq!(record["reason"], "input_json_decode_failed");
            assert_eq!(record["field_path"], *field_path);
            assert_eq!(record["recovery"], "fix_input");
            assert_eq!(record["retryable"], false);
            assert!(
                record["expected"].as_str().unwrap().contains(expected),
                "{record}"
            );
            assert!(record["hint"].as_str().unwrap().contains(hint), "{record}");
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(["scan", "secret-table", argument])
                    .arg(&path)
                    .env("RUST_LOG", "trace"),
            )?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let text = std::str::from_utf8(&output.stderr)?;
            assert!(
                text.contains(&format!("field_path=\"{field_path}\"")),
                "{text}"
            );
            assert!(text.contains(expected) && text.contains(hint), "{text}");
            assert!(!text.contains("secret"), "{text}");
        }
    }
    Ok(())
}

#[test]
fn schema_corrections_stay_bounded_for_large_or_incomplete_input()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("secret-input.json");
    let long_key = format!("secret\n{}", "secret".repeat(10_000));
    let long_value = "secret".repeat(10_000);
    let nested = format!("{}0{}", "[".repeat(10_000), "]".repeat(10_000));
    for (argument, input, field_path) in [
        (
            "--predicate-file",
            serde_json::json!({"op":"constant", "value":true, long_key:"secret-value"}).to_string(),
            Some(""),
        ),
        (
            "--predicate-file",
            serde_json::json!({"op":long_value}).to_string(),
            Some("/op"),
        ),
        (
            "--predicate-file",
            format!(r#"{{"op":"secret-operator","secret-key":{nested}}}"#),
            Some("/op"),
        ),
        (
            "--predicate-file",
            r#"{"op":"secret-operator","secret-key":}"#.into(),
            None,
        ),
        (
            "--execution-options-file",
            format!(r#"{{"prefetch_files_per_partition":{nested}}}"#),
            None,
        ),
        (
            "--execution-options-file",
            format!(r#"{{"secret-key":{nested}}}"#),
            None,
        ),
        (
            "--storage-options-file",
            format!(r#"{{"secret-key":{nested}}}"#),
            None,
        ),
    ] {
        fs::write(&path, input)?;
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args(["scan", "secret-table", argument])
                .arg(&path)
                .arg("--diagnostics=json")
                .env("RUST_LOG", "trace"),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["argument"], argument);
        assert_eq!(record["code"], "invalid_input_json");
        assert_eq!(record["field_path"].as_str(), field_path);
    }
    Ok(())
}

#[test]
fn reader_input_errors_include_safe_corrections() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("secret-predicate.json");
    let table = partitioned_table_path();
    for (args, predicate, code, reason, argument, expected, hint) in [
        (
            vec!["--column", "secret-column"],
            None,
            "invalid_projection",
            "column_not_found",
            "--column",
            "logical column",
            "schema.fields",
        ),
        (
            vec!["--column", "id", "--column", "id"],
            None,
            "invalid_projection",
            "duplicate_column",
            "--column",
            "at most once",
            "Remove repeated",
        ),
        (
            vec!["--predicate-file", path.to_str().unwrap()],
            Some(r#"{"op":"and","args":[{"op":"is_null","column":"secret-column"}]}"#),
            "unsupported_predicate",
            "column_not_found",
            "--predicate-file",
            "unambiguous",
            "schema.fields",
        ),
        (
            vec!["--predicate-file", path.to_str().unwrap()],
            Some(r#"{"op":"is_null","column":"secret.column"}"#),
            "unsupported_predicate",
            "invalid_column_reference",
            "--predicate-file",
            "without dots",
            "top-level column",
        ),
        (
            vec!["--predicate-file", path.to_str().unwrap()],
            Some(r#"{"op":"eq","column":"id","value":{"type":"utf8","value":"secret-literal"}}"#),
            "unsupported_predicate",
            "scalar_type_mismatch",
            "--predicate-file",
            "int32",
            "Match the scalar type",
        ),
        (
            vec!["--predicate-file", path.to_str().unwrap()],
            Some(
                r#"{"op":"eq","column":"id","value":{"type":"decimal128","value":"123456789123456789","precision":2,"scale":0}}"#,
            ),
            "unsupported_predicate",
            "invalid_decimal",
            "--predicate-file",
            "declared precision",
            "unscaled value",
        ),
    ] {
        if let Some(predicate) = predicate {
            fs::write(&path, predicate)?;
        }
        for filter in ["off", "trace"] {
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .arg("scan")
                    .arg(&table)
                    .args(&args)
                    .arg("--diagnostics=json")
                    .env("RUST_LOG", filter),
            )?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["phase"], "scan_planning");
            assert_eq!(record["code"], code);
            assert_eq!(record["reason"], reason);
            assert_eq!(record["argument"], argument);
            assert_eq!(record["recovery"], "fix_input");
            assert_eq!(record["retryable"], false);
            assert!(
                record["expected"].as_str().unwrap().contains(expected),
                "{record}"
            );
            assert!(record["hint"].as_str().unwrap().contains(hint), "{record}");
            if reason == "scalar_type_mismatch" {
                assert_eq!(record["field_path"], "/value/type");
                assert_eq!(record["supplied"], "utf8");
            } else {
                assert!(record.get("field_path").is_none(), "{record}");
                assert!(record.get("supplied").is_none(), "{record}");
            }
            assert!(!record.to_string().contains("123456789123456789"));
        }
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .arg("scan")
                .arg(&table)
                .args(&args)
                .env("RUST_LOG", "error"),
        )?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let text = std::str::from_utf8(&output.stderr)?;
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("event=\"scan_planning.failed\""))
                .count(),
            1,
            "{text}"
        );
        let corrections: Vec<_> = text
            .lines()
            .filter(|line| line.contains("event=\"cli.input_correction\""))
            .collect();
        assert_eq!(corrections.len(), 1, "{text}");
        assert!(
            corrections[0].contains(&format!("argument=\"{argument}\"")),
            "{text}"
        );
        assert!(corrections[0].contains(expected), "{text}");
        assert!(corrections[0].contains(hint), "{text}");
        if reason == "scalar_type_mismatch" {
            assert!(corrections[0].contains("supplied=\"utf8\""), "{text}");
            assert!(
                corrections[0].contains("field_path=\"/value/type\""),
                "{text}"
            );
        }
        assert!(!text.contains("secret"), "{text}");
        assert!(!text.contains("123456789123456789"), "{text}");
        assert!(!text.contains("cli.failed"), "{text}");
    }
    Ok(())
}

#[test]
fn scalar_type_diagnostics_allow_root_and_nested_predicates_to_be_repaired()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("secret-predicate.json");
    let comparison =
        serde_json::json!({"op":"ge","column":"id","value":{"type":"int64","value":"5"}});
    for (mut predicate, field_path) in [
        (comparison.clone(), "/value/type"),
        (
            serde_json::json!({"op":"and","args":[{"op":"constant","value":true},
            {"op":"or","args":[{"op":"constant","value":false},
                {"op":"not","arg":{"op":"not","arg":comparison}}]}]}),
            "/args/1/args/1/arg/arg/value/type",
        ),
    ] {
        let run = || {
            run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .arg("scan")
                    .arg(partitioned_table_path())
                    .args(["--table-version", "0", "--column", "id", "--predicate-file"])
                    .arg(&path)
                    .arg("--diagnostics=json")
                    .env("RUST_LOG", "off"),
            )
        };
        fs::write(&path, serde_json::to_vec(&predicate)?)?;
        let rejected = run()?;
        assert_eq!(rejected.status.code(), Some(1));
        assert!(rejected.stdout.is_empty());
        let error = assert_single_terminal_record(&rejected.stderr, "diagnostic");
        assert_eq!(error["field_path"], field_path);
        assert_eq!(error["expected"], "int32");
        assert_eq!(error["supplied"], "int64");
        *predicate
            .pointer_mut(error["field_path"].as_str().unwrap())
            .unwrap() = error["expected"].clone();
        fs::write(&path, serde_json::to_vec(&predicate)?)?;
        let repaired = run()?;
        assert!(repaired.status.success(), "{repaired:?}");
        let completion = assert_single_terminal_record(&repaired.stderr, "completion");
        assert_eq!(completion["bindings"][0]["table_version"], "0");
        assert_eq!(completion["has_more"], false);
        assert_eq!(completion["rows"], "356");
        let mut ids = Vec::new();
        for batch in StreamReader::try_new(Cursor::new(repaired.stdout), None)? {
            let batch = batch?;
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            ids.extend(column.values().iter().copied());
        }
        ids.sort_unstable();
        assert_eq!(ids, (5..=360).collect::<Vec<_>>());
    }
    Ok(())
}

#[test]
fn missing_databricks_executable_has_a_safe_json_recovery_hint()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for args in [
        vec!["inspect", "uc://secret_catalog.secret_schema.secret_table"],
        vec!["scan", "uc://secret_catalog.secret_schema.secret_table"],
    ] {
        for filter in ["off", "trace"] {
            let output = run_with_timeout(
                Command::new(env!("CARGO_BIN_EXE_dar"))
                    .args(&args)
                    .args([
                        "--databricks-profile",
                        "secret-profile",
                        "--diagnostics=json",
                    ])
                    .env("PATH", directory.path())
                    .env("RUST_LOG", filter),
            )?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["code"], "databricks_cli_missing");
            assert_eq!(record["recovery"], "fix_input");
            assert_eq!(record["retryable"], false);
            let hint = record["hint"].as_str().unwrap();
            assert!(
                hint.contains("databricks executable available on PATH"),
                "{record}"
            );
            assert!(hint.contains("same --databricks-profile"), "{record}");
        }
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
            vec!["query", "unused", "--sql-file", missing.to_str().unwrap()],
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
                if *code == "storage_initialization" {
                    for field in ["argument", "expected", "hint", "field_path"] {
                        assert!(record.get(field).is_none(), "{record}");
                    }
                }
                if let Some(argument) = args.iter().find(|argument| {
                    matches!(
                        **argument,
                        "--storage-options-file" | "--execution-options-file" | "--sql-file"
                    )
                }) {
                    assert_eq!(record["argument"], *argument);
                    assert!(record["expected"].is_string());
                    assert!(record["hint"].is_string());
                    match *code {
                        "input_file_io" => {
                            assert!(
                                record["hint"]
                                    .as_str()
                                    .unwrap()
                                    .contains("existing input file")
                            );
                            assert!(record.get("field_path").is_none());
                        }
                        "invalid_configuration" => {
                            assert_eq!(
                                record["field_path"],
                                "/max_concurrent_file_reads_per_partition"
                            );
                        }
                        _ => {}
                    }
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
