//! Exported files can be read on their own; optional manifests retain provenance.

use std::{
    fs,
    io::Cursor,
    path::Path,
    process::{Command, Output},
};

use arrow::ipc::reader::StreamReader;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    capabilities::assert_output_matches_schema, diagnostics::assert_single_terminal_record,
    query::fixture_path, run_with_timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn default_exports_need_only_the_result_file_and_preserve_unrelated_sidecars() -> TestResult {
    for command in ["scan", "query"] {
        for format in ["arrow", "json"] {
            if command == "scan" && format == "json" {
                continue;
            }
            let directory = tempfile::tempdir()?;
            fs::write(
                directory.path().join("rows.manifest.json"),
                b"unrelated file",
            )?;
            for name in ["rows", "other"] {
                let mut request = if command == "query" {
                    let mut request = query_command(directory.path(), "SELECT id FROM t LIMIT 3")?;
                    request.args(["--format", format]);
                    request
                } else {
                    let mut request = Command::new(env!("CARGO_BIN_EXE_dar"));
                    request
                        .current_dir(directory.path())
                        .args(["--diagnostics=json", "scan"])
                        .arg(fixture_path("partitioned").join("table"))
                        .args(["--column", "id", "--limit", "3"]);
                    request
                };
                let output = run_with_timeout(request.args(["--output", name]))?;
                assert!(output.status.success(), "{output:?}");
                let receipt: Value = serde_json::from_slice(&output.stdout)?;
                assert_output_matches_schema("result-artifact", &receipt);
                assert_eq!(
                    receipt,
                    json!({
                        "format_version": 2, "kind": "result_artifact",
                        "result": fs::canonicalize(directory.path())?.join(name),
                    })
                );
                let completion = assert_single_terminal_record(&output.stderr, "completion");
                assert_output_matches_schema("completion", &completion);
                assert_eq!(completion["format_version"], 3);
                let bytes = fs::read(receipt["result"].as_str().unwrap())?;
                if format == "json" {
                    let rows: Value = serde_json::from_slice(&bytes)?;
                    assert_eq!(rows["completion"], "complete");
                    assert_eq!(rows["rows"].as_array().unwrap().len(), 3);
                } else {
                    assert!(bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
                    let batches = StreamReader::try_new(Cursor::new(bytes), None)?
                        .collect::<Result<Vec<_>, _>>()?;
                    assert_eq!(
                        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        3
                    );
                }
            }
            assert_eq!(
                fs::read(directory.path().join("rows.manifest.json"))?,
                b"unrelated file"
            );
            assert!(!directory.path().join("other.manifest.json").exists());
        }
    }
    Ok(())
}

#[test]
fn manifest_requires_an_output_path_before_loading_inputs() -> TestResult {
    for command in ["scan", "query"] {
        let mut request = Command::new(env!("CARGO_BIN_EXE_dar"));
        request.args(["--diagnostics=json", command, "missing-table", "--manifest"]);
        if command == "query" {
            request.args(["--sql-file", "missing.sql"]);
        }
        let output = run_with_timeout(&mut request)?;
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["argument_kind"],
            "MissingRequiredArgument"
        );
    }
    Ok(())
}

fn query_command(directory: &Path, sql: &str) -> TestResult<Command> {
    fs::write(directory.join("query.sql"), sql)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .current_dir(directory)
        .args(["--diagnostics=json", "query"])
        .arg(fixture_path("partitioned").join("table"))
        .args(["--sql-file", "query.sql"]);
    Ok(command)
}

fn verify_result_and_manifest(output: &Output) -> TestResult<(Value, Vec<u8>)> {
    assert!(output.status.success(), "{output:?}");
    assert!(output.stdout.len() <= 8192);
    let receipt: Value = serde_json::from_slice(&output.stdout)?;
    assert_output_matches_schema("result-artifact", &receipt);
    assert_eq!(receipt.as_object().unwrap().len(), 4);
    let completion = assert_single_terminal_record(&output.stderr, "completion");
    assert_output_matches_schema("completion", &completion);
    assert_eq!(
        completion,
        json!({"format_version": 3, "kind": "completion", "command": completion["command"], "output_bytes": output.stdout.len().to_string()})
    );
    let result = fs::read(receipt["result"].as_str().unwrap())?;
    let manifest: Value =
        serde_json::from_slice(&fs::read(receipt["manifest"].as_str().unwrap())?)?;
    assert_output_matches_schema("result-manifest", &manifest);
    assert_eq!(manifest["result"]["path"], receipt["result"]);
    assert_eq!(
        manifest["result"]["sha256"],
        format!("{:x}", Sha256::digest(&result))
    );
    assert_eq!(manifest["output_bytes"], result.len().to_string());
    assert_eq!(manifest["completion"], "complete");
    assert_eq!(manifest["build"]["cli"], env!("CARGO_PKG_VERSION"));
    if manifest["result"]["format"] == "arrow" {
        assert!(result.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        let batches =
            StreamReader::try_new(Cursor::new(&result), None)?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(manifest["batches"], batches.len().to_string());
        assert_eq!(
            manifest["rows"],
            batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>()
                .to_string()
        );
    } else {
        let rows: Value = serde_json::from_slice(&result)?;
        assert_eq!(rows["completion"], "complete");
        assert_eq!(rows["row_count"], manifest["rows"]);
        assert_eq!(
            rows["rows"].as_array().unwrap().len().to_string(),
            manifest["rows"]
        );
        assert_eq!(rows["bindings"], manifest["bindings"]);
    }
    Ok((manifest, result))
}

#[test]
fn saved_query_formats_have_exact_counts_inputs_and_replay_versions() -> TestResult {
    let directory = tempfile::tempdir()?;
    let sql = "SELECT CAST(7 AS BIGINT) AS n FROM t LIMIT 3";
    for format in ["arrow", "json"] {
        let output = run_with_timeout(query_command(directory.path(), sql)?.args([
            "--format",
            format,
            "--manifest",
            "--output",
            &format!("rows.{format}"),
        ]))?;
        let (manifest, bytes) = verify_result_and_manifest(&output)?;
        assert_eq!(manifest["rows"], "3");
        assert_eq!(manifest["scope"], "query");
        assert_eq!(
            manifest["bindings"],
            json!([{"alias": "t", "table_version": "0"}])
        );
        assert_eq!(manifest["replay"]["options"]["table-version"], "0");
        assert_eq!(
            manifest["replay"]["inputs"],
            json!([{"argument": "--sql-file", "path": "query.sql", "sha256": format!("{:x}", Sha256::digest(sql.as_bytes()))}])
        );
        assert_eq!(
            manifest["replay"]["options"]["max-output-bytes"],
            if format == "json" {
                "16777216"
            } else {
                "1073741824"
            }
        );
        let replay = run_with_timeout(query_command(directory.path(), sql)?.args([
            "--format",
            format,
            "--table-version",
            manifest["bindings"][0]["table_version"].as_str().unwrap(),
            "--manifest",
            "--output",
            &format!("replay.{format}"),
        ]))?;
        let (_, replay_bytes) = verify_result_and_manifest(&replay)?;
        assert_eq!(bytes, replay_bytes);
        if format == "json" {
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes)?["rows"],
                json!([["7"], ["7"], ["7"]])
            );
        }
    }
    Ok(())
}

#[test]
fn limited_scan_file_retains_its_scope_and_zero_column_rows() -> TestResult {
    let directory = tempfile::tempdir()?;
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .current_dir(directory.path())
            .args(["--diagnostics=json", "scan"])
            .arg(fixture_path("partitioned").join("table"))
            .args([
                "--no-columns",
                "--limit",
                "3",
                "--manifest",
                "--output",
                "rows.arrow",
            ]),
    )?;
    let (manifest, bytes) = verify_result_and_manifest(&output)?;
    assert_eq!(manifest["scope"], "limited_scan");
    assert_eq!(manifest["has_more"], Value::Null);
    assert_eq!(manifest["stop_reason"], "row_limit");
    assert_eq!(manifest["rows"], "3");
    assert_eq!(
        StreamReader::try_new(Cursor::new(bytes), None)?
            .schema()
            .fields()
            .len(),
        0
    );
    assert_eq!(manifest["replay"]["options"]["no-columns"], true);
    Ok(())
}

#[test]
fn wide_and_large_json_files_do_not_expand_receipts_or_expose_sql_literals() -> TestResult {
    let directory = tempfile::tempdir()?;
    let private_literal = "private-sql-sentinel".repeat(1000);
    let columns = (0..1000)
        .map(|index| format!("1 AS column_{index}"))
        .collect::<Vec<_>>()
        .join(",");
    for (name, sql, rows) in [
        (
            "wide.json",
            format!("SELECT {columns} FROM t WHERE false"),
            "0",
        ),
        (
            "large.json",
            format!("SELECT '{private_literal}' AS value FROM t"),
            "360",
        ),
    ] {
        let output = run_with_timeout(query_command(directory.path(), &sql)?.args([
            "--format",
            "json",
            "--manifest",
            "--output",
            name,
        ]))?;
        let (manifest, bytes) = verify_result_and_manifest(&output)?;
        assert!(bytes.len() > 8192);
        assert_eq!(manifest["rows"], rows);
        for text in [
            String::from_utf8(output.stdout)?,
            String::from_utf8(output.stderr)?,
            manifest.to_string(),
        ] {
            assert!(!text.contains("private-sql-sentinel"));
            assert!(!text.contains("column_999"));
        }
    }
    Ok(())
}

#[test]
fn no_clobber_and_failed_execution_leave_existing_files_unchanged() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (manifest, name, reason) in [
        (false, "rows", "artifact_destination_exists"),
        (true, "rows", "artifact_destination_exists"),
        (
            true,
            "rows.manifest.json",
            "artifact_manifest_destination_exists",
        ),
    ] {
        fs::write(directory.path().join(name), b"keep me")?;
        let output = run_with_timeout(
            query_command(directory.path(), "SELECT * FROM t")?
                .args(["--output", "rows"])
                .args(manifest.then_some("--manifest")),
        )?;
        assert_eq!(output.status.code(), Some(3), "{output:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(fs::read(directory.path().join(name))?, b"keep me");
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
            reason
        );
        fs::remove_file(directory.path().join(name))?;
    }
    for arguments in [
        vec!["--max-output-bytes", "1"],
        vec!["--max-rows", "0"],
        vec!["--overwrite"],
    ] {
        let output = run_with_timeout(
            query_command(directory.path(), "SELECT * FROM t")?
                .args(["--format", "json", "--output", "rows"])
                .args(arguments),
        )?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(output.stderr.len() <= 8192);
        assert!(!directory.path().join("rows").exists());
        assert!(!directory.path().join("rows.manifest.json").exists());
        assert!(fs::read_dir(directory.path())?.all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".dar-")
        }));
    }
    Ok(())
}

#[test]
fn unsafe_replay_urls_fail_without_printing_the_secret() -> TestResult {
    let directory = tempfile::tempdir()?;
    for location in [
        "https://example.invalid/table?signature=private-secret",
        "s3://user:private-secret@bucket/table",
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .current_dir(directory.path())
                .args(["--diagnostics=json", "scan", location, "--output", "rows"]),
        )?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8(output.stderr)?.contains("private-secret"));
        assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn remote_file_outputs_reject_implicit_credentials_for_every_binding() -> TestResult {
    let directory = tempfile::tempdir()?;
    fs::write(
        directory.path().join("query.sql"),
        "SELECT id FROM cloud_rows",
    )?;
    fs::write(
        directory.path().join("tables.json"),
        r#"{"format_version":1,"tables":[{"alias":"cloud_rows","location":"s3://bucket/table"}]}"#,
    )?;
    for args in [
        vec!["scan", "s3://bucket/table"],
        vec!["query", "s3://bucket/table", "--sql-file", "query.sql"],
        vec![
            "query",
            "--tables-file",
            "tables.json",
            "--sql-file",
            "query.sql",
        ],
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .env("AWS_ACCESS_KEY_ID", "secret-ambient-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-ambient-password")
                .current_dir(directory.path())
                .args(&args)
                .args([
                    "--output",
                    "rows",
                    "--diagnostics=json",
                    "--deadline-ms",
                    "1000",
                ]),
        )?;
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-ambient"));
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(error["reason"], "file_output_requires_auth_source");
        if args.contains(&"--tables-file") {
            assert_eq!(error["argument"], "--tables-file");
            assert_eq!(error["field_path"], "/tables/0/authorization");
        }
        assert!(!directory.path().join("rows").exists());
        assert!(!directory.path().join("rows.manifest.json").exists());
    }
    Ok(())
}

#[test]
fn failed_file_scan_reports_the_actual_snapshot_for_a_pinned_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["--diagnostics=json", "scan"])
            .arg(fixture_path("deletion_vectors").join("table"))
            .args(["--max-rows", "0", "--output"])
            .arg(directory.path().join("rows")),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(error["reason"], "output_row_limit_exceeded");
    assert_eq!(
        error["bindings"],
        json!([{"alias": "t", "table_version": "1"}])
    );
    assert!(!error["hint"].as_str().unwrap().contains("SQL"));
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn many_bindings_stay_in_the_manifest_and_pin_independent_snapshots() -> TestResult {
    let directory = tempfile::tempdir()?;
    let tables: Vec<_> = (0..64)
        .map(|index| {
            json!({
                "alias": format!("binding_{index:02}"),
                "location": fixture_path("deletion_vectors").join("table"),
                "table_version": (index % 2).to_string(),
            })
        })
        .collect();
    let declaration = json!({"format_version": 1, "tables": tables}).to_string();
    fs::write(directory.path().join("tables.json"), &declaration)?;
    fs::write(
        directory.path().join("query.sql"),
        "SELECT count(*) AS n FROM binding_00",
    )?;
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .current_dir(directory.path())
            .args([
                "--diagnostics=json",
                "query",
                "--tables-file",
                "tables.json",
                "--sql-file",
                "query.sql",
                "--format",
                "json",
                "--manifest",
                "--output",
                "rows.json",
            ]),
    )?;
    let (manifest, bytes) = verify_result_and_manifest(&output)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes)?["rows"],
        json!([["12"]])
    );
    assert_eq!(manifest["bindings"].as_array().unwrap().len(), 64);
    for (index, binding) in manifest["bindings"].as_array().unwrap().iter().enumerate() {
        assert_eq!(binding["alias"], format!("binding_{index:02}"));
        assert_eq!(binding["table_version"], (index % 2).to_string());
    }
    assert_eq!(
        manifest["replay"]["inputs"][0]["sha256"],
        format!("{:x}", Sha256::digest(declaration.as_bytes()))
    );
    assert!(!String::from_utf8(output.stdout)?.contains("binding_63"));
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn filesystem_write_failures_never_publish_or_report_a_cli_budget() -> TestResult {
    let directory = tempfile::tempdir()?;
    let library = directory.path().join("storage-fault.so");
    let compiler = Command::new("cc")
        .args(["-shared", "-fPIC", "-o"])
        .arg(&library)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/storage_io_failure.c"
        ))
        .arg("-ldl")
        .output()?;
    assert!(compiler.status.success(), "{compiler:?}");
    for errno in [libc::ENOSPC, libc::EFBIG] {
        for format in ["json", "arrow"] {
            for (manifest, target) in [
                (false, ".dar-result-"),
                (true, ".dar-result-"),
                (true, ".dar-manifest-"),
            ] {
                let marker = directory.path().join("injected");
                let output = run_with_timeout(
                    query_command(
                        directory.path(),
                        "SELECT repeat('x', 20000) AS value FROM t LIMIT 1",
                    )?
                    .env("LD_PRELOAD", &library)
                    .env("DAR_STORAGE_FAULT_ERRNO", errno.to_string())
                    .env("DAR_STORAGE_FAULT_MARKER", &marker)
                    .env("DAR_STORAGE_FAULT_FILE", target)
                    .args(["--format", format, "--output", "rows"])
                    .args(manifest.then_some("--manifest")),
                )?;
                assert!(marker.exists(), "fault was not reached");
                fs::remove_file(marker)?;
                assert_eq!(output.status.code(), Some(3), "{output:?}");
                assert!(output.stdout.is_empty());
                let diagnostic = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(diagnostic["reason"], "artifact_write_failed");
                if errno == libc::ENOSPC {
                    assert_eq!(diagnostic["source_kind"], "storage_full");
                }
                assert_ne!(diagnostic["recovery"], "adjust_budget");
                assert!(!directory.path().join("rows").exists());
                assert!(!directory.path().join("rows.manifest.json").exists());
                assert!(fs::read_dir(directory.path())?.all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".dar-")
                }));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn interrupted_receipt_preserves_complete_results_in_both_modes() -> TestResult {
    for manifest_requested in [false, true] {
        let directory = tempfile::tempdir()?;
        let (consumer, stdout) = crate::create_full_pipe()?;
        let output = crate::run_with_stdout(
            query_command(directory.path(), "SELECT 7 AS n FROM t LIMIT 1")?
                .args([
                    "--format",
                    "json",
                    "--output",
                    "rows",
                    "--deadline-ms",
                    "500",
                ])
                .args(manifest_requested.then_some("--manifest")),
            stdout.into(),
        )?;
        drop(consumer);
        assert!(!output.status.success());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
            "command_deadline_exceeded"
        );
        let diagnostic = assert_single_terminal_record(&output.stderr, "diagnostic");
        let hint = diagnostic["hint"].as_str().unwrap();
        let bytes = fs::read(directory.path().join("rows"))?;
        if manifest_requested {
            assert!(hint.contains("matching manifest with completion=complete may already exist"));
            let manifest: Value =
                serde_json::from_slice(&fs::read(directory.path().join("rows.manifest.json"))?)?;
            assert_eq!(
                manifest["result"]["sha256"],
                format!("{:x}", Sha256::digest(&bytes))
            );
        } else {
            assert!(hint.contains("complete result may already exist"));
            assert!(!hint.contains("manifest"));
            assert!(!directory.path().join("rows.manifest.json").exists());
        }
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes)?["rows"],
            json!([["7"]])
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn cancellation_during_batch_serialization_removes_temporary_results() -> TestResult {
    use std::{
        process::Stdio,
        thread,
        time::{Duration, Instant},
    };
    let directory = tempfile::tempdir()?;
    let mut child = query_command(
        directory.path(),
        "SELECT repeat(CAST(id AS VARCHAR), 100000) AS value FROM t",
    )?
    .args([
        "--format",
        "json",
        "--output",
        "rows",
        "--max-output-bytes",
        "200000000",
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()?;
    let started = Instant::now();
    loop {
        let writing = fs::read_dir(directory.path())?
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".dar-result-")
                    && entry
                        .metadata()
                        .is_ok_and(|metadata| metadata.len() >= 65536)
            });
        if writing {
            break;
        }
        if started.elapsed() > Duration::from_secs(10) || child.try_wait()?.is_some() {
            let _ = child.kill();
            return Err(format!(
                "result write was not observed: {:?}",
                child.wait_with_output()?
            )
            .into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    assert_eq!(
        crate::wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
        Some(143)
    );
    let output = child.wait_with_output()?;
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
        "command_cancelled"
    );
    assert!(!directory.path().join("rows").exists());
    assert!(!directory.path().join("rows.manifest.json").exists());
    assert!(fs::read_dir(directory.path())?.all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".dar-")
    }));
    Ok(())
}

#[cfg(unix)]
#[test]
fn long_paths_either_fit_the_receipt_or_fail_before_publication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut parent = fs::canonicalize(directory.path())?;
    // Quotes make the JSON references grow faster than filesystem path bytes.
    let receipt_size = |parent: &Path| {
        let result = parent.join("rows");
        serde_json::to_vec(
            &json!({"format_version": 1, "kind": "result_artifact", "result": result,
            "manifest": format!("{}.manifest.json", result.to_str().unwrap())}),
        )
        .unwrap()
        .len()
            + 1
    };
    let mut path_limit_reached = false;
    while receipt_size(&parent) <= 8192 {
        parent.push("\"".repeat(80));
        match fs::create_dir(&parent) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENAMETOOLONG) => {
                path_limit_reached = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["--diagnostics=json", "scan", "--limit", "0"])
            .arg(fixture_path("partitioned").join("table"))
            .args(["--manifest", "--output"])
            .arg(parent.join("rows")),
    )?;
    assert_eq!(
        output.status.code(),
        Some(if path_limit_reached { 3 } else { 1 })
    );
    assert!(output.stdout.is_empty());
    assert!(output.stderr.len() <= 8192);
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
        if path_limit_reached {
            "artifact_directory_unavailable"
        } else {
            "artifact_receipt_too_large"
        }
    );
    if !path_limit_reached {
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["recovery"],
            "change_request"
        );
    }
    let last_created_parent = if path_limit_reached {
        parent.parent().unwrap()
    } else {
        &parent
    };
    assert_eq!(fs::read_dir(last_created_parent)?.count(), 0);
    // Reserve one component for result, manifest and temporary filenames.
    let output_parent = last_created_parent.parent().unwrap();
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["--diagnostics=json", "scan", "--limit", "0"])
            .arg(fixture_path("partitioned").join("table"))
            .args(["--manifest", "--output"])
            .arg(output_parent.join("rows")),
    )?;
    verify_result_and_manifest(&output)?;
    Ok(())
}
