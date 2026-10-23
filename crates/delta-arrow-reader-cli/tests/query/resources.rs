//! Spill preserves the SQL result and leaves the supplied directory intact.

use super::*;

const TABLE_ROWS: i32 = 262_144;

fn large_table_fixture() -> TestResult<SqlCommandFixture> {
    use arrow::datatypes::{Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use serde_json::json;
    use std::sync::Arc;

    let mut fixture = SqlCommandFixture::new("unused", "SELECT id FROM t ORDER BY payload")?;
    fixture.table = fixture.directory.path().join("table");
    fs::create_dir_all(fixture.table.join("_delta_log"))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let data_path = fixture.table.join("data.parquet");
    // One row group keeps scan splitting from changing the pressure that forces spilling.
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(TABLE_ROWS as usize))
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&data_path)?,
        Arc::clone(&schema),
        Some(properties),
    )?;
    let padding = "x".repeat(250);
    for start in (0..TABLE_ROWS).step_by(8192) {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(start..start + 8192)),
                Arc::new(StringArray::from_iter_values(
                    (start..start + 8192).map(|id| format!("{:06}{padding}", TABLE_ROWS - id)),
                )),
            ],
        )?;
        writer.write(&batch)?;
    }
    writer.close()?;
    let schema = json!({"type": "struct", "fields": [
        {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "payload", "type": "string", "nullable": false, "metadata": {}},
    ]});
    let actions = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {"id": "spill-sort", "format": {"provider": "parquet", "options": {}}, "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {}}}),
        json!({"add": {"path": "data.parquet", "partitionValues": {}, "size": fs::metadata(&data_path)?.len(), "modificationTime": 0, "dataChange": true}}),
    ];
    fs::write(
        fixture.table.join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )?;
    Ok(fixture)
}

fn spill_enabled_command(fixture: &SqlCommandFixture, spill: &std::path::Path) -> Command {
    let mut command = fixture.command();
    command
        .args(["--table-version", "0", "--memory-limit-bytes", "134217728"])
        .args(["--spill-limit-bytes", "268435456", "--spill-dir"])
        .arg(spill);
    command
}

#[test]
fn spilling_sort_preserves_every_row_and_cleans_up_on_success_and_exhaustion() -> TestResult {
    let fixture = large_table_fixture()?;
    let spill = tempfile::tempdir()?;
    fs::write(spill.path().join("keep"), b"existing user file")?;
    for budget in [None, Some("1"), Some("268435456")] {
        let mut command = fixture.command();
        command.args(["--table-version", "0", "--memory-limit-bytes", "134217728"]);
        if let Some(budget) = budget {
            command
                .args(["--spill-limit-bytes", budget, "--spill-dir"])
                .arg(spill.path());
        }
        let output = run_with_timeout(&mut command)?;
        if budget == Some("268435456") {
            let result = assert_query_success_and_decode_arrow(&output)?;
            let ids = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(result.num_rows(), TABLE_ROWS as usize);
            assert_eq!(
                ids.values().as_ref(),
                &(0..TABLE_ROWS).rev().collect::<Vec<_>>()
            );
        } else {
            assert_eq!(output.status.code(), Some(1));
            assert!(!output.stdout.ends_with(IPC_END));
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["source_kind"], "resource_exhausted");
            assert_eq!(record["bindings"][0]["table_version"], "0");
            assert_eq!(record["argument"], "--spill-limit-bytes");
            assert_eq!(
                record["reason"],
                if budget.is_some() {
                    "spill_limit_exceeded"
                } else {
                    "spill_disabled"
                }
            );
            assert_eq!(record["recovery"], "adjust_budget");
            let details: Vec<_> = record["resource_details_command"]
                .as_array()
                .unwrap()
                .iter()
                .skip(1)
                .map(|value| value.as_str().unwrap())
                .collect();
            let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args(details))?;
            assert!(output.status.success());
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout)?["detail"],
                "resources"
            );
        }
        assert_eq!(fs::read_dir(spill.path())?.count(), 1);
        assert_eq!(fs::read(spill.path().join("keep"))?, b"existing user file");
    }
    Ok(())
}

#[test]
fn large_join_and_aggregate_recover_by_changing_only_the_memory_budget() -> TestResult {
    let fixture = large_table_fixture()?;
    for sql in [
        "SELECT sum(a.id) AS total FROM t a JOIN t b ON a.payload = b.payload",
        "SELECT sum(id) AS total FROM (SELECT payload, min(id) AS id FROM t GROUP BY payload)",
    ] {
        fs::write(fixture.directory.path().join("query.sql"), sql)?;
        let spill = tempfile::tempdir()?;
        for budget in ["1", "536870912"] {
            let output = run_with_timeout(
                fixture
                    .command()
                    .args([
                        "--table-version",
                        "0",
                        "--memory-limit-bytes",
                        budget,
                        "--spill-limit-bytes",
                        "268435456",
                        "--spill-dir",
                    ])
                    .arg(spill.path()),
            )?;
            if budget == "1" {
                assert_eq!(output.status.code(), Some(1));
                assert!(!output.stdout.ends_with(IPC_END));
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["reason"], "memory_limit_exceeded");
                assert_eq!(record["argument"], "--memory-limit-bytes");
                assert_eq!(record["bindings"][0]["table_version"], "0");
            } else {
                let result = assert_query_success_and_decode_arrow(&output)?;
                assert_eq!(result.num_rows(), 1);
                assert_eq!(
                    result
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    i64::from(TABLE_ROWS) * i64::from(TABLE_ROWS - 1) / 2
                );
            }
            assert_eq!(fs::read_dir(spill.path())?.count(), 0);
        }
    }
    Ok(())
}

#[test]
fn disabled_spill_can_require_storage_even_with_a_large_memory_budget() -> TestResult {
    let fixture = large_table_fixture()?;
    fs::write(
        fixture.directory.path().join("query.sql"),
        "SELECT sum(id) FROM (SELECT id, row_number() OVER (ORDER BY payload) AS rn FROM t) WHERE rn % 2 = 0",
    )?;
    let spill = tempfile::tempdir()?;
    for enabled in [false, true] {
        let mut command = fixture.command();
        command.args(["--table-version", "0", "--memory-limit-bytes", "536870912"]);
        if enabled {
            command
                .args(["--spill-limit-bytes", "268435456", "--spill-dir"])
                .arg(spill.path());
        }
        let output = run_with_timeout(&mut command)?;
        if enabled {
            let result = assert_query_success_and_decode_arrow(&output)?;
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                i64::from(TABLE_ROWS / 2) * i64::from(TABLE_ROWS / 2 - 1)
            );
        } else {
            assert_eq!(output.status.code(), Some(1));
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["reason"], "spill_disabled");
            assert_eq!(record["argument"], "--spill-limit-bytes");
            assert!(record["expected"].as_str().unwrap().contains("--spill-dir"));
        }
        assert_eq!(fs::read_dir(spill.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn spill_options_and_directory_fail_before_opening_the_table() -> TestResult {
    let fixture = SqlCommandFixture::new("secret-unavailable-table", "SELECT 1")?;
    for arguments in [
        vec!["--spill-limit-bytes", "1"],
        vec!["--spill-dir", "secret-directory"],
        vec![
            "--spill-limit-bytes",
            "0",
            "--spill-dir",
            "secret-directory",
        ],
        vec!["--spill-limit-bytes", "-1"],
        vec!["--spill-limit-bytes", "18446744073709551616"],
    ] {
        let output = run_with_timeout(fixture.command().args(arguments))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["phase"], "configuration");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-"));
    }
    for path in [
        fixture.directory.path().join("missing"),
        fixture.directory.path().join("query.sql"),
    ] {
        let output = run_with_timeout(&mut spill_enabled_command(&fixture, &path))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "spill_directory");
        assert_eq!(record["argument"], "--spill-dir");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-"));
    }
    assert!(!fixture.directory.path().join("missing").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn unwritable_spill_parent_is_rejected_before_table_io() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    let fixture = SqlCommandFixture::new("secret-unavailable-table", "SELECT 1")?;
    let spill = tempfile::tempdir()?;
    fs::set_permissions(spill.path(), fs::Permissions::from_mode(0o500))?;
    let output = run_with_timeout(&mut spill_enabled_command(&fixture, spill.path()));
    fs::set_permissions(spill.path(), fs::Permissions::from_mode(0o700))?;
    let output = output?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        "spill_directory"
    );
    assert_eq!(fs::read_dir(spill.path())?.count(), 0);
    Ok(())
}

#[cfg(target_os = "linux")]
fn compile_spill_fault_library(directory: &std::path::Path) -> TestResult<PathBuf> {
    let library = directory.join("spill-failure.so");
    let compiler = Command::new("cc")
        .args(["-shared", "-fPIC", "-o"])
        .arg(&library)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/spill_io_failure.c"
        ))
        .arg("-ldl")
        .output()?;
    assert!(
        compiler.status.success(),
        "{}",
        String::from_utf8_lossy(&compiler.stderr)
    );
    Ok(library)
}

#[cfg(target_os = "linux")]
#[test]
fn full_spill_storage_and_write_errors_fail_without_leaking_temporary_files() -> TestResult {
    let fixture = large_table_fixture()?;
    let library = compile_spill_fault_library(fixture.directory.path())?;
    for errno in [libc::ENOSPC, libc::EDQUOT, libc::EIO] {
        let spill = tempfile::tempdir()?;
        fs::write(spill.path().join("keep"), b"existing user file")?;
        let marker = fixture.directory.path().join(format!("injected-{errno}"));
        let output = run_with_timeout(
            spill_enabled_command(&fixture, spill.path())
                .env("LD_PRELOAD", &library)
                .env("DAR_SPILL_FAULT_MARKER", &marker)
                .env("DAR_SPILL_FAULT_ERRNO", errno.to_string()),
        )?;
        assert!(
            marker.exists(),
            "the spill-write fault must have been exercised"
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(!output.stdout.ends_with(IPC_END));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["bindings"][0]["table_version"], "0");
        if errno != libc::EIO {
            assert_eq!(
                record["source_kind"],
                if errno == libc::ENOSPC {
                    "storage_full"
                } else {
                    "storage_quota_exceeded"
                }
            );
            assert_eq!(record["argument"], "--spill-dir");
            assert_eq!(record["recovery"], "fix_input");
        }
        assert!(!String::from_utf8_lossy(&output.stderr).contains(spill.path().to_str().unwrap()));
        assert_eq!(fs::read_dir(spill.path())?.count(), 1);
        assert_eq!(fs::read(spill.path().join("keep"))?, b"existing user file");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn cleanup_failure_prevents_success_and_preserves_snapshot_bindings() -> TestResult {
    let fixture = large_table_fixture()?;
    let library = compile_spill_fault_library(fixture.directory.path())?;
    for sql in ["SELECT count(*) FROM t", "SELECT nonexistent FROM t"] {
        fs::write(fixture.directory.path().join("query.sql"), sql)?;
        let spill = tempfile::tempdir()?;
        let marker = spill.path().join("fault-was-injected");
        let output = run_with_timeout(
            spill_enabled_command(&fixture, spill.path())
                .env("LD_PRELOAD", &library)
                .env("DAR_SPILL_FAULT_MARKER", &marker)
                .env("DAR_SPILL_FAULT_REMOVE", "1"),
        )?;
        assert!(marker.exists());
        assert_eq!(output.status.code(), Some(1));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "spill_cleanup");
        assert_eq!(record["source_kind"], "permission_denied");
        assert_eq!(
            record["bindings"],
            serde_json::json!([{"alias":"t","table_version":"0"}])
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains(spill.path().to_str().unwrap()));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn interruption_cleans_private_storage_during_spill_and_blocked_output() -> TestResult {
    use std::{
        os::{fd::AsRawFd, unix::fs::PermissionsExt},
        process::Stdio,
        thread,
        time::{Duration, Instant},
    };

    let fixture = large_table_fixture()?;
    for (stop, wait_for_output, exit) in [
        ("-INT", false, 130),
        ("-TERM", false, 143),
        ("-INT", true, 130),
        ("-TERM", true, 143),
        ("deadline", true, 1),
        ("disconnect", false, 3),
        ("disconnect", true, 3),
    ] {
        let spill = tempfile::tempdir()?;
        let (reader, writer) = std::io::pipe()?;
        let observer = writer.try_clone()?;
        let mut child = spill_enabled_command(&fixture, spill.path())
            .args([
                "--deadline-ms",
                if stop == "deadline" { "2000" } else { "300000" },
            ])
            .env_remove("RUST_LOG")
            .stdout(Stdio::from(writer))
            .stderr(Stdio::piped())
            .spawn()?;
        let started = Instant::now();
        loop {
            let directories: Vec<_> = fs::read_dir(spill.path())?.collect::<Result<_, _>>()?;
            let mut has_data = false;
            for directory in directories {
                assert_eq!(directory.metadata()?.permissions().mode() & 0o077, 0);
                for inner in fs::read_dir(directory.path())? {
                    for file in fs::read_dir(inner?.path())? {
                        let file = file?;
                        match file.metadata() {
                            Ok(metadata) => has_data |= metadata.len() > 0,
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
            }
            let mut available = 0;
            // SAFETY: ioctl writes one integer for this live pipe descriptor.
            assert_eq!(
                unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &mut available) },
                0
            );
            let mut writable = libc::pollfd {
                fd: observer.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            // SAFETY: poll examines this live write descriptor without blocking.
            let ready = unsafe { libc::poll(&mut writable, 1, 0) };
            assert!(ready >= 0);
            // Require data past the small schema and observed backpressure on the write end.
            if if wait_for_output {
                available >= 4096 && ready == 0
            } else {
                has_data
            } {
                break;
            }
            if started.elapsed() > Duration::from_secs(10) || child.try_wait()?.is_some() {
                let _ = child.kill();
                let _ = child.wait();
                return Err("query did not reach the selected spill/output stage".into());
            }
            thread::sleep(Duration::from_millis(5));
        }
        if stop == "disconnect" {
            drop(reader);
        } else if stop != "deadline" {
            assert!(
                Command::new("kill")
                    .args([stop, &child.id().to_string()])
                    .status()?
                    .success()
            );
        }
        let status = crate::wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?;
        assert_eq!(status.code(), Some(exit));
        let output = child.wait_with_output()?;
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(
            record["code"],
            match stop {
                "deadline" => "deadline_exceeded",
                "disconnect" => "output_write",
                _ => "command_cancelled",
            }
        );
        assert_eq!(fs::read_dir(spill.path())?.count(), 0);
    }
    Ok(())
}
