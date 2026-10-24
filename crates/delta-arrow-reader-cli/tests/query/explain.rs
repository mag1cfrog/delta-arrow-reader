//! Planned reads, focused disclosure and recovery through the public CLI.

use serde_json::{Value, json};

use super::{SqlCommandFixture, TestResult, fixture_path};
use crate::{
    capabilities::assert_output_matches_schema, diagnostics::assert_single_terminal_record,
    run_with_timeout,
};
use std::{
    fs,
    process::{Command, Output},
};

fn explain_command(fixture: &SqlCommandFixture) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .args(["--diagnostics=json", "explain"])
        .arg(&fixture.table)
        .arg("--sql-file")
        .arg(fixture.directory.path().join("query.sql"));
    command
}

fn assert_plan(output: &Output) -> Value {
    assert!(output.status.success(), "{output:?}");
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_output_matches_schema("query-plan", &plan);
    assert_eq!(plan["execution"], "not_started");
    let completion = assert_single_terminal_record(&output.stderr, "completion");
    assert_output_matches_schema("completion", &completion);
    assert_eq!(
        completion,
        json!({
            "format_version": 3, "kind": "completion", "command": "explain",
            "output_bytes": output.stdout.len().to_string(),
        })
    );
    plan
}

#[test]
fn metadata_planning_never_fetches_data_but_execution_does() -> TestResult {
    use crate::mock_cloud_storage::{
        CloudStorageProtocol, MockCloudStorageServer, requested_object_path,
    };
    let is_metadata_request = |request: &str| {
        let path = requested_object_path(request);
        path == "bucket" || path.starts_with("bucket/table/_delta_log/")
    };
    for name in ["partitioned", "deletion_vectors"] {
        let mut objects = std::collections::BTreeMap::new();
        for entry in fs::read_dir(fixture_path(name).join("table/_delta_log"))? {
            let path = entry?.path();
            if path.is_file() {
                objects.insert(
                    format!(
                        "table/_delta_log/{}",
                        path.file_name().unwrap().to_str().unwrap()
                    ),
                    fs::read(path)?,
                );
            }
        }
        let server =
            MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {})?;
        let fixture = SqlCommandFixture::new("s3://bucket/table", "SELECT id FROM t WHERE id > 0")?;
        let credentials = fixture.directory.path().join("credentials.json");
        fs::write(
            &credentials,
            json!({
                "aws_endpoint": server.endpoint, "aws_region": "us-east-1", "aws_allow_http": "true",
                "aws_access_key_id": "fixture-key", "aws_secret_access_key": "secret-fixture-password",
            })
            .to_string(),
        )?;
        for selection in [
            vec![],
            vec!["--node", "0", "--field", "0", "--statistics"],
            vec!["--scan", "0"],
            vec!["--scan", "0", "--node", "0", "--field", "0", "--statistics"],
        ] {
            let output = run_with_timeout(
                explain_command(&fixture)
                    .args(selection)
                    .args(["--auth-source", "json", "--storage-options-file"])
                    .arg(&credentials),
            )?;
            assert_plan(&output);
            assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
        }
        {
            let requests = server.requests.lock().unwrap();
            assert!(!requests.is_empty());
            assert!(
                requests.iter().all(|r| is_metadata_request(r)),
                "{requests:?}"
            );
        }
        let output = run_with_timeout(
            fixture
                .command()
                .args(["--auth-source", "json", "--storage-options-file"])
                .arg(&credentials),
        )?;
        assert!(!output.status.success());
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|r| !is_metadata_request(r))
        );
        server.deny_all_requests();
        let denied = run_with_timeout(
            explain_command(&fixture)
                .args(["--auth-source", "json", "--storage-options-file"])
                .arg(&credentials),
        )?;
        assert!(!denied.status.success());
        assert!(denied.stdout.is_empty());
        assert_single_terminal_record(&denied.stderr, "diagnostic");
        assert!(!String::from_utf8_lossy(&denied.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn summary_types_pushdown_and_self_join_scans_keep_separate_identifiers() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id FROM t WHERE id > 0",
    )?;
    let summary = assert_plan(&run_with_timeout(&mut explain_command(&fixture))?);
    assert_eq!(
        summary["bindings"],
        json!([{"alias": "t", "table_version": "0"}])
    );
    assert_eq!(summary["scan_count"], 1);
    assert_eq!(summary["column_count"], 1);
    assert_eq!(summary.as_object().unwrap().len(), 7);
    let field = assert_plan(&run_with_timeout(
        explain_command(&fixture).args(["--field", "0"]),
    )?);
    assert_eq!(
        field["node"]["fields"],
        json!([{"index": 0, "data_type": "Int32", "nullable": true}])
    );
    assert_eq!(field["node"].as_object().unwrap().len(), 2);
    let scan = assert_plan(&run_with_timeout(
        explain_command(&fixture).args(["--scan", "0"]),
    )?);
    assert_eq!(scan["scan"]["binding"], "t");
    assert_eq!(scan["scan"]["projection"], json!([0]));
    assert_eq!(
        scan["scan"]["pushed_filters"],
        json!({"exact": 1, "inexact": 0})
    );
    fs::write(
        fixture.directory.path().join("query.sql"),
        "SELECT a.id FROM t a JOIN t b ON a.id = b.id",
    )?;
    let summary = assert_plan(&run_with_timeout(&mut explain_command(&fixture))?);
    assert_eq!(summary["scan_count"], 2);
    for id in ["0", "1"] {
        let scan = assert_plan(&run_with_timeout(
            explain_command(&fixture).args(["--scan", id]),
        )?);
        assert_eq!(scan["scan"]["id"].as_u64(), Some(id.parse()?));
        assert_eq!(scan["scan"]["binding"], "t");
    }
    Ok(())
}

#[test]
fn selected_output_is_atomic_at_the_exact_byte_boundary_and_recoverable() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("deletion_vectors").join("table"),
        "SELECT id FROM t",
    )?;
    let original = run_with_timeout(explain_command(&fixture).args([
        "--table-version",
        "0",
        "--node",
        "0",
        "--field",
        "0",
        "--statistics",
        "--scan",
        "0",
    ]))?;
    let expected = assert_plan(&original);
    assert!(expected.get("node").is_some());
    assert!(expected.get("scan").is_some());
    assert!(expected.get("node_count").is_none());
    for budget in [original.stdout.len() - 1, original.stdout.len()] {
        let output = run_with_timeout(explain_command(&fixture).args([
            "--table-version",
            "0",
            "--node",
            "0",
            "--field",
            "0",
            "--statistics",
            "--scan",
            "0",
            "--max-output-bytes",
            &budget.to_string(),
        ]))?;
        if budget == original.stdout.len() {
            assert_eq!(assert_plan(&output), expected);
        } else {
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let failure = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(failure["reason"], "plan_output_limit_exceeded");
            assert_eq!(failure["bindings"], expected["bindings"]);
            assert_eq!(failure["argument"], "--max-output-bytes");
        }
    }
    Ok(())
}

#[test]
fn wide_results_expand_only_selected_types_and_never_dump_names_or_metadata() -> TestResult {
    let columns = (0..200)
        .map(|i| format!("id AS c{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        format!("SELECT {columns} FROM t"),
    )?;
    let summary = run_with_timeout(&mut explain_command(&fixture))?;
    assert_eq!(assert_plan(&summary)["column_count"], 200);
    assert!(summary.stdout.len() < 4096);
    let mut all = explain_command(&fixture);
    for i in 0..200 {
        all.args(["--field", &i.to_string()]);
    }
    let oversized = run_with_timeout(&mut all)?;
    assert!(oversized.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&oversized.stderr, "diagnostic")["reason"],
        "plan_output_limit_exceeded"
    );
    let focused = run_with_timeout(explain_command(&fixture).args([
        "--field",
        "199",
        "--table-version",
        "0",
    ]))?;
    assert_eq!(
        assert_plan(&focused)["node"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let expanded =
        run_with_timeout(all.args(["--max-output-bytes", "32768", "--table-version", "0"]))?;
    assert_eq!(
        assert_plan(&expanded)["node"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        200
    );
    Ok(())
}

#[test]
fn sql_literals_generated_names_and_type_parameters_never_escape() -> TestResult {
    let table = fixture_path("partitioned").join("table");
    for sql in [
        "SELECT 'https://user:secret-password@example.test' FROM t",
        "SELECT id AS \"secret-name\" FROM t WHERE label = 'secret-filter'",
    ] {
        let fixture = SqlCommandFixture::new(&table, sql)?;
        for selection in [
            vec![],
            vec!["--node", "0", "--field", "0", "--statistics"],
            vec!["--scan", "0"],
            vec!["--scan", "0", "--node", "0", "--field", "0", "--statistics"],
        ] {
            let output = run_with_timeout(explain_command(&fixture).args(selection))?;
            assert_plan(&output);
            for bytes in [&output.stdout, &output.stderr] {
                assert!(!String::from_utf8_lossy(bytes).contains("secret"));
                assert!(!String::from_utf8_lossy(bytes).contains("example.test"));
            }
        }
    }
    for sql in [
        "SELECT named_struct('secret-child', id) FROM t",
        "SELECT arrow_cast(id, 'Timestamp(Microsecond, Some(\"secret-zone\"))') FROM t",
    ] {
        let fixture = SqlCommandFixture::new(&table, sql)?;
        assert_plan(&run_with_timeout(&mut explain_command(&fixture))?);
        let output = run_with_timeout(explain_command(&fixture).args(["--field", "0"]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let failure = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(failure["argument"], "--field");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn unsupported_sql_and_selections_fail_without_result_execution() -> TestResult {
    for sql in [
        "EXPLAIN ANALYZE SELECT * FROM t",
        "COPY t TO 'secret-output'",
        "SELECT * FROM read_parquet('secret-file')",
    ] {
        let fixture = SqlCommandFixture::new("secret-missing-table", sql)?;
        let output = run_with_timeout(&mut explain_command(&fixture))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
            "invalid_sql_input"
        );
    }
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT id FROM t",
    )?;
    for selection in [
        vec!["--field", "1"],
        vec!["--node", "10000"],
        vec!["--scan", "1"],
        vec!["--field", "0", "--field", "0"],
        vec!["--scan", "0", "--field", "1"],
    ] {
        let output = run_with_timeout(explain_command(&fixture).args(selection))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_single_terminal_record(&output.stderr, "diagnostic");
    }
    Ok(())
}

#[test]
fn missing_statistics_and_oversized_untrusted_metadata_remain_bounded() -> TestResult {
    use crate::inspection::{column, table_with_fields};
    let hostile = "secret-ignore-instructions".repeat(500);
    let table = table_with_fields(
        vec![column(&hostile, json!("long"), json!({"comment": hostile}))],
        false,
    );
    let fixture = SqlCommandFixture::new(table.path(), "SELECT * FROM t")?;
    let summary = assert_plan(&run_with_timeout(&mut explain_command(&fixture))?);
    let mut found_scan = false;
    for index in 0..summary["node_count"].as_u64().unwrap() {
        let output = run_with_timeout(explain_command(&fixture).args([
            "--node",
            &index.to_string(),
            "--field",
            "0",
            "--statistics",
        ]))?;
        assert!(output.stdout.len() < 4096);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
        let plan = assert_plan(&output);
        if plan["node"]["operator"] == "DeltaScanExec" {
            found_scan = true;
            assert_eq!(
                plan["node"]["statistics"]["rows"],
                json!({"precision": "unknown"})
            );
            assert_eq!(
                plan["node"]["statistics"]["selected_files"],
                json!({
                    "files": "1", "input_rows_estimate": null, "input_bytes_estimate": "100",
                })
            );
        }
    }
    assert!(found_scan);
    Ok(())
}

#[test]
fn every_loaded_binding_survives_optimization_and_explicit_budget_recovery() -> TestResult {
    use crate::inspection::{column, table_with_fields};
    let table = table_with_fields(vec![column("id", json!("long"), json!({}))], false);
    let directory = tempfile::tempdir()?;
    let sql = directory.path().join("query.sql");
    fs::write(&sql, "SELECT 1")?;
    let tables = directory.path().join("tables.json");
    let bindings = (0..64).map(|i| json!({
        "alias": format!("a{i:02}{}", "x".repeat(60)), "location": table.path(), "table_version": "0",
    })).collect::<Vec<_>>();
    fs::write(
        &tables,
        json!({"format_version": 1, "tables": bindings}).to_string(),
    )?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .args(["--diagnostics=json", "explain", "--sql-file"])
        .arg(sql)
        .arg("--tables-file")
        .arg(tables);
    let output = run_with_timeout(&mut command)?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let failure = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(failure["reason"], "plan_output_limit_exceeded");
    assert_eq!(failure["bindings"].as_array().unwrap().len(), 64);
    let recovered = assert_plan(&run_with_timeout(
        command.args(["--max-output-bytes", "16384"]),
    )?);
    assert_eq!(recovered["bindings"], failure["bindings"]);
    assert_eq!(recovered["scan_count"], 0);
    Ok(())
}
