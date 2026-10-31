//! Planning failures identify unavailable functions without exposing SQL values.

use std::process::Command;

use serde_json::json;

use super::{SqlCommandFixture, TestResult, fixture_path};
use crate::{
    capabilities::assert_output_matches_schema, diagnostics::assert_single_terminal_record,
    run_with_timeout,
};

#[test]
fn unknown_functions_are_actionable_in_query_and_explain() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT\n  get_json_object('{\"state\":\"secret-value\"}', '$.state') FROM t",
    )?;
    for command in ["query", "explain"] {
        for json_diagnostics in [false, true] {
            let mut invocation = Command::new(env!("CARGO_BIN_EXE_dar"));
            invocation
                .arg(command)
                .arg(&fixture.table)
                .arg("--sql-file")
                .arg(fixture.directory.path().join("query.sql"))
                .args(["--table-version", "0"]);
            if json_diagnostics {
                invocation.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut invocation)?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8(output.stderr)?;
            assert!(stderr.contains("get_json_object"), "{stderr}");
            assert!(!stderr.contains("secret-value"), "{stderr}");
            assert!(!stderr.contains("$.state"), "{stderr}");
            if json_diagnostics {
                let record = assert_single_terminal_record(stderr.as_bytes(), "diagnostic");
                assert_eq!(record["code"], "sql_planning");
                assert_eq!(record["reason"], "unknown_function");
                assert_eq!(record["function"], "get_json_object");
                assert_eq!(record["line"], 2);
                assert_eq!(record["column"], 3);
                assert_eq!(record["recovery"], "fix_input");
                assert_eq!(record["retryable"], false);
                assert_eq!(record["argument"], "--sql-file");
                assert!(record["hint"].as_str().unwrap().contains("Replace"));
                assert_eq!(
                    record["bindings"],
                    json!([{"alias":"t", "table_version":"0"}])
                );
                assert!(record.get("planning_errors").is_none());
                assert_output_matches_schema("diagnostic", &record);
            } else {
                assert!(stderr.contains("Replace"), "{stderr}");
            }
        }
    }
    Ok(())
}

#[test]
fn independent_planning_errors_keep_their_individual_causes() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT get_json_object('secret-json', '$.state'), sha2('secret-value', 256), \
         secret_missing_column FROM t",
    )?;
    for command in ["query", "explain"] {
        for json_diagnostics in [false, true] {
            let mut invocation = Command::new(env!("CARGO_BIN_EXE_dar"));
            invocation
                .arg(command)
                .arg(&fixture.table)
                .arg("--sql-file")
                .arg(fixture.directory.path().join("query.sql"));
            if json_diagnostics {
                invocation.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut invocation)?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!stderr.contains("secret"), "{stderr}");
            assert!(stderr.contains("get_json_object"), "{stderr}");
            assert!(stderr.contains("sha2"), "{stderr}");
            assert!(stderr.contains("column_not_found"), "{stderr}");
            if !json_diagnostics {
                continue;
            }
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["reason"], "multiple_planning_errors");
            assert_eq!(record["source_kind"], "multiple_errors");
            assert_eq!(record["recovery"], "fix_input");
            let causes = record["planning_errors"].as_array().unwrap();
            assert_eq!(causes.len(), 3);
            assert_eq!(causes[0]["reason"], "unknown_function");
            assert_eq!(causes[0]["function"], "get_json_object");
            assert_eq!(causes[1]["reason"], "unknown_function");
            assert_eq!(causes[1]["function"], "sha2");
            assert_eq!(causes[2]["reason"], "column_not_found");
            assert_eq!(causes[2]["source_kind"], "schema");
            assert_output_matches_schema("diagnostic", &record);
        }
    }
    Ok(())
}

#[test]
fn unusual_function_identifiers_are_located_without_echoing_them() -> TestResult {
    for name in [
        "https://user:secret@host/?token=secret",
        "secret\ncontrol",
        &"x".repeat(200),
    ] {
        let fixture = SqlCommandFixture::new(
            fixture_path("partitioned").join("table"),
            format!("SELECT \"{name}\"('secret-value') FROM t"),
        )?;
        let output = run_with_timeout(&mut fixture.command())?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], "unknown_function");
        assert!(record.get("function").is_none());
        assert_eq!(record["line"], 1);
        assert_eq!(record["column"], 8);
        assert!(!record.to_string().contains("secret"));
        assert!(!record.to_string().contains(name));
    }
    Ok(())
}

#[test]
fn long_error_lists_are_bounded_and_marked_incomplete() -> TestResult {
    let expressions: Vec<_> = (0..20)
        .map(|index| format!("missing_function_{index}(id)"))
        .collect();
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        format!("SELECT {} FROM t", expressions.join(", ")),
    )?;
    let output = run_with_timeout(&mut fixture.command())?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    let causes = record["planning_errors"].as_array().unwrap();
    assert!(!causes.is_empty() && causes.len() < expressions.len());
    assert_eq!(causes[0]["function"], "missing_function_0");
    assert!(
        causes
            .iter()
            .all(|cause| cause["reason"] == "unknown_function")
    );
    assert_eq!(record["planning_errors_truncated"], true);
    assert!(output.stderr.len() <= 8192);
    assert_output_matches_schema("diagnostic", &record);
    Ok(())
}
