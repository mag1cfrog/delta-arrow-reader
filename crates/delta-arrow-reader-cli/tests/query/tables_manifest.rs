//! Manifest admission, relative locations, and named snapshot provenance at the CLI boundary.

use std::{fs, path::Path, process::Command};

use arrow::array::Int32Array;
use serde_json::{Value, json};

use super::{
    TestResult, assert_single_terminal_record, decode_query_result, fixture_path, run_with_timeout,
};

fn manifest_directory() -> TestResult<tempfile::TempDir> {
    let directory = tempfile::Builder::new()
        .prefix("secret-manifest-")
        .tempdir()?;
    fs::create_dir(directory.path().join("inputs"))?;
    fs::write(directory.path().join("query.sql"), "SELECT 1")?;
    Ok(directory)
}

fn manifest_command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command.current_dir(directory).args([
        "query",
        "--tables-file",
        "inputs/tables.json",
        "--sql-file",
        "query.sql",
        "--diagnostics=json",
    ]);
    command
}

fn copy_directory(source: &Path, destination: &Path) -> TestResult {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[test]
fn discovered_manifest_joins_use_relative_locations_and_independent_versions() -> TestResult {
    let directory = manifest_directory()?;
    for name in ["partitioned", "deletion_vectors"] {
        copy_directory(
            &fixture_path(name).join("table"),
            &directory.path().join("data").join(name),
        )?;
    }
    let mut discovery_bytes = Vec::new();
    let mut discover = |invocation: &Value| -> TestResult<Value> {
        let arguments: Vec<_> = invocation
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        assert_eq!(arguments[0], "dar");
        assert!(!arguments.iter().any(|argument| {
            matches!(
                *argument,
                "--full" | "--all" | "--detail" | "--authorization"
            )
        }));
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .args(&arguments[1..]),
        )?;
        assert!(output.status.success(), "{arguments:?}: {output:?}");
        assert!(output.stderr.is_empty());
        discovery_bytes.push(output.stdout.len());
        Ok(serde_json::from_slice(&output.stdout)?)
    };
    let index = discover(&json!(["dar", "capabilities"]))?;
    let query = index["commands"]
        .as_array()
        .unwrap()
        .iter()
        .find(|command| command["name"] == "query")
        .unwrap();
    let query_guide = discover(&query["details"])?;
    assert_eq!(
        query_guide["argument_conflicts"].as_array().unwrap().len(),
        1
    );
    let conflicts = &query_guide["argument_conflicts"][0];
    assert_eq!(conflicts["argument"], "--tables-file");
    for argument in [
        "table",
        "--table-version",
        "--storage-options-file",
        "--auth-source",
    ] {
        assert!(
            conflicts["conflicts_with"]
                .as_array()
                .unwrap()
                .contains(&json!(argument))
        );
    }
    let input = query_guide["input_schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|input| input["argument"] == "--tables-file")
        .unwrap();
    let mut invocation = query_guide["schema_command"].clone();
    *invocation.as_array_mut().unwrap().last_mut().unwrap() = input["name"].clone();
    let guide = discover(&invocation)?;
    assert!(guide.get("schema").is_none());
    assert!(guide.get("parser_constraints").is_none());
    assert!(guide.get("authorization_guides").is_none());
    assert!(
        guide["description"]
            .as_str()
            .unwrap()
            .contains("manifest directory")
    );
    fs::write(
        directory.path().join("query.sql"),
        query_guide["examples"]["join"]["sql"].as_str().unwrap(),
    )?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command.env_clear().current_dir(directory.path()).args(
        query_guide["examples"]["join"]["command"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|value| match value.as_str().unwrap() {
                "tables.json" => "inputs/tables.json",
                argument => argument,
            }),
    );
    let mut manifest = guide["example"].clone();
    assert_eq!(manifest["tables"].as_array().unwrap().len(), 2);
    manifest["tables"][0]["location"] = json!("../data/partitioned");
    manifest["tables"][1]["location"] = json!("../data/deletion_vectors");
    let path = directory.path().join("inputs/tables.json");
    for (version, expected) in [
        ("1", vec![Some(1), None, Some(3), Some(4), None]),
        ("0", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]),
    ] {
        if version == "0" {
            manifest["tables"][1]["table_version"] = json!("0");
        }
        fs::write(&path, manifest.to_string())?;
        let output = run_with_timeout(&mut command)?;
        let actual = decode_query_result(&output)?;
        assert_eq!(actual.num_columns(), 2);
        assert_eq!(actual.schema().field(0).name(), "id");
        assert_eq!(actual.schema().field(1).name(), "id");
        assert_eq!(
            actual.column(0).as_ref(),
            &Int32Array::from_iter_values(1..=5)
        );
        assert_eq!(actual.column(1).as_ref(), &Int32Array::from(expected));
        let record = assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(
            record["bindings"],
            json!([
                {"alias":"orders","table_version":"0"},
                {"alias":"customers","table_version":version}
            ])
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    eprintln!(
        "JOIN discovery: {} lookups, {} bytes, no full definitions or authorization lookups",
        discovery_bytes.len(),
        discovery_bytes.iter().sum::<usize>(),
    );
    Ok(())
}

fn assert_manifest_rejected(directory: &Path, contents: impl AsRef<[u8]>) -> TestResult<Value> {
    fs::write(directory.join("inputs/tables.json"), contents)?;
    let output = run_with_timeout(&mut manifest_command(directory))?;
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "invalid_input_json");
    assert_eq!(record["argument"], "--tables-file");
    Ok(record)
}

#[test]
fn invalid_later_bindings_fail_before_opening_the_first_table() -> TestResult {
    let directory = manifest_directory()?;
    for (alias, location, reason, field) in [
        (
            "orders",
            "secret-missing-table",
            "duplicate_table_alias",
            "alias",
        ),
        ("t", "secret-missing-table", "reserved_table_alias", "alias"),
        (
            "select",
            "secret-missing-table",
            "reserved_table_alias",
            "alias",
        ),
        (
            "public",
            "secret-missing-table",
            "reserved_table_alias",
            "alias",
        ),
        (
            "secret.Alias",
            "secret-missing-table",
            "invalid_table_alias",
            "alias",
        ),
        (
            "customers",
            "s3://bucket/table?secret-token",
            "invalid_table_location",
            "location",
        ),
        (
            "customers",
            "https://secret-user:secret-password@host/table",
            "invalid_table_location",
            "location",
        ),
        (
            "customers",
            "https://host/table#secret-token",
            "invalid_table_location",
            "location",
        ),
        (
            "customers",
            "secret\npath",
            "invalid_table_location",
            "location",
        ),
        ("customers", "", "invalid_table_location", "location"),
    ] {
        // The first missing table would fail with status 1 if loading began before validation finished.
        let manifest = json!({"format_version":1,"tables":[
            {"alias":"orders","location":"secret-missing-table"},
            {"alias":alias,"location":location}
        ]});
        let record = assert_manifest_rejected(directory.path(), manifest.to_string())?;
        assert_eq!(record["reason"], reason);
        assert_eq!(record["field_path"], format!("/tables/1/{field}"));
    }
    Ok(())
}

#[test]
fn manifest_schema_limits_and_duplicate_fields_are_enforced() -> TestResult {
    let directory = manifest_directory()?;
    for input in [
        r#"[1,[]]"#,
        r#"{"format_version":1,"tables":[]}"#,
        r#"{"format_version":2,"tables":[]}"#,
        r#"{"format_version":1.0,"tables":[]}"#,
        r#"{"format_version":1,"format_version":1,"tables":[]}"#,
        r#"{"format_version":1,"tables":[["orders","secret-table"]]}"#,
        r#"{"format_version":1,"tables":[{"alias":"orders","alias":"customers","location":"secret-table"}]}"#,
        r#"{"format_version":1,"tables":[{"alias":"orders","location":"secret-table","secret-field":"secret-value"}]}"#,
        r#"{"format_version":1,"tables":[{"alias":"orders","location":"secret-table","authorization":{"source":"json","secret-value":"secret-token"}}]}"#,
    ] {
        assert_manifest_rejected(directory.path(), input)?;
    }
    for version in [
        json!(null),
        json!(0),
        json!("+1"),
        json!("01"),
        json!("-0"),
        json!("18446744073709551616"),
    ] {
        assert_manifest_rejected(
            directory.path(),
            json!({"format_version":1,"tables":[
                {"alias":"orders","location":"secret-table","table_version":version}
            ]})
            .to_string(),
        )?;
    }
    let record = assert_manifest_rejected(directory.path(), vec![b' '; 1024 * 1024 + 1])?;
    assert_eq!(record["reason"], "input_file_too_large");
    assert_manifest_rejected(
        directory.path(),
        format!(
            "{{\"format_version\":1,\"tables\":[{{\"alias\":\"orders\",\"location\":{}0{}}}]}}",
            "[".repeat(200),
            "]".repeat(200)
        ),
    )?;
    Ok(())
}

#[test]
fn manifest_conflicts_are_rejected_before_reading_input_files() -> TestResult {
    let directory = manifest_directory()?;
    for arguments in [
        vec!["secret-table"],
        vec!["--table-version", "0"],
        vec!["--storage-options-file", "secret-missing.json"],
        vec!["--auth-source", "env"],
    ] {
        let output = run_with_timeout(manifest_command(directory.path()).args(arguments))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_cli_argument");
        assert_eq!(record["argument_kind"], "ArgumentConflict");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    let output = run_with_timeout(manifest_command(directory.path()).arg("--help"))?;
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)?.contains("--tables-file"));
    Ok(())
}

#[test]
fn authorization_requires_a_named_source_and_file_references() -> TestResult {
    let directory = manifest_directory()?;
    for authorization in [
        json!(null),
        json!([]),
        json!({}),
        json!({"source":null}),
        json!({"source":{"json":null}}),
        json!({"source":"secret-unknown-source"}),
        json!({"source":"JSON"}),
        json!({"source":"env","storage_options_file":null}),
        json!({"source":"json","storage_options_file":{"secret-key":"secret-value"}}),
        json!({"source":"json","aws_access_key_id":"secret-key"}),
        json!({"source":"env","env_file":"secret-file.env"}),
    ] {
        assert_manifest_rejected(
            directory.path(),
            json!({"format_version":1,"tables":[
                {"alias":"orders","location":"secret-missing-table"},
                {"alias":"customers","location":"s3://bucket/table","authorization":authorization}
            ]})
            .to_string(),
        )?;
    }
    assert_manifest_rejected(
        directory.path(),
        r#"{"format_version":1,"tables":[
        {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"json","source":"env"}}
    ]}"#,
    )?;
    for path in [
        "",
        " ",
        "secret\0file",
        "https://secret.example/options.json",
    ] {
        let record = assert_manifest_rejected(directory.path(), json!({"format_version":1,"tables":[
            {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"json","storage_options_file":path}}
        ]}).to_string())?;
        assert_eq!(record["reason"], "invalid_authorization_reference");
        assert_eq!(
            record["field_path"],
            "/tables/0/authorization/storage_options_file"
        );
    }
    Ok(())
}

#[test]
fn manifest_table_count_keeps_completion_records_bounded() -> TestResult {
    let directory = manifest_directory()?;
    let tables: Vec<_> = (0..64)
        .map(|index| {
            json!({
                "alias": format!("table_{index:057}"),
                "location": fixture_path("partitioned").join("table"),
            })
        })
        .collect();
    let mut manifest = json!({"format_version":1,"tables":tables});
    fs::write(
        directory.path().join("inputs/tables.json"),
        manifest.to_string(),
    )?;
    let output = run_with_timeout(&mut manifest_command(directory.path()))?;
    decode_query_result(&output)?;
    assert!(output.stderr.len() <= 8192);
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "completion")["bindings"]
            .as_array()
            .unwrap()
            .len(),
        64
    );

    manifest["tables"].as_array_mut().unwrap().push(json!({
        "alias":"customers","location":"secret-missing-table"
    }));
    let record = assert_manifest_rejected(directory.path(), manifest.to_string())?;
    assert_eq!(record["reason"], "too_many_tables");
    Ok(())
}
