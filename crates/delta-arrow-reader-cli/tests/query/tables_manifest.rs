//! Manifest admission, relative locations, and named snapshot provenance at the CLI boundary.

use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};

use super::{
    TestResult, assert_query_success_and_decode_arrow, assert_single_terminal_record, fixture_path,
    run_with_timeout,
};

fn create_manifest_query_directory() -> TestResult<tempfile::TempDir> {
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
    let directory = create_manifest_query_directory()?;
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
    assert!(query_guide.get("join").is_none());
    let options = query_guide["options"]["--tables-file"].as_str().unwrap();
    for argument in [
        "TABLE",
        "--table-version",
        "--storage-options-file",
        "--auth-source",
        "--databricks-profile",
    ] {
        assert!(options.contains(argument));
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
        guide["sql_example"].as_str().unwrap(),
    )?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command.env_clear().current_dir(directory.path()).args(
        guide["query_example"]
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
        let actual = super::json::assert_query_success_and_decode_json(&output)?;
        let fields = actual["schema"]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[1]["name"], "id");
        let expected_rows: Vec<_> = (1..=5)
            .zip(expected)
            .map(|(id, joined_id)| {
                json!([id.to_string(), joined_id.map(|value| value.to_string())])
            })
            .collect();
        assert_eq!(actual["rows"], json!(expected_rows));
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
    assert_eq!(discovery_bytes.len(), 3);
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
fn json_joins_preserve_all_bindings_duplicate_columns_and_nulls() -> TestResult {
    let directory = create_manifest_query_directory()?;
    fs::write(
        directory.path().join("query.sql"),
        "SELECT o.id, c.id FROM orders o LEFT JOIN customers c ON o.id = c.id WHERE o.id <= 5 ORDER BY o.id, c.id",
    )?;
    fs::write(
        directory.path().join("inputs/tables.json"),
        serde_json::to_vec(&json!({"format_version": 1, "tables": [
            {"alias": "orders", "location": fixture_path("partitioned").join("table"), "table_version": "0"},
            {"alias": "customers", "location": fixture_path("deletion_vectors").join("table"), "table_version": "1"}
        ]}))?,
    )?;
    let output = run_with_timeout(manifest_command(directory.path()).args(["--format", "json"]))?;
    let result = super::json::assert_query_success_and_decode_json(&output)?;
    assert_eq!(
        result["rows"],
        json!([["1", "1"], ["2", null], ["3", "3"], ["4", "4"], ["5", null]])
    );
    assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 2);
    assert_eq!(result["schema"]["fields"][0]["name"], "id");
    assert_eq!(result["schema"]["fields"][1]["name"], "id");
    assert_eq!(
        result["bindings"],
        json!([
            {"alias": "orders", "table_version": "0"},
            {"alias": "customers", "table_version": "1"}
        ])
    );
    let overflow = run_with_timeout(manifest_command(directory.path()).args([
        "--format",
        "json",
        "--max-output-bytes",
        &(output.stdout.len() - 1).to_string(),
    ]))?;
    assert_eq!(overflow.status.code(), Some(1));
    assert!(overflow.stdout.is_empty());
    let diagnostic = assert_single_terminal_record(&overflow.stderr, "diagnostic");
    assert_eq!(diagnostic["reason"], "json_output_limit_exceeded");
    assert_eq!(diagnostic["recovery"], "adjust_budget");
    assert_eq!(diagnostic["argument"], "--max-output-bytes");
    assert!(
        diagnostic["hint"]
            .as_str()
            .unwrap()
            .contains("--output PATH")
    );
    let exact = run_with_timeout(manifest_command(directory.path()).args([
        "--format",
        "json",
        diagnostic["argument"].as_str().unwrap(),
        &output.stdout.len().to_string(),
    ]))?;
    assert_eq!(
        super::json::assert_query_success_and_decode_json(&exact)?,
        result
    );
    // An explicit byte budget survives a format change and can still reject Arrow.
    let arrow_overflow = run_with_timeout(manifest_command(directory.path()).args([
        "--format",
        "arrow",
        "--max-output-bytes",
        &(output.stdout.len() - 1).to_string(),
    ]))?;
    assert_eq!(arrow_overflow.status.code(), Some(1));
    let diagnostic = assert_single_terminal_record(&arrow_overflow.stderr, "diagnostic");
    assert_eq!(diagnostic["reason"], "output_byte_limit_exceeded");
    assert_eq!(diagnostic["recovery"], "adjust_budget");
    assert_eq!(diagnostic["argument"], "--max-output-bytes");
    assert!(
        diagnostic["hint"]
            .as_str()
            .unwrap()
            .contains("snapshot versions")
    );
    let saved = run_with_timeout(manifest_command(directory.path()).args([
        "--format",
        "arrow",
        "--max-output-bytes",
        "8192",
    ]))?;
    assert_eq!(assert_query_success_and_decode_arrow(&saved)?.num_rows(), 5);
    let receipt = assert_single_terminal_record(&saved.stderr, "completion");
    assert_eq!(receipt["bindings"], result["bindings"]);
    Ok(())
}

#[test]
fn failed_queries_report_snapshots_for_retry_after_tables_change() -> TestResult {
    let directory = create_manifest_query_directory()?;
    let fixture = fixture_path("deletion_vectors").join("table");
    let commit = "_delta_log/00000000000000000001.json";
    let mut tables = Vec::new();
    for alias in ["orders", "customers"] {
        let location = directory.path().join(alias);
        copy_directory(&fixture, &location)?;
        fs::remove_file(location.join(commit))?;
        tables.push(json!({"alias": alias, "location": location}));
    }
    let mut manifest = json!({"format_version": 1, "tables": tables});
    let manifest_path = directory.path().join("inputs/tables.json");
    fs::write(&manifest_path, manifest.to_string())?;
    fs::write(
        directory.path().join("query.sql"),
        "SELECT o.id FROM orders o JOIN customers c ON o.id = c.id ORDER BY o.id",
    )?;
    let expected_bindings = json!([
        {"alias": "orders", "table_version": "0"},
        {"alias": "customers", "table_version": "0"}
    ]);
    for (format, budget, reason) in [
        ("json", "--max-output-bytes", "json_output_limit_exceeded"),
        ("json", "--max-rows", "output_row_limit_exceeded"),
        ("arrow", "--max-output-bytes", "output_byte_limit_exceeded"),
        ("arrow", "--max-rows", "output_row_limit_exceeded"),
    ] {
        let failed = run_with_timeout(
            manifest_command(directory.path()).args(["--format", format, budget, "1"]),
        )?;
        assert_eq!(failed.status.code(), Some(1));
        if format == "json" {
            assert!(failed.stdout.is_empty());
        }
        let diagnostic = assert_single_terminal_record(&failed.stderr, "diagnostic");
        assert_eq!(diagnostic["reason"], reason);
        assert_eq!(diagnostic["bindings"], expected_bindings);
        crate::capabilities::assert_output_matches_schema("diagnostic", &diagnostic);
        assert!(!String::from_utf8_lossy(&failed.stderr).contains("secret"));
        for (table, binding) in manifest["tables"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .zip(diagnostic["bindings"].as_array().unwrap())
        {
            table["table_version"] = binding["table_version"].clone();
        }
    }
    for alias in ["orders", "customers"] {
        fs::copy(
            fixture.join(commit),
            directory.path().join(alias).join(commit),
        )?;
    }
    let unpinned = super::json::assert_query_success_and_decode_json(&run_with_timeout(
        manifest_command(directory.path()).args(["--format", "json"]),
    )?)?;
    assert_eq!(unpinned["bindings"][0]["table_version"], "1");
    assert_ne!(unpinned["row_count"], "12");

    fs::write(&manifest_path, manifest.to_string())?;
    let recovered = super::json::assert_query_success_and_decode_json(&run_with_timeout(
        manifest_command(directory.path()).args(["--format", "json"]),
    )?)?;
    assert_eq!(recovered["bindings"], expected_bindings);
    assert_eq!(
        recovered["rows"],
        json!((1..=12).map(|id| vec![id.to_string()]).collect::<Vec<_>>())
    );
    Ok(())
}

#[test]
fn invalid_later_bindings_fail_before_opening_the_first_table() -> TestResult {
    let directory = create_manifest_query_directory()?;
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
        (
            "customers",
            "uc://main.sales.orders\n",
            "invalid_table_location",
            "location",
        ),
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
    let directory = create_manifest_query_directory()?;
    for (input, field_path) in [
        (
            r#"{"format_version":1,"format_version":1,"tables":[]}"#,
            "/format_version",
        ),
        (
            r#"{"format_version":1,"tables":[{"alias":"orders","alias":"customers","location":"secret-table"}]}"#,
            "/tables/0/alias",
        ),
        (
            r#"{"format_version":1,"tables":[{"alias":"orders","location":"secret-table","authorization":{"source":"env","source":"json"}}]}"#,
            "/tables/0/authorization/source",
        ),
    ] {
        let record = assert_manifest_rejected(directory.path(), input)?;
        assert_eq!(record["field_path"], field_path);
        assert_eq!(record["expected"], "one occurrence of this field");
    }
    for (table, field_path, expected) in [
        (
            json!({"alias": 0, "location": "secret-table"}),
            "/tables/0/alias",
            "a string",
        ),
        (
            json!({"alias": "orders", "location": null}),
            "/tables/0/location",
            "a string",
        ),
        (
            json!({"location": "secret-table"}),
            "/tables/0/alias",
            "a required field",
        ),
        (
            json!({"alias": "orders", "location": "secret-table", "authorization": []}),
            "/tables/0/authorization",
            "a JSON object",
        ),
        (
            json!({"alias": "orders", "location": "secret-table", "authorization": {}}),
            "/tables/0/authorization/source",
            "a required field",
        ),
        (
            json!({"alias": "orders", "location": "secret-table", "secret/key": "secret-value"}),
            "/tables/0",
            "object fields: alias, location, table_version, authorization",
        ),
        (
            json!({"alias": "orders", "location": "secret-table", "authorization": {"source": "env", "secret/key": "secret-value"}}),
            "/tables/0/authorization",
            "object fields: source, storage_options_file, env_file, databricks_profile",
        ),
        (
            json!({"alias": "orders", "location": "secret-table", "authorization": {"source": "env", "alias": "secret-value"}}),
            "/tables/0/authorization",
            "object fields: source, storage_options_file, env_file, databricks_profile",
        ),
    ] {
        let record = assert_manifest_rejected(
            directory.path(),
            json!({"format_version": 1, "tables": [table]}).to_string(),
        )?;
        assert_eq!(record["field_path"], field_path);
        assert_eq!(record["expected"], expected);
    }
    for field in [
        "source",
        "storage_options_file",
        "env_file",
        "databricks_profile",
    ] {
        let record = assert_manifest_rejected(directory.path(), json!({"format_version": 1, "tables": [
            {"alias": "orders", "location": "secret-table", "authorization": {"source": "env", field: false}}
        ]}).to_string())?;
        assert_eq!(
            record["field_path"],
            format!("/tables/0/authorization/{field}")
        );
        assert_eq!(record["expected"], "a string");
    }
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
        let record = assert_manifest_rejected(
            directory.path(),
            json!({"format_version":1,"tables":[
                {"alias":"orders","location":"secret-table","table_version":version}
            ]})
            .to_string(),
        )?;
        assert_eq!(record["field_path"], "/tables/0/table_version");
        assert_eq!(
            record["expected"],
            "canonical decimal integer string from 0 to 18446744073709551615"
        );
        assert_eq!(
            record["hint"],
            "Use a decimal string within the stated range, with no '+', leading zeros, or negative zero."
        );
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
    let directory = create_manifest_query_directory()?;
    for arguments in [
        vec!["secret-table"],
        vec!["--table-version", "0"],
        vec!["--storage-options-file", "secret-missing.json"],
        vec!["--auth-source", "env"],
        vec!["--env-file", "secret-missing.env"],
        vec!["--databricks-profile", "secret-profile"],
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
    let directory = create_manifest_query_directory()?;
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
        json!({"source":"env-file"}),
        json!({"source":"env-file","env_file":null}),
        json!({"source":"env-file","env_file":{"secret-key":"secret-value"}}),
        json!({"source":"env-file","env_file":"secret-file.env","databricks_profile":"work"}),
        json!({"source":"databricks","env_file":"secret-file.env","databricks_profile":"work"}),
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
    assert_manifest_rejected(
        directory.path(),
        r#"{"format_version":1,"tables":[
        {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"env-file","env_file":"secret-one.env","env_file":"secret-two.env"}}
    ]}"#,
    )?;
    for path in [
        "",
        " ",
        "secret\0file",
        "https://secret.example/options.json",
    ] {
        for (source, field) in [("json", "storage_options_file"), ("env-file", "env_file")] {
            let record = assert_manifest_rejected(directory.path(), json!({"format_version":1,"tables":[
                {"alias":"orders","location":"s3://bucket/table","authorization":{"source":source,field:path}}
            ]}).to_string())?;
            assert_eq!(record["reason"], "invalid_authorization_reference");
            assert_eq!(
                record["field_path"],
                format!("/tables/0/authorization/{field}")
            );
        }
    }
    Ok(())
}

#[test]
fn manifest_table_count_keeps_terminal_records_bounded() -> TestResult {
    let directory = create_manifest_query_directory()?;
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
    for format in ["arrow", "json"] {
        let output =
            run_with_timeout(manifest_command(directory.path()).args(["--format", format]))?;
        if format == "arrow" {
            assert_query_success_and_decode_arrow(&output)?;
        } else {
            super::json::assert_query_success_and_decode_json(&output)?;
        }
        assert!(output.stderr.len() > 4096);
        assert!(output.stderr.len() <= 8192);
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "completion")["bindings"]
                .as_array()
                .unwrap()
                .len(),
            64
        );
        let failed = run_with_timeout(manifest_command(directory.path()).args([
            "--format",
            format,
            "--max-rows",
            "0",
        ]))?;
        assert_eq!(failed.status.code(), Some(1));
        assert!(failed.stderr.len() <= 8192);
        let diagnostic = assert_single_terminal_record(&failed.stderr, "diagnostic");
        assert_eq!(diagnostic["bindings"].as_array().unwrap().len(), 64);
        crate::capabilities::assert_output_matches_schema("diagnostic", &diagnostic);
    }

    manifest["tables"].as_array_mut().unwrap().push(json!({
        "alias":"customers","location":"secret-missing-table"
    }));
    let record = assert_manifest_rejected(directory.path(), manifest.to_string())?;
    assert_eq!(record["reason"], "too_many_tables");
    Ok(())
}
