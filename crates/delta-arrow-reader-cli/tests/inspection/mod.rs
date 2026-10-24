//! Summary inspection, focused recovery, and compatibility with full inspection.

use super::*;
use serde_json::json;

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(super) fn table_with_fields(
    fields: Vec<Value>,
    unsupported_protocol: bool,
) -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("_delta_log")).unwrap();
    let protocol = if unsupported_protocol {
        json!({"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["secret-feature"], "writerFeatures": ["secret-feature"]})
    } else {
        json!({"minReaderVersion": 1, "minWriterVersion": 2})
    };
    let schema = json!({"type": "struct", "fields": fields});
    fs::write(directory.path().join("_delta_log/00000000000000000000.json"), format!("{}\n{}\n{}\n",
        json!({"protocol": protocol}),
        json!({"metaData": {
            "id": "test", "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {},
        }}),
        json!({"add": {"path": "unreadable.parquet", "partitionValues": {}, "size": 100, "modificationTime": 0, "dataChange": true}})
    )).unwrap();
    directory
}

pub(super) fn column(name: &str, data_type: Value, metadata: Value) -> Value {
    json!({"name": name, "type": data_type, "nullable": true, "metadata": metadata})
}

fn inspect(table: &Path, args: &[&str]) -> Output {
    run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .env_clear()
            .args(["--diagnostics=json", "inspect"])
            .arg(table)
            .args(args),
    )
    .unwrap()
}

fn assert_summary_success(output: &Output, budget: usize) -> Value {
    assert!(output.status.success(), "{output:?}");
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stdout.len() <= budget);
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    capabilities::assert_output_matches_schema("inspection-summary", &result);
    let completion = diagnostics::assert_single_terminal_record(&output.stderr, "completion");
    assert_eq!(completion["output_bytes"], output.stdout.len().to_string());
    assert_eq!(
        completion["bindings"][0]["table_version"],
        result["table_version"]
    );
    result
}

#[test]
fn discovery_finds_only_the_requested_column_in_a_wide_pinned_snapshot() -> TestResult {
    let fields = (0..1000)
        .map(|index| {
            column(
                &format!("column_{index:04}"),
                json!("integer"),
                json!({"comment": "private metadata".repeat(20)}),
            )
        })
        .collect();
    let table = table_with_fields(fields, false);
    let guide = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
        "capabilities",
        "--command",
        "inspect",
    ]))?;
    assert!(guide.status.success());
    assert!(guide.stdout.len() < 4096);
    let guide: Value = serde_json::from_slice(&guide.stdout)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    for argument in guide["example"].as_array().unwrap().iter().skip(1) {
        command.arg(if argument == "./table" {
            table.path().as_os_str()
        } else {
            std::ffi::OsStr::new(argument.as_str().unwrap())
        });
    }
    let overview = assert_summary_success(&run_with_timeout(&mut command)?, 4096);
    assert_eq!(overview.as_object().unwrap().len(), 4);
    assert_eq!(
        overview["checks"],
        json!({"scan_protocol": "passed", "data_files": "not_checked"})
    );
    let version = overview["table_version"].as_str().unwrap();
    // A newer schema must not change the meaning of a pinned lookup.
    fs::write(
        table.path().join("_delta_log/00000000000000000001.json"),
        format!(
            "{}\n",
            json!({"metaData": {
                "id": "test", "format": {"provider": "parquet", "options": {}},
                "schemaString": json!({"type": "struct", "fields": [column("new_name", json!("long"), json!({}))]}).to_string(),
                "partitionColumns": [], "configuration": {},
            }})
        ),
    )?;
    let names = assert_summary_success(
        &inspect(
            table.path(),
            &[
                "--view",
                "summary",
                "--table-version",
                version,
                "--column-prefix",
                "column_0999",
            ],
        ),
        4096,
    );
    assert_eq!(names["column_names"], json!(["column_0999"]));
    assert!(names.get("fields").is_none() && names.get("checks").is_none());
    let field = assert_summary_success(
        &inspect(
            table.path(),
            &[
                "--view",
                "summary",
                "--table-version",
                version,
                "--column",
                names["column_names"][0].as_str().unwrap(),
            ],
        ),
        4096,
    );
    assert_eq!(
        field["fields"],
        json!([{"name": "column_0999", "data_type": "Int32", "nullable": true}])
    );
    assert!(field.get("checks").is_none());
    let selected = assert_summary_success(
        &inspect(
            table.path(),
            &[
                "--view",
                "summary",
                "--table-version",
                version,
                "--column",
                "column_0999",
                "--column",
                "column_0998",
            ],
        ),
        4096,
    );
    assert_eq!(
        selected["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["column_0999", "column_0998"]
    );
    let no_match = assert_summary_success(
        &inspect(
            table.path(),
            &[
                "--view",
                "summary",
                "--table-version",
                version,
                "--column-prefix",
                "COLUMN",
            ],
        ),
        4096,
    );
    assert_eq!(no_match["column_names"], json!([]));
    let too_many = inspect(
        table.path(),
        &[
            "--view",
            "summary",
            "--table-version",
            version,
            "--column-prefix",
            "column_",
        ],
    );
    assert_eq!(too_many.status.code(), Some(1));
    assert!(too_many.stdout.is_empty());
    let error = diagnostics::assert_single_terminal_record(&too_many.stderr, "diagnostic");
    assert!(
        error["hint"]
            .as_str()
            .unwrap()
            .contains("Narrow the selection")
    );
    Ok(())
}

#[test]
fn selected_details_have_exact_byte_limits_and_opt_in_metadata() {
    let name = "列".repeat(1600);
    let table = table_with_fields(
        vec![
            column(
                &name,
                json!("decimal(18,4)"),
                json!({"comment": "private metadata".repeat(1000)}),
            ),
            column(
                "id",
                json!("integer"),
                json!({"comment": "private metadata".repeat(1000)}),
            ),
        ],
        false,
    );
    assert_summary_success(&inspect(table.path(), &["--view", "summary"]), 4096);
    let short = assert_summary_success(
        &inspect(table.path(), &["--view", "summary", "--column", "id"]),
        4096,
    );
    assert_eq!(
        short["fields"],
        json!([{"name": "id", "data_type": "Int32", "nullable": true}])
    );
    let metadata_overflow = inspect(
        table.path(),
        &["--view", "summary", "--column", "id", "--include-metadata"],
    );
    assert_eq!(metadata_overflow.status.code(), Some(1));
    assert!(metadata_overflow.stdout.is_empty());
    diagnostics::assert_single_terminal_record(&metadata_overflow.stderr, "diagnostic");
    for selector in ["--column", "--column-prefix"] {
        let rejected = inspect(table.path(), &["--view", "summary", selector, &name]);
        assert_eq!(rejected.status.code(), Some(1));
        assert!(rejected.stdout.is_empty());
        let diagnostic = diagnostics::assert_single_terminal_record(&rejected.stderr, "diagnostic");
        assert_eq!(diagnostic["recovery"], "adjust_budget");
        assert!(!String::from_utf8_lossy(&rejected.stderr).contains(&name));
        let accepted = inspect(
            table.path(),
            &[
                "--view",
                "summary",
                selector,
                &name,
                "--max-output-bytes",
                "65536",
            ],
        );
        let result = assert_summary_success(&accepted, 65536);
        let exact = accepted.stdout.len().to_string();
        let below = (accepted.stdout.len() - 1).to_string();
        let exact_output = inspect(
            table.path(),
            &[
                "--view",
                "summary",
                selector,
                &name,
                "--max-output-bytes",
                &exact,
            ],
        );
        assert_eq!(exact_output.stdout, accepted.stdout);
        assert_summary_success(&exact_output, accepted.stdout.len());
        let overflow = inspect(
            table.path(),
            &[
                "--view",
                "summary",
                selector,
                &name,
                "--max-output-bytes",
                &below,
            ],
        );
        assert_eq!(overflow.status.code(), Some(1));
        assert!(overflow.stdout.is_empty());
        if selector == "--column" {
            assert_eq!(
                result["fields"][0],
                json!({"name": name, "data_type": {"Decimal128": [18, 4]}, "nullable": true})
            );
        } else {
            assert_eq!(result["column_names"], json!([name]));
        }
    }
    let selected = inspect(
        table.path(),
        &[
            "--view",
            "summary",
            "--column",
            &name,
            "--include-metadata",
            "--max-output-bytes",
            "65536",
        ],
    );
    let selected = assert_summary_success(&selected, 65536);
    assert_eq!(
        selected["fields"][0]["metadata"]["comment"],
        "private metadata".repeat(1000)
    );
    let legacy = inspect(table.path(), &[]);
    let explicit_full = inspect(table.path(), &["--view", "full"]);
    assert_eq!(legacy.stdout, explicit_full.stdout);
    assert!(legacy.status.success());
    let legacy: Value = serde_json::from_slice(&legacy.stdout).unwrap();
    assert_eq!(
        legacy["schema"]["fields"][0]["metadata"],
        selected["fields"][0]["metadata"]
    );
    assert_eq!(legacy["schema"]["fields"][0]["dict_id"], 0);
}

#[test]
fn empty_nested_and_unsupported_snapshots_remain_inspectable() {
    for unsupported in [false, true] {
        let table = table_with_fields(vec![], unsupported);
        let result = assert_summary_success(&inspect(table.path(), &["--view", "summary"]), 4096);
        assert_eq!(
            result["checks"]["scan_protocol"],
            if unsupported { "failed" } else { "passed" }
        );
        assert_eq!(result["checks"]["data_files"], "not_checked");
        let fields = assert_summary_success(
            &inspect(table.path(), &["--view", "summary", "--column-prefix", ""]),
            4096,
        );
        assert_eq!(fields["column_names"], json!([]));
    }
    let mut nested = json!("timestamp");
    for _ in 0..20 {
        nested = json!({"type": "struct", "fields": [column(&"nested_".repeat(40), nested, json!({"comment": "secret nested metadata"}))]});
    }
    let table = table_with_fields(vec![column("record", nested, json!({}))], false);
    assert_summary_success(&inspect(table.path(), &["--view", "summary"]), 4096);
    let overflow = inspect(table.path(), &["--view", "summary", "--column", "record"]);
    assert_eq!(overflow.status.code(), Some(1));
    assert!(overflow.stdout.is_empty());
    diagnostics::assert_single_terminal_record(&overflow.stderr, "diagnostic");
    let output = inspect(
        table.path(),
        &[
            "--view",
            "summary",
            "--column",
            "record",
            "--max-output-bytes",
            "16384",
        ],
    );
    let result = assert_summary_success(&output, 16384);
    let mut field = &result["fields"][0];
    for _ in 0..20 {
        assert!(field.get("metadata").is_none());
        field = &field["data_type"]["Struct"][0];
        assert_eq!(field["name"], "nested_".repeat(40));
    }
    assert_eq!(
        field["data_type"],
        json!({"Timestamp": ["Microsecond", "UTC"]})
    );
}

#[test]
fn selection_errors_are_safe_and_never_fall_back_to_full_inspection() {
    let table = table_with_fields(vec![column("id", json!("integer"), json!({}))], false);
    for args in [
        vec!["--column", "id"],
        vec!["--view", "full", "--max-output-bytes", "10"],
        vec!["--view", "summary", "--column", "secret-missing"],
        vec!["--view", "summary", "--column", "id", "--column", "id"],
        vec![
            "--view",
            "summary",
            "--column",
            "id",
            "--column-prefix",
            "id",
        ],
        vec!["--view", "summary", "--include-metadata"],
        vec!["--view", "summary", "--max-output-bytes", "0"],
    ] {
        let output = inspect(table.path(), &args);
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
    }
}

#[test]
fn projection_and_predicate_repairs_use_selected_names_and_types() -> TestResult {
    let table = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    );
    let directory = tempfile::tempdir()?;
    let predicate_path = directory.path().join("predicate.json");
    let overview = assert_summary_success(&inspect(&table, &["--view", "summary"]), 4096);
    let version = overview["table_version"].as_str().unwrap();
    for failure in ["projection", "predicate_column", "predicate_type"] {
        let predicate = json!({"op": "ge", "column": "secret-wrong-column", "value": {"type": "int32", "value": "1"}});
        fs::write(&predicate_path, serde_json::to_vec(&predicate)?)?;
        let mut scan = Command::new(env!("CARGO_BIN_EXE_dar"));
        scan.args([
            "--diagnostics=json",
            "scan",
            "--table-version",
            version,
            "--limit",
            "1",
        ])
        .arg(&table);
        if failure == "projection" {
            scan.args(["--column", "secret-wrong-column"]);
        } else {
            scan.arg("--predicate-file").arg(&predicate_path);
        }
        if failure == "predicate_type" {
            // Ordinary type errors already carry the exact expected type and need no lookup.
            let wrong_type =
                json!({"op": "ge", "column": "id", "value": {"type": "int64", "value": "1"}});
            fs::write(&predicate_path, serde_json::to_vec(&wrong_type)?)?;
        }
        let rejected = run_with_timeout(&mut scan)?;
        assert_eq!(rejected.status.code(), Some(1));
        assert!(rejected.stdout.is_empty());
        let error = diagnostics::assert_single_terminal_record(&rejected.stderr, "diagnostic");
        if failure == "predicate_type" {
            assert_eq!(error["expected"], "int32");
        } else {
            let hint = error["hint"].as_str().unwrap();
            assert!(
                hint.contains("--view summary --column-prefix PREFIX")
                    && hint.contains("--table-version N")
            );
            let names = assert_summary_success(
                &inspect(
                    &table,
                    &[
                        "--view",
                        "summary",
                        "--table-version",
                        version,
                        "--column-prefix",
                        "id",
                    ],
                ),
                4096,
            );
            assert_eq!(names["column_names"], json!(["id"]));
            let selected = assert_summary_success(
                &inspect(
                    &table,
                    &[
                        "--view",
                        "summary",
                        "--table-version",
                        version,
                        "--column",
                        "id",
                    ],
                ),
                4096,
            );
            assert_eq!(
                selected["fields"],
                json!([{"name": "id", "data_type": "Int32", "nullable": true}])
            );
        }
        let repaired =
            json!({"op": "ge", "column": "id", "value": {"type": "int32", "value": "1"}});
        fs::write(&predicate_path, serde_json::to_vec(&repaired)?)?;
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args([
                    "--diagnostics=json",
                    "scan",
                    "--table-version",
                    version,
                    "--limit",
                    "1",
                    "--column",
                    "id",
                ])
                .arg(&table)
                .arg("--predicate-file")
                .arg(&predicate_path),
        )?;
        assert!(output.status.success(), "{output:?}");
        assert!(output.stdout.len() < 4096);
        let completion = diagnostics::assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(completion["rows"], "1");
        let rows = StreamReader::try_new(Cursor::new(output.stdout), None)?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
    Ok(())
}
