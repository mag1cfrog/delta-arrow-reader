//! A wide snapshot stays usable through focused discovery, sampling, and SQL.

use std::path::Path;

use super::*;

fn create_wide_table(directory: &Path) -> TestResult<Value> {
    let source = table_path("deletion_vectors");
    let log = fs::read_to_string(source.join("_delta_log/00000000000000000000.json"))?;
    let mut actions: Vec<Value> = log
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let metadata = actions
        .iter_mut()
        .find_map(|action| action.get_mut("metaData"))
        .unwrap();
    let mut schema: Value = serde_json::from_str(metadata["schemaString"].as_str().unwrap())?;
    let fields = schema["fields"].as_array_mut().unwrap();
    fields[0]["metadata"] = json!({"comment": "private field metadata".repeat(20_000)});
    // Nullable columns added by schema evolution need no replacement data file.
    fields.extend((3..1000).map(|index| json!({
        "name": format!("unrelated_{index:04}"), "type": "long", "nullable": true, "metadata": {}
    })));
    metadata["schemaString"] = schema.to_string().into();
    let added_file = actions
        .iter()
        .find_map(|action| action.get("add"))
        .unwrap()
        .clone();
    let filename = added_file["path"].as_str().unwrap();
    fs::copy(source.join(filename), directory.join(filename))?;
    fs::create_dir(directory.join("_delta_log"))?;
    fs::write(
        directory.join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )?;
    Ok(added_file)
}

#[test]
fn wide_table_discovery_preview_query_and_repair_keep_the_requested_snapshot() -> TestResult {
    let directory = tempfile::tempdir()?;
    let added_file = create_wide_table(directory.path())?;
    let mut response_sizes = Vec::new();
    let mut run = |arguments: &[&str]| -> TestResult<Output> {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .current_dir(directory.path())
                .args(arguments),
        )?;
        response_sizes.push((
            arguments.join(" "),
            output.stdout.len(),
            output.stderr.len(),
        ));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("private field metadata"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("private field metadata"));
        Ok(output)
    };
    let index: Value = serde_json::from_slice(&run(&["capabilities"])?.stdout)?;
    for name in ["inspect", "preview", "query"] {
        let entry = index["commands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == name)
            .unwrap();
        let invocation: Vec<_> = entry["details"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|argument| argument.as_str().unwrap())
            .collect();
        let guide: Value = serde_json::from_slice(&run(&invocation)?.stdout)?;
        assert!(guide.get("arguments").is_none());
        assert!(guide.get("read_support").is_none());
    }
    let summary = run(&["inspect", ".", "--view", "summary", "--diagnostics=json"])?;
    assert!(summary.status.success());
    let summary: Value = serde_json::from_slice(&summary.stdout)?;
    assert!(summary.get("fields").is_none());
    let version = summary["table_version"].as_str().unwrap();
    assert_eq!(version, "0");
    // A newer empty snapshot makes an accidental retry against latest observable.
    fs::write(
        directory
            .path()
            .join("_delta_log/00000000000000000001.json"),
        format!(
            "{}\n",
            json!({
                "remove": {"path": added_file["path"], "deletionTimestamp": 1, "dataChange": true}
            })
        ),
    )?;
    let names = run(&[
        "inspect",
        ".",
        "--view",
        "summary",
        "--table-version",
        version,
        "--column-prefix",
        "i",
        "--diagnostics=json",
    ])?;
    assert!(names.status.success());
    let names: Value = serde_json::from_slice(&names.stdout)?;
    assert_eq!(names["column_names"], json!(["id"]));
    assert!(names.get("fields").is_none());
    assert!(names.get("checks").is_none());
    let column = names["column_names"][0].as_str().unwrap();
    let types = run(&[
        "inspect",
        ".",
        "--view",
        "summary",
        "--table-version",
        version,
        "--column",
        column,
        "--diagnostics=json",
    ])?;
    assert!(types.status.success());
    let types: Value = serde_json::from_slice(&types.stdout)?;
    assert_eq!(
        types["fields"],
        json!([{"name": "id", "data_type": "Int32", "nullable": true}])
    );
    let missing = run(&[
        "preview",
        ".",
        "--table-version",
        version,
        "--column",
        "missing_id",
        "--limit",
        "2",
        "--diagnostics=json",
    ])?;
    assert!(!missing.status.success());
    assert!(missing.stdout.is_empty());
    assert_single_terminal_record(&missing.stderr, "diagnostic");
    let too_wide = run(&[
        "preview",
        ".",
        "--table-version",
        version,
        "--limit",
        "2",
        "--diagnostics=json",
    ])?;
    assert_eq!(too_wide.status.code(), Some(1));
    assert!(too_wide.stdout.is_empty());
    let diagnostic = assert_single_terminal_record(&too_wide.stderr, "diagnostic");
    assert_eq!(diagnostic["reason"], "preview_schema_limit_exceeded");
    assert!(diagnostic["hint"].as_str().unwrap().contains("--column"));
    let preview = run(&[
        "preview",
        ".",
        "--table-version",
        version,
        "--column",
        column,
        "--limit",
        "2",
        "--diagnostics=json",
    ])?;
    let sample = decode_sample(&preview)?;
    assert_eq!(sample["bindings"][0]["table_version"], version);
    assert_eq!(sample["schema"]["fields"], types["fields"]);
    assert_eq!(sample["rows"], json!([["1"], ["2"]]));
    assert_eq!(sample["stop_reason"], "row_limit");
    assert!(sample["has_more"].is_null());
    // Reading an evolved column proves the fixture is a readable 1000-column table.
    let evolved = decode_sample(&run(&[
        "preview",
        ".",
        "--table-version",
        version,
        "--column",
        "unrelated_0999",
        "--limit",
        "1",
        "--diagnostics=json",
    ])?)?;
    assert_eq!(evolved["rows"], json!([[null]]));
    fs::write(
        directory.path().join("count.sql"),
        "SELECT count(*) AS n FROM t",
    )?;
    let query = run(&[
        "query",
        ".",
        "--table-version",
        version,
        "--sql-file",
        "count.sql",
        "--format",
        "json",
        "--diagnostics=json",
    ])?;
    assert!(query.status.success(), "{query:?}");
    let result: Value = serde_json::from_slice(&query.stdout)?;
    assert_output_matches_schema("rows", &result);
    assert_eq!(result["bindings"], sample["bindings"]);
    assert_eq!(result["completion"], "complete");
    assert_eq!(result["scope"], "query");
    assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 1);
    assert_eq!(result["rows"], json!([["12"]]));
    let receipt = assert_single_terminal_record(&query.stderr, "completion");
    assert_eq!(receipt["bindings"], result["bindings"]);
    assert_eq!(receipt["rows"], "1");
    assert_eq!(receipt["output_bytes"], query.stdout.len().to_string());
    let latest = decode_sample(&run(&[
        "preview",
        ".",
        "--column",
        column,
        "--diagnostics=json",
    ])?)?;
    assert_eq!(latest["bindings"][0]["table_version"], "1");
    assert_eq!(latest["rows"], json!([]));
    eprintln!("Wide snapshot workflow (command, stdout bytes, stderr bytes): {response_sizes:?}");
    Ok(())
}
