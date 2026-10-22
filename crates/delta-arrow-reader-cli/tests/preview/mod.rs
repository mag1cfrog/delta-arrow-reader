//! Bounded samples retain native scan projection, predicates, and snapshot selection.

#[cfg(target_os = "linux")]
mod deadlines;
mod wide_tables;

use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

use serde_json::{Value, json};

use super::{
    capabilities::assert_output_matches_schema, diagnostics::assert_single_terminal_record,
    run_with_timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn table_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus")
        .join(name)
        .join("table")
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .env_clear()
        .args(["preview", "--diagnostics=json"])
        .arg(table_path("partitioned"));
    command
}

fn decode_sample(output: &Output) -> TestResult<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sample: Value = serde_json::from_slice(&output.stdout)?;
    assert_output_matches_schema("rows", &sample);
    assert_eq!(sample["completion"], "sample");
    assert!(sample.get("scope").is_none());
    let receipt = assert_single_terminal_record(&output.stderr, "completion");
    assert_output_matches_schema("completion", &receipt);
    assert_eq!(receipt["format_version"], 2);
    assert_eq!(receipt["command"], "preview");
    assert_eq!(receipt["bindings"], sample["bindings"]);
    assert_eq!(receipt["rows"], sample["row_count"]);
    assert_eq!(receipt["output_bytes"], output.stdout.len().to_string());
    assert_eq!(
        sample["row_count"],
        sample["rows"].as_array().unwrap().len().to_string()
    );
    Ok(sample)
}

#[test]
fn row_limits_never_infer_exhaustion_without_observing_it() -> TestResult {
    for (limit, count, reason, more) in [
        (0, 0, "row_limit", Value::Null),
        (1, 1, "row_limit", Value::Null),
        (12, 12, "row_limit", Value::Null),
        (13, 12, "end_of_input", json!(false)),
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args([
                    "preview",
                    "--diagnostics=json",
                    "--table-version",
                    "0",
                    "--column",
                    "id",
                    "--limit",
                    &limit.to_string(),
                ])
                .arg(table_path("deletion_vectors")),
        )?;
        let sample = decode_sample(&output)?;
        assert_eq!(sample["row_count"], count.to_string());
        assert_eq!(sample["stop_reason"], reason);
        assert_eq!(sample["has_more"], more);
        assert_eq!(sample["schema"]["fields"].as_array().unwrap().len(), 1);
    }
    let empty_columns = decode_sample(&run_with_timeout(command().args([
        "--no-columns",
        "--limit",
        "2",
    ]))?)?;
    assert_eq!(empty_columns["schema"]["fields"], json!([]));
    assert_eq!(empty_columns["rows"], json!([[], []]));
    Ok(())
}

#[test]
fn preview_reuses_projection_predicate_options_and_pinned_versions() -> TestResult {
    let directory = tempfile::tempdir()?;
    let predicate = directory.path().join("predicate.json");
    let execution = directory.path().join("execution.json");
    for backend in ["direct", "delta_kernel"] {
        fs::write(
            &execution,
            serde_json::to_vec(&json!({"parquet_backend": backend}))?,
        )?;
        fs::write(
            &predicate,
            serde_json::to_vec(
                &json!({"op": "lt", "column": "id", "value": {"type": "int32", "value": "3"}}),
            )?,
        )?;
        let output = run_with_timeout(
            command()
                .args([
                    "--column",
                    "value",
                    "--column",
                    "id",
                    "--target-partitions",
                    "1",
                    "--table-version",
                    "0",
                ])
                .arg("--predicate-file")
                .arg(&predicate)
                .arg("--execution-options-file")
                .arg(&execution),
        )?;
        let sample = decode_sample(&output)?;
        assert_eq!(sample["schema"]["fields"][0]["name"], "value");
        assert_eq!(sample["schema"]["fields"][1]["name"], "id");
        assert_eq!(sample["row_count"], "2");
        for row in sample["rows"].as_array().unwrap() {
            assert!(row[1].as_str().unwrap().parse::<i32>()? < 3);
        }
        fs::write(&predicate, r#"{"op":"constant","value":false}"#)?;
        let sample = decode_sample(&run_with_timeout(
            command().arg("--predicate-file").arg(&predicate),
        )?)?;
        assert_eq!(sample["rows"], json!([]));
        assert_eq!(sample["stop_reason"], "end_of_input");
    }
    for version in ["0", "1"] {
        let sample = decode_sample(&run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .args([
                    "preview",
                    "--diagnostics=json",
                    "--column",
                    "id",
                    "--table-version",
                    version,
                ])
                .arg(table_path("deletion_vectors")),
        )?)?;
        assert_eq!(sample["bindings"][0]["table_version"], version);
        assert_eq!(sample["row_count"], if version == "0" { "12" } else { "9" });
    }
    Ok(())
}

#[test]
fn whole_response_budget_preserves_exact_boundaries_and_labels_shortened_samples() -> TestResult {
    let output = run_with_timeout(command().args(["--column", "id", "--target-partitions", "1"]))?;
    let sample = decode_sample(&output)?;
    let budget = output.stdout.len().to_string();
    assert_eq!(
        decode_sample(&run_with_timeout(command().args([
            "--column",
            "id",
            "--target-partitions",
            "1",
            "--max-output-bytes",
            &budget
        ]))?)?,
        sample
    );
    let shorter = decode_sample(&run_with_timeout(command().args([
        "--column",
        "id",
        "--max-output-bytes",
        &(output.stdout.len() - 10).to_string(),
    ]))?)?;
    assert_eq!(shorter["stop_reason"], "byte_limit");
    assert_eq!(shorter["has_more"], true);
    assert!(shorter["rows"].as_array().unwrap().len() < sample["rows"].as_array().unwrap().len());
    let rejected = run_with_timeout(command().args(["--max-output-bytes", "1"]))?;
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    let diagnostic = assert_single_terminal_record(&rejected.stderr, "diagnostic");
    assert_eq!(diagnostic["reason"], "preview_schema_limit_exceeded");
    assert_eq!(diagnostic["recovery"], "change_request");
    assert_eq!(diagnostic["argument"], "--column");
    for (option, value) in [
        ("--deadline-ms", "0"),
        ("--limit", "-1"),
        ("--max-output-bytes", "0"),
    ] {
        let output = run_with_timeout(command().args([option, value]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_single_terminal_record(&output.stderr, "diagnostic");
    }
    Ok(())
}
