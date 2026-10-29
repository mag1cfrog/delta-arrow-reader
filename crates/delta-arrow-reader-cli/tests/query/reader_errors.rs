//! Reader failures retain their safe cause through the SQL command.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter;
use serde_json::json;

use super::*;

fn string_table_fixture(delta_type: &str) -> TestResult<SqlCommandFixture> {
    let mut fixture = SqlCommandFixture::new(
        "unused",
        "SELECT COUNT(*) AS row_count, COUNT(DISTINCT secret_value) AS distinct_values FROM t",
    )?;
    fixture.table = fixture.directory.path().join("secret-table");
    fs::create_dir_all(fixture.table.join("_delta_log"))?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "secret_value",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec![
            "secret_alpha",
            "secret_beta",
            "secret_alpha",
        ]))],
    )?;
    let path = fixture.table.join("secret-data.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let schema = json!({"type": "struct", "fields": [
        {"name": "secret_value", "type": delta_type, "nullable": true, "metadata": {}},
    ]});
    let actions = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {"id": "reader-diagnostic", "format": {"provider": "parquet", "options": {}}, "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {}}}),
        json!({"add": {"path": "secret-data.parquet", "partitionValues": {}, "size": fs::metadata(path)?.len(), "modificationTime": 0, "dataChange": true}}),
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

#[test]
fn sql_reader_failures_preserve_the_preview_cause_without_success_completion() -> TestResult {
    let control = string_table_fixture("string")?;
    let output =
        run_with_timeout(
            control
                .command()
                .args(["--format", "json", "--table-version", "0"]),
        )?;
    assert!(output.status.success(), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(result["rows"], json!([["3", "2"]]));
    assert_eq!(result["completion"], "complete");
    assert_single_terminal_record(&output.stderr, "completion");

    let invalid = string_table_fixture("long")?;
    let preview = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .arg("preview")
            .arg(&invalid.table)
            .args(["--diagnostics=json", "--table-version", "0"]),
    )?;
    assert_eq!(preview.status.code(), Some(1));
    let preview_error = assert_single_terminal_record(&preview.stderr, "diagnostic");
    assert_eq!(preview_error["phase"], "data_file_read");
    assert_eq!(preview_error["code"], "data_file_read");
    assert_eq!(preview_error["reason"], "parquet_schema_match_failed");
    assert_eq!(preview_error["source_kind"], "unexpected_column_type");

    for format in ["json", "arrow"] {
        let output =
            run_with_timeout(
                invalid
                    .command()
                    .args(["--format", format, "--table-version", "0"]),
            )?;
        assert_eq!(output.status.code(), Some(1));
        if format == "json" {
            assert!(output.stdout.is_empty());
        } else {
            assert!(!output.stdout.ends_with(IPC_END));
        }
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(error["phase"], "datafusion");
        assert_eq!(error["code"], "sql_execution");
        assert_eq!(error["reason"], "sql_execution");
        assert_eq!(
            error["bindings"],
            json!([{"alias": "t", "table_version": "0"}])
        );
        assert_eq!(
            error["cause"],
            json!({
                "phase": "data_file_read", "code": "data_file_read",
                "reason": "parquet_schema_match_failed", "source_kind": "unexpected_column_type",
            })
        );
        assert_eq!(error["recovery"], "unknown");
        assert!(error["retryable"].is_null());
        assert!(error.get("hint").is_none());
    }
    Ok(())
}
