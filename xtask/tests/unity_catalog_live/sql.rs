//! Offline SQL fixture setup and failure checks for both query invocations.

use arrow_array::Int64Array;

use super::*;

pub(super) fn prepare_fixture(root: &Path) -> TestResult {
    fs::create_dir_all(root.join("local-table/_delta_log"))?;
    fs::write(
        root.join("aggregate.sql"),
        "SELECT SUM(secret_value) AS total FROM uc_rows",
    )?;
    fs::write(
        root.join("join.sql"),
        "SELECT l.secret_value FROM uc_rows u JOIN local_rows l ON u.id = l.id",
    )?;
    for (operation, reference, batch) in [
        (
            "sql_aggregate",
            "aggregate.arrow",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "total",
                    DataType::Int64,
                    true,
                )])),
                vec![Arc::new(Int64Array::from(vec![60]))],
            )?,
        ),
        (
            "sql_join",
            "join.arrow",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "secret_value",
                    DataType::Int32,
                    true,
                )])),
                vec![Arc::new(Int32Array::from(vec![Some(10), None, Some(10)]))],
            )?,
        ),
    ] {
        write_arrow_file(&root.join(reference), &batch)?;
        let mut stream = StreamWriter::try_new(Vec::new(), &batch.schema())?;
        stream.write(&batch)?;
        stream.finish()?;
        let stdout = stream.into_inner()?;
        let mut bindings = vec![json!({"alias": "uc_rows", "table_version": "7"})];
        if operation == "sql_join" {
            bindings.push(json!({"alias": "local_rows", "table_version": "3"}));
        }
        let completion = json!({
            "format_version": 1, "kind": "completion", "terminal": true,
            "command": "query", "completion": "complete", "scope": "query",
            "stop_reason": "end_of_input", "has_more": false, "bindings": bindings,
            "rows": batch.num_rows().to_string(), "batches": "1", "output_bytes": stdout.len().to_string(),
        });
        fs::write(root.join(format!("{operation}.stdout")), stdout)?;
        fs::write(
            root.join(format!("{operation}.stderr")),
            format!("{completion}\n"),
        )?;
    }
    Ok(())
}

#[test]
fn queries_capture_sql_and_bind_each_table_to_its_own_snapshot() -> TestResult {
    let fixture = OfflineFixture::new()?;
    let output = fixture.command().output()?;
    assert!(output.status.success(), "{output:?}");
    let root = fixture.directory.path();
    for (operation, source, row_limit) in [
        ("sql_aggregate", "aggregate.sql", "1"),
        ("sql_join", "join.sql", "3"),
    ] {
        let arguments = fs::read_to_string(root.join(format!("{operation}.args")))?;
        let arguments: Vec<_> = arguments.lines().collect();
        assert_eq!(arguments[0], "query");
        assert!(arguments.contains(&"--diagnostics=json"));
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--max-rows", row_limit])
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--max-output-bytes", "16777216"])
        );
        assert_eq!(
            fs::read(root.join(format!("{operation}.captured.sql")))?,
            fs::read(root.join(source))?
        );
        let manifest: Value =
            serde_json::from_slice(&fs::read(root.join(format!("{operation}.tables.json")))?)?;
        assert_eq!(manifest["format_version"], 1);
        let tables = manifest["tables"].as_array().unwrap();
        assert_eq!(tables.len(), if operation == "sql_join" { 2 } else { 1 });
        assert_eq!(tables[0]["alias"], "uc_rows");
        assert_eq!(tables[0]["location"], fixture.config["table"]);
        assert_eq!(tables[0]["table_version"], "7");
        assert_eq!(tables[0]["authorization"]["source"], "databricks");
        assert_eq!(
            tables[0]["authorization"]["databricks_profile"],
            fixture.config["databricks_profile"]
        );
        let options = tables[0]["authorization"]["storage_options_file"]
            .as_str()
            .unwrap();
        let inspect_args = fs::read_to_string(root.join("inspect.args"))?;
        assert!(inspect_args.lines().any(|arg| arg == options));
        if operation == "sql_join" {
            assert_eq!(
                tables[1],
                json!({"alias": "local_rows", "location": root.join("local-table"), "table_version": "3"})
            );
        }
    }
    report(&output)?;
    Ok(())
}

#[test]
fn invalid_sql_inputs_fail_before_any_cli_runs() -> TestResult {
    for failure in [
        "missing_sql",
        "empty_sql",
        "large_sql",
        "non_utf8_sql",
        "corrupt_reference",
        "empty_reference",
        "remote_table",
        "no_delta_log",
    ] {
        let mut fixture = OfflineFixture::new()?;
        let root = fixture.directory.path();
        match failure {
            "missing_sql" => fs::remove_file(root.join("join.sql"))?,
            "empty_sql" => fs::write(root.join("join.sql"), " \n")?,
            "large_sql" => fs::write(root.join("join.sql"), "x".repeat(64 * 1024 + 1))?,
            "non_utf8_sql" => fs::write(root.join("join.sql"), [0xff])?,
            "corrupt_reference" => fs::write(root.join("join.arrow"), "secret-invalid-arrow")?,
            "empty_reference" => write_arrow_file(
                &root.join("join.arrow"),
                &RecordBatch::new_empty(Arc::new(Schema::empty())),
            )?,
            "remote_table" => {
                fixture.config["sql"]["local_table"] = json!("s3://secret-bucket/table");
                fixture.save_config()?;
            }
            "no_delta_log" => fs::remove_dir(root.join("local-table/_delta_log"))?,
            _ => unreachable!(),
        }
        let output = fixture.command().output()?;
        assert!(!output.status.success(), "{failure}");
        assert_eq!(report(&output)?["step"], "configuration");
        assert!(!root.join("databricks.args").exists());
    }
    Ok(())
}

#[test]
fn query_failures_preserve_check_status_and_redact_private_output() -> TestResult {
    for operation in ["sql_aggregate", "sql_join"] {
        for failure in [
            "values",
            "schema",
            "ipc",
            "snapshot",
            "counter",
            "diagnostic",
            "exit",
        ] {
            let fixture = OfflineFixture::new()?;
            let root = fixture.directory.path();
            let completion_file = format!("{operation}.stderr");
            match failure {
                "values" | "schema" => {
                    let reference = if operation == "sql_aggregate" {
                        "aggregate.arrow"
                    } else {
                        "join.arrow"
                    };
                    let reader = arrow_ipc::reader::FileReader::try_new(
                        fs::File::open(root.join(reference))?,
                        None,
                    )?;
                    let batch = reader.into_iter().next().unwrap()?;
                    let batch = if failure == "values" {
                        // Duplicating a row preserves row count and type but changes the multiset.
                        if operation == "sql_join" {
                            RecordBatch::try_new(
                                batch.schema(),
                                vec![Arc::new(Int32Array::from(vec![10, 10, 10]))],
                            )?
                        } else {
                            RecordBatch::try_new(
                                batch.schema(),
                                vec![Arc::new(Int64Array::from(vec![61]))],
                            )?
                        }
                    } else {
                        RecordBatch::try_new(
                            Arc::new(Schema::new(vec![Field::new(
                                "secret_wrong_column",
                                batch.schema().field(0).data_type().clone(),
                                true,
                            )])),
                            batch.columns().to_vec(),
                        )?
                    };
                    write_arrow_file(&root.join(reference), &batch)?;
                }
                "ipc" => {
                    let path = root.join(format!("{operation}.stdout"));
                    let length = fs::metadata(&path)?.len() - 8;
                    fs::OpenOptions::new()
                        .write(true)
                        .open(path)?
                        .set_len(length)?;
                    fixture.change_json(&completion_file, |value| {
                        value["output_bytes"] = json!(length.to_string())
                    })?;
                }
                "snapshot" => fixture.change_json(&completion_file, |value| {
                    let index = if operation == "sql_join" { 1 } else { 0 };
                    value["bindings"][index]["table_version"] = json!("99");
                })?,
                "counter" => {
                    fixture.change_json(&completion_file, |value| value["batches"] = json!("2"))?
                }
                "diagnostic" => fs::write(root.join(completion_file), "secret-query-with-token\n")?,
                "exit" => {}
                _ => unreachable!(),
            }
            let mut command = fixture.command();
            if failure == "exit" {
                command.env("UC_FIXTURE_FAIL_AT", operation);
            }
            let output = command.output()?;
            assert!(!output.status.success(), "{operation} {failure}");
            let result = report(&output)?;
            assert_eq!(result["step"], operation, "{failure}");
            assert_eq!(result["checks"][operation], "failed");
            assert_eq!(result["checks"]["scan"], "passed");
            if operation == "sql_aggregate" {
                assert_eq!(result["checks"]["sql_join"], "not_run");
                assert!(!root.join("sql_join.args").exists());
            } else {
                assert_eq!(result["checks"]["sql_aggregate"], "passed");
            }
        }
    }
    Ok(())
}
