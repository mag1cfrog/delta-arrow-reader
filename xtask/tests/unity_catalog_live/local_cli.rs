//! Contract checks using a built CLI and local external-writer fixtures.

use std::path::PathBuf;

use arrow_array::Int64Array;

use super::*;

#[test]
#[ignore = "requires DAR_LOCAL_BINARY pointing to a built CLI; uses only local synthetic data"]
fn harness_checks_a_real_cli_against_a_local_delta_fixture() -> TestResult {
    let binary = PathBuf::from(std::env::var_os("DAR_LOCAL_BINARY").ok_or("set DAR_LOCAL_BINARY")?)
        .canonicalize()?;
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned").canonicalize()?;
    let mut fixture = OfflineFixture::new()?;
    let root = fixture.directory.path();
    fs::copy(corpus.join("expected.arrow"), root.join("table.arrow"))?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    write_arrow_file(
        &root.join("scan.arrow"),
        &RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![30]))])?,
    )?;
    fs::write(
        root.join("predicate.json"),
        r#"{"op":"eq","column":"id","value":{"type":"int32","value":"10"}}"#,
    )?;
    fixture.config["table_version"] = json!(0);
    fixture.config["scan"]["columns"] = json!(["value"]);
    fixture.config["storage_options_file"] = Value::Null;
    let local_table = corpus.parent().unwrap().join("nested_mapping/table");
    fixture.config["sql"]["local_table"] = json!(local_table);
    fixture.config["sql"]["local_table_version"] = json!(1);
    fs::write(
        root.join("aggregate.sql"),
        "SELECT COUNT(*) AS row_count, SUM(value) AS total FROM uc_rows",
    )?;
    write_arrow_file(
        &root.join("aggregate.arrow"),
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("row_count", DataType::Int64, false),
                Field::new("total", DataType::Int64, true),
            ])),
            // Computed from the fixture generator: ids 1-360, value = 3*id except every fourth is null.
            vec![
                Arc::new(Int64Array::from(vec![360])),
                Arc::new(Int64Array::from(vec![145800])),
            ],
        )?,
    )?;
    fs::write(
        root.join("join.sql"),
        "SELECT u.id, u.value AS uc_value, l.value AS local_value FROM uc_rows u JOIN local_rows l ON u.id = l.id",
    )?;
    write_arrow_file(
        &root.join("join.arrow"),
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("uc_value", DataType::Int32, true),
                Field::new("local_value", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Int32Array::from(vec![
                    Some(3),
                    Some(6),
                    Some(9),
                    None,
                    Some(15),
                    Some(18),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    None,
                    Some(30),
                    Some(40),
                    None,
                    Some(60),
                ])),
            ],
        )?,
    )?;
    for operation in ["sql_aggregate", "sql_join"] {
        let mut tables = vec![
            json!({"alias": "uc_rows", "location": corpus.join("table"), "table_version": "0"}),
        ];
        if operation == "sql_join" {
            tables.push(
                json!({"alias": "local_rows", "location": local_table, "table_version": "1"}),
            );
        }
        fs::write(
            root.join(format!("{operation}.local.json")),
            json!({"format_version": 1, "tables": tables}).to_string(),
        )?;
    }
    fixture.save_config()?;
    // Redirect UC access locally; the real CLI executes the captured SQL and emits both contracts.
    let output = fixture
        .command()
        .env("DAR_LOCAL_BINARY", binary)
        .env("DAR_LOCAL_TABLE", corpus.join("table"))
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let result = report(&output)?;
    assert_eq!(
        result["checks"],
        json!({"inspect": "passed", "scan": "passed", "sql_aggregate": "passed", "sql_join": "passed"})
    );
    Ok(())
}
