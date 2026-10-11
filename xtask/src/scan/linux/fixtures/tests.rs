use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn read_actions(table: &Path) -> TestResult<Vec<Value>> {
    Ok(
        fs::read_to_string(table.join("_delta_log/00000000000000000000.json"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?,
    )
}

#[test]
fn float64_fixture_preserves_adjacent_values_and_declares_its_schema() -> TestResult {
    assert_eq!(FLOAT64_VALUES[0].to_bits() + 1, FLOAT64_VALUES[1].to_bits());
    let temporary = tempfile::tempdir()?;
    let table = temporary.path().join("float64-table");
    let expected = create_float64_table(&table)?;
    let file = table.join("part.parquet");
    let actual = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&file)?)?
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(actual, vec![expected]);
    let actions = read_actions(&table)?;
    assert_eq!(actions.len(), 3);
    assert_eq!(
        actions[0],
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
    );
    assert_eq!(actions[1]["metaData"]["format"]["provider"], "parquet");
    let schema: Value =
        serde_json::from_str(actions[1]["metaData"]["schemaString"].as_str().unwrap())?;
    assert_eq!(
        schema,
        json!({"type": "struct", "fields": [
            {"name": "id", "type": "integer", "nullable": true, "metadata": {}},
            {"name": "value", "type": "double", "nullable": true, "metadata": {}},
        ]})
    );
    assert_eq!(actions[2]["add"]["path"], "part.parquet");
    assert_eq!(actions[2]["add"]["size"], fs::metadata(file)?.len());
    Ok(())
}

#[test]
fn repeated_fixture_preserves_file_bytes_and_partition_metadata() -> TestResult {
    let source = crate::repository::repository_root()
        .join("tests/reader/fixtures/external_writer/corpus/partitioned/table");
    let actions = read_actions(&source)?;
    let template = actions.iter().find_map(|action| action.get("add")).unwrap();
    let original = fs::read(source.join(template["path"].as_str().unwrap()))?;
    let metadata: Vec<_> = actions
        .iter()
        .filter(|action| action.get("protocol").is_some() || action.get("metaData").is_some())
        .cloned()
        .collect();
    let temporary = tempfile::tempdir()?;
    for count in [0, 3] {
        let table = temporary.path().join(format!("table-{count}"));
        create_repeated_table(&source, &table, count)?;
        let repeated = read_actions(&table)?;
        assert_eq!(repeated.len(), metadata.len() + count);
        assert_eq!(&repeated[..metadata.len()], &metadata);
        assert_eq!(fs::read_dir(&table)?.count(), count + 1);
        for (index, action) in repeated[metadata.len()..].iter().enumerate() {
            let name = format!("part-{index:03}.parquet");
            assert_eq!(fs::read(table.join(&name))?, original);
            let mut expected = template.clone();
            expected["path"] = json!(name);
            assert_eq!(action, &json!({"add": expected}));
        }
    }
    Ok(())
}
