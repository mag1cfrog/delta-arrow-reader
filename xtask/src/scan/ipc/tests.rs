use std::collections::HashMap;

use arrow_array::Int32Array;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field, Schema};

use super::*;

mod completion;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn table(batches: &[&[Option<i32>]]) -> TestResult<IpcTable> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let batches = batches
        .iter()
        .map(|values| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
        })
        .collect::<std::result::Result<_, _>>()?;
    Ok(IpcTable { schema, batches })
}

fn stream(table: &IpcTable) -> TestResult<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), &table.schema)?;
    for batch in &table.batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner()?)
}

#[test]
fn scan_matches_rows_across_batches_in_any_order_and_respects_limits() -> TestResult {
    let expected = table(&[&[Some(1), None, Some(2), Some(1)]])?;
    table(&[&[Some(2)], &[], &[Some(1), Some(1), None]])?.check_matches(&expected, None)?;
    for value in [Some(1), Some(2), None] {
        table(&[&[value]])?.check_matches(&expected, Some(1))?;
    }
    table(&[])?.check_matches(&expected, Some(0))?;
    expected.check_matches(&expected, Some(1000))?;
    Ok(())
}

#[test]
fn scan_rejects_missing_extra_duplicate_and_unknown_rows() -> TestResult {
    let expected = table(&[&[Some(1), None, Some(2)]])?;
    for values in [
        vec![Some(1), None],
        vec![Some(1), None, Some(2), Some(3)],
        vec![Some(1), Some(1), Some(2)],
        vec![Some(1), None, Some(99)],
    ] {
        assert!(table(&[&values])?.check_matches(&expected, None).is_err());
    }
    for values in [vec![], vec![Some(1), Some(2)], vec![Some(99)]] {
        assert!(
            table(&[&values])?
                .check_matches(&expected, Some(1))
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn scan_preserves_row_counts_without_columns() -> TestResult {
    let path = Path::new("zero-column stream");
    let expected = table(&[&[Some(1), Some(2)], &[None]])?.project(&[], path)?;
    let actual = IpcTable::read_stream(&stream(&expected)?, path)?;
    assert!(actual.schema.fields().is_empty());
    assert_eq!(actual.row_count(), 3);
    actual.check_matches(&expected, None)?;
    let wrong_count = table(&[&[Some(1), Some(2)]])?.project(&[], path)?;
    assert!(wrong_count.check_matches(&expected, None).is_err());
    Ok(())
}

#[test]
fn empty_scan_keeps_schema_without_record_batches() -> TestResult {
    let expected = table(&[])?;
    let actual = IpcTable::read_stream(&stream(&expected)?, Path::new("empty stream"))?;
    assert!(actual.batches.is_empty());
    assert_eq!(actual.schema, expected.schema);
    actual.check_matches(&expected, None)?;
    assert!(
        actual
            .project(&[], Path::new("empty stream"))?
            .check_matches(&expected, None)
            .is_err()
    );
    Ok(())
}

#[test]
fn scan_rejects_truncated_corrupt_and_trailing_ipc_bytes() -> TestResult {
    let valid = stream(&table(&[&[Some(1), None]])?)?;
    for payload in [
        vec![],
        valid[..valid.len() - 8].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        vec![
            255, 255, 255, 255, 4, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0,
        ],
        [valid.as_slice(), b"junk", &valid[valid.len() - 8..]].concat(),
        [valid.as_slice(), valid.as_slice()].concat(),
    ] {
        assert!(IpcTable::read_stream(&payload, Path::new("invalid stream")).is_err());
    }
    Ok(())
}

#[test]
fn scan_schema_requires_field_order_names_types_and_nullability() -> TestResult {
    let expected = table(&[])?;
    for field in [
        Field::new("wrong", DataType::Int32, true),
        Field::new("id", DataType::Int64, true),
        Field::new("id", DataType::Int32, false),
    ] {
        let actual = IpcTable {
            schema: Arc::new(Schema::new(vec![field])),
            batches: vec![],
        };
        assert!(actual.check_matches(&expected, None).is_err());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let expected = IpcTable {
        schema,
        batches: vec![],
    };
    let actual = expected.project(&[1, 0], Path::new("reordered schema"))?;
    assert!(actual.check_matches(&expected, None).is_err());
    Ok(())
}

fn nested_table(
    child_name: &str,
    values: &[Option<i32>],
    parents: &[Option<i32>],
    metadata: HashMap<String, String>,
) -> TestResult<IpcTable> {
    let fields: Fields =
        vec![Field::new(child_name, DataType::Int32, true).with_metadata(metadata.clone())].into();
    let array = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(values.to_vec()))],
        Int32Array::from(parents.to_vec()).nulls().cloned(),
    )?;
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("profile", DataType::Struct(fields), true).with_metadata(metadata.clone())],
        metadata,
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    Ok(IpcTable {
        schema,
        batches: vec![batch],
    })
}

#[test]
fn nested_scan_ignores_metadata_but_checks_names_values_and_parent_nulls() -> TestResult {
    let parents = &[Some(1), None, Some(1)];
    let expected = nested_table(
        "score",
        &[Some(10), Some(999), None],
        parents,
        HashMap::new(),
    )?;
    let metadata = HashMap::from([("delta.columnMapping.id".into(), "1".into())]);
    let actual = nested_table(
        "score",
        &[Some(10), Some(123), None],
        parents,
        metadata.clone(),
    )?;
    IpcTable::read_stream(&stream(&actual)?, Path::new("nested stream"))?
        .check_matches(&expected, None)?;
    for actual in [
        nested_table(
            "renamed",
            &[Some(10), Some(999), None],
            parents,
            metadata.clone(),
        )?,
        nested_table(
            "score",
            &[Some(11), Some(999), None],
            parents,
            metadata.clone(),
        )?,
        nested_table("score", &[Some(10), None, None], &[Some(1); 3], metadata)?,
    ] {
        assert!(actual.check_matches(&expected, None).is_err());
    }
    Ok(())
}
