use super::*;

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn capability_corpus_checks_dvs_nulls_duplicates_types_and_corrupt_values() -> Result<()> {
    let mut state = super::super::tests::state()?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    fs::create_dir(root.join("reader"))?;
    let source = state.harness.join("../../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/deletion_vectors/table");
    let source = table::parquet(&mut state.decoder, &[parquet_file(&source)?], &[])?;
    for (case, deleted, wanted, semantics, expected_ids) in [
        (
            "all",
            false,
            Value::Null,
            Value::Null,
            (1..=12).collect::<Vec<i32>>(),
        ),
        (
            "dv",
            true,
            Value::Null,
            Value::Null,
            vec![1, 3, 4, 6, 7, 8, 9, 10, 11],
        ),
        (
            "ids",
            true,
            json!([1, 2, 5, 6, 12]),
            Value::Null,
            vec![1, 6],
        ),
        ("empty", true, json!([2, 5, 12]), Value::Null, vec![]),
        (
            "nulls",
            false,
            Value::Null,
            json!("nulls"),
            vec![4, 5, 8, 10, 12],
        ),
        ("duplicate", false, Value::Null, json!("duplicate"), vec![2]),
    ] {
        let selected = BooleanArray::from(
            ids(&source)?
                .iter()
                .map(|id| id.is_some_and(|id| expected_ids.contains(&id)))
                .collect::<Vec<_>>(),
        );
        let expected = filter_record_batch(&source, &selected)?;
        let expected = if semantics == "nulls" {
            expected.project(&[2, 0, 1])?
        } else {
            expected
        };
        let output = root.join("reader").join(format!("{case}.arrow"));
        table::write(&output, &expected)?;
        let record = json!({"status":"success","queries":[{"result":format!("{case}.arrow"),"output_rows":expected_ids.len(),"completion_ns":null}]});
        let request = json!({"action":"capability-export","record":record,"output":root,"deleted":deleted,"ids":wanted,"semantics":semantics});
        assert_eq!(state.call(&request)?, expected_ids.len());
        for kind in [
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            let schema = Arc::new(Schema::new(
                expected
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        if f.name() == "label" {
                            f.as_ref().clone().with_data_type(kind.clone())
                        } else {
                            f.as_ref().clone()
                        }
                    })
                    .collect::<Vec<_>>(),
            ));
            let alternate = table::cast(&expected, schema)?;
            fs::remove_file(&output)?;
            table::write(&output, &alternate)?;
            state.call(&request)?;
        }
        let mut bad = request.clone();
        bad["record"]["queries"][0]["output_rows"] = json!(99);
        assert!(state.call(&bad).is_err());
        bad["record"]["queries"] = json!([]);
        assert!(state.call(&bad).is_err());
        if expected.num_rows() > 0 {
            fs::remove_file(&output)?;
            table::write(&output, &expected.slice(0, expected.num_rows() - 1))?;
            assert!(state.call(&request).is_err());
        }
    }
    let input = root.join("original.arrow");
    let output = root.join("corrupt.arrow");
    let data = RecordBatch::try_from_iter(vec![(
        "l_orderkey",
        Arc::new(Int64Array::from(vec![1, 2])) as arrow_array::ArrayRef,
    )])?;
    table::write(&input, &data)?;
    state.call(&json!({"action":"capability-corrupt","input":input,"output":output}))?;
    assert!(table::equal(&table::stream(&output, None)?, &data).is_err());
    assert!(
        state
            .call(&json!({"action":"capability-corrupt","input":input,"output":output}))
            .is_err()
    );
    Ok(())
}

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn late_stream_fixture_has_31_good_files_and_one_conversion_failure() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut state = super::super::tests::state()?;
    let source = state.harness.join("../../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/deletion_vectors/table");
    let staged = root.join("no-dv");
    fs::create_dir(&staged)?;
    fs::create_dir(staged.join("_delta_log"))?;
    fs::copy(parquet_file(&source)?, staged.join("source.parquet"))?;
    let log = "_delta_log/00000000000000000000.json";
    fs::copy(source.join(log), staged.join(log))?;
    state.call(&json!({"action":"capability-stream","output":root}))?;
    let actions = fs::read_to_string(root.join("stream-table").join(log))?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        actions.iter().filter(|a| a.get("add").is_some()).count(),
        32
    );
    for index in [0, 30, 31] {
        let path = root.join(format!("stream-table/{index:03}.parquet"));
        let batch = table::parquet(&mut state.decoder, &[path], &[])?;
        assert_eq!(batch.num_rows(), 8192);
        let label = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            label
                .iter()
                .all(|s| s == Some(if index < 31 { "1" } else { "bad" }))
        );
    }
    Ok(())
}
