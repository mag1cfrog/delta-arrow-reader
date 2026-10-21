use super::*;
use arrow_array::{
    ArrayRef, BinaryArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use std::{fs::File, sync::Arc};

#[test]
fn numeric_comparison_preserves_nan_payloads_signed_zero_nulls_and_row_order() -> Result<()> {
    for double in [false, true] {
        let values = |change: Option<usize>| -> ArrayRef {
            if double {
                let mut bits = [0x7ff8000000000001u64, 0x8000000000000000, 0];
                if let Some(i) = change {
                    bits[i] = if i == 1 { 0 } else { bits[i] + 1 };
                }
                Arc::new(Float64Array::from_iter(
                    bits.map(|b| Some(f64::from_bits(b)))
                        .into_iter()
                        .chain([None]),
                ))
            } else {
                let mut bits = [0x7fc00001u32, 0x80000000, 0];
                if let Some(i) = change {
                    bits[i] = if i == 1 { 0 } else { bits[i] + 1 };
                }
                Arc::new(Float32Array::from_iter(
                    bits.map(|b| Some(f32::from_bits(b)))
                        .into_iter()
                        .chain([None]),
                ))
            }
        };
        let expected = RecordBatch::try_from_iter(vec![
            (
                "row_id",
                Arc::new(Int64Array::from(vec![3, 0, 1, 2])) as ArrayRef,
            ),
            ("value", values(None)),
        ])?;
        let sorted = table::sort(&expected, "row_id")?;
        table::equal(&table::sort(&sorted, "row_id")?, &sorted)?;
        for index in [0, 1] {
            let wrong = RecordBatch::try_new(
                expected.schema(),
                vec![expected.column(0).clone(), values(Some(index))],
            )?;
            assert!(table::equal(&table::sort(&wrong, "row_id")?, &sorted).is_err());
        }
        let no_null = arrow_cast::cast(
            &Int64Array::from(vec![0; 4]),
            expected.column(1).data_type(),
        )?;
        let wrong =
            RecordBatch::try_new(expected.schema(), vec![expected.column(0).clone(), no_null])?;
        assert!(table::equal(&wrong, &expected).is_err());
        assert!(table::equal(&expected.slice(0, 3), &expected).is_err());
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("result.arrow");
        let options = arrow_ipc::writer::IpcWriteOptions::default().try_with_compression(Some(
            if double {
                arrow_ipc::CompressionType::LZ4_FRAME
            } else {
                arrow_ipc::CompressionType::ZSTD
            },
        ))?;
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            File::create(&path)?,
            &expected.schema(),
            options,
        )?;
        writer.write(&expected.slice(0, 1))?;
        writer.write(&expected.slice(1, 3))?;
        writer.finish()?;
        table::equal(&table::stream(&path, None)?, &expected)?;
    }
    Ok(())
}

#[test]
fn length_comparison_accepts_views_and_rejects_null_order_type_and_value_changes() -> Result<()> {
    let expected = RecordBatch::try_from_iter(vec![
        ("row_id", Arc::new(Int64Array::from(vec![1, 0])) as ArrayRef),
        ("text", Arc::new(StringArray::from(vec![Some("é"), None]))),
        (
            "bytes",
            Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
        ),
    ])?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("text", DataType::Utf8View, true),
        Field::new("bytes", DataType::BinaryView, true),
    ]));
    let views = table::cast(&expected, schema)?;
    table::equal(&table::cast(&views, expected.schema())?, &expected)?;
    assert!(table::equal(&table::sort(&expected, "row_id")?, &expected).is_err());
    assert!(table::cast(&expected.project(&[1, 0, 2])?, expected.schema()).is_err());
    let nonnull = Arc::new(Schema::new(
        expected
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(false))
            .collect::<Vec<_>>(),
    ));
    assert!(table::cast(&expected, nonnull).is_err());
    let big = RecordBatch::try_from_iter(vec![(
        "row_id",
        Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
    )])?;
    assert!(
        table::cast(
            &big,
            Arc::new(Schema::new(vec![Field::new(
                "row_id",
                DataType::Int32,
                false
            )]))
        )
        .is_err()
    );
    Ok(())
}

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn experiment_worker_loads_dense_parquet_and_sparse_ipc_references() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut fields = vec![(
        "row_id".to_owned(),
        Arc::new(Int64Array::from(vec![2, 0, 1])) as ArrayRef,
    )];
    fields.extend((0..8).map(|i| {
        (
            format!("float_{i}"),
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(f32::from_bits(0x7fc00001)),
                None,
            ])) as ArrayRef,
        )
    }));
    let expected = RecordBatch::try_from_iter(fields)?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(root.join("data.parquet"))?,
        expected.schema(),
        None,
    )?;
    writer.write(&expected)?;
    writer.close()?;
    table::write(&root.join("reference.arrow"), &expected)?;
    let manifest = json!({"files":[{"path":"data.parquet"}],"references":{"float":{"path":"reference.arrow"}}});
    let mut state = super::super::tests::state()?;
    let mut request = json!({"action":"experiment-reference","driver":"numeric","fixture":root,"manifest":manifest,"column_type":"float","selection":"dense"});
    let path = root.join("actual.arrow");
    table::write(&path, &table::sort(&expected, "row_id")?)?;
    let check = json!({"action":"experiment-check","driver":"numeric","path":path});
    assert!(state.call(&check).is_err());
    for selection in ["dense", "sparse"] {
        request["selection"] = json!(selection);
        assert_eq!(state.call(&request)?, 3);
        state.call(&check)?;
    }
    let wrong = root.join("wrong.arrow");
    table::write(&wrong, &expected.slice(0, 2))?;
    let mut check = check;
    check["path"] = json!(wrong);
    assert!(state.call(&check).is_err());
    Ok(())
}
