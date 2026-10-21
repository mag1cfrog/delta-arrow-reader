use super::*;
use arrow::{
    array::{Array, Decimal128Array, StringArray},
    compute::concat_batches,
    ipc::reader::StreamReader,
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    basic::Type,
    file::reader::{FileReader, SerializedFileReader},
};

#[test]
fn numeric_files_keep_types_nulls_decimal_widths_and_sparse_row_ids() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    for decimals in [false, true] {
        let output = temporary
            .path()
            .join(if decimals { "decimal" } else { "numeric" });
        numeric(&output, decimals, 16384)?;
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(output.join("part-0.parquet"))?)?;
        assert_eq!(reader.metadata().num_row_groups(), 1);
        let batches = reader
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let actual = concat_batches(&batches[0].schema(), &batches)?;
        let expected = numeric_batch(decimals, 0, 16384)?;
        // ArrayData compares stored float bits, including NaN payloads.
        for (a, b) in actual.columns().iter().zip(expected.columns()) {
            assert_eq!(a.to_data(), b.to_data());
        }
        let physical = SerializedFileReader::new(File::open(output.join("part-0.parquet"))?)?;
        for (index, (name, dtype)) in numeric_types(decimals).iter().enumerate() {
            let column = actual.column(2 + index * 8);
            assert!(column.null_count() > 0 && column.null_count() < column.len());
            assert_eq!(actual.column(3 + index * 8).null_count(), 0);
            if let DataType::Decimal128(precision, _) = dtype {
                let values = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let limit = 10i128.pow(u32::from(*precision));
                assert!(values.iter().flatten().all(|v| v.abs() < limit));
                assert!(values.iter().flatten().any(|v| v < 0));
                assert!(values.iter().flatten().any(|v| v > 0));
                let schema = physical
                    .metadata()
                    .file_metadata()
                    .schema_descr()
                    .column(2 + index * 8);
                assert_eq!(schema.physical_type(), Type::FIXED_LEN_BYTE_ARRAY);
                assert_eq!(schema.type_length(), if *precision == 20 { 9 } else { 16 });
            }
            let reference =
                StreamReader::try_new(File::open(output.join(format!("{name}.arrow")))?, None)?;
            let ids = reference
                .flat_map(|b| {
                    b.unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, vec![0, 8191, 16382, 24573, 32764]);
        }
        assert!(numeric(&output, decimals, 16384).is_err());
    }
    Ok(())
}

#[test]
fn length_values_preserve_empty_strings_nulls_and_page_boundary_distributions() -> Result<()> {
    let mut rng = Random(452);
    for case in [
        "selected-long",
        "selected-long-batch",
        "mixed-bad-first",
        "mixed-alternating",
    ] {
        assert!(value_size(case, 0, 0, &mut rng) >= 262144);
        assert_eq!(value_size(case, 1, 0, &mut rng), 1);
    }
    assert_eq!(value_size("mixed-bad-first", 4 * 8192 - 1, 0, &mut rng), 1);
    assert!((512..576).contains(&value_size("mixed-bad-first", 4 * 8192, 0, &mut rng)));
    assert!((512..576).contains(&value_size("mixed-alternating", 8192, 0, &mut rng)));
    assert_eq!(value_size("mixed-alternating", 8192, 1, &mut rng), 0);
    assert!(length_rows("typo").is_err());
    let batch = length_batch("short", 1, 2100)?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((ids.value(0), ids.value(2099)), (2100, 4199));
    let strings = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..2100 {
        assert_eq!(strings.is_null(row), row % 7 == 0);
        if strings.is_valid(row) {
            assert!(strings.value(row).ends_with('\u{e9}'));
        }
    }
    let batch = length_batch("length-heavy", 0, 100)?;
    let strings = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(strings.iter().all(|v| v == Some("")));
    Ok(())
}

#[test]
fn invalid_options_leave_no_output() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let output = temporary.path().join("fixture");
    for arguments in [
        vec!["--case", "typo"],
        vec!["--case", "short", "--compression", "bad"],
        vec!["--case", "short"],
        vec!["--decimals"],
    ] {
        let mut args = vec!["--output".to_owned(), output.to_string_lossy().into_owned()];
        args.extend(arguments.into_iter().map(str::to_owned));
        assert!(run("length-pages", &args).is_err());
        assert!(!output.exists());
    }
    Ok(())
}
