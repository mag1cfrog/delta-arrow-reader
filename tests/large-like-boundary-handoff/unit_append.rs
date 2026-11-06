#[cfg(test)]
mod c06_large_boundary_observer {
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{Array, ArrayRef, LargeStringArray, StringViewArray, StringViewBuilder};
    use arrow::buffer::Buffer;
    use arrow::compute::cast;
    use arrow::datatypes::DataType;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;
    use serde_json::{Value, json};

    use super::SparkLike;

    const BOUNDARY: usize = i32::MAX as usize + 1;
    const HALF_BOUNDARY: usize = BOUNDARY / 2;

    fn begin(case: &str) -> Instant {
        assert_eq!(std::env::var("C06_ENABLE_REAL_LARGE_BOUNDARY").as_deref(), Ok("1"));
        assert_eq!(std::env::var("C06_LARGE_BOUNDARY_CASE").as_deref(), Ok(case));
        assert!(!std::env::var("C06_LARGE_BOUNDARY_RUN_TOKEN").unwrap_or_default().is_empty());
        eprintln!("c06 large boundary begin: {}", json!({
            "case":case,"run_token":std::env::var("C06_LARGE_BOUNDARY_RUN_TOKEN").unwrap(),
            "boundary_bytes":BOUNDARY,"proof_kind":"real-rewrite-and-representation-only",
            "regex_matching":false,"apache_spark_observation":false,
        }));
        Instant::now()
    }

    // Independent oracle for these simple inputs: every output byte is either
    // the original ASCII 'a', or a backslash after doubling ordinary backslashes.
    // Compare every byte in bounded chunks; never allocate a giant expected value.
    fn check_complete_bytes(actual: &[u8], byte: u8) -> usize {
        let expected = [byte; 4096];
        let mut checked = 0;
        for chunk in actual.chunks(expected.len()) {
            assert!(chunk == &expected[..chunk.len()], "complete byte comparison failed");
            checked += chunk.len();
        }
        assert_eq!(checked, actual.len());
        checked
    }

    fn check_large(array: &LargeStringArray, rows: usize, width: usize, byte: u8) -> Result<Value> {
        assert_eq!(array.data_type(), &DataType::LargeUtf8);
        assert_eq!(array.len(), rows);
        assert_eq!(array.null_count(), 0);
        array.to_data().validate_full()?;
        let offsets = array.value_offsets();
        assert_eq!(offsets.len(), rows + 1);
        for (row, offset) in offsets.iter().enumerate() {
            assert_eq!(*offset, i64::try_from(row.checked_mul(width).unwrap()).unwrap());
        }
        for row in 0..rows {
            assert!(array.is_valid(row), "unexpected invalid row");
            assert_eq!(array.value(row).len(), width);
        }
        let checked = check_complete_bytes(array.value_data(), byte);
        assert_eq!(checked, rows.checked_mul(width).unwrap());
        Ok(json!({"type":"LargeUtf8","rows":array.len(),"null_count":array.null_count(),
            "actual_payload_bytes":array.value_data().len(),"final_offset":offsets.last(),
            "all_offsets_checked":offsets.len(),"all_validity_rows_checked":array.len(),
            "complete_bytes_checked":checked,"arrow_validate_full":true}))
    }

    fn check_views(array: &StringViewArray, rows: usize, width: usize, byte: u8) -> Result<Value> {
        assert_eq!(array.data_type(), &DataType::Utf8View);
        assert_eq!(array.len(), rows);
        assert_eq!(array.null_count(), 0);
        array.to_data().validate_full()?;
        let mut logical_bytes = 0_usize;
        for row in 0..rows {
            assert!(array.is_valid(row), "unexpected invalid view row");
            let value = array.value(row);
            assert_eq!(value.len(), width);
            logical_bytes = logical_bytes.checked_add(check_complete_bytes(value.as_bytes(), byte)).unwrap();
        }
        let physical_bytes: usize = array.data_buffers().iter().map(Buffer::len).sum();
        Ok(json!({"type":"Utf8View","rows":array.len(),"null_count":array.null_count(),
            "actual_logical_bytes":logical_bytes,"actual_backing_bytes":physical_bytes,
            "all_views_checked":array.views().len(),"all_validity_rows_checked":array.len(),
            "complete_logical_bytes_checked":logical_bytes,"arrow_validate_full":true}))
    }

    fn repeated_views(seed: &str, rows: usize) -> Result<StringViewArray> {
        let mut builder = StringViewBuilder::with_capacity(rows);
        builder.try_append_value_n(seed, rows)?;
        Ok(builder.finish())
    }

    fn native_args(source: ScalarValue, pattern: ColumnarValue) -> Result<[ColumnarValue; 2]> {
        let result = SparkLike::new('!').native_like_args(ColumnarValue::Scalar(source), pattern, None);
        // Avoid formatting a possible multi-gigabyte pattern in a failing error.
        assert!(result.is_ok(), "native_like_args returned an error; payload formatting suppressed");
        result
    }

    fn finish(case: &str, started: Instant, input: Value, output: Value, source_type: &str) {
        eprintln!("c06 large boundary complete: {}", json!({
            "case":case,"run_token":std::env::var("C06_LARGE_BOUNDARY_RUN_TOKEN").unwrap(),
            "proof_kind":"real-rewrite-and-representation-only","complete":true,
            "input":input,"output":output,"source_type":source_type,"source_value":"z",
            "elapsed_seconds":started.elapsed().as_secs_f64(),
            "regex_matching":false,"apache_spark_observation":false,
        }));
    }

    #[test]
    #[ignore = "allocates actual multi-gigabyte Arrow data; use the sealed serialized runner"]
    fn aggregate_large_utf8() -> Result<()> {
        let started = begin("aggregate_large_utf8");
        let seed = "a".repeat(1_048_576);
        let views = repeated_views(&seed, 2_048)?;
        let view_input = check_views(&views, 2_048, seed.len(), b'a')?;
        let views: ArrayRef = Arc::new(views);
        let large = cast(&views, &DataType::LargeUtf8)?;
        drop(views);
        drop(seed);
        let input_array = large.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let mut input = check_large(input_array, 2_048, 1_048_576, b'a')?;
        assert_eq!(input_array.value_data().len(), BOUNDARY);
        assert!(*input_array.value_offsets().last().unwrap() > i64::from(i32::MAX));
        input["construction_views"] = view_input;
        let [source, pattern] = native_args(ScalarValue::LargeUtf8(Some("z".into())), ColumnarValue::Array(large))?;
        assert!(matches!(source, ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(ref s))) if s == "z"));
        let ColumnarValue::Array(pattern) = pattern else { panic!("expected actual LargeUtf8 array") };
        let output = pattern.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let observed = check_large(output, 2_048, 1_048_576, b'a')?;
        assert_eq!(output.value_data().len(), BOUNDARY);
        finish("aggregate_large_utf8", started, input, observed, "LargeUtf8 scalar");
        Ok(())
    }

    #[test]
    #[ignore = "allocates actual multi-gigabyte Arrow data; use the sealed serialized runner"]
    fn dynamic_view_expansion() -> Result<()> {
        let started = begin("dynamic_view_expansion");
        let seed = "\\".repeat(4_096);
        let views = repeated_views(&seed, 262_144)?;
        let input = check_views(&views, 262_144, 4_096, b'\\')?;
        assert_eq!(input["actual_logical_bytes"].as_u64(), Some(HALF_BOUNDARY as u64));
        drop(seed);
        let [source, pattern] = native_args(ScalarValue::Utf8View(Some("z".into())), ColumnarValue::Array(Arc::new(views)))?;
        assert!(matches!(source, ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(ref s))) if s == "z"));
        let ColumnarValue::Array(pattern) = pattern else { panic!("expected actual LargeUtf8 array") };
        let output = pattern.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let observed = check_large(output, 262_144, 8_192, b'\\')?;
        assert_eq!(output.value_data().len(), BOUNDARY);
        assert!(*output.value_offsets().last().unwrap() > i64::from(i32::MAX));
        finish("dynamic_view_expansion", started, input, observed, "LargeUtf8 scalar");
        Ok(())
    }

    #[test]
    #[ignore = "scalar casting retains several real multi-gigabyte copies; use the sealed serialized runner"]
    fn scalar_view_expansion() -> Result<()> {
        let started = begin("scalar_view_expansion");
        let original = "\\".repeat(HALF_BOUNDARY);
        let input_bytes = check_complete_bytes(original.as_bytes(), b'\\');
        assert_eq!(input_bytes, HALF_BOUNDARY);
        let input = json!({"type":"Utf8View scalar","actual_bytes":original.len(),
            "actual_capacity":original.capacity(),"null":false,"complete_bytes_checked":input_bytes});
        // Move the actual scalar into the helper. No observer clone of the 1 GiB input.
        let [source, pattern] = native_args(ScalarValue::Utf8View(Some("z".into())),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(original))))?;
        assert!(matches!(source, ColumnarValue::Scalar(ScalarValue::Utf8View(Some(ref s))) if s == "z"));
        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(rewritten))) = pattern else {
            panic!("expected actual non-NULL Utf8View scalar after native cast")
        };
        assert_eq!(rewritten.len(), BOUNDARY);
        let scalar_capacity = rewritten.capacity();
        let scalar_checked = check_complete_bytes(rewritten.as_bytes(), b'\\');
        // Reuse only the returned scalar's actual owned allocation. The safe view
        // API checks the real buffer range and UTF-8; there are no fabricated bytes.
        let buffer = Buffer::from_vec(rewritten.into_bytes());
        let actual_len = buffer.len();
        let mut builder = StringViewBuilder::with_capacity(1);
        let block = builder.append_block(buffer);
        builder.try_append_view(block, 0, u32::try_from(actual_len).unwrap())?;
        let view = builder.finish();
        let mut observed = check_views(&view, 1, BOUNDARY, b'\\')?;
        let descriptor = view.views()[0];
        assert_eq!((descriptor as u32) as usize, actual_len);
        assert_eq!((descriptor >> 64) as u32, block);
        assert_eq!((descriptor >> 96) as u32, 0);
        observed["returned_scalar_type"] = json!("Utf8View");
        observed["returned_scalar_bytes_checked"] = json!(scalar_checked);
        observed["returned_scalar_capacity"] = json!(scalar_capacity);
        observed["actual_view_length"] = json!((descriptor as u32) as usize);
        observed["actual_view_buffer_index"] = json!((descriptor >> 64) as u32);
        observed["actual_view_offset"] = json!((descriptor >> 96) as u32);
        observed["view_uses_returned_owned_bytes"] = json!(true);
        assert!(actual_len > i32::MAX as usize);
        finish("scalar_view_expansion", started, input, observed, "Utf8View scalar");
        Ok(())
    }
}
