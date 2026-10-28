#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures_util::stream;
use serde_json::{Value, json};

use super::*;

async fn encode_and_check_preview_batch(
    batch: RecordBatch,
    max_rows: u64,
    max_bytes: u64,
) -> Result<Vec<u8>, Error> {
    encode_preview(
        batch.schema(),
        stream::iter([Ok(batch)]),
        42,
        OutputLimits {
            max_rows,
            max_bytes,
        },
    )
    .await
    .map(|(bytes, stats)| {
        assert_eq!(stats.bytes_written, bytes.len() as u64);
        bytes
    })
}

fn string_batch(values: Vec<&str>) -> RecordBatch {
    RecordBatch::try_from_iter([("text", Arc::new(StringArray::from(values)) as ArrayRef)]).unwrap()
}

#[tokio::test]
async fn sample_state_distinguishes_exhaustion_from_limit_without_lookahead() {
    for (values, limit, reason, more) in [
        (vec![], 2, "end_of_input", json!(false)),
        (vec!["one"], 2, "end_of_input", json!(false)),
        (vec!["one", "two"], 2, "row_limit", Value::Null),
        (vec!["one", "two", "three"], 2, "row_limit", Value::Null),
    ] {
        let bytes = encode_and_check_preview_batch(string_batch(values), limit, 8192)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["completion"], "sample");
        assert_eq!(result["stop_reason"], reason);
        assert_eq!(result["has_more"], more);
    }
    let schema = string_batch(vec![]).schema();
    let never_poll = stream::poll_fn(|_| panic!("zero-row preview polled its input"));
    let (bytes, _) = encode_preview(
        schema,
        never_poll,
        0,
        OutputLimits {
            max_rows: 0,
            max_bytes: 8192,
        },
    )
    .await
    .unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["rows"], json!([]));
    assert_eq!(result["stop_reason"], "row_limit");
    assert_eq!(result["has_more"], Value::Null);
}

#[tokio::test]
async fn byte_boundary_includes_utf8_escaping_count_digits_and_sample_footer() {
    for count in [1, 9, 10] {
        let batch = string_batch(vec!["中文\n\""; count]);
        let bytes = encode_and_check_preview_batch(batch.clone(), 20, 8192)
            .await
            .unwrap();
        assert_eq!(
            encode_and_check_preview_batch(batch.clone(), 20, bytes.len() as u64)
                .await
                .unwrap(),
            bytes
        );
        let shorter = encode_and_check_preview_batch(batch, 20, bytes.len() as u64 - 1).await;
        if count == 1 {
            assert!(matches!(
                shorter,
                Err(Error::OutputLimit {
                    reason: "preview_row_too_large"
                })
            ));
        } else {
            let shorter = shorter.unwrap();
            assert!(shorter.len() < bytes.len());
            let result: Value = serde_json::from_slice(&shorter).unwrap();
            assert_eq!(result["rows"].as_array().unwrap().len(), count - 1);
            assert_eq!(result["stop_reason"], "byte_limit");
            assert_eq!(result["has_more"], true);
        }
    }
}

#[tokio::test]
async fn oversized_row_returns_only_the_valid_prefix_and_stops_reading() {
    let large = "private".repeat(20_000);
    let input = string_batch(vec!["small", &large]);
    let schema = input.schema();
    let input =
        stream::iter([Ok(input)]).chain(stream::poll_fn(|_| panic!("read after byte limit")));
    let (bytes, _) = encode_preview(
        schema,
        input,
        0,
        OutputLimits {
            max_rows: 20,
            max_bytes: 8192,
        },
    )
    .await
    .unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["rows"], json!([["small"]]));
    assert_eq!(result["stop_reason"], "byte_limit");
    assert_eq!(result["has_more"], true);
    assert!(matches!(
        encode_and_check_preview_batch(string_batch(vec![&large]), 20, 8192).await,
        Err(Error::OutputLimit {
            reason: "preview_row_too_large"
        })
    ));
}

#[tokio::test]
async fn a_row_that_cannot_fit_its_footer_does_not_wait_for_another_batch() {
    let input = string_batch(vec!["one", "two"]);
    let complete = encode_and_check_preview_batch(input.clone(), 20, 8192)
        .await
        .unwrap();
    // Both row bodies fit, but the second row cannot fit even the shorter byte-limit footer.
    let max_bytes = complete.len() as u64 - 10;
    let schema = input.schema();
    let stream = stream::iter([Ok(input)]).chain(stream::poll_fn(|_| {
        panic!("read after a whole-response byte overflow")
    }));
    let (bytes, _) = encode_preview(
        schema,
        stream,
        0,
        OutputLimits {
            max_rows: 20,
            max_bytes,
        },
    )
    .await
    .unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["rows"], json!([["one"]]));
    assert_eq!(result["stop_reason"], "byte_limit");
}

#[tokio::test]
async fn empty_wide_and_zero_column_samples_keep_schema_and_row_semantics() {
    let fields = (0..1000)
        .map(|i| Field::new(format!("column_{i}"), DataType::Int64, true))
        .collect::<Vec<_>>();
    let wide = RecordBatch::new_empty(Arc::new(Schema::new(fields)));
    assert!(matches!(
        encode_and_check_preview_batch(wide, 20, 8192).await,
        Err(Error::OutputLimit {
            reason: "preview_schema_limit_exceeded"
        })
    ));
    let empty = string_batch(vec![]);
    let bytes = encode_and_check_preview_batch(empty.clone(), 20, 8192)
        .await
        .unwrap();
    assert_eq!(
        encode_and_check_preview_batch(empty.clone(), 20, bytes.len() as u64)
            .await
            .unwrap(),
        bytes
    );
    assert!(matches!(
        encode_and_check_preview_batch(empty, 20, bytes.len() as u64 - 1).await,
        Err(Error::OutputLimit {
            reason: "preview_schema_limit_exceeded"
        })
    ));
    let no_columns = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(2)),
    )
    .unwrap();
    let result: Value = serde_json::from_slice(
        &encode_and_check_preview_batch(no_columns, 3, 8192)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["rows"], json!([[], []]));
    assert_eq!(result["row_count"], "2");
}

#[tokio::test]
async fn metadata_is_omitted_and_late_stream_errors_discard_the_sample() {
    let field =
        Field::new("id", DataType::Int64, false).with_metadata(std::collections::HashMap::from([
            ("private".into(), "x".repeat(100_000)),
        ]));
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![9007199254740993]))],
    )
    .unwrap();
    let result: Value = serde_json::from_slice(
        &encode_and_check_preview_batch(batch.clone(), 20, 8192)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["rows"], json!([["9007199254740993"]]));
    assert!(result["schema"]["fields"][0].get("metadata").is_none());
    let stream = stream::iter([Ok(batch), Err(Error::Cancelled { status: 130 })]);
    assert!(matches!(
        encode_preview(
            schema,
            stream,
            0,
            OutputLimits {
                max_rows: 20,
                max_bytes: 8192
            }
        )
        .await,
        Err(Error::Cancelled { status: 130 })
    ));
}
