#![allow(clippy::unwrap_used)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arrow::array::{ArrayRef, Int64Array, RecordBatchOptions, StringArray, StructArray};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use futures_util::stream;
use serde_json::json;

use super::*;

async fn encode_and_check_query_batch(
    batch: RecordBatch,
    max_rows: u64,
    max_bytes: u64,
) -> Result<Vec<u8>, Error> {
    write_query(
        Vec::new(),
        batch.schema(),
        stream::iter([Ok(batch)]),
        &[json!({"alias": "t", "table_version": "42"})],
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

#[tokio::test]
async fn byte_budget_covers_escaping_schema_envelope_and_newline() {
    let batch = RecordBatch::try_from_iter([
        (
            "same",
            Arc::new(Int64Array::from(vec![9007199254740993])) as ArrayRef,
        ),
        (
            "same",
            Arc::new(StringArray::from(vec!["中文\n\""])) as ArrayRef,
        ),
    ])
    .unwrap();
    let bytes = encode_and_check_query_batch(batch.clone(), 1, 8192)
        .await
        .unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["rows"], json!([["9007199254740993", "中文\n\""]]));
    assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 2);
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert_eq!(
        encode_and_check_query_batch(batch.clone(), 1, bytes.len() as u64)
            .await
            .unwrap(),
        bytes
    );
    assert!(matches!(
        encode_and_check_query_batch(batch.clone(), 1, bytes.len() as u64 - 1).await,
        Err(Error::OutputLimit {
            reason: "json_output_limit_exceeded"
        })
    ));
    assert!(matches!(
        encode_and_check_query_batch(batch, 0, 8192).await,
        Err(Error::OutputLimit {
            reason: "output_row_limit_exceeded"
        })
    ));
}

#[tokio::test]
async fn schema_survives_empty_results_and_zero_column_rows() {
    let field = Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
        true,
    )
    .with_metadata(std::collections::HashMap::from([(
        "private".into(),
        "omit".repeat(100_000),
    )]));
    let schema = Arc::new(Schema::new(vec![field]));
    let bytes = encode_and_check_query_batch(RecordBatch::new_empty(schema), 0, 8192)
        .await
        .unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["rows"], json!([]));
    assert_eq!(result["row_count"], "0");
    assert_eq!(
        result["schema"]["fields"][0]["data_type"],
        json!({"Timestamp": ["Nanosecond", "Asia/Shanghai"]})
    );
    assert!(!String::from_utf8(bytes).unwrap().contains("metadata"));
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(2)),
    )
    .unwrap();
    let result: Value =
        serde_json::from_slice(&encode_and_check_query_batch(batch, 2, 8192).await.unwrap())
            .unwrap();
    assert_eq!(result["schema"]["fields"], json!([]));
    assert_eq!(result["rows"], json!([[], []]));
    assert_eq!(result["row_count"], "2");
}

#[tokio::test]
async fn oversized_rows_stop_polling_and_late_errors_discard_buffered_rows() {
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)])) as ArrayRef,
    )])
    .unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let stream = stream::iter([Ok(batch.clone())]).chain(stream::once(async move {
        observed.store(true, Ordering::SeqCst);
        Ok(batch)
    }));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    assert!(
        write_query(
            Vec::new(),
            schema,
            stream,
            &[json!({"alias": "t", "table_version": "0"})],
            OutputLimits {
                max_rows: 100,
                max_bytes: 8192
            }
        )
        .await
        .is_err()
    );
    assert!(!polled.load(Ordering::SeqCst));
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let stream = stream::iter([Ok(batch.clone()), Err(Error::Cancelled { status: 130 })]);
    assert!(matches!(
        write_query(
            Vec::new(),
            batch.schema(),
            stream,
            &[json!({"alias": "t", "table_version": "0"})],
            OutputLimits {
                max_rows: 1,
                max_bytes: 8192
            }
        )
        .await,
        Err(Error::Cancelled { status: 130 })
    ));
}

#[tokio::test]
async fn unsupported_nested_types_fail_even_without_rows() {
    for data_type in [
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Union(
            UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
            UnionMode::Dense,
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Int64, true)),
        ),
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        ))),
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Interval(IntervalUnit::YearMonth)),
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("private", data_type, true)]));
        assert!(matches!(
            write_query(
                Vec::new(),
                schema,
                stream::empty(),
                &[json!({"alias": "t", "table_version": "0"})],
                OutputLimits {
                    max_rows: 0,
                    max_bytes: 8192
                }
            )
            .await,
            Err(Error::UnsupportedJsonType { command: "query" })
        ));
    }
}

#[tokio::test]
async fn deeply_nested_structs_preserve_schema_and_value_order() {
    let mut array: ArrayRef = Arc::new(Int64Array::from(vec![9007199254740993]));
    let mut expected = json!("9007199254740993");
    for _ in 0..24 {
        let fields = vec![Field::new("item", array.data_type().clone(), false)].into();
        array = Arc::new(StructArray::new(fields, vec![array], None));
        expected = json!([expected]);
    }
    let batch = RecordBatch::try_from_iter([("nested", array)]).unwrap();
    let result: Value =
        serde_json::from_slice(&encode_and_check_query_batch(batch, 1, 8192).await.unwrap())
            .unwrap();
    assert_eq!(result["rows"], json!([[expected]]));
    let mut field = &result["schema"]["fields"][0];
    for _ in 0..24 {
        field = &field["data_type"]["Struct"][0];
    }
    assert_eq!(field["data_type"], "Int64");
}
