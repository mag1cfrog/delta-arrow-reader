//! Typed JSON rows for complete queries and small preview samples.
//!
//! JSON stdout is emitted only after exhaustion within its budget. File output
//! writes rows incrementally into unpublished storage. Both paths encode each
//! value directly, without allocating an intermediate JSON row.

mod preview;
mod value;

pub(crate) use preview::write_preview_to_stdout;

use std::io::{self, Write};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::Value;

use super::{
    OutputLimits, OutputStats, field_descriptions::field_details, write_on_output_thread,
    write_stdout,
};
use crate::Error;
use value::{JsonRow, validate_json_type_support};

pub(crate) const DEFAULT_MAX_BYTES: usize = 8 * 1024;

pub(super) struct BoundedJsonBuffer {
    pub(super) bytes: Vec<u8>,
    pub(super) max_bytes: u64,
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.max_bytes.saturating_sub(self.bytes.len() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                super::OutputBytesExceeded,
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BoundedJsonBuffer {
    fn with_rows_header(
        schema: &SchemaRef,
        bindings: &[Value],
        max_bytes: u64,
        command: &'static str,
    ) -> Result<Self, Error> {
        let mut output = Self {
            bytes: Vec::new(),
            max_bytes,
        };
        write_rows_header(&mut output, schema, bindings, command)?;
        Ok(output)
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        write_json_bytes(self, bytes)
    }

    fn append_json(&mut self, value: &impl Serialize) -> Result<(), Error> {
        write_json_value(self, value)
    }
}

fn write_json_bytes(output: &mut impl Write, bytes: &[u8]) -> Result<(), Error> {
    output.write_all(bytes).map_err(classify_json_write_error)
}

fn write_json_value(output: &mut impl Write, value: &impl Serialize) -> Result<(), Error> {
    serde_json::to_writer(output, value).map_err(|source| {
        if source.is_io() {
            classify_json_write_error(source.into())
        } else {
            Error::JsonOutput { source }
        }
    })
}

fn classify_json_write_error(source: io::Error) -> Error {
    if source
        .get_ref()
        .is_some_and(|error| error.is::<super::OutputBytesExceeded>())
    {
        Error::OutputLimit {
            reason: "json_output_limit_exceeded",
        }
    } else {
        Error::Output { source }
    }
}

fn write_rows_header(
    output: &mut impl Write,
    schema: &SchemaRef,
    bindings: &[Value],
    command: &'static str,
) -> Result<(), Error> {
    for field in schema.fields() {
        validate_json_type_support(field.data_type(), command)?;
    }
    write_json_bytes(
        output,
        b"{\"format_version\":1,\"kind\":\"rows\",\"bindings\":",
    )?;
    write_json_value(output, &bindings)?;
    write_json_bytes(output, b",\"schema\":{\"fields\":[")?;
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            write_json_bytes(output, b",")?;
        }
        write_json_value(output, &field_details(field, false))?;
    }
    write_json_bytes(output, b"]},\"rows\":[")
}

pub(crate) async fn write_query_to_stdout(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    bindings: Vec<Value>,
    limits: OutputLimits,
) -> Result<OutputStats, Error> {
    write_on_output_thread(move || async move {
        let (bytes, stats) = write_query(Vec::new(), schema, stream, &bindings, limits).await?;
        write_stdout(&bytes)?;
        Ok(stats)
    })
    .await
}

pub(super) async fn write_query<W: Write>(
    output: W,
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>>,
    bindings: &[Value],
    limits: OutputLimits,
) -> Result<(W, OutputStats), Error> {
    let mut output = super::ByteBudgetWriter {
        inner: output,
        bytes_written: 0,
        max_bytes: Some(limits.max_bytes),
    };
    write_rows_header(&mut output, &schema, bindings, "query")?;
    let mut stream = Box::pin(stream);
    let mut stats = OutputStats::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() as u64 > limits.max_rows.saturating_sub(stats.rows_written) {
            return Err(Error::OutputLimit {
                reason: "output_row_limit_exceeded",
            });
        }
        for index in 0..batch.num_rows() {
            if stats.rows_written > 0 {
                write_json_bytes(&mut output, b",")?;
            }
            write_json_value(
                &mut output,
                &JsonRow {
                    columns: batch.columns(),
                    index,
                },
            )?;
            stats.rows_written += 1;
        }
        stats.batches_written += 1;
    }
    write_json_bytes(&mut output, b"],\"row_count\":")?;
    write_json_value(&mut output, &stats.rows_written.to_string())?;
    write_json_bytes(
        &mut output,
        b",\"completion\":\"complete\",\"scope\":\"query\"}\n",
    )?;
    stats.bytes_written = output.bytes_written;
    Ok((output.inner, stats))
}

#[cfg(test)]
mod tests {
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
}
