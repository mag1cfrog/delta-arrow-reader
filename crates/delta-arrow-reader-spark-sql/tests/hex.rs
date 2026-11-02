//! HEX preserves string and binary bytes and their declared result fields.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryViewArray, FixedSizeBinaryArray, Int32Array, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_result, register_inputs};

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

fn records(capture: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = capture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, count);
    assert!(
        records[1..]
            .iter()
            .all(|case| case["expected"]["status"] == "ok")
    );
    Ok(records)
}

async fn compare(
    session: &SessionContext,
    case: &Value,
    representation: &str,
    batch_size: usize,
    differences: &mut Vec<String>,
) {
    let actual = capture_result(session, case).await;
    let observed = match &actual {
        Ok(value) => value.clone(),
        Err(error) => json!({"error":error.to_string()}),
    };
    eprintln!(
        "c06 observation: {}",
        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"actual":observed})
    );
    let expected = &case["expected"];
    if !actual.as_ref().is_ok_and(|value| {
        value["analyzed_schema"] == expected["analyzed_schema"]
            && value["physical_schema"] == expected["physical_schema"]
            && value["rows"] == expected["rows"]
    }) {
        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
    }
}

#[tokio::test]
async fn hex_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    let mut compared = 0;
    for (capture, count) in [
        (include_str!("fixtures/hex-spark.jsonl"), 22),
        (include_str!("fixtures/hex-expanded-spark.jsonl"), 48),
    ] {
        let records = records(capture, count)?;
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                if representation == 2 && count == 48 {
                    let batches = session.table("hex_long_required").await?.collect().await?;
                    assert!(batches.iter().any(|batch| {
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringViewArray>()
                            .is_some_and(|array| !array.data_buffers().is_empty())
                    }));
                    assert!(batches.iter().any(|batch| {
                        batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<BinaryViewArray>()
                            .is_some_and(|array| !array.data_buffers().is_empty())
                    }));
                }
                for case in &records[1..] {
                    compared += 1;
                    compare(
                        &session,
                        case,
                        &representation.to_string(),
                        batch_size,
                        &mut differences,
                    )
                    .await;
                }
            }
        }
    }
    assert_eq!(compared, (22 + 48) * 6);
    eprintln!("C06 compared {compared}, mismatches {}", differences.len());
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn register_fixed_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
) -> TestResult {
    let widths = metadata["arrow_fixed_binary_widths"]
        .as_object()
        .ok_or("missing Arrow widths")?;
    for input in metadata["registered_inputs"]
        .as_array()
        .ok_or("missing inputs")?
    {
        let name = input["name"].as_str().ok_or("missing table name")?;
        let Some(width) = widths.get(name) else {
            continue;
        };
        let width: i32 = width.as_i64().ok_or("missing width")?.try_into()?;
        let rows = input["rows"].as_array().ok_or("missing rows")?;
        let keys = rows
            .iter()
            .map(|row| Ok(row[0].as_str().ok_or("missing key")?.parse()?))
            .collect::<TestResult<Vec<i32>>>()?;
        let bytes = rows
            .iter()
            .map(|row| {
                row[1]
                    .as_str()
                    .map(|value| {
                        (0..value.len())
                            .step_by(2)
                            .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
                            .collect::<TestResult<Vec<u8>>>()
                    })
                    .transpose()
            })
            .collect::<TestResult<Vec<_>>>()?;
        // Real Arrow slices exclude surrounding values even for empty and zero-width arrays.
        let padding = Some(vec![0x7f; width.try_into()?]);
        let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::once(padding.as_deref())
                .chain(bytes.iter().map(|value| value.as_deref()))
                .chain(std::iter::once(padding.as_deref())),
            width,
        )?
        .slice(1, rows.len());
        assert_eq!(values.len(), rows.len());
        let fields = input["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false)
                .with_metadata(serde_json::from_value(fields[0]["metadata"].clone())?),
            Field::new(
                "b",
                DataType::FixedSizeBinary(width),
                fields[1]["nullable"]
                    .as_bool()
                    .ok_or("missing nullability")?,
            )
            .with_metadata(serde_json::from_value(fields[1]["metadata"].clone())?),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                Arc::new(values),
            ],
        )?;
        let mut partitions = vec![Vec::new(); 2];
        for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[chunk % 2]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(name, Arc::new(MemTable::try_new(schema, partitions)?))?;
    }
    Ok(())
}

#[tokio::test]
async fn hex_fixed_binary_matches_spark() -> TestResult {
    let records = records(include_str!("fixtures/hex-expanded-spark.jsonl"), 48)?;
    let mut differences = Vec::new();
    let mut compared = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        register_fixed_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("hex_fixed_"))
            {
                compared += 1;
                compare(
                    &session,
                    case,
                    "FixedSizeBinary",
                    batch_size,
                    &mut differences,
                )
                .await;
            }
        }
    }
    assert_eq!(compared, 30 * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn hex_preserves_native_encoding_functions() -> TestResult {
    let native_sql = "SELECT decode('00FF61','hex') AS b,encode('Aé','hex') AS s";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = session(32);
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        capture_result(
            &session,
            &json!({"sql":"SELECT hex('é') AS s,hex(X'00ff61') AS b","ansi":ansi}),
        )
        .await?;
        assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    }
    Ok(())
}
