//! Binary concatenation and the accepted string NULL policy match independent Spark.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

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
) -> TestResult {
    let observed = capture_observation(session, case).await?;
    eprintln!(
        "c06 observation: {}",
        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"actual":observed})
    );
    let expected = &case["expected"];
    if observed.get("error").is_some()
        || observed["analyzed_schema"] != expected["analyzed_schema"]
        || observed["physical_schema"] != expected["physical_schema"]
        || observed["rows"] != expected["rows"]
    {
        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
    }
    Ok(())
}

#[tokio::test]
async fn binary_concat_matches_spark() -> TestResult {
    let records = records(include_str!("fixtures/binary-concat-spark.jsonl"), 110)?;
    let differences = compare_representations(&records).await?;
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn binary_concat_null_and_byte_controls_match_spark() -> TestResult {
    let records = records(include_str!("fixtures/binary-concat-null-spark.jsonl"), 44)?;
    let differences = compare_representations(&records).await?;
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn compare_representations(records: &[Value]) -> TestResult<Vec<String>> {
    let mut differences = Vec::new();
    let mut compared = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            register_inputs(&session, &records[0], representation, batch_size)?;
            if representation == 2
                && records[0]["registered_inputs"]
                    .as_array()
                    .is_some_and(|inputs| {
                        inputs
                            .iter()
                            .any(|input| input["name"] == "concat_long_required")
                    })
            {
                let batches = session
                    .table("concat_long_required")
                    .await?
                    .collect()
                    .await?;
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
                .await?;
            }
        }
    }
    assert_eq!(compared, (records.len() - 1) * 6);
    eprintln!("C06 compared {compared}, mismatches {}", differences.len());
    Ok(differences)
}

#[tokio::test]
async fn binary_concat_long_and_fixed_matches_spark() -> TestResult {
    let records = records(
        include_str!("fixtures/binary-concat-expanded-spark.jsonl"),
        108,
    )?;
    // Keep complete public field comparisons for all variable representations.
    let mut differences = compare_representations(&records).await?;
    let variable_differences = differences.len();
    let mut compared = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        register_fixed_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("concat_fixed_"))
            {
                compared += 1;
                compare(
                    &session,
                    case,
                    "FixedSizeBinary",
                    batch_size,
                    &mut differences,
                )
                .await?;
            }
        }
    }
    assert_eq!(compared, 90 * 2);
    eprintln!(
        "C06 fixed compared {compared}, mismatches {}",
        differences.len() - variable_differences
    );
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
                        assert!(value.len().is_multiple_of(2));
                        (0..value.len())
                            .step_by(2)
                            .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
                            .collect::<TestResult<Vec<u8>>>()
                    })
                    .transpose()
            })
            .collect::<TestResult<Vec<_>>>()?;
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
async fn binary_concat_preserves_native_datafusion() -> TestResult {
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(b"excluded padding!".as_slice()),
        Some(b"long native bytes".as_slice()),
        None,
        Some(b"other byte value!".as_slice()),
        Some(b"excluded padding!".as_slice()),
    ]));
    for data_type in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(17),
    ] {
        let values = cast(&values, &data_type)?.slice(1, 3);
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", data_type, true)
                .with_metadata([("origin".into(), "native".into())].into()),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
        let plain = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(1)
                .with_target_partitions(2),
        );
        let session = session(1);
        for target in [&plain, &session] {
            target.register_table(
                "native_concat",
                Arc::new(MemTable::try_new(
                    schema.clone(),
                    vec![vec![batch.clone()]],
                )?),
            )?;
        }
        let expected_projection = plain
            .sql("SELECT b FROM native_concat")
            .await?
            .collect()
            .await?;
        let sql = "SELECT concat(b,b) AS pair,concat(b) AS single FROM native_concat";
        let expected = if matches!(schema.field(0).data_type(), DataType::FixedSizeBinary(_)) {
            None
        } else {
            Some(plain.sql(sql).await?.collect().await?)
        };
        for ansi in [None, Some(true), Some(false)] {
            if let Some(ansi) = ansi {
                capture_result(
                    &session,
                    &json!({"sql":"SELECT concat(b,b) AS r FROM native_concat", "ansi":ansi}),
                )
                .await?;
            }
            let projected = session
                .sql("SELECT b FROM native_concat")
                .await?
                .collect()
                .await?;
            assert!(
                projected
                    .iter()
                    .all(|batch| { batch.column(0).data_type() == schema.field(0).data_type() })
            );
            assert_eq!(projected, expected_projection);
            if let Some(expected) = &expected {
                assert_eq!(&session.sql(sql).await?.collect().await?, expected);
            } else {
                for target in [&plain, &session] {
                    let Err(error) = target.sql(sql).await?.collect().await else {
                        return Err("native FixedSizeBinary CONCAT unexpectedly executed".into());
                    };
                    assert!(
                        matches!(&error, DataFusionError::Context(context, cause)
                            if context == "type_coercion"
                            && matches!(cause.as_ref(), DataFusionError::Plan(message)
                                if message == "Cannot automatically convert FixedSizeBinary(17) to Utf8")),
                        "{error:?}"
                    );
                }
            }
        }
    }
    Ok(())
}
