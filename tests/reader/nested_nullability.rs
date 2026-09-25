//! Nested schema violations must return errors while preserving parent NULL masks.

use std::{error::Error, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use delta_arrow_reader::{
    DeltaReaderError, DeltaReaderPhase, DeltaScanExecutionOptions, DeltaTableBuilder,
};
use futures_util::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn field(name: &str, data_type: Value, nullable: bool) -> Value {
    json!({"name":name, "type":data_type, "nullable":nullable, "metadata":{}})
}

fn struct_type(nullable: bool) -> Value {
    json!({"type":"struct", "fields":[field("zip", json!("integer"), nullable)]})
}

fn structure(
    values: &[Option<i32>],
    valid: Option<&[bool]>,
    nullable: bool,
) -> TestResult<ArrayRef> {
    Ok(Arc::new(StructArray::try_new(
        vec![Field::new("zip", DataType::Int32, nullable)].into(),
        vec![Arc::new(Int32Array::from(values.to_vec()))],
        valid.map(|valid| NullBuffer::from(valid.to_vec())),
    )?))
}

fn map(keys: ArrayRef, values: ArrayRef, value_nullable: bool) -> TestResult<ArrayRef> {
    let len = i32::try_from(keys.len())?;
    let entries = StructArray::try_new(
        vec![
            Field::new("key", keys.data_type().clone(), false),
            Field::new("value", values.data_type().clone(), value_nullable),
        ]
        .into(),
        vec![keys, values],
        None,
    )?;
    Ok(Arc::new(MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from((0..=len).collect::<Vec<_>>())),
        entries,
        None,
        false,
    )?))
}

// Each collection row contains one element. The inner struct supplies the
// validity pattern, independently of the enclosing collection or struct.
fn wrap(layout: &str, values: ArrayRef, delta_type: Value) -> TestResult<(ArrayRef, Value)> {
    let len = i32::try_from(values.len())?;
    let ids = || Arc::new(Int32Array::from_iter_values(0..len)) as ArrayRef;
    Ok(match layout {
        "struct" => (values, delta_type),
        "nested_struct" => (
            Arc::new(StructArray::try_new(
                vec![Field::new("inner", values.data_type().clone(), true)].into(),
                vec![values],
                None,
            )?),
            json!({"type":"struct", "fields":[field("inner", delta_type, true)]}),
        ),
        "list_struct" => (
            Arc::new(ListArray::try_new(
                Arc::new(Field::new("element", values.data_type().clone(), true)),
                OffsetBuffer::new(ScalarBuffer::from((0..=len).collect::<Vec<_>>())),
                values,
                None,
            )?),
            json!({"type":"array", "elementType":delta_type, "containsNull":true}),
        ),
        "map_key_struct" => (
            map(values, ids(), false)?,
            json!({"type":"map", "keyType":delta_type, "valueType":"integer", "valueContainsNull":false}),
        ),
        "map_value_struct" => (
            map(ids(), values, true)?,
            json!({"type":"map", "keyType":"integer", "valueType":delta_type, "valueContainsNull":true}),
        ),
        _ => return Err(format!("unknown nested layout: {layout}").into()),
    })
}

fn fixture(array: ArrayRef, delta_type: Value) -> TestResult<RealParquetDeltaTable> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1024))
                .build(),
        ),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    RealParquetDeltaTable::new_with_raw_parquet(
        "nested-nullability",
        &bytes,
        batch.num_rows(),
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &json!({"metaData":{
            "id":"nested-nullability", "format":{"provider":"parquet","options":{}},
            "schemaString":json!({"type":"struct", "fields":[field("v", delta_type, true)]}).to_string(),
            "partitionColumns":[], "configuration":{}
        }}),
    )
}

fn assert_schema_error(error: &DeltaReaderError, field: &str) {
    assert_eq!(error.phase(), DeltaReaderPhase::DataFileRead, "{error}");
    assert_eq!(error.code(), "data_file_read");
    assert_eq!(
        error.to_string(),
        "delta reader error: phase=data_file_read code=data_file_read reason=parquet_batch_reshape_failed"
    );
    let mut source = error.source();
    let mut found_arrow = false;
    while let Some(error) = source {
        if let Some(delta_kernel::Error::Arrow(ArrowError::InvalidArgumentError(message))) =
            error.downcast_ref::<delta_kernel::Error>()
        {
            assert!(message.contains("unmasked nulls"), "{message}");
            assert!(message.contains(&format!("\"{field}\"")), "{message}");
            found_arrow = true;
        }
        source = error.source();
    }
    assert!(
        found_arrow,
        "underlying Arrow validation error was lost: {error:?}"
    );
}

fn assert_values(batches: &[RecordBatch], expected: &ArrayRef) -> TestResult {
    let schema = batches.first().ok_or("missing output batches")?.schema();
    let batch = concat_batches(&schema, batches)?;
    assert_eq!(batch.num_rows(), expected.len());
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.column(0).to_data(), expected.to_data());
    Ok(())
}

async fn check_fixture(
    fixture: &RealParquetDeltaTable,
    expected: Result<&ArrayRef, &str>,
) -> TestResult {
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    for threshold in [None, Some(usize::MAX)] {
        let scan = table
            .scan()
            .with_execution_options(
                DeltaScanExecutionOptions::new()
                    .with_parquet_full_file_read_threshold_bytes(threshold)?,
            )
            .build()
            .await?;
        let schema = scan.schema();
        let result = scan.into_stream().try_collect::<Vec<_>>().await;
        match expected {
            Ok(expected) => {
                let batches = result?;
                assert!(batches.iter().all(|batch| batch.schema() == schema));
                assert_values(&batches, expected)?;
            }
            Err(field) => {
                assert_schema_error(&result.err().ok_or("invalid input was accepted")?, field)
            }
        }
    }
    #[cfg(feature = "datafusion")]
    {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions};
        for batch_size in [1, 1024] {
            let context = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_target_partitions(1),
            );
            context.register_table(
                "t",
                Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions::default(),
                )?),
            )?;
            let result = context.sql("SELECT * FROM t").await?.collect().await;
            match expected {
                Ok(expected) => assert_values(&result?, expected)?,
                Err(field) => {
                    let error = result.err().ok_or("invalid SQL input was accepted")?;
                    let mut source: Option<&(dyn Error + 'static)> = Some(&error);
                    let mut reader_error = None;
                    while let Some(error) = source {
                        if let Some(error) = error.downcast_ref::<DeltaReaderError>() {
                            reader_error = Some(error);
                            break;
                        }
                        source = error.source();
                    }
                    assert_schema_error(
                        reader_error.ok_or("DataFusion lost the reader error")?,
                        field,
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_required_struct_children_return_schema_errors() -> TestResult {
    for layout in [
        "struct",
        "nested_struct",
        "list_struct",
        "map_key_struct",
        "map_value_struct",
    ] {
        for values in [vec![None], vec![Some(10), None, Some(30)]] {
            let (array, delta) = wrap(layout, structure(&values, None, true)?, struct_type(false))?;
            check_fixture(&fixture(array, delta)?, Err("zip")).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_required_map_values_return_schema_errors() -> TestResult {
    for structure_value in [false, true] {
        let (values, delta) = if structure_value {
            (
                structure(&[Some(10), None], Some(&[true, false]), true)?,
                struct_type(true),
            )
        } else {
            (
                Arc::new(Int32Array::from(vec![Some(10), None])) as ArrayRef,
                json!("integer"),
            )
        };
        let array = map(Arc::new(Int32Array::from(vec![1, 2])), values, true)?;
        let delta = json!({"type":"map", "keyType":"integer", "valueType":delta, "valueContainsNull":false});
        check_fixture(&fixture(array, delta)?, Err("value")).await?;
    }
    Ok(())
}

#[tokio::test]
async fn null_parents_mask_required_child_storage() -> TestResult {
    for layout in ["struct", "nested_struct", "list_struct", "map_value_struct"] {
        for (values, valid) in [
            (vec![None, Some(10), None], vec![false, true, false]),
            (vec![None, None], vec![false, false]),
        ] {
            let (array, delta) = wrap(
                layout,
                structure(&values, Some(&valid), true)?,
                struct_type(false),
            )?;
            let (expected, _) = wrap(
                layout,
                structure(&values, Some(&valid), false)?,
                struct_type(false),
            )?;
            check_fixture(&fixture(array, delta)?, Ok(&expected)).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn required_nested_values_and_nullable_children_remain_valid() -> TestResult {
    for layout in [
        "struct",
        "nested_struct",
        "list_struct",
        "map_key_struct",
        "map_value_struct",
    ] {
        for nullable in [false, true] {
            let values = if nullable {
                vec![Some(10), None]
            } else {
                vec![Some(10), Some(20)]
            };
            let (array, delta) = wrap(
                layout,
                structure(&values, None, true)?,
                struct_type(nullable),
            )?;
            let (expected, _) = wrap(
                layout,
                structure(&values, None, nullable)?,
                struct_type(nullable),
            )?;
            check_fixture(&fixture(array, delta)?, Ok(&expected)).await?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "manual before/after nested schema reshaping performance measurement"]
async fn nested_nullability_benchmark() -> TestResult {
    use std::time::Instant;
    let case = std::env::var("NESTED_BENCH_CASE").unwrap_or_else(|_| "struct".into());
    let rows: usize = std::env::var("NESTED_BENCH_ROWS")
        .unwrap_or_else(|_| "65536".into())
        .parse()?;
    let samples: usize = std::env::var("NESTED_BENCH_SAMPLES")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
    if !(1..=1_000_000).contains(&rows) || !(1..=1000).contains(&samples) {
        return Err("benchmark rows or samples out of range".into());
    }
    let values: Vec<_> = (0..rows as i32).map(Some).collect();
    let (array, delta) = wrap(&case, structure(&values, None, true)?, struct_type(false))?;
    let (expected, _) = wrap(&case, structure(&values, None, false)?, struct_type(false))?;
    let fixture = fixture(array, delta)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    for sample in 0..samples + 4 {
        let start = Instant::now();
        let scan = table.scan().build().await?;
        let planning_ns = start.elapsed().as_nanos();
        let stream = scan.into_stream();
        let metrics = stream.metrics();
        let start = Instant::now();
        let batches = stream.try_collect::<Vec<_>>().await?;
        let read_ns = start.elapsed().as_nanos();
        assert_values(&batches, &expected)?;
        let m = metrics.snapshot();
        if sample >= 4 {
            println!(
                "{}",
                json!({"case":case, "rows":rows, "sample":sample-4,
                "planning_ns":planning_ns, "read_ns":read_ns,
                "files":m.file_tasks_started, "emitted":m.scheduler_rows_emitted,
                "range_gets":m.parquet_data_file_range_get_operations,
                "full_gets":m.parquet_data_file_full_get_operations, "bytes":m.parquet_data_file_bytes_received})
            );
        }
    }
    Ok(())
}
