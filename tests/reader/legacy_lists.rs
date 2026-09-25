//! Physical LIST layouts must agree with parquet-rs without losing values or nesting.

use std::{error::Error, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    compute::{cast, concat, concat_batches},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaTableBuilder,
};
use futures_util::TryStreamExt;
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    data_type::{
        BoolType, ByteArray, ByteArrayType, DataType as ParquetDataType, DoubleType,
        FixedLenByteArray, FixedLenByteArrayType, FloatType, Int32Type, Int64Type, Int96,
        Int96Type,
    },
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use serde_json::{Value, json};

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Column<'a, T> = (&'a [T], &'a [i16], &'a [i16]);

// ArrowWriter always writes canonical lists, so use the low-level writer to
// retain the physical layout under test. Every group repeats the same rows.
fn parquet_bytes<T: ParquetDataType>(
    schema: &str,
    columns: &[Column<'_, T::T>],
    groups: usize,
) -> TestResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        Arc::new(parse_message_type(schema)?),
        Arc::new(WriterProperties::builder().build()),
    )?;
    for _ in 0..groups {
        let mut group = writer.next_row_group()?;
        for (values, definitions, repetitions) in columns {
            let mut column = group.next_column()?.ok_or("missing physical column")?;
            column
                .typed::<T>()
                .write_batch(values, Some(definitions), Some(repetitions))?;
            column.close()?;
        }
        assert!(group.next_column()?.is_none());
        group.close()?;
    }
    writer.close()?;
    Ok(bytes)
}

fn field(name: &str, data_type: Value, nullable: bool) -> Value {
    json!({"name": name, "type": data_type, "nullable": nullable, "metadata": {}})
}

fn array_type(element: Value, nullable: bool) -> Value {
    json!({"type": "array", "elementType": element, "containsNull": nullable})
}

fn fixture(bytes: &[u8], fields: Vec<Value>, rows: usize) -> TestResult<RealParquetDeltaTable> {
    mapped_fixture(bytes, fields, rows, false)
}

fn mapped_fixture(
    bytes: &[u8],
    fields: Vec<Value>,
    rows: usize,
    mapped: bool,
) -> TestResult<RealParquetDeltaTable> {
    let protocol = if mapped {
        json!({"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}})
    } else {
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
    };
    let configuration = if mapped {
        json!({"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "20"})
    } else {
        json!({})
    };
    RealParquetDeltaTable::new_with_raw_parquet(
        "legacy-lists",
        bytes,
        rows,
        &protocol,
        &json!({"metaData": {
            "id": "legacy-lists", "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": [], "configuration": configuration
        }}),
    )
}

fn list(
    values: ArrayRef,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
    nullable: bool,
) -> TestResult<ArrayRef> {
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("element", values.data_type().clone(), nullable)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        valid.map(NullBuffer::from),
    )?))
}

fn repeated(array: &ArrayRef, times: usize) -> TestResult<ArrayRef> {
    Ok(concat(&vec![array.as_ref(); times])?)
}

fn assert_values(batches: &[RecordBatch], expected: &ArrayRef, normalize_type: bool) -> TestResult {
    let schema = batches.first().ok_or("expected output batches")?.schema();
    let batch = concat_batches(&schema, batches)?;
    assert_eq!(batch.num_rows(), expected.len());
    assert_eq!(batch.num_columns(), 1);
    let actual = if normalize_type {
        // The raw Parquet oracle retains physical element names. DataFusion
        // may use Utf8View/BinaryView. Normalize only these representation details.
        cast(batch.column(0), expected.data_type())?
    } else {
        Arc::clone(batch.column(0))
    };
    assert_eq!(actual.to_data(), expected.to_data());
    Ok(())
}

fn assert_parquet_values(bytes: &[u8], expected: &ArrayRef) -> TestResult {
    let batches = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?
        .with_batch_size(1)
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    assert_values(&batches, expected, true)
}

async fn check_fixture(fixture: &RealParquetDeltaTable, expected: &ArrayRef) -> TestResult {
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
        let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
        assert!(batches.iter().all(|batch| batch.schema() == schema));
        assert_values(&batches, expected, false)?;
    }
    #[cfg(feature = "datafusion")]
    {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions};
        for views in [false, true] {
            let context = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(1)
                    .with_target_partitions(1),
            );
            context.register_table(
                "t",
                Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        use_arrow_view_types: views,
                        ..Default::default()
                    },
                )?),
            )?;
            let batches = context.sql("SELECT * FROM t").await?.collect().await?;
            assert_values(&batches, expected, views)?;
        }
    }
    Ok(())
}

async fn check_primitive<T: ParquetDataType>(
    physical: &str,
    delta: &str,
    values: &[T::T],
    expected_values: ArrayRef,
) -> TestResult {
    let bytes = parquet_bytes::<T>(
        &format!("message m {{ OPTIONAL GROUP v (LIST) {{ REPEATED {physical}; }} }}"),
        &[(values, &[0, 1, 2, 2, 2], &[0, 0, 0, 1, 0])],
        2,
    )?;
    // NULL, [], [a, b], [c], twice across two row groups.
    let expected = repeated(
        &list(
            expected_values,
            vec![0, 0, 0, 2, 3],
            Some(vec![false, true, true, true]),
            false,
        )?,
        2,
    )?;
    assert_parquet_values(&bytes, &expected)?;
    let fixture = fixture(
        &bytes,
        vec![field("v", array_type(json!(delta), false), true)],
        8,
    )?;
    check_fixture(&fixture, &expected).await
}

#[tokio::test]
async fn two_level_primitive_lists_preserve_all_physical_types() -> TestResult {
    let int96: Vec<_> = [0, 1_000, 2_000]
        .into_iter()
        .map(|nanos| {
            let mut value = Int96::new();
            value.set_data(nanos, 0, 2_440_588);
            value
        })
        .collect();
    check_primitive::<Int96Type>(
        "INT96 array",
        "timestamp",
        &int96,
        Arc::new(TimestampMicrosecondArray::from(vec![0, 1, 2]).with_timezone("UTC")),
    )
    .await?;
    check_primitive::<BoolType>(
        "BOOLEAN array",
        "boolean",
        &[true, false, true],
        Arc::new(BooleanArray::from(vec![true, false, true])),
    )
    .await?;
    check_primitive::<Int32Type>(
        "INT32 array",
        "integer",
        &[10, 20, -30],
        Arc::new(Int32Array::from(vec![10, 20, -30])),
    )
    .await?;
    check_primitive::<Int32Type>(
        "INT32 array (INT_8)",
        "byte",
        &[-128, 0, 127],
        Arc::new(Int8Array::from(vec![-128, 0, 127])),
    )
    .await?;
    check_primitive::<Int32Type>(
        "INT32 array (INT_16)",
        "short",
        &[-32768, 0, 32767],
        Arc::new(Int16Array::from(vec![-32768, 0, 32767])),
    )
    .await?;
    check_primitive::<Int32Type>(
        "INT32 array (DATE)",
        "date",
        &[-1, 0, 20000],
        Arc::new(Date32Array::from(vec![-1, 0, 20000])),
    )
    .await?;
    check_primitive::<Int64Type>(
        "INT64 array",
        "long",
        &[i64::MIN, 0, i64::MAX],
        Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
    )
    .await?;
    check_primitive::<Int64Type>(
        "INT64 array (TIMESTAMP_MICROS)",
        "timestamp",
        &[-1, 0, 123456789],
        Arc::new(TimestampMicrosecondArray::from(vec![-1, 0, 123456789]).with_timezone("UTC")),
    )
    .await?;
    check_primitive::<FloatType>(
        "FLOAT array",
        "float",
        &[-1.5, 0.0, 2.5],
        Arc::new(Float32Array::from(vec![-1.5, 0.0, 2.5])),
    )
    .await?;
    check_primitive::<DoubleType>(
        "DOUBLE array",
        "double",
        &[-1.5, 0.0, 2.5],
        Arc::new(Float64Array::from(vec![-1.5, 0.0, 2.5])),
    )
    .await?;
    let strings = [
        ByteArray::from(""),
        ByteArray::from("hello"),
        ByteArray::from("世界"),
    ];
    check_primitive::<ByteArrayType>(
        "BINARY array (UTF8)",
        "string",
        &strings,
        Arc::new(StringArray::from(vec!["", "hello", "世界"])),
    )
    .await?;
    check_primitive::<ByteArrayType>(
        "BINARY array",
        "binary",
        &strings,
        Arc::new(BinaryArray::from(vec![
            b"".as_slice(),
            b"hello",
            "世界".as_bytes(),
        ])),
    )
    .await?;
    let decimals: Vec<FixedLenByteArray> = [-123_i128, 0, 456]
        .into_iter()
        .map(|v| v.to_be_bytes().to_vec().into())
        .collect();
    check_primitive::<FixedLenByteArrayType>(
        "FIXED_LEN_BYTE_ARRAY(16) array (DECIMAL(20, 2))",
        "decimal(20,2)",
        &decimals,
        Arc::new(Decimal128Array::from(vec![-123, 0, 456]).with_precision_and_scale(20, 2)?),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn list_wrappers_preserve_null_empty_and_nullable_elements() -> TestResult {
    for outer in ["OPTIONAL", "REQUIRED"] {
        for wrapper in ["list", "bag", "element"] {
            for nullable in [false, true] {
                let element = if nullable { "OPTIONAL" } else { "REQUIRED" };
                let outer_def = i16::from(outer == "OPTIONAL");
                let value_def = outer_def + 1 + i16::from(nullable);
                // [] and [10, 20], followed by [NULL] when allowed.
                let mut defs = vec![outer_def, value_def, value_def];
                let mut reps = vec![0, 0, 1];
                let mut values = vec![Some(10), Some(20)];
                let mut offsets = vec![0, 0, 2];
                if nullable {
                    defs.push(outer_def + 1);
                    reps.push(0);
                    values.push(None);
                    offsets.push(3);
                }
                let bytes = parquet_bytes::<Int32Type>(
                    &format!(
                        "message m {{ {outer} GROUP v (LIST) {{ REPEATED GROUP {wrapper} {{ {element} INT32 value; }} }} }}"
                    ),
                    &[(&[10, 20], &defs, &reps)],
                    2,
                )?;
                let expected = repeated(
                    &list(Arc::new(Int32Array::from(values)), offsets, None, nullable)?,
                    2,
                )?;
                assert_parquet_values(&bytes, &expected)?;
                let fixture = fixture(
                    &bytes,
                    vec![field(
                        "v",
                        array_type(json!("integer"), nullable),
                        outer == "OPTIONAL",
                    )],
                    expected.len(),
                )?;
                check_fixture(&fixture, &expected).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn legacy_struct_lists_keep_single_field_structs_and_reorder_children() -> TestResult {
    for wrapper in ["array", "v_tuple", "arbitrary"] {
        for single in [false, true] {
            if single && wrapper == "arbitrary" {
                continue;
            }
            let second = if single { "" } else { "REQUIRED INT32 a;" };
            let defs = [0, 1, 2, 2];
            let reps = [0, 0, 0, 1];
            let mut columns: Vec<Column<'_, i32>> = vec![(&[10, 20], &defs, &reps)];
            if !single {
                columns.push((&[30, 40], &defs, &reps));
            }
            let bytes = parquet_bytes::<Int32Type>(
                &format!(
                    "message m {{ OPTIONAL GROUP v (LIST) {{ REPEATED GROUP {wrapper} {{ REQUIRED INT32 b; {second} }} }} }}"
                ),
                &columns,
                2,
            )?;
            let (fields, arrays, delta) = if single {
                (
                    vec![Field::new("b", DataType::Int32, false)],
                    vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef],
                    vec![field("b", json!("integer"), false)],
                )
            } else {
                (
                    vec![
                        Field::new("a", DataType::Int32, false),
                        Field::new("b", DataType::Int32, false),
                    ],
                    vec![
                        Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef,
                        Arc::new(Int32Array::from(vec![10, 20])),
                    ],
                    vec![
                        field("a", json!("integer"), false),
                        field("b", json!("integer"), false),
                    ],
                )
            };
            let elements = Arc::new(StructArray::try_new(fields.into(), arrays, None)?) as ArrayRef;
            let expected = repeated(
                &list(
                    elements,
                    vec![0, 0, 0, 2],
                    Some(vec![false, true, true]),
                    false,
                )?,
                2,
            )?;
            let fixture = fixture(
                &bytes,
                vec![field(
                    "v",
                    array_type(json!({"type": "struct", "fields": delta}), false),
                    true,
                )],
                6,
            )?;
            check_fixture(&fixture, &expected).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn nested_legacy_lists_preserve_both_offsets() -> TestResult {
    for wrapper in ["list", "array", "v_tuple"] {
        for annotation in ["", "(LIST)"] {
            let bytes = parquet_bytes::<Int32Type>(
                &format!(
                    "message m {{ OPTIONAL GROUP v (LIST) {{ REPEATED GROUP {wrapper} {annotation} {{ REPEATED INT32 item; }} }} }}"
                ),
                &[(&[10, 20, 30], &[0, 1, 2, 3, 3, 3], &[0, 0, 0, 1, 2, 1])],
                2,
            )?;
            // NULL, [], [[], [10,20], [30]].
            let inner = list(
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                vec![0, 0, 2, 3],
                None,
                false,
            )?;
            let expected = repeated(
                &list(
                    inner,
                    vec![0, 0, 0, 3],
                    Some(vec![false, true, true]),
                    false,
                )?,
                2,
            )?;
            assert_parquet_values(&bytes, &expected)?;
            let fixture = fixture(
                &bytes,
                vec![field(
                    "v",
                    array_type(array_type(json!("integer"), false), false),
                    true,
                )],
                6,
            )?;
            check_fixture(&fixture, &expected).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn annotated_legacy_wrappers_follow_the_arrow_decoder() -> TestResult {
    // parquet-rs treats an annotated wrapper as a wrapper even when its name
    // would otherwise denote a one-field struct. Exercise that exception
    // separately from the repeated-child exception in the nested-list cases.
    for wrapper in ["array", "v_tuple"] {
        let bytes = parquet_bytes::<Int32Type>(
            &format!(
                "message m {{ OPTIONAL GROUP v (LIST) {{ REPEATED GROUP {wrapper} (LIST) {{ OPTIONAL INT32 item; }} }} }}"
            ),
            &[(&[10, 20], &[0, 1, 2, 3, 3], &[0, 0, 0, 1, 1])],
            2,
        )?;
        let expected = repeated(
            &list(
                Arc::new(Int32Array::from(vec![None, Some(10), Some(20)])),
                vec![0, 0, 0, 3],
                Some(vec![false, true, true]),
                true,
            )?,
            2,
        )?;
        assert_parquet_values(&bytes, &expected)?;
        let fixture = fixture(
            &bytes,
            vec![field("v", array_type(json!("integer"), true), true)],
            expected.len(),
        )?;
        check_fixture(&fixture, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_list_annotations_follow_decoder_precedence() -> TestResult {
    use parquet::{
        basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType},
        schema::types::Type,
    };
    // Group metadata can retain conflicting legacy and logical annotations.
    // The Arrow decoder checks the logical annotation first when deciding
    // whether a named, one-field repeated group is a struct or a wrapper.
    for wrapper in ["array", "v_tuple"] {
        for (converted, logical, structure) in [
            (ConvertedType::LIST, LogicalType::Map, true),
            (ConvertedType::MAP, LogicalType::List, false),
        ] {
            let item = Arc::new(
                Type::primitive_type_builder("item", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()?,
            );
            let repeated = Arc::new(
                Type::group_type_builder(wrapper)
                    .with_repetition(Repetition::REPEATED)
                    .with_converted_type(converted)
                    .with_logical_type(Some(logical.clone()))
                    .with_fields(vec![item])
                    .build()?,
            );
            let list_type = Arc::new(
                Type::group_type_builder("v")
                    .with_repetition(Repetition::OPTIONAL)
                    .with_converted_type(ConvertedType::LIST)
                    .with_fields(vec![repeated])
                    .build()?,
            );
            let schema = Arc::new(
                Type::group_type_builder("m")
                    .with_fields(vec![list_type])
                    .build()?,
            );
            let mut bytes = Vec::new();
            let mut writer = SerializedFileWriter::new(
                &mut bytes,
                schema,
                Arc::new(WriterProperties::builder().build()),
            )?;
            let mut group = writer.next_row_group()?;
            let mut column = group.next_column()?.ok_or("missing item column")?;
            column
                .typed::<Int32Type>()
                .write_batch(&[10, 20], Some(&[2, 2]), Some(&[0, 1]))?;
            column.close()?;
            group.close()?;
            writer.close()?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes))?;
            let info = builder.parquet_schema().root_schema().get_fields()[0].get_fields()[0]
                .get_basic_info();
            assert_eq!(info.converted_type(), converted);
            assert_eq!(info.logical_type_ref(), Some(&logical));
            let values = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
            let (values, delta) = if structure {
                (
                    Arc::new(StructArray::try_new(
                        vec![Field::new("item", DataType::Int32, false)].into(),
                        vec![values],
                        None,
                    )?) as ArrayRef,
                    json!({"type":"struct", "fields":[field("item", json!("integer"), false)]}),
                )
            } else {
                (values, json!("integer"))
            };
            let expected = list(values, vec![0, 2], None, false)?;
            assert_parquet_values(&bytes, &expected)?;
            let fixture = fixture(&bytes, vec![field("v", array_type(delta, false), true)], 1)?;
            check_fixture(&fixture, &expected).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn mixed_list_encodings_preserve_null_inner_lists() -> TestResult {
    for child in [
        "REPEATED INT32 array;",
        "REPEATED GROUP list { REQUIRED INT32 element; }",
    ] {
        let bytes = parquet_bytes::<Int32Type>(
            &format!(
                "message m {{ OPTIONAL GROUP v (LIST) {{ REPEATED GROUP list {{ OPTIONAL GROUP element (LIST) {{ {child} }} }} }} }}"
            ),
            &[(&[10, 20], &[0, 1, 2, 3, 4, 4], &[0, 0, 0, 1, 1, 2])],
            2,
        )?;
        // NULL, [], [NULL, [], [10,20]], twice.
        let inner = list(
            Arc::new(Int32Array::from(vec![10, 20])),
            vec![0, 0, 0, 2],
            Some(vec![false, true, true]),
            false,
        )?;
        let expected = repeated(
            &list(inner, vec![0, 0, 0, 3], Some(vec![false, true, true]), true)?,
            2,
        )?;
        assert_parquet_values(&bytes, &expected)?;
        let fixture = fixture(
            &bytes,
            vec![field(
                "v",
                array_type(array_type(json!("integer"), false), true),
                true,
            )],
            expected.len(),
        )?;
        check_fixture(&fixture, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn unannotated_repeated_fields_are_required_lists() -> TestResult {
    for structure in [false, true] {
        let schema = if structure {
            "message m { REPEATED GROUP v { REQUIRED INT32 n; } }"
        } else {
            "message m { REPEATED INT32 v; }"
        };
        let bytes = parquet_bytes::<Int32Type>(schema, &[(&[10, 20], &[0, 1, 1], &[0, 0, 1])], 2)?;
        let values = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let (elements, delta) = if structure {
            (
                Arc::new(StructArray::try_new(
                    vec![Field::new("n", DataType::Int32, false)].into(),
                    vec![values],
                    None,
                )?) as ArrayRef,
                json!({"type":"struct", "fields":[field("n", json!("integer"), false)]}),
            )
        } else {
            (values, json!("integer"))
        };
        let expected = repeated(&list(elements, vec![0, 0, 2], None, false)?, 2)?;
        assert_parquet_values(&bytes, &expected)?;
        let fixture = fixture(&bytes, vec![field("v", array_type(delta, false), false)], 4)?;
        check_fixture(&fixture, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_lists_preserve_column_mapping_and_struct_reordering() -> TestResult {
    for wrapper in ["array", "phys_v_tuple"] {
        for single in [true, false] {
            let extra = if single {
                ""
            } else {
                "REQUIRED INT32 phys_a = 3;"
            };
            let mut columns: Vec<Column<'_, i32>> = vec![(&[10, 20], &[2, 2], &[0, 1])];
            if !single {
                columns.push((&[30, 40], &[2, 2], &[0, 1]));
            }
            let bytes = parquet_bytes::<Int32Type>(
                &format!(
                    "message m {{ OPTIONAL GROUP phys_v (LIST) = 1 {{ REPEATED GROUP {wrapper} = 2 {{ REQUIRED INT32 phys_b = 4; {extra} }} }} }}"
                ),
                &columns,
                2,
            )?;
            let mapped = |logical: &str, physical: &str, id| {
                let mut field = field(logical, json!("integer"), false);
                field["metadata"] = json!({"delta.columnMapping.id":id, "delta.columnMapping.physicalName":physical});
                field
            };
            let mapped_arrow = |logical: &str, physical: &str, id: i32| {
                Field::new(logical, DataType::Int32, false).with_metadata(
                    std::collections::HashMap::from([
                        ("delta.columnMapping.id".into(), id.to_string()),
                        ("delta.columnMapping.physicalName".into(), physical.into()),
                    ]),
                )
            };
            let mut delta_fields = vec![];
            let mut arrow_fields = vec![];
            let mut arrays: Vec<ArrayRef> = vec![];
            if !single {
                delta_fields.push(mapped("x", "phys_a", 3));
                arrow_fields.push(mapped_arrow("x", "phys_a", 3));
                arrays.push(Arc::new(Int32Array::from(vec![30, 40])));
            }
            delta_fields.push(mapped("y", "phys_b", 4));
            arrow_fields.push(mapped_arrow("y", "phys_b", 4));
            arrays.push(Arc::new(Int32Array::from(vec![10, 20])));
            let mut v = field(
                "v",
                array_type(json!({"type":"struct", "fields":delta_fields}), false),
                true,
            );
            v["metadata"] = json!({"delta.columnMapping.id":1, "delta.columnMapping.physicalName":"phys_v", "delta.columnMapping.nested.ids":{"phys_v.element":2}});
            let fixture = mapped_fixture(&bytes, vec![v], 2, true)?;
            let values =
                Arc::new(StructArray::try_new(arrow_fields.into(), arrays, None)?) as ArrayRef;
            let expected = repeated(&list(values, vec![0, 2], None, false)?, 2)?;
            check_fixture(&fixture, &expected).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn two_level_lists_allow_files_with_no_element_values() -> TestResult {
    for (defs, valid) in [
        (vec![0, 0], vec![false, false]),
        (vec![1, 1], vec![true, true]),
        (vec![0, 1], vec![false, true]),
    ] {
        let bytes = parquet_bytes::<Int32Type>(
            "message m { OPTIONAL GROUP v (LIST) { REPEATED INT32 array; } }",
            &[(&[], &defs, &[0, 0])],
            2,
        )?;
        let expected = repeated(
            &list(
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                vec![0, 0, 0],
                Some(valid),
                false,
            )?,
            2,
        )?;
        assert_parquet_values(&bytes, &expected)?;
        let fixture = fixture(
            &bytes,
            vec![field("v", array_type(json!("integer"), false), true)],
            4,
        )?;
        check_fixture(&fixture, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn two_level_list_reproduction_reads_exactly_one_row() -> TestResult {
    for outer in ["OPTIONAL", "REQUIRED"] {
        for name in ["array", "element", "arbitrary"] {
            let def = if outer == "OPTIONAL" { 2 } else { 1 };
            let bytes = parquet_bytes::<Int32Type>(
                &format!("message m {{ {outer} GROUP v (LIST) {{ REPEATED INT32 {name}; }} }}"),
                &[(&[10, 20], &[def, def], &[0, 1])],
                1,
            )?;
            let expected = list(
                Arc::new(Int32Array::from(vec![10, 20])),
                vec![0, 2],
                None,
                false,
            )?;
            assert_parquet_values(&bytes, &expected)?;
            let fixture = fixture(
                &bytes,
                vec![field(
                    "v",
                    array_type(json!("integer"), false),
                    outer == "OPTIONAL",
                )],
                1,
            )?;
            check_fixture(&fixture, &expected).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn legacy_lists_preserve_values_with_hidden_scalar_predicates() -> TestResult {
    let bytes = parquet_bytes::<Int32Type>(
        "message m { REQUIRED INT32 id; OPTIONAL GROUP v (LIST) { REPEATED INT32 array; } }",
        &[
            (&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]),
            (&[10, 20], &[0, 1, 2, 2], &[0, 0, 0, 1]),
        ],
        2,
    )?;
    let fixture = fixture(
        &bytes,
        vec![
            field("id", json!("integer"), false),
            field("v", array_type(json!("integer"), false), true),
        ],
        6,
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let null = list(
        Arc::new(Int32Array::from(Vec::<i32>::new())),
        vec![0, 0],
        Some(vec![false]),
        false,
    )?;
    let present = list(
        Arc::new(Int32Array::from(vec![10, 20])),
        vec![0, 0, 2],
        None,
        false,
    )?;
    for (op, value, expected) in [
        (DeltaComparison::Eq, 0, repeated(&null, 2)?),
        (DeltaComparison::Gt, 0, repeated(&present, 2)?),
    ] {
        for hidden in [false, true] {
            for limit in [None, Some(1)] {
                let mut scan = table
                    .scan()
                    .with_predicate(DeltaPredicate::Compare {
                        column: "id".into(),
                        op,
                        value: DeltaScalar::Int32(value),
                    })
                    .with_projection(if hidden { vec![] } else { vec!["v"] });
                if let Some(limit) = limit {
                    scan = scan.with_limit(limit);
                }
                let batches = scan
                    .build()
                    .await?
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await?;
                let expected_rows = limit.unwrap_or(expected.len());
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    expected_rows
                );
                if hidden {
                    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
                } else {
                    assert_values(&batches, &expected.slice(0, expected_rows), false)?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn legacy_lists_work_inside_structs_maps_and_list_structs() -> TestResult {
    let inner = list(
        Arc::new(Int32Array::from(vec![10, 20])),
        vec![0, 2, 2],
        None,
        false,
    )?;
    let inner_type = array_type(json!("integer"), false);
    for case in ["struct", "map", "list_struct"] {
        let (schema, columns, delta, expected) = match case {
            "struct" => (
                "message m { REQUIRED GROUP v { OPTIONAL GROUP items (LIST) { REPEATED INT32 array; } } }",
                vec![(&[10, 20][..], &[2, 2, 1][..], &[0, 1, 0][..])],
                json!({"type":"struct", "fields":[field("items", inner_type.clone(), true)]}),
                Arc::new(StructArray::try_new(
                    vec![Field::new("items", inner.data_type().clone(), true)].into(),
                    vec![Arc::clone(&inner)],
                    None,
                )?) as ArrayRef,
            ),
            "map" => {
                let entries = StructArray::try_new(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", inner.data_type().clone(), true),
                    ]
                    .into(),
                    vec![Arc::new(Int32Array::from(vec![7, 8])), Arc::clone(&inner)],
                    None,
                )?;
                let map = MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
                    entries,
                    None,
                    false,
                )?;
                (
                    "message m { REQUIRED GROUP v (MAP) { REPEATED GROUP key_value { REQUIRED INT32 key; OPTIONAL GROUP value (LIST) { REPEATED INT32 array; } } } }",
                    vec![
                        (&[7, 8][..], &[1, 1][..], &[0, 1][..]),
                        (&[10, 20][..], &[3, 3, 2][..], &[0, 2, 1][..]),
                    ],
                    json!({"type":"map", "keyType":"integer", "valueType":inner_type.clone(), "valueContainsNull":true}),
                    Arc::new(map) as ArrayRef,
                )
            }
            _ => {
                let elements = Arc::new(StructArray::try_new(
                    vec![Field::new("items", inner.data_type().clone(), true)].into(),
                    vec![Arc::clone(&inner)],
                    None,
                )?) as ArrayRef;
                (
                    "message m { REQUIRED GROUP v (LIST) { REPEATED GROUP array { OPTIONAL GROUP items (LIST) { REPEATED INT32 array; } } } }",
                    vec![(&[10, 20][..], &[3, 3, 2][..], &[0, 2, 1][..])],
                    array_type(
                        json!({"type":"struct", "fields":[field("items", inner_type.clone(), true)]}),
                        false,
                    ),
                    list(elements, vec![0, 2], None, false)?,
                )
            }
        };
        let bytes = parquet_bytes::<Int32Type>(schema, &columns, 2)?;
        let expected = repeated(&expected, 2)?;
        assert_parquet_values(&bytes, &expected)?;
        let fixture = fixture(&bytes, vec![field("v", delta, false)], expected.len())?;
        check_fixture(&fixture, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn malformed_lists_return_data_file_errors_without_task_panics() -> TestResult {
    for child in ["REQUIRED INT32 item", "OPTIONAL INT32 item"] {
        let max_def = if child.starts_with("OPTIONAL") { 2 } else { 1 };
        let bytes = parquet_bytes::<Int32Type>(
            &format!("message m {{ OPTIONAL GROUP v (LIST) {{ {child}; }} }}"),
            &[(&[10], &[max_def], &[0])],
            1,
        )?;
        let fixture = fixture(
            &bytes,
            vec![field("v", array_type(json!("integer"), false), true)],
            1,
        )?;
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        let result = table
            .scan()
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await;
        let error = result.err().ok_or("malformed list was accepted")?;
        assert_eq!(error.code(), "data_file_read");
        let mut source = error.source();
        let mut messages = String::new();
        while let Some(error) = source {
            messages.push_str(&error.to_string());
            source = error.source();
        }
        assert!(
            messages.contains("List child must be repeated"),
            "{messages}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "manual before/after LIST schema alignment performance measurement"]
async fn legacy_list_benchmark() -> TestResult {
    use std::time::Instant;
    let case = std::env::var("LIST_BENCH_CASE").unwrap_or_else(|_| "list".into());
    let rows: usize = std::env::var("LIST_BENCH_ROWS")
        .unwrap_or_else(|_| "65536".into())
        .parse()?;
    let samples: usize = std::env::var("LIST_BENCH_SAMPLES")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
    if !(1..=1_000_000).contains(&rows) || !(1..=1000).contains(&samples) {
        return Err("benchmark rows or samples out of range".into());
    }
    let values: Vec<i32> = (0..rows as i32).flat_map(|v| [v, -v]).collect();
    let (schema, delta, expected, defs, reps) = match case.as_str() {
        "scalar" => (
            "message m { REQUIRED INT32 v; }",
            json!("integer"),
            Arc::new(Int32Array::from(values.clone())) as ArrayRef,
            vec![0; rows * 2],
            vec![0; rows * 2],
        ),
        "list" | "struct" => {
            let (schema, delta, elements) = if case == "list" {
                (
                    "message m { REQUIRED GROUP v (LIST) { REPEATED GROUP list { REQUIRED INT32 element; } } }",
                    json!("integer"),
                    Arc::new(Int32Array::from(values.clone())) as ArrayRef,
                )
            } else {
                (
                    "message m { REQUIRED GROUP v (LIST) { REPEATED GROUP list { REQUIRED GROUP element { REQUIRED INT32 n; } } } }",
                    json!({"type":"struct", "fields":[field("n", json!("integer"), false)]}),
                    Arc::new(StructArray::try_new(
                        vec![Field::new("n", DataType::Int32, false)].into(),
                        vec![Arc::new(Int32Array::from(values.clone()))],
                        None,
                    )?) as ArrayRef,
                )
            };
            (
                schema,
                array_type(delta, false),
                list(
                    elements,
                    (0..=rows as i32).map(|n| n * 2).collect(),
                    None,
                    false,
                )?,
                vec![1; rows * 2],
                (0..rows).flat_map(|_| [0, 1]).collect(),
            )
        }
        _ => return Err("unknown LIST_BENCH_CASE".into()),
    };
    let bytes = parquet_bytes::<Int32Type>(schema, &[(&values, &defs, &reps)], 2)?;
    let expected = repeated(&expected, 2)?;
    let fixture = fixture(&bytes, vec![field("v", delta, false)], expected.len())?;
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
        assert_values(&batches, &expected, false)?;
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
