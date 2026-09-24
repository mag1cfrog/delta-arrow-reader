//! INT96 must reach Arrow at Delta's microsecond precision, before any values overflow.

use std::{error::Error, fs, sync::Arc};

use arrow::{
    array::{Array, AsArray, ListArray, MapArray, StructArray},
    datatypes::{DataType, Field, Int32Type, Schema, TimeUnit, TimestampMicrosecondType},
    record_batch::RecordBatch,
};
use futures_util::TryStreamExt;
use parquet::{
    data_type::{
        ByteArray, ByteArrayType, DataType as ParquetDataType, Int32Type as ParquetInt32,
        Int64Type, Int96, Int96Type,
    },
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};
use serde_json::json;

use super::{
    ParquetMetadataCache, PhysicalParquetStreamOptions,
    tests::{TestDir, metrics, reader, task},
};
use crate::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaTable,
    DeltaTableBuilder,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

// Independent epoch-nanosecond inputs, never obtained by decoding or casting the fixture.
// 9999-12-31, 1600-01-01, a modern date, the epoch, pre-epoch sub-microseconds,
// and both sides of the signed nanosecond limits. The last group is entirely NULL.
const EPOCH_NANOS: &[Option<i128>] = &[
    Some(253_402_214_400_000_000_000),
    Some(-11_676_096_000_000_000_000),
    Some(1_704_067_200_123_456_000),
    Some(0),
    Some(-1),
    Some(-1_001),
    Some(999),
    Some(1_001),
    Some(i64::MAX as i128 - 1),
    Some(i64::MAX as i128 + 1),
    Some(i64::MIN as i128 + 1),
    Some(i64::MIN as i128 - 1),
    Some(253_402_300_799_999_999_000),
    Some(-11_676_095_999_876_543_000),
    None,
    None,
    None,
    None,
];

fn expected_micros() -> Vec<Option<i64>> {
    EPOCH_NANOS
        .iter()
        .map(|n| n.map(|n| i64::try_from(n.div_euclid(1_000)).unwrap()))
        .collect()
}

fn int96(epoch_nanos: i128) -> Int96 {
    let day = u32::try_from(epoch_nanos.div_euclid(86_400_000_000_000) + 2_440_588).unwrap();
    let nanos = u64::try_from(epoch_nanos.rem_euclid(86_400_000_000_000)).unwrap();
    let mut value = Int96::new();
    value.set_data(nanos as u32, (nanos >> 32) as u32, day);
    value
}

fn write_column<T: ParquetDataType>(
    group: &mut SerializedRowGroupWriter<'_, &mut Vec<u8>>,
    values: &[T::T],
    definitions: Option<&[i16]>,
    repetitions: Option<&[i16]>,
) -> TestResult {
    let mut column = group.next_column()?.ok_or("missing fixture column")?;
    column
        .typed::<T>()
        .write_batch(values, definitions, repetitions)?;
    column.close()?;
    Ok(())
}

fn flat_file(dictionary: bool) -> TestResult<Vec<u8>> {
    let schema = Arc::new(parse_message_type(
        "message m {
        OPTIONAL INT96 ts = 2;
        REQUIRED BINARY label (STRING) = 3;
        REQUIRED INT32 id = 1;
        REQUIRED INT64 nanos (TIMESTAMP(NANOS,false)) = 4;
        REQUIRED INT64 micros (TIMESTAMP(MICROS,true)) = 5;
        REQUIRED INT64 millis (TIMESTAMP(MILLIS,false)) = 6;
    }",
    )?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build(),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut bytes, schema, props)?;
    for (group_index, rows) in EPOCH_NANOS.chunks(3).enumerate() {
        let mut group = writer.next_row_group()?;
        let values = rows
            .iter()
            .flatten()
            .copied()
            .map(int96)
            .collect::<Vec<_>>();
        let definitions = rows
            .iter()
            .map(|n| i16::from(n.is_some()))
            .collect::<Vec<_>>();
        write_column::<Int96Type>(&mut group, &values, Some(&definitions), None)?;
        write_column::<ByteArrayType>(
            &mut group,
            &vec![ByteArray::from("value"); rows.len()],
            None,
            None,
        )?;
        let ids = (group_index * 3..group_index * 3 + rows.len())
            .map(|i| i as i32)
            .collect::<Vec<_>>();
        write_column::<ParquetInt32>(&mut group, &ids, None, None)?;
        for unit_value in [
            1_704_067_200_123_456_789_i64,
            1_704_067_200_123_456,
            1_704_067_200_123,
        ] {
            write_column::<Int64Type>(&mut group, &vec![unit_value; rows.len()], None, None)?;
        }
        group.close()?;
    }
    writer.close()?;
    Ok(bytes)
}

fn target_schema(views: bool, timezone: Option<Arc<str>>) -> Arc<Schema> {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, timezone);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("ts", timestamp.clone(), true),
        Field::new("nanos", timestamp.clone(), false),
        Field::new("micros", timestamp.clone(), false),
        Field::new("millis", timestamp, false),
        Field::new(
            "label",
            if views {
                DataType::Utf8View
            } else {
                DataType::Utf8
            },
            false,
        ),
        Field::new("missing", DataType::Int32, true),
    ]))
}

fn timestamp_values(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name(column)
                .unwrap()
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
        })
        .collect()
}

fn row_ids(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

#[tokio::test]
async fn int96_decodes_at_microseconds_on_every_builder_path() -> TestResult {
    let mut failures = Vec::new();
    for dictionary in [false, true] {
        let root = TestDir::new("int96-builders")?;
        let bytes = flat_file(dictionary)?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        for views in [false, true] {
            for timezone in [None, Some("UTC".into())] {
                for cached in [false, true] {
                    for ranged in [false, true] {
                        let options = DeltaScanExecutionOptions::new()
                            .with_parquet_full_file_read_threshold_bytes(
                                dictionary.then_some(bytes.len()),
                            )?;
                        let reader = reader(&root, options, metrics())?;
                        let reader = if cached {
                            reader.with_metadata_cache(Arc::new(ParquetMetadataCache::default()))
                        } else {
                            reader
                        };
                        let mut task = task("part.parquet", Some(bytes.len() as u64))?;
                        if ranged {
                            task.parquet_byte_range = Some(0..bytes.len() as u64);
                        }
                        let schema = target_schema(views, timezone.clone());
                        // Repeat with the same reader so the second cached read is a cache hit.
                        for read in 0..2 {
                            let mut stream = reader
                                .open_physical_parquet_stream(
                                    &task,
                                    &schema,
                                    PhysicalParquetStreamOptions {
                                        output_batch_size_rows: Some(2),
                                        include_original_row_index: true,
                                        ..Default::default()
                                    },
                                )
                                .await?;
                            let mut batches = Vec::new();
                            let mut indexes = Vec::new();
                            while let Some((batch, index)) =
                                stream.next_batch_with_original_row_indexes().await?
                            {
                                assert_eq!(batch.schema(), schema);
                                indexes.extend_from_slice(
                                    index.ok_or("missing original row indexes")?.values(),
                                );
                                assert_eq!(
                                    batch.column_by_name("missing").unwrap().null_count(),
                                    batch.num_rows()
                                );
                                batches.push(batch);
                            }
                            assert_eq!(indexes, (0..EPOCH_NANOS.len() as i64).collect::<Vec<_>>());
                            assert_eq!(
                                row_ids(&batches),
                                (0..EPOCH_NANOS.len() as i32).collect::<Vec<_>>()
                            );
                            for (column, expected) in [
                                ("nanos", 1_704_067_200_123_456),
                                ("micros", 1_704_067_200_123_456),
                                ("millis", 1_704_067_200_123_000),
                            ] {
                                assert_eq!(
                                    timestamp_values(&batches, column),
                                    vec![Some(expected); EPOCH_NANOS.len()],
                                    "INT64 {column}"
                                );
                            }
                            let actual = timestamp_values(&batches, "ts");
                            if actual != expected_micros() {
                                failures.push(format!("dictionary={dictionary} views={views} timezone={timezone:?} cached={cached} ranged={ranged} read={read}: {actual:?}"));
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "INT96 decoder mismatches:\n{}",
        failures.join("\n")
    );
    Ok(())
}

async fn public_table(
    mapped: bool,
    timestamp_ntz: bool,
    deleted: &[u64],
) -> TestResult<(TestDir, DeltaTable)> {
    let root = TestDir::new("int96-public")?;
    let bytes = flat_file(true)?;
    fs::write(root.path().join("part.parquet"), &bytes)?;
    fs::create_dir_all(root.path().join("_delta_log"))?;
    let mut fields = Vec::new();
    for (id, physical, kind, nullable) in [
        (1, "id", "integer", false),
        (
            2,
            "ts",
            if timestamp_ntz {
                "timestamp_ntz"
            } else {
                "timestamp"
            },
            true,
        ),
        (3, "label", "string", false),
        (7, "missing", "integer", true),
    ] {
        let name = if mapped && physical == "ts" {
            "renamed_ts"
        } else {
            physical
        };
        let metadata = if mapped {
            json!({"delta.columnMapping.id":id, "delta.columnMapping.physicalName":physical})
        } else {
            json!({})
        };
        fields.push(json!({"name":name,"type":kind,"nullable":nullable,"metadata":metadata}));
    }
    let configuration = if mapped {
        json!({"delta.columnMapping.mode":"name", "delta.columnMapping.maxColumnId":"7"})
    } else {
        json!({})
    };
    let mut features = if mapped {
        vec!["timestampNtz", "columnMapping"]
    } else {
        vec!["timestampNtz"]
    };
    if !deleted.is_empty() {
        features.push("deletionVectors");
    }
    let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":features,"writerFeatures":features}});
    let metadata = json!({"metaData":{"id":"int96-test","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":fields}).to_string(),"partitionColumns":[],"configuration":configuration}});
    let mut add = json!({"add":{"path":"part.parquet","partitionValues":{},"size":bytes.len(),"modificationTime":0,"dataChange":true,"stats":json!({"numRecords":EPOCH_NANOS.len()}).to_string()}});
    if !deleted.is_empty() {
        use delta_kernel::actions::deletion_vector_writer::{
            KernelDeletionVector, StreamingDeletionVectorWriter,
        };
        let mut dv_bytes = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut dv_bytes);
        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(deleted.iter().copied());
        let result = writer.write_deletion_vector(dv)?;
        writer.finalize()?;
        fs::write(
            root.path()
                .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"),
            dv_bytes,
        )?;
        add["add"]["deletionVector"] = json!({"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":result.offset,"sizeInBytes":result.size_in_bytes,"cardinality":result.cardinality});
    }
    fs::write(
        root.path().join("_delta_log/00000000000000000000.json"),
        format!("{protocol}\n{metadata}\n{add}\n"),
    )?;
    let table = DeltaTableBuilder::new(root.path().to_string_lossy())
        .load_table()
        .await?;
    Ok((root, table))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int96_public_streaming_preserves_values_and_filtered_projection() -> TestResult {
    for mapped in [false, true] {
        for ntz in [false, true] {
            let (_root, table) = public_table(mapped, ntz, &[]).await?;
            let column = if mapped { "renamed_ts" } else { "ts" };
            let batches = table
                .scan()
                .with_target_partitions(1)?
                .build()
                .await?
                .into_stream()
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(
                timestamp_values(&batches, column),
                expected_micros(),
                "mapped={mapped} ntz={ntz}"
            );
            for value in [253_402_214_400_000_000, -11_676_096_000_000_000, -1, 0, 1] {
                let predicate = DeltaPredicate::Compare {
                    column: column.into(),
                    op: DeltaComparison::Eq,
                    value: DeltaScalar::TimestampMicrosecond {
                        value,
                        timezone: if ntz { None } else { Some("UTC".into()) },
                    },
                };
                let batches = table
                    .scan()
                    .with_target_partitions(1)?
                    .with_projection(["id"])
                    .with_predicate(predicate)
                    .build()
                    .await?
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await?;
                let expected = expected_micros()
                    .iter()
                    .enumerate()
                    .filter_map(|(id, v)| (*v == Some(value)).then_some(id as i32))
                    .collect::<Vec<_>>();
                assert_eq!(
                    row_ids(&batches),
                    expected,
                    "mapped={mapped} ntz={ntz} value={value}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int96_datafusion_preserves_values_with_both_view_settings() -> TestResult {
    use crate::datafusion::{DeltaTableProvider, ScanOptions};
    use datafusion::{
        common::ScalarValue,
        prelude::{SessionConfig, SessionContext, col, lit},
    };
    for mapped in [false, true] {
        for ntz in [false, true] {
            let (_root, table) = public_table(mapped, ntz, &[]).await?;
            let column = if mapped { "renamed_ts" } else { "ts" };
            for use_arrow_view_types in [false, true] {
                let provider = Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        use_arrow_view_types,
                        ..Default::default()
                    },
                )?);
                let context = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(2),
                );
                let batches = context.read_table(provider.clone())?.collect().await?;
                assert_eq!(timestamp_values(&batches, column), expected_micros());
                let value = 253_402_214_400_000_000;
                let predicate = col(column).eq(lit(ScalarValue::TimestampMicrosecond(
                    Some(value),
                    if ntz { None } else { Some("UTC".into()) },
                )));
                let batches = context
                    .read_table(provider)?
                    .filter(predicate)?
                    .select_columns(&["id"])?
                    .collect()
                    .await?;
                assert_eq!(row_ids(&batches), vec![0]);
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int96_deletion_vectors_use_original_indexes_after_filtering() -> TestResult {
    let deleted = [0, 3, 12, 16];
    for ntz in [false, true] {
        let (_root, table) = public_table(true, ntz, &deleted).await?;
        let batches = table
            .scan()
            .with_target_partitions(1)?
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await?;
        let expected = expected_micros()
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| (!deleted.contains(&(i as u64))).then_some(v))
            .collect::<Vec<_>>();
        assert_eq!(timestamp_values(&batches, "renamed_ts"), expected);
        let predicate = DeltaPredicate::Compare {
            column: "renamed_ts".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::TimestampMicrosecond {
                value: 0,
                timezone: if ntz { None } else { Some("UTC".into()) },
            },
        };
        let batches = table
            .scan()
            .with_target_partitions(1)?
            .with_projection(["id"])
            .with_predicate(predicate)
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await?;
        let expected_ids = expected_micros()
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                (v.is_some_and(|v| v > 0) && !deleted.contains(&(i as u64))).then_some(i as i32)
            })
            .collect::<Vec<_>>();
        assert_eq!(row_ids(&batches), expected_ids);

        #[cfg(feature = "datafusion")]
        for use_arrow_view_types in [false, true] {
            use crate::datafusion::{DeltaTableProvider, ScanOptions};
            use datafusion::{
                common::ScalarValue,
                prelude::{SessionConfig, SessionContext, col, lit},
            };
            let provider = Arc::new(DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    use_arrow_view_types,
                    ..Default::default()
                },
            )?);
            let context = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_target_partitions(1)
                    .with_batch_size(2),
            );
            let predicate = col("renamed_ts").gt(lit(ScalarValue::TimestampMicrosecond(
                Some(0),
                if ntz { None } else { Some("UTC".into()) },
            )));
            let batches = context
                .read_table(provider)?
                .filter(predicate)?
                .select_columns(&["id"])?
                .collect()
                .await?;
            assert_eq!(row_ids(&batches), expected_ids);
        }
    }
    Ok(())
}

#[tokio::test]
async fn int96_nested_struct_list_and_map_preserve_values_and_nulls() -> TestResult {
    let parquet_schema = Arc::new(parse_message_type(
        "message m {
        OPTIONAL group s {
            OPTIONAL INT96 ts = 11;
            OPTIONAL INT64 nanos (TIMESTAMP(NANOS,false)) = 12;
        }
        OPTIONAL group items (LIST) {
            REPEATED group list { OPTIONAL INT96 element = 21; }
        }
        OPTIONAL group mapping (MAP) {
            REPEATED group key_value {
                REQUIRED BINARY key (STRING);
                OPTIONAL INT96 value = 31;
            }
        }
        REQUIRED INT96 tail = 40;
    }",
    )?);
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(
        &mut bytes,
        parquet_schema,
        Arc::new(WriterProperties::default()),
    )?;
    let mut group = writer.next_row_group()?;
    let timestamps = EPOCH_NANOS[..3]
        .iter()
        .map(|n| int96(n.unwrap()))
        .collect::<Vec<_>>();
    write_column::<Int96Type>(&mut group, &timestamps, Some(&[2, 0, 1, 2, 2]), None)?;
    write_column::<Int64Type>(&mut group, &[123_456_789; 3], Some(&[2, 0, 1, 2, 2]), None)?;
    write_column::<Int96Type>(
        &mut group,
        &[timestamps[0], timestamps[1], timestamps[2], timestamps[0]],
        Some(&[3, 2, 3, 0, 1, 3, 3]),
        Some(&[0, 1, 1, 0, 0, 0, 0]),
    )?;
    write_column::<ByteArrayType>(
        &mut group,
        &["a", "b", "c", "d"].map(ByteArray::from),
        Some(&[2, 2, 0, 1, 2, 2]),
        Some(&[0, 1, 0, 0, 0, 0]),
    )?;
    write_column::<Int96Type>(
        &mut group,
        &timestamps,
        Some(&[3, 2, 0, 1, 3, 3]),
        Some(&[0, 1, 0, 0, 0, 0]),
    )?;
    write_column::<Int96Type>(&mut group, &[timestamps[0]; 5], None, None)?;
    group.close()?;
    writer.close()?;
    let root = TestDir::new("int96-nested")?;
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
    for views in [false, true] {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("ts", micro.clone(), true),
                        Field::new("nanos", micro.clone(), true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("element", micro.clone(), true))),
                true,
            ),
            Field::new(
                "mapping",
                DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(
                            vec![
                                Field::new(
                                    "key",
                                    if views {
                                        DataType::Utf8View
                                    } else {
                                        DataType::Utf8
                                    },
                                    false,
                                ),
                                Field::new("value", micro.clone(), true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new("tail", micro.clone(), false),
        ]));
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(bytes.len() as u64))?;
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                &schema,
                PhysicalParquetStreamOptions {
                    output_batch_size_rows: Some(2),
                    ..Default::default()
                },
            )
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            batches.push(batch);
        }
        let batch = arrow::compute::concat_batches(&schema, &batches)?;
        let expected = expected_micros();
        let structure = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            structure.nulls().unwrap().iter().collect::<Vec<_>>(),
            [true, false, true, true, true]
        );
        assert_eq!(
            structure
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            [expected[0], None, None, expected[1], expected[2]]
        );
        assert_eq!(
            structure
                .column(1)
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(123_456), None, None, Some(123_456), Some(123_456)]
        );
        let list = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let lists = (0..5)
            .map(|i| {
                (!list.is_null(i)).then(|| {
                    list.value(i)
                        .as_primitive::<TimestampMicrosecondType>()
                        .iter()
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            lists,
            [
                Some(vec![expected[0], None, expected[1]]),
                None,
                Some(vec![]),
                Some(vec![expected[2]]),
                Some(vec![expected[0]])
            ]
        );
        let map = batch.column(2).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), [0, 2, 2, 2, 3, 4]);
        assert!(map.is_null(1));
        assert!(map.is_valid(2));
        assert_eq!(
            map.values()
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            [expected[0], None, expected[1], expected[2]]
        );
        assert_eq!(timestamp_values(&[batch], "tail"), vec![expected[0]; 5]);
    }
    Ok(())
}

#[test]
fn int96_decode_schema_preserves_embedded_hints_and_field_identity() -> TestResult {
    use super::{ORIGINAL_ROW_INDEX_COLUMN, arrow_reader_options, metadata_with_decode_schema};
    use parquet::arrow::{
        PARQUET_FIELD_ID_META_KEY, add_encoded_arrow_schema_to_metadata,
        arrow_reader::ArrowReaderMetadata,
    };
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let parquet_schema = Arc::new(parse_message_type(&format!(
        "message m {{
        OPTIONAL group items (LIST) = 1 {{ REPEATED group list {{ OPTIONAL INT96 element = 2; }} }}
        REQUIRED INT64 nanos (TIMESTAMP(NANOS,false)) = 3;
        OPTIONAL BINARY label (STRING) = 4;
        REQUIRED INT32 {ORIGINAL_ROW_INDEX_COLUMN};
    }}"
    ))?);
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for timezone in [None, Some("UTC".into()), Some("Asia/Tokyo".into())] {
            for kind in 0..5 {
                let schema = |unit| {
                    let element = Arc::new(
                        Field::new("element", DataType::Timestamp(unit, timezone.clone()), true)
                            .with_metadata(
                                [
                                    (PARQUET_FIELD_ID_META_KEY.into(), "2".into()),
                                    ("leaf-key".into(), "leaf-value".into()),
                                ]
                                .into(),
                            ),
                    );
                    let list = match kind {
                        0 => DataType::List(element),
                        1 => DataType::LargeList(element),
                        2 => DataType::FixedSizeList(element, 2),
                        3 => DataType::ListView(element),
                        4 => DataType::LargeListView(element),
                        _ => unreachable!(),
                    };
                    Schema::new_with_metadata(
                        vec![
                            Field::new("items", list, true).with_metadata(
                                [
                                    (PARQUET_FIELD_ID_META_KEY.into(), "1".into()),
                                    ("root-key".into(), "root-value".into()),
                                ]
                                .into(),
                            ),
                            Field::new(
                                "nanos",
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                                false,
                            )
                            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "3".into())].into()),
                            Field::new(
                                "label",
                                DataType::Dictionary(
                                    Box::new(DataType::Int16),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
                            )
                            .with_metadata([(PARQUET_FIELD_ID_META_KEY.into(), "4".into())].into()),
                            Field::new(ORIGINAL_ROW_INDEX_COLUMN, DataType::Int32, false),
                        ],
                        [("schema-key".into(), "schema-value".into())].into(),
                    )
                };
                let mut properties = WriterProperties::default();
                add_encoded_arrow_schema_to_metadata(&schema(unit), &mut properties);
                let mut bytes = Vec::new();
                SerializedFileWriter::new(
                    &mut bytes,
                    Arc::clone(&parquet_schema),
                    Arc::new(properties),
                )?
                .close()?;
                let file = SerializedFileReader::new(bytes::Bytes::from(bytes))?;
                let options = arrow_reader_options(false, false)?;
                let metadata = ArrowReaderMetadata::try_new(
                    Arc::new(file.metadata().clone()),
                    options.clone(),
                )?;
                let converted = metadata_with_decode_schema(metadata, options, false)?;
                assert_eq!(
                    converted.schema().as_ref(),
                    &schema(TimeUnit::Microsecond),
                    "unit={unit:?}, timezone={timezone:?}, list kind={kind}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn int96_disjoint_ranges_preserve_values_and_original_indexes() -> TestResult {
    let root = TestDir::new("int96-ranges")?;
    let bytes = flat_file(true)?;
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?
        .with_metadata_cache(Arc::new(ParquetMetadataCache::default()));
    let schema = target_schema(true, Some("UTC".into()));
    let mut batches = Vec::new();
    let mut indexes = Vec::new();
    let size = bytes.len() as u64;
    let mut nonempty_ranges = 0;
    // Many small ranges exercise empty tasks and boundaries inside row groups.
    for start in (0..size).step_by(137) {
        let mut task = task("part.parquet", Some(size))?;
        task.parquet_byte_range = Some(start..(start + 137).min(size));
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                &schema,
                PhysicalParquetStreamOptions {
                    include_original_row_index: true,
                    output_batch_size_rows: Some(2),
                    ..Default::default()
                },
            )
            .await?;
        let before = indexes.len();
        while let Some((batch, original)) = stream.next_batch_with_original_row_indexes().await? {
            indexes.extend_from_slice(original.ok_or("missing original row indexes")?.values());
            batches.push(batch);
        }
        nonempty_ranges += usize::from(indexes.len() > before);
    }
    assert!(nonempty_ranges > 1);
    assert_eq!(indexes, (0..EPOCH_NANOS.len() as i64).collect::<Vec<_>>());
    assert_eq!(timestamp_values(&batches, "ts"), expected_micros());
    Ok(())
}
