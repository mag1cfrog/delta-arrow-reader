//! Small generated fixtures exercise the public scan path without benchmark data.

use std::{fs, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        TimestampMicrosecondArray,
    },
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
};
use serde_json::json;

use super::support::RealParquetDeltaTable;
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions,
    DeltaScanMetricsSnapshot, DeltaTableBuilder, TryStreamExt,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const ROWS: usize = 65_539;
const MATCHES: &[i64] = &[0, 1, 8191, 8192, 16383, 16384, 32767, 32768, 32769, 65536];

fn fixture_properties() -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_max_row_group_row_count(Some(32_769))
        .set_data_page_row_count_limit(16_384)
        .set_data_page_size_limit(1024 * 1024)
        .set_write_batch_size(1_024)
}

fn create_table(
    properties: WriterPropertiesBuilder,
    repeated_value: Option<i64>,
) -> TestResult<RealParquetDeltaTable> {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut columns = vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef];
    let mut random = 19_u64;
    for column in 0..8 {
        fields.push(Field::new(
            format!("payload_{column}"),
            DataType::Int64,
            true,
        ));
        let values = (0..ROWS).map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            (column != 7 && random & 1 == 0).then_some(repeated_value.unwrap_or(random as i64))
        });
        columns.push(Arc::new(Int64Array::from_iter(values)) as ArrayRef);
    }
    fields.push(Field::new("is_match", DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|id| i64::from(MATCHES.contains(&id))),
    )));
    // Required INT32 and fixed-length decimal fields have nullable Delta targets below.
    let typed: Vec<(&str, ArrayRef)> = vec![
        (
            "narrow",
            Arc::new(Int32Array::from_iter_values(0..ROWS as i32)),
        ),
        (
            "date",
            Arc::new(Date32Array::from_iter_values(0..ROWS as i32)),
        ),
        (
            "timestamp",
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(0..ROWS as i64).with_timezone("UTC"),
            ),
        ),
        (
            "decimal32",
            Arc::new(
                Decimal128Array::from_iter_values(0..ROWS as i128)
                    .with_precision_and_scale(9, 2)?,
            ),
        ),
        (
            "decimal64",
            Arc::new(
                Decimal128Array::from_iter_values(0..ROWS as i128)
                    .with_precision_and_scale(18, 2)?,
            ),
        ),
        (
            "decimal9bytes",
            Arc::new(
                Decimal128Array::from_iter((0..ROWS).map(|i| {
                    (i % 3 != 0).then_some((i as i128 - ROWS as i128 / 2) * 1_000_000_000_000)
                }))
                .with_precision_and_scale(20, 4)?,
            ),
        ),
        (
            "decimal16bytes",
            Arc::new(
                Decimal128Array::from_iter_values(
                    (0..ROWS).map(|i| (i as i128 - ROWS as i128 / 2) * 10_i128.pow(30)),
                )
                .with_precision_and_scale(38, 10)?,
            ),
        ),
        (
            "float",
            Arc::new(Float32Array::from_iter_values(
                (0..ROWS).map(|i| i as f32 / 2.0),
            )),
        ),
        (
            "double",
            Arc::new(Float64Array::from_iter_values(
                (0..ROWS).map(|i| i as f64 / 2.0),
            )),
        ),
    ];
    for (name, column) in typed {
        fields.push(Field::new(
            name,
            column.data_type().clone(),
            column.null_count() > 0,
        ));
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties.build()))?;
    writer.write(&batch)?;
    let bytes = writer.into_inner()?;
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            let dtype = match f.data_type() {
                DataType::Int32 | DataType::Int64 => "long".to_owned(),
                DataType::Date32 => "date".to_owned(),
                DataType::Timestamp(_, _) => "timestamp".to_owned(),
                DataType::Float32 => "float".to_owned(),
                DataType::Float64 => "double".to_owned(),
                // Widen precision and scale while the file still stores nine-byte values.
                DataType::Decimal128(_, _) if f.name() == "decimal9bytes" => "decimal(24,6)".to_owned(),
                DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
                other => return Err(format!("unexpected fixture type: {other:?}").into()),
            };
            Ok(json!({
                "name": f.name(), "type": dtype,
                "nullable": f.is_nullable() || matches!(f.name().as_str(), "narrow" | "decimal16bytes"),
                "metadata": {}
            }))
        })
        .collect::<TestResult<_>>()?;
    let metadata = json!({"metaData": {"id":"intra-page", "format":{"provider":"parquet","options":{}},
        "schemaString":json!({"type":"struct","fields":fields}).to_string(),"partitionColumns":[],"configuration":{}}});
    RealParquetDeltaTable::new_with_raw_parquet(
        "intra-page",
        &bytes,
        ROWS,
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &metadata,
    )
}

fn id_filter(matches: &[i64]) -> DeltaPredicate {
    DeltaPredicate::Or(
        matches
            .iter()
            .map(|id| DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Int64(*id),
            })
            .collect(),
    )
}

async fn scan(
    root: &RealParquetDeltaTable,
    options: DeltaScanExecutionOptions,
    predicate: DeltaPredicate,
) -> TestResult<(RecordBatch, DeltaScanMetricsSnapshot)> {
    let table = DeltaTableBuilder::new(root.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let scan = table
        .scan()
        .with_target_partitions(1)?
        .with_predicate(predicate)
        .with_execution_options(options)
        .build()
        .await?;
    let stream = scan.into_stream();
    let metrics = stream.metrics();
    let batches = stream.try_collect::<Vec<_>>().await?;
    let schema = table.schema();
    Ok((concat_batches(&schema, &batches)?, metrics.snapshot()))
}

fn ordinary_options() -> DeltaScanExecutionOptions {
    DeltaScanExecutionOptions::new().with_experimental_intra_page_reads(false)
}

fn add_dv(root: &RealParquetDeltaTable) -> TestResult {
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };
    let mut bytes = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
    let mut dv = KernelDeletionVector::new();
    dv.add_deleted_row_indexes([0, 32_769]);
    let result = writer.write_deletion_vector(dv)?;
    writer.finalize()?;
    fs::write(
        root.path()
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"),
        bytes,
    )?;
    let log = root.path().join("_delta_log/00000000000000000000.json");
    let mut actions = fs::read_to_string(&log)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    actions[0] = json!({"protocol": {"minReaderVersion":3,"minWriterVersion":7,
        "readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}});
    actions[2]["add"]["deletionVector"] = json!({
        "storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA",
        "offset":result.offset,"sizeInBytes":result.size_in_bytes,"cardinality":result.cardinality
    });
    fs::write(
        log,
        actions.iter().map(|a| format!("{a}\n")).collect::<String>(),
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the retained long-value DV fixture and a calibrated S3 endpoint"]
async fn intra_page_delta_length_streaming_matches_independent_dv_oracle() -> TestResult {
    use arrow::{
        compute::{sort_to_indices, take_record_batch},
        ipc::reader::StreamReader,
    };
    use delta_arrow_reader::DeltaStorageOptions;

    let uri = std::env::var("DELTA_LENGTH_TABLE_URI")?;
    let reference = std::env::var("DELTA_LENGTH_REFERENCE")?;
    let selection = std::env::var("DELTA_LENGTH_SELECTION")?;
    assert!(matches!(selection.as_str(), "grouped" | "scattered"));
    let reference = StreamReader::try_new(fs::File::open(reference)?, None)?;
    let schema = reference.schema();
    let expected = concat_batches(&schema, &reference.collect::<Result<Vec<_>, _>>()?)?;
    assert!(expected.num_rows() > 0);
    let order = sort_to_indices(expected.column(0), None, None)?;
    let expected = take_record_batch(&expected, &order)?;
    let projection: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut storage = DeltaStorageOptions::new();
    for (key, variable) in [
        ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
        ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
        ("aws_session_token", "AWS_SESSION_TOKEN"),
        ("aws_region", "AWS_REGION"),
        ("aws_endpoint", "AWS_ENDPOINT_URL"),
        ("aws_allow_http", "AWS_ALLOW_HTTP"),
    ] {
        if let Ok(value) = std::env::var(variable) {
            storage.insert(key.into(), value);
        }
    }
    let mut bytes = Vec::new();
    for enabled in [false, true] {
        let options = DeltaScanExecutionOptions::new().with_experimental_intra_page_reads(enabled);
        let table = DeltaTableBuilder::new(&uri)
            .with_storage_options(storage.clone())
            .with_execution_options(options)
            .load_table()
            .await?;
        assert_eq!(table.version(), 0);
        let scan = table
            .scan()
            .with_projection(projection.clone())
            .with_target_partitions(8)?
            .with_predicate(DeltaPredicate::Compare {
                column: selection.clone(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Int32(1),
            })
            .with_execution_options(options)
            .build()
            .await?;
        let stream = scan.into_stream();
        let metrics = stream.metrics();
        let batches = stream.try_collect::<Vec<_>>().await?;
        for batch in &batches {
            assert_eq!(batch.schema().fields(), schema.fields());
        }
        let actual = concat_batches(&schema, &batches)?;
        let order = sort_to_indices(actual.column(0), None, None)?;
        let actual = take_record_batch(&actual, &order)?;
        assert_eq!(actual, expected, "partial reads enabled: {enabled}");
        bytes.push(
            metrics
                .snapshot()
                .parquet_data_file_bytes_received
                .ok_or("missing Parquet byte count")?,
        );
    }
    // A matching result alone could hide a missing profile or silent fallback.
    assert!(
        bytes[1] <= bytes[0] * 3 / 4,
        "no variable-length partial I/O: {bytes:?}"
    );
    eprintln!("Streaming {selection}: ordinary/partial Parquet bytes {bytes:?}");
    Ok(())
}

#[tokio::test]
async fn intra_page_tracking_column_name_preserves_user_data() -> TestResult {
    // A user column can have the same name as the internal virtual row number,
    // including a nullable field added after the Parquet file was written.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "__delta_arrow_reader_original_row_index",
            DataType::Int64,
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(Int64Array::from_iter_values(1..ROWS as i64 + 1)),
        ],
    )?;
    let fields: Vec<_> = schema.fields().iter().map(|field| {
        json!({"name":field.name(),"type":"long","nullable":field.is_nullable(),"metadata":{}})
    }).collect();
    for present_in_file in [false, true] {
        let physical = if present_in_file {
            batch.clone()
        } else {
            batch.project(&[0])?
        };
        let mut writer = ArrowWriter::try_new(
            Vec::new(),
            physical.schema(),
            Some(fixture_properties().build()),
        )?;
        writer.write(&physical)?;
        let root = RealParquetDeltaTable::new_with_raw_parquet(
            "intra-page-column-name",
            &writer.into_inner()?,
            ROWS,
            &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            &json!({"metaData":{"id":"intra-page-column-name",
                "format":{"provider":"parquet","options":{}},
                "schemaString":json!({"type":"struct","fields":fields}).to_string(),
                "partitionColumns":[],"configuration":{}}}),
        )?;
        for dv in [false, true] {
            if dv {
                add_dv(&root)?;
            }
            let ids: Vec<_> = MATCHES
                .iter()
                .copied()
                .filter(|id| !dv || ![0, 32_769].contains(id))
                .collect();
            let expected = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from_iter(
                        ids.iter().map(|id| present_in_file.then_some(id + 1)),
                    )),
                ],
            )?;
            for enabled in [false, true] {
                let (actual, _) = scan(
                    &root,
                    DeltaScanExecutionOptions::new().with_experimental_intra_page_reads(enabled),
                    id_filter(MATCHES),
                )
                .await?;
                assert_eq!(
                    actual, expected,
                    "present={present_in_file}, DV={dv}, enabled={enabled}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn intra_page_public_scan_without_transport_evidence_matches_baseline() -> TestResult {
    assert!(DeltaScanExecutionOptions::default().experimental_intra_page_reads());
    for codec in [
        Compression::UNCOMPRESSED,
        Compression::ZSTD(Default::default()),
    ] {
        let root = create_table(fixture_properties().set_compression(codec), None)?;
        for dv in [false, true] {
            if dv {
                add_dv(&root)?;
            }
            let (expected, baseline) = scan(&root, ordinary_options(), id_filter(MATCHES)).await?;
            let (actual, experimental) = scan(
                &root,
                DeltaScanExecutionOptions::default(),
                id_filter(MATCHES),
            )
            .await?;
            assert_eq!(actual, expected);
            assert_eq!(actual.num_rows(), MATCHES.len() - if dv { 2 } else { 0 });
            let narrow = actual.column_by_name("narrow").unwrap();
            assert_eq!(narrow.data_type(), &DataType::Int64);
            assert!(actual.schema().field_with_name("narrow")?.is_nullable());
            assert_eq!(narrow.to_data(), actual.column(0).to_data());
            let ids = actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let decimal9 = actual.column_by_name("decimal9bytes").unwrap();
            let decimal16 = actual.column_by_name("decimal16bytes").unwrap();
            assert_eq!(decimal9.data_type(), &DataType::Decimal128(24, 6));
            assert_eq!(decimal16.data_type(), &DataType::Decimal128(38, 10));
            assert!(
                actual
                    .schema()
                    .field_with_name("decimal16bytes")?
                    .is_nullable()
            );
            let expected9 = Decimal128Array::from_iter(ids.values().iter().map(|id| {
                (id % 3 != 0).then_some((*id as i128 - ROWS as i128 / 2) * 100_000_000_000_000)
            }))
            .with_precision_and_scale(24, 6)?;
            let expected16 = Decimal128Array::from_iter_values(
                ids.values()
                    .iter()
                    .map(|id| (*id as i128 - ROWS as i128 / 2) * 10_i128.pow(30)),
            )
            .with_precision_and_scale(38, 10)?;
            assert_eq!(
                decimal9.as_any().downcast_ref::<Decimal128Array>().unwrap(),
                &expected9
            );
            assert_eq!(
                decimal16
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap(),
                &expected16
            );
            // Local reads supply no remote transport evidence. The default must
            // retain ordinary I/O until the cost model has usable observations.
            assert!(
                experimental.parquet_data_file_bytes_received
                    == baseline.parquet_data_file_bytes_received,
                "{codec:?}, DV={dv}: experimental {experimental:?}, baseline {baseline:?}"
            );
            let dense_predicate = DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::GtEq,
                value: DeltaScalar::Int64(0),
            };
            let (dense, _) = scan(
                &root,
                DeltaScanExecutionOptions::default(),
                dense_predicate.clone(),
            )
            .await?;
            let (expected, _) = scan(&root, ordinary_options(), dense_predicate).await?;
            assert_eq!(dense, expected);
            assert_eq!(dense.num_rows(), ROWS - if dv { 2 } else { 0 });
            let (empty, _) = scan(
                &root,
                DeltaScanExecutionOptions::default(),
                id_filter(&[-1]),
            )
            .await?;
            assert_eq!(empty.num_rows(), 0);
            // A nullable payload used by both the predicate and projection must
            // retain complete bytes for Parquet's predicate cache.
            let compound = DeltaPredicate::And(vec![
                id_filter(MATCHES),
                DeltaPredicate::IsNotNull {
                    column: "payload_0".into(),
                },
            ]);
            let (expected, _) = scan(&root, ordinary_options(), compound.clone()).await?;
            let (actual, _) = scan(&root, DeltaScanExecutionOptions::default(), compound).await?;
            assert_eq!(actual, expected);
            assert!(actual.num_rows() > 0 && actual.num_rows() < MATCHES.len());
        }
    }
    Ok(())
}

#[tokio::test]
async fn intra_page_does_not_add_fixed_binary_to_delta_binary_conversion() -> TestResult {
    use arrow::array::FixedSizeBinaryArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::FixedSizeBinary(9), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..16_384)),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                (0..16_384).map(|_| [7_u8; 9]),
            )?),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(fixture_properties().build()))?;
    writer.write(&batch)?;
    let metadata = json!({"metaData": {"id":"fixed-binary", "format":{"provider":"parquet","options":{}},
        "schemaString":json!({"type":"struct","fields":[
            {"name":"id","type":"long","nullable":false,"metadata":{}},
            {"name":"value","type":"binary","nullable":false,"metadata":{}}
        ]}).to_string(),"partitionColumns":[],"configuration":{}}});
    let root = RealParquetDeltaTable::new_with_raw_parquet(
        "fixed-binary",
        &writer.into_inner()?,
        batch.num_rows(),
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &metadata,
    )?;
    let mut baseline = None;
    for options in [ordinary_options(), DeltaScanExecutionOptions::default()] {
        let error = scan(&root, options, id_filter(&[1, 8_191]))
            .await
            .err()
            .ok_or("unsupported fixed binary conversion succeeded")?
            .to_string();
        assert!(error.contains("parquet_schema_match_failed"), "{error}");
        if let Some(expected) = &baseline {
            assert_eq!(&error, expected);
        } else {
            baseline = Some(error);
        }
    }
    Ok(())
}

#[tokio::test]
async fn intra_page_unsupported_layouts_match_baseline() -> TestResult {
    for (properties, repeated_value) in [
        (
            fixture_properties().set_compression(Compression::SNAPPY),
            None,
        ),
        (
            fixture_properties().set_compression(Compression::ZSTD(Default::default())),
            Some(7),
        ),
        (fixture_properties().set_dictionary_enabled(true), Some(7)),
        (fixture_properties().set_offset_index_disabled(true), None),
        (
            fixture_properties().set_writer_version(WriterVersion::PARQUET_2_0),
            None,
        ),
    ] {
        let root = create_table(properties, repeated_value)?;
        let (expected, _) = scan(&root, ordinary_options(), id_filter(MATCHES)).await?;
        let (actual, _) = scan(
            &root,
            DeltaScanExecutionOptions::default(),
            id_filter(MATCHES),
        )
        .await?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[tokio::test]
async fn intra_page_explicit_range_policies_keep_their_reads() -> TestResult {
    use delta_arrow_reader::diagnostics::parquet_range_planning::Policy as ParquetRangeReadPolicy;
    let root = create_table(fixture_properties(), None)?;
    for policy in [
        ParquetRangeReadPolicy::ExactRanges,
        ParquetRangeReadPolicy::MergeRangesWithinOneMegabyte,
        ParquetRangeReadPolicy::StoreImplementation,
    ] {
        let options = ordinary_options().with_parquet_range_read_policy(policy);
        let (expected, baseline) = scan(&root, options, id_filter(MATCHES)).await?;
        let (actual, experimental) = scan(
            &root,
            options.with_experimental_intra_page_reads(true),
            id_filter(MATCHES),
        )
        .await?;
        assert_eq!(actual, expected);
        assert_eq!(
            experimental.parquet_data_file_bytes_received,
            baseline.parquet_data_file_bytes_received
        );
        assert_eq!(
            experimental.parquet_data_file_range_get_operations,
            baseline.parquet_data_file_range_get_operations
        );
    }
    Ok(())
}

#[tokio::test]
async fn intra_page_projection_limits_and_selection_reset_preserve_rows() -> TestResult {
    let root = create_table(fixture_properties(), None)?;
    add_dv(&root)?;
    let table = DeltaTableBuilder::new(root.path().to_string_lossy())
        .load_table()
        .await?;
    // The first group exceeds the tracking limit, while the next is sparse.
    let overflow = DeltaPredicate::Or(vec![
        DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Lt,
            value: DeltaScalar::Int64(8_193),
        },
        id_filter(MATCHES),
    ]);
    let (expected, _) = scan(&root, ordinary_options(), overflow.clone()).await?;
    let (actual, _) = scan(&root, DeltaScanExecutionOptions::default(), overflow).await?;
    assert_eq!(actual, expected);

    // Skip the first row group, hide the predicate column and apply the limit
    // after DV filtering. Empty projections must still retain their row count.
    let matches = [32_769, 65_536, 65_537, 65_538];
    let (expected, _) = scan(&root, ordinary_options(), id_filter(&matches)).await?;
    assert_eq!(expected.num_rows(), 3);
    for indices in [&[][..], &[8][..], &[3, 1][..]] {
        for limit in [0, 1, 2, usize::MAX] {
            let projection = indices
                .iter()
                .map(|i| table.schema().field(*i).name().clone());
            let scan = table
                .scan()
                .with_target_partitions(1)?
                .with_projection(projection)
                .with_predicate(id_filter(&matches))
                .with_limit(limit)
                .with_execution_options(DeltaScanExecutionOptions::default())
                .build()
                .await?;
            let stream = scan.into_stream();
            let schema = stream.schema();
            let actual = concat_batches(&schema, &stream.try_collect::<Vec<_>>().await?)?;
            assert_eq!(actual, expected.project(indices)?.slice(0, limit.min(3)));
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn intra_page_datafusion_repartitioned_dv_scan_matches_baseline() -> TestResult {
    use datafusion::{
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use delta_arrow_reader::datafusion::{
        DeltaTableProvider, IntraFileRepartitioning, ScanOptions, collect_scan_metrics,
    };
    let root = create_table(
        fixture_properties().set_compression(Compression::ZSTD(Default::default())),
        None,
    )?;
    for dv in [false, true] {
        if dv {
            add_dv(&root)?;
        }
        let table = DeltaTableBuilder::new(root.path().to_string_lossy())
            .load_table()
            .await?;
        let mut baseline = None;
        for enabled in [false, true] {
            let context = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(127)
                    .with_target_partitions(4)
                    .with_repartition_file_min_size(1),
            );
            let provider = DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_experimental_intra_page_reads(enabled),
                    target_partitions: Some(4),
                    intra_file_repartitioning: IntraFileRepartitioning::Always,
                    ..Default::default()
                },
            )?;
            context.register_table("bench", Arc::new(provider))?;
            // A pushed comparison selects the same scattered rows. Multi-value
            // IN on a data column currently remains a DataFusion residual filter.
            let plan = context
                .sql("SELECT * FROM bench WHERE is_match = 1 ORDER BY id")
                .await?
                .create_physical_plan()
                .await?;
            let metrics = collect_scan_metrics(plan.as_ref());
            let display = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string();
            let batches = collect(plan, context.task_ctx()).await?;
            let actual = concat_batches(&table.schema(), &batches)?;
            let snapshot = metrics[0].snapshot().reader_metrics;
            assert!(snapshot.file_tasks_started > 1);
            assert_eq!(actual.num_rows(), MATCHES.len() - if dv { 2 } else { 0 });
            if let Some((expected, bytes)) = &baseline {
                assert_eq!(&actual, expected);
                assert!(
                    snapshot.parquet_data_file_bytes_received == *bytes,
                    "DV={dv}: baseline bytes={bytes:?}, experimental={snapshot:?}; plan={display}"
                );
            } else {
                baseline = Some((actual, snapshot.parquet_data_file_bytes_received));
            }
        }
    }
    Ok(())
}

#[allow(deprecated)]
#[tokio::test]
async fn intra_page_corrupt_or_truncated_input_fails_the_scan() -> TestResult {
    use parquet::{file::metadata::ParquetMetaDataReader, thrift::TSerializable};
    let root = create_table(fixture_properties(), None)?;
    let path = root.path().join(root.data_file_path());
    let original = Bytes::from(fs::read(&path)?);
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&original)?;
    let offset = metadata.row_group(0).column(1).data_page_offset() as usize;
    let mut cursor = std::io::Cursor::new(&original[offset..]);
    parquet::format::PageHeader::read_from_in_protocol(
        &mut thrift::protocol::TCompactInputProtocol::new(&mut cursor),
    )?;
    let body = offset + cursor.position() as usize;
    let mut corrupt = original.to_vec();
    corrupt[body..body + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    fs::write(&path, corrupt)?;
    assert!(
        scan(
            &root,
            DeltaScanExecutionOptions::default(),
            id_filter(MATCHES)
        )
        .await
        .is_err()
    );
    fs::write(&path, &original[..original.len() - 10])?;
    assert!(
        scan(
            &root,
            DeltaScanExecutionOptions::default(),
            id_filter(MATCHES)
        )
        .await
        .is_err()
    );
    Ok(())
}
