//! Small generated fixtures exercise the public scan path without benchmark data.

use std::{fs, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array},
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
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties.build()))?;
    writer.write(&batch)?;
    let bytes = writer.into_inner()?;
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(), "type": "long", "nullable": f.is_nullable(), "metadata": {}
            })
        })
        .collect();
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

fn intra_page_options() -> DeltaScanExecutionOptions {
    DeltaScanExecutionOptions::new().with_experimental_intra_page_reads(true)
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
async fn intra_page_public_scan_matches_baseline_and_reads_fewer_bytes() -> TestResult {
    assert!(!DeltaScanExecutionOptions::default().experimental_intra_page_reads());
    for codec in [
        Compression::UNCOMPRESSED,
        Compression::ZSTD(Default::default()),
    ] {
        let root = create_table(fixture_properties().set_compression(codec), None)?;
        for dv in [false, true] {
            if dv {
                add_dv(&root)?;
            }
            let (expected, baseline) = scan(&root, Default::default(), id_filter(MATCHES)).await?;
            let (actual, optimized) = scan(&root, intra_page_options(), id_filter(MATCHES)).await?;
            assert_eq!(actual, expected);
            assert_eq!(actual.num_rows(), MATCHES.len() - if dv { 2 } else { 0 });
            assert!(
                optimized.parquet_data_file_bytes_received
                    < baseline.parquet_data_file_bytes_received,
                "{codec:?}, DV={dv}: optimized {optimized:?}, baseline {baseline:?}"
            );
            let dense_predicate = DeltaPredicate::Compare {
                column: "id".into(),
                op: DeltaComparison::GtEq,
                value: DeltaScalar::Int64(0),
            };
            let (dense, _) = scan(&root, intra_page_options(), dense_predicate.clone()).await?;
            let (expected, _) = scan(&root, Default::default(), dense_predicate).await?;
            assert_eq!(dense, expected);
            assert_eq!(dense.num_rows(), ROWS - if dv { 2 } else { 0 });
            let (empty, _) = scan(&root, intra_page_options(), id_filter(&[-1])).await?;
            assert_eq!(empty.num_rows(), 0);
            // A nullable payload used by both the predicate and projection must
            // retain complete bytes for Parquet's predicate cache.
            let compound = DeltaPredicate::And(vec![
                id_filter(MATCHES),
                DeltaPredicate::IsNotNull {
                    column: "payload_0".into(),
                },
            ]);
            let (expected, _) = scan(&root, Default::default(), compound.clone()).await?;
            let (actual, _) = scan(&root, intra_page_options(), compound).await?;
            assert_eq!(actual, expected);
            assert!(actual.num_rows() > 0 && actual.num_rows() < MATCHES.len());
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
        let (expected, _) = scan(&root, Default::default(), id_filter(MATCHES)).await?;
        let (actual, _) = scan(&root, intra_page_options(), id_filter(MATCHES)).await?;
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
        let options = DeltaScanExecutionOptions::new().with_parquet_range_read_policy(policy);
        let (expected, baseline) = scan(&root, options, id_filter(MATCHES)).await?;
        let (actual, optimized) = scan(
            &root,
            options.with_experimental_intra_page_reads(true),
            id_filter(MATCHES),
        )
        .await?;
        assert_eq!(actual, expected);
        assert_eq!(
            optimized.parquet_data_file_bytes_received,
            baseline.parquet_data_file_bytes_received
        );
        assert_eq!(
            optimized.parquet_data_file_range_get_operations,
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
    let (expected, _) = scan(&root, Default::default(), overflow.clone()).await?;
    let (actual, _) = scan(&root, intra_page_options(), overflow).await?;
    assert_eq!(actual, expected);

    // Skip the first row group, hide the predicate column and apply the limit
    // after DV filtering. Empty projections must still retain their row count.
    let matches = [32_769, 65_536, 65_537, 65_538];
    let (expected, _) = scan(&root, Default::default(), id_filter(&matches)).await?;
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
                .with_execution_options(intra_page_options())
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
                    snapshot.parquet_data_file_bytes_received < *bytes,
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
        scan(&root, intra_page_options(), id_filter(MATCHES))
            .await
            .is_err()
    );
    fs::write(&path, &original[..original.len() - 10])?;
    assert!(
        scan(&root, intra_page_options(), id_filter(MATCHES))
            .await
            .is_err()
    );
    Ok(())
}
