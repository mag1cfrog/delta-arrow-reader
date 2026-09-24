//! Row-count and deletion-vector contracts when no data columns are projected.

use std::error::Error;

use arrow::{array::StringArray, datatypes::DataType, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaTableBuilder,
    ParquetReaderBackend,
};
use futures_util::TryStreamExt;

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn fixtures() -> TestResult<Vec<(RealParquetDeltaTable, usize)>> {
    Ok(vec![
        (RealParquetDeltaTable::new_default("empty-no-dv")?, 3),
        (
            RealParquetDeltaTable::new_with_deletion_vector("empty-dv-none", &[])?,
            3,
        ),
        (
            RealParquetDeltaTable::new_with_deletion_vector("empty-dv-some", &[1])?,
            2,
        ),
        (
            RealParquetDeltaTable::new_with_deletion_vector("empty-dv-all", &[0, 1, 2])?,
            0,
        ),
        (
            RealParquetDeltaTable::new_with_two_files_and_deletion_vector(
                "empty-mixed-files",
                &[0],
            )?,
            3,
        ),
        (
            // With 127-row batches, the first and last batches are entirely deleted.
            RealParquetDeltaTable::new_with_rows_and_deletion_vector(
                "empty-fully-deleted-batches",
                260,
                &(0..127).chain(254..260).collect::<Vec<_>>(),
            )?,
            127,
        ),
        (
            RealParquetDeltaTable::new_with_row_groups_and_deletion_vector(
                "empty-multiple-batches",
                2,
                8_193,
                &[0, 1_023, 1_024, 8_191, 8_192, 8_193, 16_385],
            )?,
            16_379,
        ),
    ])
}

fn assert_empty_batches(batches: &[RecordBatch], expected_rows: usize) {
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected_rows
    );
}

#[tokio::test]
async fn streaming_empty_projection_preserves_live_rows() -> TestResult {
    for (fixture, expected_rows) in fixtures()? {
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        for threshold in [None, Some(usize::MAX)] {
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(threshold)?;
            let scan = table
                .scan()
                .with_projection(Vec::<String>::new())
                .with_execution_options(options)
                .with_target_partitions(2)?
                .build()
                .await?;
            assert!(scan.schema().fields().is_empty());
            let batches = scan
                .into_stream()
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| format!("{} {threshold:?}: {error}", fixture.path().display()))?;
            assert_empty_batches(&batches, expected_rows);
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_partition_only_projection_preserves_live_rows() -> TestResult {
    for deleted in [&[][..], &[1][..], &[0, 1, 2][..]] {
        let fixture = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "empty-partition-data",
            "west",
            deleted,
        )?;
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        for projection in [vec![], vec!["region"]] {
            let scan = table
                .scan()
                .with_projection(projection.clone())
                .build()
                .await?;
            let expected_schema = scan.schema();
            let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                3 - deleted.len()
            );
            for batch in batches {
                assert_eq!(batch.schema(), expected_schema);
                if !projection.is_empty() {
                    assert_eq!(batch.schema().field(0).name(), "region");
                    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
                    let regions = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("expected partition strings")?;
                    assert!(regions.iter().all(|value| value == Some("west")));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_empty_projection_filters_hidden_columns_before_limit() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
        "empty-hidden-filter",
        12,
        &[0, 3, 4, 8, 11],
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        // ids 6, 7, 8, 10, 11 survive both the predicate and the DV.
        for (minimum, available) in [(4, 5), (100, 0)] {
            for limit in [None, Some(0), Some(1), Some(3), Some(20)] {
                let mut builder = table
                    .scan()
                    .with_projection(Vec::<String>::new())
                    .with_predicate(DeltaPredicate::Compare {
                        column: "id".into(),
                        op: DeltaComparison::GtEq,
                        value: DeltaScalar::Int32(minimum),
                    })
                    .with_execution_options(
                        DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                    );
                if let Some(limit) = limit {
                    builder = builder.with_limit(limit);
                }
                let batches = builder
                    .build()
                    .await?
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await?;
                assert_empty_batches(&batches, available.min(limit.unwrap_or(usize::MAX)));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use std::sync::Arc;

    use ::datafusion::{
        datasource::TableProvider,
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use arrow::{
        array::{Int64Array, StringViewArray},
        compute::cast,
    };
    use delta_arrow_reader::datafusion::{
        DeltaTableProvider, IntraFileRepartitioning, ScanOptions, collect_scan_metrics,
    };

    use super::*;

    #[tokio::test]
    async fn count_and_empty_provider_projection_preserve_live_rows() -> TestResult {
        for (fixture, expected_rows) in fixtures()? {
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            for use_arrow_view_types in [false, true] {
                let context = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_batch_size(127)
                        .with_repartition_file_scans(false),
                );
                let provider = Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        use_arrow_view_types,
                        ..Default::default()
                    },
                )?);
                let plan = provider
                    .scan(&context.state(), Some(&vec![]), &[], None)
                    .await?;
                assert!(plan.schema().fields().is_empty());
                assert_empty_batches(&collect(plan, context.task_ctx()).await?, expected_rows);
                context.register_table("t", provider)?;
                let batches = context
                    .sql("SELECT count(*) AS n FROM t")
                    .await?
                    .collect()
                    .await?;
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                let count = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("expected Int64 count")?;
                assert_eq!(count.value(0), i64::try_from(expected_rows)?);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn partition_only_sql_preserves_live_rows_and_schema() -> TestResult {
        for deleted in [&[][..], &[1][..], &[0, 1, 2][..]] {
            let fixture = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
                "empty-partition-sql",
                "west",
                deleted,
            )?;
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            for use_arrow_view_types in [false, true] {
                let context =
                    SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
                let provider = Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        use_arrow_view_types,
                        ..Default::default()
                    },
                )?);
                let expected_schema = Arc::new(provider.schema().project(&[2])?);
                context.register_table("t", provider)?;
                let batches = context.sql("SELECT region FROM t").await?.collect().await?;
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    3 - deleted.len()
                );
                for batch in batches {
                    assert_eq!(batch.schema(), expected_schema);
                    let regions = cast(batch.column(0), &DataType::Utf8View)?;
                    let regions = regions
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or("expected partition strings")?;
                    assert!(regions.iter().all(|value| value == Some("west")));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn ranged_empty_projection_uses_file_absolute_deletion_indexes() -> TestResult {
        let fixture = RealParquetDeltaTable::new_with_row_groups_and_deletion_vector(
            "empty-ranged-dv",
            4,
            16,
            &[0, 1, 15, 16, 33, 34, 35, 63],
        )?;
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        for use_arrow_view_types in [false, true] {
            for split in [false, true] {
                let context = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_batch_size(3)
                        .with_target_partitions(8)
                        .with_repartition_file_min_size(1)
                        .with_repartition_file_scans(split),
                );
                context.register_table(
                    "t",
                    Arc::new(DeltaTableProvider::try_new(
                        table.clone(),
                        ScanOptions {
                            use_arrow_view_types,
                            target_partitions: Some(8),
                            intra_file_repartitioning: IntraFileRepartitioning::Always,
                            ..Default::default()
                        },
                    )?),
                )?;
                for (query, expected_rows, expected_deleted) in [
                    ("SELECT 1 AS marker FROM t", 56, Some(8)),
                    // Hidden id enables row-group pruning and a row filter; ids 17 and 34-36 are deleted.
                    (
                        "SELECT 1 AS marker FROM t WHERE id >= 17 AND id <= 48",
                        28,
                        Some(4),
                    ),
                    (
                        "SELECT 1 AS marker FROM t WHERE id >= 17 AND id <= 48 LIMIT 5",
                        5,
                        None,
                    ),
                ] {
                    let plan = context.sql(query).await?.create_physical_plan().await?;
                    let metrics = collect_scan_metrics(plan.as_ref());
                    assert_eq!(metrics.len(), 1);
                    let batches = collect(plan, context.task_ctx()).await?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        expected_rows,
                        "{query}, split={split}"
                    );
                    let snapshot = metrics[0].snapshot().reader_metrics;
                    if let Some(expected_deleted) = expected_deleted {
                        assert_eq!(snapshot.file_tasks_started, if split { 8 } else { 1 });
                        assert_eq!(snapshot.deletion_vector_rows_deleted, expected_deleted);
                    }
                    assert_eq!(snapshot.deletion_vector_coordinate_rejections, 0);
                    assert_eq!(snapshot.deletion_vector_failures, 0);
                }
            }
        }
        Ok(())
    }
}
