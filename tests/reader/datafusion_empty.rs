//! Empty input must remain a valid source for every DataFusion consumer.

use super::*;
use datafusion::{common::config::ConfigOptions, logical_expr::Expr};
use delta_arrow_reader::datafusion::ScanMetrics;

const BACKENDS: [ParquetReaderBackend; 2] = [
    ParquetReaderBackend::Direct,
    ParquetReaderBackend::DeltaKernel,
];

fn empty_sources() -> TestResult<[(TestTable, &'static str, Vec<Expr>); 3]> {
    let cases = [
        (TestTable::empty("empty-contract")?, "TRUE", vec![]),
        (
            TestTable::partitioned("stats-pruned-contract")?,
            "id < 0",
            vec![col("id").lt(lit(0))],
        ),
        (
            TestTable::partitioned("partition-pruned-contract")?,
            "region = 'missing'",
            vec![col("region").eq(lit("missing"))],
        ),
    ];
    // Removed data files make accidental reads fail on either backend, even
    // where the backend does not expose Parquet I/O counters.
    for (fixture, _, _) in &cases {
        let log = fs::read_to_string(fixture.0.join("_delta_log/00000000000000000000.json"))?;
        let mut actions = log
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for action in &mut actions {
            if action.get("metaData").is_some() {
                *action = metadata_with_note();
            }
        }
        fixture.write_log(&actions)?;
        for entry in fs::read_dir(&fixture.0)? {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                fs::remove_file(path)?;
            }
        }
    }
    Ok(cases)
}

fn assert_no_file_execution(metrics: &ScanMetrics) {
    let snapshot = metrics.snapshot();
    let reader = &snapshot.reader_metrics;
    assert_eq!(reader.files_planned, 0);
    assert_eq!(reader.scan_partitions_planned, 0);
    assert_eq!(reader.scan_partitions_started, 0);
    assert_eq!(reader.scan_partitions_completed, 0);
    assert_eq!(reader.file_tasks_started, 0);
    assert_eq!(reader.file_tasks_completed, 0);
    assert_eq!(reader.scheduler_rows_emitted, 0);
    assert_eq!(reader.deletion_vector_payloads_loaded, 0);
    assert_eq!(snapshot.configured_batch_size_rows, None);
    for counter in [
        reader.parquet_data_file_range_get_operations,
        reader.parquet_data_file_full_get_operations,
        reader.parquet_data_file_bytes_received,
    ] {
        assert!(counter.is_none_or(|count| count == 0), "{reader:?}");
    }
}

#[tokio::test]
async fn empty_scan_execution_preserves_schema_bounds_and_metrics() -> TestResult {
    for (fixture, condition, filters) in empty_sources()? {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let table = DeltaTableBuilder::new(fixture.uri())
                .with_warmup(warmup)
                .load_table()
                .await?;
            for backend in BACKENDS {
                for views in [false, true] {
                    let context = SessionContext::new_with_config(
                        SessionConfig::new()
                            .with_target_partitions(4)
                            .with_batch_size(7),
                    );
                    let provider = DeltaTableProvider::try_new(
                        table.clone(),
                        ScanOptions {
                            execution_options: DeltaScanExecutionOptions::new()
                                .with_parquet_backend(backend),
                            use_arrow_view_types: views,
                            ..ScanOptions::default()
                        },
                    )?;
                    for projection in [
                        None,
                        Some(vec![0]),
                        Some(vec![1]),
                        Some(vec![2]),
                        Some(vec![2, 1, 0]),
                        Some(vec![]),
                    ] {
                        let result = provider
                            .scan(&context.state(), projection.as_ref(), &filters, None)
                            .await;
                        // Kernel data filters are inexact. DataFusion must keep
                        // their columns in the provider projection for residual
                        // evaluation, even if the final SQL output omits them.
                        if backend == ParquetReaderBackend::DeltaKernel
                            && condition == "id < 0"
                            && projection
                                .as_ref()
                                .is_some_and(|indices| !indices.contains(&0))
                        {
                            let error = result.err().ok_or("missing residual column accepted")?;
                            assert!(
                                error
                                    .to_string()
                                    .contains("inexact_filter_columns_not_projected")
                            );
                            continue;
                        }
                        let plan = result?;
                        let expected_schema = match &projection {
                            Some(indices) => Arc::new(provider.schema().project(indices)?),
                            None => provider.schema(),
                        };
                        assert_eq!(plan.schema(), expected_schema, "{condition} {backend:?}");
                        assert_eq!(
                            plan.properties().output_partitioning().partition_count(),
                            1,
                            "{condition} {backend:?} {warmup:?} {projection:?}"
                        );
                        assert!(
                            displayable(plan.as_ref())
                                .indent(true)
                                .to_string()
                                .contains("snapshot_version=0, partitions=1")
                        );
                        let metrics = collect_scan_metrics(plan.as_ref());
                        assert_eq!(metrics.len(), 1);
                        let before = metrics[0].snapshot();
                        for partition in [1, usize::MAX] {
                            let error = plan
                                .execute(partition, context.task_ctx())
                                .err()
                                .ok_or("invalid partition accepted")?;
                            assert!(
                                error
                                    .to_string()
                                    .contains("scan_partition_index_out_of_range")
                            );
                        }
                        // Creating/dropping a stream and re-executing the same plan
                        // must not enter the reader scheduler or start a file task.
                        drop(plan.execute(0, context.task_ctx())?);
                        for _ in 0..2 {
                            let mut stream = plan.execute(0, context.task_ctx())?;
                            assert_eq!(stream.schema(), expected_schema);
                            assert!(stream.next().await.is_none());
                            assert!(stream.next().await.is_none());
                        }
                        assert!(collect_plan(&context, plan).await?.is_empty());
                        assert_no_file_execution(&metrics[0]);
                        assert_eq!(metrics[0].snapshot(), before);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn empty_sql(context: &SessionContext, query: &str) -> TestResult<Vec<RecordBatch>> {
    let plan = context.sql(query).await?.create_physical_plan().await?;
    let schema = plan.schema();
    let metrics = collect_scan_metrics(plan.as_ref());
    let batches = collect_plan(context, plan).await?;
    for batch in &batches {
        assert_eq!(batch.schema(), schema, "{query}");
    }
    for scan in metrics {
        assert_no_file_execution(&scan);
    }
    Ok(batches)
}

#[tokio::test]
async fn empty_scan_sql_sort_limit_distinct_and_window() -> TestResult {
    for (fixture, condition, _) in empty_sources()? {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let table = DeltaTableBuilder::new(fixture.uri())
                .with_warmup(warmup)
                .load_table()
                .await?;
            for backend in BACKENDS {
                for target in [1, 4] {
                    let context = SessionContext::new_with_config(
                        SessionConfig::new().with_target_partitions(target),
                    );
                    context.register_table(
                        "t",
                        Arc::new(DeltaTableProvider::try_new(
                            table.clone(),
                            ScanOptions {
                                execution_options: DeltaScanExecutionOptions::new()
                                    .with_parquet_backend(backend),
                                ..ScanOptions::default()
                            },
                        )?),
                    )?;
                    for query in [
                        format!("SELECT id, region FROM t WHERE {condition} ORDER BY id"),
                        format!(
                            "SELECT region FROM t WHERE {condition} ORDER BY id DESC NULLS LAST LIMIT 2"
                        ),
                        format!("SELECT id FROM t WHERE {condition} LIMIT 3 OFFSET 1"),
                        format!("SELECT id FROM t WHERE {condition} LIMIT 0"),
                        format!("SELECT DISTINCT id FROM t WHERE {condition} ORDER BY id"),
                        format!(
                            "SELECT id, COUNT(*) AS n FROM t WHERE {condition} GROUP BY id ORDER BY id"
                        ),
                        format!(
                            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM t WHERE {condition}"
                        ),
                        format!("SELECT 7 AS marker FROM t WHERE {condition} ORDER BY marker"),
                    ] {
                        let batches = empty_sql(&context, &query).await?;
                        assert_eq!(
                            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                            0,
                            "{query} {backend:?}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_scan_sql_aggregates_joins_and_union_preserve_nonempty_results() -> TestResult {
    for (fixture, condition, _) in empty_sources()? {
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        for backend in BACKENDS {
            let context =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
            context.register_table(
                "t",
                Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        execution_options: DeltaScanExecutionOptions::new()
                            .with_parquet_backend(backend),
                        ..ScanOptions::default()
                    },
                )?),
            )?;
            let batches = empty_sql(&context, &format!(
                "SELECT COUNT(*) AS n, COUNT(id) AS nonnull, SUM(id) AS total, MIN(id) AS lo, MAX(id) AS hi FROM t WHERE {condition}"
            )).await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let batch = batches
                .iter()
                .find(|batch| batch.num_rows() == 1)
                .ok_or("missing aggregate row")?;
            for index in 0..2 {
                let count = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("count type")?;
                assert!(!count.is_null(0));
                assert_eq!(count.value(0), 0);
            }
            for index in 2..5 {
                assert!(batch.column(index).is_null(0));
            }
            let batches = empty_sql(&context, &format!(
                "SELECT input.k AS id, empty.id AS matched FROM (VALUES (CAST(11 AS INT)), (CAST(22 AS INT))) AS input(k) LEFT JOIN (SELECT id FROM t WHERE {condition}) AS empty ON input.k = empty.id ORDER BY input.k"
            )).await?;
            assert_eq!(ids(&batches), [11, 22]);
            for batch in &batches {
                assert_eq!(batch.column(1).null_count(), batch.num_rows());
            }
            let batches = empty_sql(
                &context,
                &format!(
                "SELECT id FROM t WHERE {condition} UNION ALL SELECT CAST(99 AS INT) AS id ORDER BY id"
                ),
            )
            .await?;
            assert_eq!(ids(&batches), [99]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_scan_plan_rewrites_keep_one_partition() -> TestResult {
    let fixture = TestTable::empty("empty-rewrites")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let context = SessionContext::new();
    for backend in BACKENDS {
        for policy in [
            IntraFileRepartitioning::WhenBelowTarget,
            IntraFileRepartitioning::Always,
        ] {
            let provider = DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend),
                    intra_file_repartitioning: policy,
                    ..ScanOptions::default()
                },
            )?;
            let original = provider.scan(&context.state(), None, &[], None).await?;
            let mut plan = Arc::clone(&original).with_new_children(vec![])?;
            let mut config = ConfigOptions::default();
            config.optimizer.repartition_file_min_size = 0;
            for target in [1, 4, 16] {
                if let Some(rewritten) = plan.repartitioned(target, &config)? {
                    plan = rewritten;
                }
                assert_eq!(plan.properties().output_partitioning().partition_count(), 1);
                assert_eq!(plan.schema(), original.schema());
                assert!(collect_plan(&context, Arc::clone(&plan)).await?.is_empty());
                let metrics = collect_scan_metrics(plan.as_ref());
                assert_eq!(metrics.len(), 1);
                assert_no_file_execution(&metrics[0]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_scan_refresh_after_removing_all_files_preserves_snapshot() -> TestResult {
    for backend in BACKENDS {
        let fixture = TestTable::partitioned("empty-after-remove")?;
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_warmup(WarmupMode::QueryPlanning)
            .load_table()
            .await?;
        let provider = DeltaTableProvider::try_new(
            table,
            ScanOptions {
                execution_options: DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                ..ScanOptions::default()
            },
        )?;
        fixture.write_log_version(1, &[
            json!({"remove":{"path":"west.parquet","dataChange":true,"deletionTimestamp":1587968587000_i64}}),
            json!({"remove":{"path":"east.parquet","dataChange":true,"deletionTimestamp":1587968587000_i64}}),
        ])?;
        let refreshed = provider.refresh().await?;
        assert_eq!(provider.schema(), refreshed.schema());
        let context = SessionContext::new();
        context.register_table("old", Arc::new(provider))?;
        context.register_table("t", Arc::new(refreshed))?;
        assert_eq!(
            ids(&context
                .sql("SELECT id FROM old ORDER BY id")
                .await?
                .collect()
                .await?),
            [1, 2, 3, 4]
        );
        let plan = context
            .sql("SELECT id FROM t ORDER BY id")
            .await?
            .create_physical_plan()
            .await?;
        assert!(
            displayable(plan.as_ref())
                .indent(true)
                .to_string()
                .contains("snapshot_version=1, partitions=1")
        );
        let metrics = collect_scan_metrics(plan.as_ref());
        assert_eq!(metrics.len(), 1);
        assert!(collect_plan(&context, plan).await?.is_empty());
        assert_no_file_execution(&metrics[0]);
        assert_eq!(metrics[0].snapshot().reader_metrics.snapshot_version, 1);
    }
    Ok(())
}

#[tokio::test]
async fn empty_scan_runtime_filter_keeps_real_file_tasks() -> TestResult {
    let fixture = TestTable::partitioned("runtime-empty")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    for backend in BACKENDS {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        context.register_table(
            "t",
            Arc::new(DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend),
                    ..ScanOptions::default()
                },
            )?),
        )?;
        let plan = context
            .sql("SELECT id FROM t WHERE id % 2 = 10 ORDER BY id")
            .await?
            .create_physical_plan()
            .await?;
        let metrics = collect_scan_metrics(plan.as_ref());
        assert_eq!(metrics.len(), 1);
        assert!(collect_plan(&context, plan).await?.is_empty());
        let reader = metrics[0].snapshot().reader_metrics;
        assert_eq!(reader.files_planned, 2);
        assert_eq!(reader.file_tasks_started, 2);
        assert_eq!(reader.file_tasks_completed, 2);
        assert_eq!(
            ids(&context
                .sql("SELECT id FROM t ORDER BY id")
                .await?
                .collect()
                .await?),
            [1, 2, 3, 4]
        );
    }
    Ok(())
}

/// Run in release mode with --ignored --nocapture --test-threads=1. Freeze the
/// baseline binary before editing production code, and isolate each case in its
/// own process. Result validation and metrics collection are outside timing.
#[tokio::test]
#[ignore = "manual before/after empty-scan performance measurement"]
async fn empty_scan_benchmark() -> TestResult {
    use std::time::Instant;
    let case = std::env::var("EMPTY_SCAN_BENCH_CASE").unwrap_or_else(|_| "unfiltered".into());
    let samples: usize = std::env::var("EMPTY_SCAN_BENCH_SAMPLES")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
    let rows: i32 = std::env::var("EMPTY_SCAN_BENCH_ROWS")
        .unwrap_or_else(|_| "131072".into())
        .parse()?;
    if !(1..=1000).contains(&samples) || !(16..=1_000_000).contains(&rows) {
        return Err("samples must be 1..=1000 and rows 16..=1000000".into());
    }
    let fixture = TestTable::empty("empty-scan-bench")?;
    if case != "empty" {
        let west = fixture.write_parquet("west.parquet", &(1..=rows).collect::<Vec<_>>())?;
        let east =
            fixture.write_parquet("east.parquet", &(rows + 1..=rows * 2).collect::<Vec<_>>())?;
        fixture.write_log(&[
            protocol(1),
            metadata(),
            add("west.parquet", west, "west", rows as u64, 1, rows),
            add(
                "east.parquet",
                east,
                "east",
                rows as u64,
                rows + 1,
                rows * 2,
            ),
        ])?;
    }
    let (query, expected) = match case.as_str() {
        "empty" => ("SELECT id FROM t".to_owned(), vec![]),
        "pruned" => ("SELECT id FROM t WHERE id < 0".to_owned(), vec![]),
        "unfiltered" => (
            "SELECT id FROM t".to_owned(),
            (1..=rows * 2).collect::<Vec<_>>(),
        ),
        "selective" => (
            format!("SELECT id FROM t WHERE id > {}", rows * 2 - 16),
            (rows * 2 - 15..=rows * 2).collect::<Vec<_>>(),
        ),
        "sorted" => (
            format!("SELECT id FROM t WHERE id > {} ORDER BY id", rows * 2 - 16),
            (rows * 2 - 15..=rows * 2).collect::<Vec<_>>(),
        ),
        _ => return Err("unknown EMPTY_SCAN_BENCH_CASE".into()),
    };
    let table = DeltaTableBuilder::new(fixture.uri())
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    for backend in BACKENDS {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        context.register_table(
            "t",
            Arc::new(DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend),
                    ..ScanOptions::default()
                },
            )?),
        )?;
        for sample in 0..samples + 4 {
            let start = Instant::now();
            let plan = context.sql(&query).await?.create_physical_plan().await?;
            let planning_ns = start.elapsed().as_nanos();
            let metrics = collect_scan_metrics(plan.as_ref());
            let start = Instant::now();
            let batches = collect_plan(&context, plan).await?;
            let read_ns = start.elapsed().as_nanos();
            let mut actual = ids(&batches);
            actual.sort_unstable();
            assert_eq!(actual, expected);
            assert_eq!(metrics.len(), 1);
            let reader = metrics[0].snapshot().reader_metrics;
            if sample >= 4 {
                println!(
                    "{}",
                    json!({"case":case,"backend":format!("{backend:?}"),
                    "sample":sample-4,"rows_per_file":rows,"planning_ns":planning_ns,"read_ns":read_ns,
                    "files":reader.file_tasks_started,"emitted":reader.scheduler_rows_emitted,
                    "range_gets":reader.parquet_data_file_range_get_operations,
                    "full_gets":reader.parquet_data_file_full_get_operations,"bytes":reader.parquet_data_file_bytes_received})
                );
            }
        }
    }
    Ok(())
}
