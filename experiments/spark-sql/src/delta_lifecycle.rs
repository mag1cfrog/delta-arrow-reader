//! Spark frontend checks over the reader's existing real-Parquet fixtures.

use super::*;
use arrow::{array::Int32Array, datatypes::DataType, record_batch::RecordBatch};
use datafusion::{common::DataFusionError, datasource::TableProvider};
use delta_arrow_reader::{
    DeltaReaderError, DeltaReaderPhase, DeltaScanExecutionOptions, DeltaSnapshotSelection,
    WarmupMode,
};

#[allow(dead_code)]
#[path = "../../../tests/reader/support.rs"]
mod reader_support;
use reader_support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn context(partitions: usize) -> TestResult<SessionContext> {
    let ctx = session().map_err(|error| error as Box<dyn Error>)?;
    Ok(SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(ctx.state())
            .with_config(
                ctx.copied_config()
                    .with_target_partitions(partitions)
                    .with_repartition_file_min_size(1)
                    .with_batch_size(256),
            )
            .build(),
    ))
}

async fn plan(ctx: &SessionContext, sql: &str) -> TestResult<Arc<dyn ExecutionPlan>> {
    let named = resolve(
        ctx,
        sql,
        &json!({"spark.sql.ansi.enabled":"true", "spark.sql.session.timeZone":"UTC"}),
    )
    .await
    .map_err(|error| error as Box<dyn Error>)?;
    let physical = ctx
        .execute_logical_plan(named.plan)
        .await?
        .create_physical_plan()
        .await?;
    Ok(rename_physical_plan(physical, &named.fields)?)
}

fn ids(batch: &RecordBatch) -> &[i32] {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
}

async fn read_ids(ctx: &SessionContext, physical: Arc<dyn ExecutionPlan>) -> TestResult<Vec<i32>> {
    let mut stream = execute_stream(physical, ctx.task_ctx())?;
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        rows.extend_from_slice(ids(&batch?));
    }
    rows.sort_unstable();
    Ok(rows)
}

fn assert_idle(physical: &dyn ExecutionPlan) {
    let scans = collect_scan_metrics(physical);
    assert_eq!(scans.len(), 1, "must retain the host DeltaScanExec");
    let metrics = scans[0].snapshot().reader_metrics;
    assert_eq!(metrics.file_tasks_started, 0);
    assert_eq!(metrics.scheduler_rows_emitted, 0);
    assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
    assert_eq!(metrics.parquet_data_file_bytes_received, Some(0));
}

fn reader_error(error: &DataFusionError) -> &DeltaReaderError {
    let mut source: &(dyn Error + 'static) = error;
    loop {
        if let Some(reader) = source.downcast_ref::<DeltaReaderError>() {
            return reader;
        }
        source = source
            .source()
            .expect("must preserve the reader error source");
    }
}

#[tokio::test]
async fn spark_delta_deletion_vectors_preserve_rows_and_schema() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
        "spark-dv-row-groups",
        3_000,
        &[0, 2_999, 3_000, 5_999],
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    let expected = (2_999..=6_000)
        .filter(|id| ![3_000, 3_001, 6_000].contains(id))
        .collect::<Vec<_>>();
    for views in [true, false] {
        let ctx = context(8)?;
        ctx.register_table(
            "dv",
            Arc::new(DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    target_partitions: Some(8),
                    use_arrow_view_types: views,
                    ..Default::default()
                },
            )?),
        )?;
        // Planning and alias/schema handling must work while row data is unavailable.
        let data = fixture.path().join(fixture.data_file_path());
        let hidden = data.with_extension("unavailable");
        fs::rename(&data, &hidden)?;
        let sql =
            "SELECT id AS row_id, customer_name AS customer FROM dv WHERE id >= 2999 ORDER BY id";
        let physical = plan(&ctx, sql).await?;
        assert_idle(physical.as_ref());
        assert_eq!(physical.schema().field(0).name(), "row_id");
        assert_eq!(physical.schema().field(0).data_type(), &DataType::Int32);
        assert!(!physical.schema().field(0).is_nullable());
        assert_eq!(physical.schema().field(1).name(), "customer");
        assert_eq!(
            physical.schema().field(1).data_type(),
            &if views {
                DataType::Utf8View
            } else {
                DataType::Utf8
            }
        );
        assert!(physical.schema().field(1).is_nullable());
        fs::rename(&hidden, &data)?;
        let metrics = collect_scan_metrics(physical.as_ref());
        let mut stream = execute_stream(physical, ctx.task_ctx())?;
        let mut actual = Vec::new();
        let mut batches = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for (row, id) in ids(&batch).iter().enumerate() {
                assert_eq!(
                    array_value_to_string(batch.column(1).as_ref(), row)?,
                    format!("customer-{id}")
                );
                actual.push(*id);
            }
            batches += 1;
        }
        assert_eq!(actual, expected);
        assert!(batches > 1);
        let metrics = metrics[0].snapshot().reader_metrics;
        assert_eq!(metrics.scan_partitions_started, 8);
        assert_eq!(metrics.deletion_vector_rows_deleted, 3);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_coordinate_rejections, 0);
        assert_eq!(metrics.scheduler_rows_emitted, expected.len() as u64);
        let native = ctx
            .sql("SELECT id FROM dv WHERE id >= 2999")
            .await?
            .create_physical_plan()
            .await?;
        assert_eq!(read_ids(&ctx, native).await?, expected);
        let empty = plan(&ctx, "SELECT id AS row_id FROM dv WHERE id < 0").await?;
        assert_eq!(empty.schema().fields().len(), 1);
        assert!(read_ids(&ctx, empty).await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn spark_delta_refresh_keeps_retained_plans_and_streams() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_files("spark-snapshots")?;
    let uri = fixture.path().to_string_lossy().into_owned();
    let empty = DeltaTableBuilder::new(&uri)
        .with_snapshot_selection(DeltaSnapshotSelection::Version(0))
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    let old = DeltaTableBuilder::new(&uri)
        .with_snapshot_selection(DeltaSnapshotSelection::Version(1))
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    let provider = Arc::new(DeltaTableProvider::try_new(old, ScanOptions::default())?);
    let ctx = context(1)?;
    ctx.register_table("versions", provider.clone())?;
    let retained = plan(&ctx, "SELECT id AS row_id FROM versions").await?;
    assert_idle(retained.as_ref());
    let log = fixture.path().join("_delta_log");
    let mut metadata: Value = fs::read_to_string(log.join("00000000000000000000.json"))?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|v| v.get("metaData").is_some())
        .unwrap();
    let mut schema: Value =
        serde_json::from_str(metadata["metaData"]["schemaString"].as_str().unwrap())?;
    schema["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"note","type":"string","nullable":true,"metadata":{}}));
    metadata["metaData"]["schemaString"] = json!(schema.to_string());
    let first_add: Value = serde_json::from_str(
        fs::read_to_string(log.join("00000000000000000001.json"))?
            .lines()
            .next()
            .unwrap(),
    )?;
    let remove = json!({"remove":{"path":first_add["add"]["path"],"dataChange":true}});
    fs::write(
        log.join("00000000000000000002.json"),
        format!("{metadata}\n{remove}\n"),
    )?;
    let refreshed = Arc::new(provider.refresh().await?);
    assert_eq!(provider.schema().fields().len(), 2);
    assert_eq!(refreshed.schema().fields().len(), 3);
    ctx.deregister_table("versions")?;
    ctx.register_table("versions", refreshed)?;
    ctx.register_table(
        "empty_version",
        Arc::new(DeltaTableProvider::try_new(empty, ScanOptions::default())?),
    )?;
    // Eager snapshots and existing plans must survive refresh, registration replacement and log removal.
    fs::rename(&log, fixture.path().join("disabled-log"))?;
    let latest = plan(
        &ctx,
        "SELECT id AS row_id, note AS added_column FROM versions",
    )
    .await?;
    assert_idle(latest.as_ref());
    assert_eq!(
        collect_scan_metrics(retained.as_ref())[0]
            .snapshot()
            .reader_metrics
            .snapshot_version,
        1
    );
    assert_eq!(
        collect_scan_metrics(latest.as_ref())[0]
            .snapshot()
            .reader_metrics
            .snapshot_version,
        2
    );
    let (left, right) = tokio::join!(
        read_ids(&ctx, retained.clone()),
        read_ids(&ctx, retained.clone())
    );
    assert_eq!(left?, [1, 2, 3, 4]);
    assert_eq!(right?, [1, 2, 3, 4]);
    assert_eq!(read_ids(&ctx, retained).await?, [1, 2, 3, 4]);
    let mut stream = execute_stream(latest, ctx.task_ctx())?;
    let mut actual = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        assert_eq!(batch.column(1).null_count(), batch.num_rows());
        actual.extend_from_slice(ids(&batch));
    }
    actual.sort_unstable();
    assert_eq!(actual, [3, 4]);
    assert!(
        read_ids(&ctx, plan(&ctx, "SELECT id FROM empty_version").await?)
            .await?
            .is_empty()
    );
    let native = ctx
        .sql("SELECT id FROM versions")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(read_ids(&ctx, native).await?, [3, 4]);
    Ok(())
}

#[tokio::test]
async fn spark_delta_stream_drop_and_late_failure_keep_partial_results() -> TestResult {
    let fixture =
        RealParquetDeltaTable::new_with_two_large_files("spark-stream-lifecycle", 20_000)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    let ctx = context(1)?;
    let execution_options = DeltaScanExecutionOptions::new()
        .with_prefetch_files_per_partition(0)
        .with_max_concurrent_file_reads_per_partition(1)?
        .with_max_concurrent_file_reads_per_scan(Some(1))?
        .with_output_buffer_batches_per_partition(1)?;
    ctx.register_table(
        "streaming",
        Arc::new(DeltaTableProvider::try_new(
            table,
            ScanOptions {
                target_partitions: Some(1),
                execution_options,
                ..Default::default()
            },
        )?),
    )?;
    let adds = fs::read_to_string(fixture.path().join("_delta_log/00000000000000000001.json"))?;
    let second: Value = serde_json::from_str(adds.lines().nth(1).unwrap())?;
    let relative = second["add"]["path"].as_str().unwrap();
    let data = fixture.path().join(relative);
    let hidden = data.with_extension("unavailable");
    fs::rename(&data, &hidden)?;
    let sql = "SELECT id AS row_id, spark_partition_id() AS pid, monotonically_increasing_id() AS mid FROM streaming";
    let physical = plan(&ctx, sql).await?;
    assert_idle(physical.as_ref());
    let metrics = collect_scan_metrics(physical.as_ref());
    let mut stream = execute_stream(physical, ctx.task_ctx())?;
    let first = stream.next().await.ok_or("missing first batch")??;
    assert_eq!(ids(&first)[0], 1);
    assert_eq!(array_value_to_string(first.column(2).as_ref(), 0)?, "0");
    drop(stream);
    // The reader updates delivery metrics after the consumer receives the batch.
    for _ in 0..1000 {
        if metrics[0].snapshot().reader_metrics.scheduler_rows_emitted > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let partial = metrics[0].snapshot().reader_metrics;
    assert_eq!(partial.file_tasks_started, 1);
    assert_eq!(partial.file_tasks_completed, 0);
    assert_eq!(partial.scan_partitions_completed, 0);
    assert!((1..=512).contains(&partial.scheduler_rows_emitted));

    let failing = plan(&ctx, sql).await?;
    assert_idle(failing.as_ref());
    let metrics = collect_scan_metrics(failing.as_ref());
    let mut stream = execute_stream(failing.clone(), ctx.task_ctx())?;
    let mut count = 0;
    let error = loop {
        match stream.next().await.ok_or("missing late read error")? {
            Ok(batch) => {
                for (row, id) in ids(&batch).iter().enumerate() {
                    assert_eq!(*id, count + 1);
                    assert_eq!(
                        array_value_to_string(batch.column(2).as_ref(), row)?,
                        count.to_string()
                    );
                    count += 1;
                }
            }
            Err(error) => break error,
        }
    };
    assert_eq!(
        count, 20_000,
        "must emit the first file before failing on the second"
    );
    assert_eq!(reader_error(&error).phase(), DeltaReaderPhase::DataFileRead);
    assert_eq!(reader_error(&error).code(), "data_file_read");
    assert!(!error.to_string().contains(relative));
    assert!(stream.next().await.is_none());
    let partial = metrics[0].snapshot().reader_metrics;
    assert_eq!(partial.file_tasks_started, 2);
    assert_eq!(partial.file_tasks_completed, 1);
    assert_eq!(partial.scheduler_rows_emitted, 20_000);
    fs::rename(&hidden, &data)?;
    assert_eq!(
        read_ids(&ctx, failing).await?,
        (1..=40_000).collect::<Vec<_>>()
    );
    let native = ctx
        .sql("SELECT id FROM streaming")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        read_ids(&ctx, native).await?,
        (1..=40_000).collect::<Vec<_>>()
    );
    Ok(())
}
