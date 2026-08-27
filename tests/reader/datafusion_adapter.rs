//! Integration tests for the DataFusion table provider.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    collections::HashSet,
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, Int64Array, MapArray,
        StringArray, StringViewArray, StructArray,
    },
    datatypes::{DataType, Field, Schema, UInt16Type},
    record_batch::RecordBatch,
};
use datafusion::{
    common::DataFusionError,
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::{TableProviderFilterPushDown, col, lit},
    physical_plan::{ExecutionPlan, displayable},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaReaderError, DeltaReaderExecutionOptions, DeltaReaderPhase, DeltaTableBuilder,
    ParquetReaderBackend,
    datafusion::{
        DeltaTableProvider, IntraFileRepartitioning, ScanOptions, collect_metrics, register_table,
    },
};
use futures_util::StreamExt;
use parquet::{
    arrow::ArrowWriter,
    file::reader::{FileReader, SerializedFileReader},
};
use serde_json::{Value, json};

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TestTable(PathBuf);

impl TestTable {
    fn empty(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        table.write_log(&[protocol(1), metadata()])?;
        Ok(table)
    }

    fn partitioned(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        let west = table.write_parquet("west.parquet", &[1, 2])?;
        let east = table.write_parquet("east.parquet", &[3, 4])?;
        table.write_log(&[
            protocol(1),
            metadata(),
            add("west.parquet", west, "west", 2, 1, 2),
            add("east.parquet", east, "east", 2, 3, 4),
        ])?;
        Ok(table)
    }

    fn skewed(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        let large_ids = (1..=10_000).collect::<Vec<_>>();
        let large = table.write_parquet("large.parquet", &large_ids)?;
        let small = table.write_parquet("small.parquet", &[10_001, 10_002])?;
        table.write_log(&[
            protocol(1),
            metadata(),
            add("large.parquet", large, "west", 10_000, 1, 10_000),
            add("small.parquet", small, "east", 2, 10_001, 10_002),
        ])?;
        Ok(table)
    }

    fn unsupported(name: &str) -> TestResult<Self> {
        let table = Self::new(name)?;
        table.write_log(&[protocol(4), metadata()])?;
        Ok(table)
    }

    fn new(name: &str) -> TestResult<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = Path::new("target")
            .join("delta-arrow-reader-provider-tests")
            .join(format!("{}-{name}-{nanos}", std::process::id()));
        fs::create_dir_all(path.join("_delta_log"))?;
        Ok(Self(path))
    }

    fn uri(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }

    fn write_parquet(&self, name: &str, ids: &[i32]) -> TestResult<u64> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )?;
        let path = self.0.join(name);
        let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(fs::metadata(path)?.len())
    }

    fn write_log(&self, actions: &[Value]) -> TestResult {
        let contents = actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            self.0.join("_delta_log/00000000000000000000.json"),
            format!("{contents}\n"),
        )?;
        Ok(())
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn protocol(min_reader_version: i32) -> Value {
    json!({
        "protocol": {
            "minReaderVersion": min_reader_version,
            "minWriterVersion": 2
        }
    })
}

fn metadata() -> Value {
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
            {"name": "region", "type": "string", "nullable": true, "metadata": {}}
        ]
    });
    json!({
        "metaData": {
            "id": "delta-arrow-reader-provider-test",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": ["region"],
            "configuration": {},
            "createdTime": 1587968585495_i64
        }
    })
}

fn add(path: &str, size: u64, region: &str, num_records: u64, min_id: i32, max_id: i32) -> Value {
    let stats = json!({
        "numRecords": num_records,
        "minValues": {"id": min_id},
        "maxValues": {"id": max_id},
        "nullCount": {"id": 0}
    });
    json!({
        "add": {
            "path": path,
            "partitionValues": {"region": region},
            "size": size,
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "stats": stats.to_string()
        }
    })
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(batch.schema().index_of("id").expect("id column"))
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 id")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn regions(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let regions = batch
                .column(batch.schema().index_of("region").expect("region column"))
                .as_any()
                .downcast_ref::<DictionaryArray<UInt16Type>>()
                .expect("dictionary region");
            let values = regions
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 dictionary values");
            regions
                .keys()
                .iter()
                .map(|key| {
                    values
                        .value(usize::from(key.expect("non-null region")))
                        .to_owned()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn collect_plan(
    context: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> TestResult<Vec<RecordBatch>> {
    Ok(datafusion::physical_plan::collect(plan, context.task_ctx()).await?)
}

#[tokio::test]
async fn optimizer_repartitions_parquet_files_through_normal_sql_planning() -> TestResult {
    let fixture = TestTable::partitioned("optimizer-file-repartitioning")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(1),
    );
    register_table(&context, "orders", table, ScanOptions::default())?;

    let plan = context
        .sql("SELECT count(*) AS row_count, sum(id) AS id_sum FROM orders")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        display.contains("DeltaDataFusionExec: snapshot_version=0, partitions=4"),
        "{display}"
    );

    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].snapshot().reader.scan_partitions_planned, 4);
    let batches = collect_plan(&context, plan).await?;
    let batch = batches.first().ok_or("aggregate returned no batch")?;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("count was not Int64")?
            .value(0),
        4
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("sum was not Int64")?
            .value(0),
        10
    );
    Ok(())
}

#[tokio::test]
async fn intra_file_repartitioning_policy_controls_full_plan_rebalancing() -> TestResult {
    let fixture = TestTable::skewed("optimizer-full-plan-rebalancing")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;

    for (name, policy, expected_tasks) in [
        (
            "default_orders",
            IntraFileRepartitioning::WhenBelowTarget,
            2,
        ),
        ("rebalanced_orders", IntraFileRepartitioning::Always, 3),
    ] {
        let context = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(2)
                .with_repartition_file_min_size(1),
        );
        register_table(
            &context,
            name,
            table.clone(),
            ScanOptions {
                target_partitions: Some(2),
                intra_file_repartitioning: policy,
                ..Default::default()
            },
        )?;
        let plan = context
            .sql(&format!(
                "SELECT count(*) AS row_count, sum(id) AS id_sum FROM {name}"
            ))
            .await?
            .create_physical_plan()
            .await?;
        let metrics = collect_metrics(plan.as_ref());
        let batches = collect_plan(&context, plan).await?;
        let batch = batches.first().ok_or("aggregate returned no batch")?;
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("count was not Int64")?
                .value(0),
            10_002
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("sum was not Int64")?
                .value(0),
            50_025_003
        );
        assert_eq!(
            metrics[0].snapshot().reader.file_tasks_started,
            expected_tasks
        );
    }

    Ok(())
}

#[tokio::test]
async fn repartitioned_scan_preserves_predicates_and_deletion_vector_coordinates() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
        "provider-repartitioned-dv",
        3_000,
        &[0, 2_999, 3_000, 5_999],
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(8)
            .with_repartition_file_min_size(1),
    );
    register_table(
        &context,
        "orders",
        table,
        ScanOptions {
            target_partitions: Some(8),
            ..Default::default()
        },
    )?;

    let plan = context
        .sql("SELECT id FROM orders WHERE id >= 2999 ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("partitions=8"), "{display}");

    let metrics = collect_metrics(plan.as_ref());
    let actual = ids(&collect_plan(&context, plan).await?);
    let expected = (2_999..=6_000)
        .filter(|id| ![3_000, 3_001, 6_000].contains(id))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let metrics = metrics[0].snapshot().reader;
    assert_eq!(metrics.scan_partitions_started, 8);
    assert_eq!(metrics.file_tasks_started, 8);
    assert!(
        (1..=metrics.file_tasks_started).contains(&metrics.deletion_vector_payloads_loaded),
        "unexpected payload load count: {} for {} tasks",
        metrics.deletion_vector_payloads_loaded,
        metrics.file_tasks_started
    );
    assert_eq!(metrics.deletion_vectors_applied, 2);
    assert_eq!(metrics.deletion_vector_rows_deleted, 3);
    assert_eq!(metrics.deletion_vector_failures, 0);
    assert_eq!(metrics.deletion_vector_rejections, 0);
    Ok(())
}

#[tokio::test]
async fn repartitioned_scan_preserves_physical_to_logical_transforms() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_column_mapping("provider-repartitioned-mapping")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(1),
    );
    register_table(&context, "mapped", table, ScanOptions::default())?;

    let plan = context
        .sql("SELECT customer_name, id FROM mapped WHERE id >= 2 ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("partitions=2"), "{display}");
    let batches = collect_plan(&context, plan).await?;

    assert_eq!(ids(&batches), [2, 3]);
    let names = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("customer_name was not Utf8View")
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, [Some("bob"), None]);
    Ok(())
}

#[tokio::test]
async fn large_repartitioned_dv_scan_matches_unsplit_under_concurrent_reexecution() -> TestResult {
    const ROW_GROUPS: usize = 32;
    const ROWS_PER_GROUP: usize = 8_192;
    const TARGET_PARTITIONS: usize = 64;
    let rows = ROW_GROUPS
        .checked_mul(ROWS_PER_GROUP)
        .ok_or("row overflow")?;
    let mut stored_deleted_rows = Vec::with_capacity(ROW_GROUPS * 3 + 2);
    for row_group in 0..ROW_GROUPS {
        let start = u64::try_from(
            row_group
                .checked_mul(ROWS_PER_GROUP)
                .ok_or("row overflow")?,
        )?;
        stored_deleted_rows.extend([
            start,
            start + u64::try_from(ROWS_PER_GROUP / 2)?,
            start + u64::try_from(ROWS_PER_GROUP - 1)?,
        ]);
    }
    stored_deleted_rows.extend([0, u64::try_from(rows - 1)?]);
    let mut deleted_rows = stored_deleted_rows.clone();
    deleted_rows.sort_unstable();
    deleted_rows.dedup();

    let fixture = RealParquetDeltaTable::new_with_row_groups_and_deletion_vector(
        "provider-large-repartitioned-dv",
        ROW_GROUPS,
        ROWS_PER_GROUP,
        &stored_deleted_rows,
    )?;
    assert_eq!(fixture.rows(), rows);
    let parquet = SerializedFileReader::new(fs::File::open(
        fixture.path().join(fixture.data_file_path()),
    )?)?;
    assert_eq!(parquet.metadata().num_row_groups(), ROW_GROUPS);

    let execution_options = DeltaReaderExecutionOptions::new()
        .with_parquet_full_file_read_threshold_bytes(Some(usize::MAX))?;
    let options = ScanOptions {
        execution_options,
        target_partitions: Some(TARGET_PARTITIONS),
        intra_file_repartitioning: IntraFileRepartitioning::Always,
        ..Default::default()
    };
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1_024)
            .with_target_partitions(TARGET_PARTITIONS)
            .with_repartition_file_min_size(1),
    );
    register_fixture(&context, "orders", &fixture, options.clone()).await?;
    let plan = context
        .sql("SELECT id FROM orders ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        display.contains(&format!("partitions={TARGET_PARTITIONS}")),
        "{display}"
    );
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);

    let (first, second) = tokio::join!(
        collect_plan(&context, Arc::clone(&plan)),
        collect_plan(&context, Arc::clone(&plan)),
    );
    let deleted_ids = deleted_rows
        .iter()
        .map(|row| i32::try_from(row + 1))
        .collect::<Result<HashSet<_>, _>>()?;
    let expected = (1..=i32::try_from(rows)?)
        .filter(|id| !deleted_ids.contains(id))
        .collect::<Vec<_>>();
    for actual in [first?, second?] {
        assert_eq!(ids(&actual), expected);
    }

    let metrics = metrics[0].snapshot().reader;
    let executions = 2_u64;
    let expected_tasks = u64::try_from(TARGET_PARTITIONS)? * executions;
    assert_eq!(
        metrics.scan_partitions_planned,
        u64::try_from(TARGET_PARTITIONS)?
    );
    assert_eq!(metrics.scan_partitions_started, expected_tasks);
    assert_eq!(metrics.scan_partitions_completed, expected_tasks);
    assert_eq!(metrics.file_tasks_started, expected_tasks);
    assert_eq!(metrics.file_tasks_completed, expected_tasks);
    assert_eq!(
        metrics.rows_produced,
        u64::try_from(expected.len())? * executions
    );
    assert!(
        (1..=expected_tasks).contains(&metrics.deletion_vector_payloads_loaded),
        "unexpected payload load count: {} for {expected_tasks} tasks",
        metrics.deletion_vector_payloads_loaded
    );
    assert_eq!(
        metrics.deletion_vectors_applied,
        u64::try_from(ROW_GROUPS)? * executions
    );
    assert_eq!(
        metrics.deletion_vector_rows_deleted,
        u64::try_from(deleted_rows.len())? * executions
    );
    assert_eq!(metrics.deletion_vector_failures, 0);
    assert_eq!(metrics.deletion_vector_rejections, 0);
    assert_eq!(
        metrics.parquet_task_bytes_admitted,
        Some(fixture.data_file_size() * executions)
    );
    assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
    assert!(
        metrics
            .parquet_data_file_range_get_operations
            .is_some_and(|value| value > 0)
    );

    let control = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1_024)
            .with_target_partitions(TARGET_PARTITIONS)
            .with_repartition_file_scans(false),
    );
    register_fixture(&control, "orders", &fixture, options).await?;
    let control_plan = control
        .sql("SELECT id FROM orders ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let control_display = displayable(control_plan.as_ref()).indent(true).to_string();
    assert!(
        control_display.contains("partitions=1"),
        "{control_display}"
    );
    let control_metrics = collect_metrics(control_plan.as_ref());
    assert_eq!(ids(&collect_plan(&control, control_plan).await?), expected);
    let control_metrics = control_metrics[0].snapshot().reader;
    assert_eq!(control_metrics.file_tasks_started, 1);
    assert_eq!(control_metrics.deletion_vector_payloads_loaded, 1);
    assert_eq!(
        control_metrics.deletion_vector_rows_deleted,
        u64::try_from(deleted_rows.len())?
    );
    assert_eq!(
        control_metrics.parquet_data_file_full_get_operations,
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn repartitioned_scan_fails_closed_when_dv_payload_is_missing() -> TestResult {
    const DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
    let fixture = RealParquetDeltaTable::new_with_row_groups_and_deletion_vector(
        "provider-repartitioned-missing-dv",
        4,
        2_048,
        &[0, 2_047, 2_048, 8_191],
    )?;
    fs::remove_file(fixture.path().join(DV_FILE))?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(8)
            .with_repartition_file_min_size(1),
    );
    register_fixture(
        &context,
        "orders",
        &fixture,
        ScanOptions {
            target_partitions: Some(8),
            ..Default::default()
        },
    )
    .await?;
    let plan = context
        .sql("SELECT id FROM orders")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("partitions=8"), "{display}");
    let metrics = collect_metrics(plan.as_ref());

    let error = datafusion::physical_plan::collect(plan, context.task_ctx())
        .await
        .expect_err("missing deletion-vector payload unexpectedly succeeded");
    let display = error.to_string();
    assert!(
        display.contains("deletion_vector_payload_read_failed"),
        "{display}"
    );
    assert!(!display.contains(DV_FILE), "{display}");
    let metrics = metrics[0].snapshot().reader;
    assert!(metrics.file_tasks_started > 0);
    assert_eq!(metrics.file_tasks_completed, 0);
    assert_eq!(metrics.batches_produced, 0);
    assert_eq!(metrics.rows_produced, 0);
    assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
    assert!(
        (1..=metrics.file_tasks_started).contains(&metrics.deletion_vector_failures),
        "unexpected failure count: {} for {} started tasks",
        metrics.deletion_vector_failures,
        metrics.file_tasks_started
    );
    assert_eq!(metrics.deletion_vectors_applied, 0);
    assert_eq!(metrics.deletion_vector_rows_deleted, 0);
    assert_eq!(metrics.deletion_vector_rejections, 0);
    Ok(())
}

async fn register_fixture(
    context: &SessionContext,
    name: &str,
    fixture: &RealParquetDeltaTable,
    options: ScanOptions,
) -> TestResult {
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    register_table(context, name, table, options)?;
    Ok(())
}

fn register_allowed_regions(context: &SessionContext, regions: Vec<&str>) -> TestResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "region",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(regions))],
    )?;
    context.register_table(
        "allowed_regions",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(())
}

fn external_reader_error(error: &DataFusionError) -> TestResult<&DeltaReaderError> {
    let DataFusionError::External(source) = error else {
        return Err("DataFusion error did not preserve an external reader error".into());
    };
    source
        .downcast_ref::<DeltaReaderError>()
        .ok_or_else(|| "external source was not DeltaReaderError".into())
}

#[tokio::test]
async fn options_protocol_schema_pushdown_and_debug_match_the_provider_contract() -> TestResult {
    let fixture = TestTable::partitioned("provider-contract")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let defaults = ScanOptions::default();
    assert_eq!(
        defaults.execution_options,
        DeltaReaderExecutionOptions::default()
    );
    assert_eq!(defaults.target_partitions, None);
    assert!(defaults.use_arrow_view_types);
    assert_eq!(
        defaults.intra_file_repartitioning,
        IntraFileRepartitioning::WhenBelowTarget
    );

    let provider = DeltaTableProvider::try_new(table.clone(), defaults)?;
    assert_eq!(table.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        provider.schema().field(1).data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
    );
    assert_eq!(provider.table_type(), TableType::Base);
    let debug = format!("{provider:?}");
    assert!(!debug.contains(&fixture.uri()));
    assert!(!debug.contains("provider-contract"));

    let filters = [
        col("region").eq(lit("west")),
        col("id").gt(lit(1_i32)),
        col("id") + lit(1_i32),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();
    assert_eq!(
        provider.supports_filters_pushdown(&filter_refs)?,
        [
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
        ]
    );

    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let unsupported = provider
        .scan(&context.state(), None, &[col("id") + lit(1_i32)], None)
        .await
        .expect_err("unsupported pushed filter must fail");
    let datafusion::common::DataFusionError::External(source) = unsupported else {
        return Err("scan error did not preserve DeltaReaderError".into());
    };
    let reader = source
        .downcast_ref::<delta_arrow_reader::DeltaReaderError>()
        .ok_or("external source was not DeltaReaderError")?;
    assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);

    let full = provider.scan(&context.state(), None, &[], Some(1)).await?;
    assert_eq!(full.properties().output_partitioning().partition_count(), 2);
    assert_eq!(
        full.partition_statistics(None)?,
        Arc::new(datafusion::common::Statistics::new_unknown(&full.schema()))
    );
    let mut full_ids = ids(&collect_plan(&context, Arc::clone(&full)).await?);
    full_ids.sort_unstable();
    assert_eq!(full_ids, [1, 2, 3, 4]);
    let metrics = collect_metrics(full.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].registration_name(), None);

    let explicit_target = DeltaTableProvider::try_new(
        table.clone(),
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let one_partition = explicit_target
        .scan(&context.state(), None, &[], None)
        .await?;
    assert_eq!(
        one_partition
            .properties()
            .output_partitioning()
            .partition_count(),
        1
    );

    for projection in [vec![2], vec![0, 0]] {
        let error = provider
            .scan(&context.state(), Some(&projection), &[], None)
            .await
            .expect_err("invalid projection must fail");
        let datafusion::common::DataFusionError::External(source) = error else {
            return Err("projection error did not preserve DeltaReaderError".into());
        };
        let reader = source
            .downcast_ref::<delta_arrow_reader::DeltaReaderError>()
            .ok_or("external source was not DeltaReaderError")?;
        assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);
    }

    let projection = vec![1, 0];
    let projected = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await?;
    let projected_batches = collect_plan(&context, projected).await?;
    let mut projected_regions = regions(&projected_batches);
    projected_regions.sort();
    let mut projected_ids = ids(&projected_batches);
    projected_ids.sort_unstable();
    assert_eq!(projected_regions, ["east", "east", "west", "west"]);
    assert_eq!(projected_ids, [1, 2, 3, 4]);

    let empty_projection = Vec::new();
    let empty = provider
        .scan(&context.state(), Some(&empty_projection), &[], None)
        .await?;
    let empty_batches = collect_plan(&context, empty).await?;
    assert!(empty_batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        empty_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let zero_target = DeltaTableProvider::try_new(
        table.clone(),
        ScanOptions {
            target_partitions: Some(0),
            ..Default::default()
        },
    )
    .expect_err("zero target must fail");
    assert_eq!(zero_target.phase(), DeltaReaderPhase::Configuration);

    let unsupported_fixture = TestTable::unsupported("unsupported-provider")?;
    let unsupported = DeltaTableBuilder::new(unsupported_fixture.uri())
        .load_table()
        .await?;
    let error = DeltaTableProvider::try_new(unsupported, Default::default())
        .expect_err("unsupported protocol must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
    Ok(())
}

#[tokio::test]
async fn registration_sql_metrics_duplicates_and_repeated_scans_are_exact() -> TestResult {
    let fixture = TestTable::partitioned("registration")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let context = SessionContext::new();

    for invalid in ["", "1orders", "line-items", "select"] {
        let error = register_table(&context, invalid, table.clone(), ScanOptions::default())
            .expect_err("invalid name must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::DataFusion);
    }

    let registered = register_table(
        &context,
        "Orders",
        table.clone(),
        ScanOptions {
            target_partitions: Some(2),
            ..Default::default()
        },
    )?;
    assert_eq!(registered.name, "Orders");
    assert_eq!(registered.version, table.version());
    let registered_provider = context.table_provider("orders").await?;
    assert!(!format!("{registered_provider:?}").contains("Orders"));
    let duplicate = register_table(&context, "orders", table, ScanOptions::default())
        .expect_err("duplicate registration must fail");
    assert_eq!(duplicate.phase(), DeltaReaderPhase::DataFusion);
    assert!(!duplicate.to_string().contains("Orders"));
    assert!(
        duplicate
            .source()
            .and_then(|source| {
                source.downcast_ref::<Box<datafusion::common::DataFusionError>>()
            })
            .is_some()
    );

    let mut dataframe_ids = ids(&context
        .table("orders")
        .await?
        .select_columns(&["id"])?
        .collect()
        .await?);
    dataframe_ids.sort_unstable();
    assert_eq!(dataframe_ids, [1, 2, 3, 4]);

    let first = context
        .sql("SELECT region, id FROM orders WHERE id > 1 ORDER BY id LIMIT 2")
        .await?
        .create_physical_plan()
        .await?;
    let first_handles = collect_metrics(first.as_ref());
    assert_eq!(first_handles.len(), 1);
    assert_eq!(first_handles[0].registration_name(), Some("Orders"));
    assert_eq!(first_handles[0].snapshot().reader.file_tasks_started, 0);
    assert_eq!(
        first_handles[0].snapshot().reader.estimated_input_rows,
        Some(4)
    );
    let first_batches = collect_plan(&context, first).await?;
    assert_eq!(ids(&first_batches), [2, 3]);
    assert_eq!(regions(&first_batches), ["west", "east"]);
    assert_eq!(first_handles[0].snapshot().reader.rows_produced, 3);

    let second = context
        .sql("SELECT id FROM orders WHERE region = 'west' ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let second_handles = collect_metrics(second.as_ref());
    assert_eq!(second_handles.len(), 1);
    assert_eq!(second_handles[0].registration_name(), Some("Orders"));
    assert_eq!(second_handles[0].snapshot().reader.file_tasks_started, 0);
    assert_eq!(
        second_handles[0].snapshot().reader.estimated_input_rows,
        Some(2)
    );
    assert_eq!(ids(&collect_plan(&context, second).await?), [1, 2]);
    assert_eq!(first_handles[0].snapshot().reader.rows_produced, 3);
    assert_eq!(second_handles[0].snapshot().reader.rows_produced, 2);

    assert_eq!(
        ids(&context
            .sql("SELECT id FROM orders WHERE region = 'west' AND id > 1")
            .await?
            .collect()
            .await?),
        [2]
    );
    assert_eq!(
        ids(&context
            .sql("SELECT id FROM orders WHERE id + 1 > 3 ORDER BY id")
            .await?
            .collect()
            .await?),
        [3, 4]
    );
    assert_eq!(
        ids(&context
            .sql("SELECT o.id FROM orders AS o WHERE o.region = 'east' ORDER BY o.id")
            .await?
            .collect()
            .await?),
        [3, 4]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_runtime_owns_concurrent_dataframe_execution() -> TestResult {
    let fixture = TestTable::partitioned("caller-runtime")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let context = SessionContext::new();
    register_table(&context, "orders", table, ScanOptions::default())?;

    let left = context.sql("SELECT id FROM orders WHERE id <= 2").await?;
    let right = context.sql("SELECT id FROM orders WHERE id > 2").await?;
    let (left, right) = tokio::try_join!(left.collect(), right.collect())?;
    assert_eq!(ids(&left), [1, 2]);
    assert_eq!(ids(&right), [3, 4]);
    Ok(())
}

#[tokio::test]
async fn direct_exact_and_kernel_residual_execution_return_the_same_rows() -> TestResult {
    let fixture = TestTable::partitioned("backend-parity")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let mut outputs = Vec::new();

    for (name, backend) in [
        ("direct_orders", ParquetReaderBackend::DirectParquet),
        ("kernel_orders", ParquetReaderBackend::DeltaKernel),
    ] {
        let context = SessionContext::new();
        let execution_options = DeltaReaderExecutionOptions::new().with_reader_backend(backend);
        let provider = DeltaTableProvider::try_new(
            table.clone(),
            ScanOptions {
                execution_options,
                target_partitions: Some(2),
                intra_file_repartitioning: Default::default(),
                use_arrow_view_types: true,
            },
        )?;
        let data_filter = col("id").gt(lit(1_i32));
        assert_eq!(
            provider.supports_filters_pushdown(&[&data_filter])?,
            [match backend {
                ParquetReaderBackend::DirectParquet => TableProviderFilterPushDown::Exact,
                ParquetReaderBackend::DeltaKernel => TableProviderFilterPushDown::Inexact,
            }]
        );
        context.register_table(name, Arc::new(provider))?;
        let mut batches = context
            .sql(&format!("SELECT id FROM {name} WHERE id > 1"))
            .await?
            .collect()
            .await?;
        batches.sort_by_key(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 id")
                .value(0)
        });
        outputs.push(ids(&batches));
    }

    assert_eq!(outputs[0], [2, 3, 4]);
    assert_eq!(outputs[1], outputs[0]);
    Ok(())
}

#[tokio::test]
async fn sql_join_dynamic_filter_prunes_before_file_admission() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture = RealParquetDeltaTable::new_with_two_partition_values("provider-dynamic-pruning")?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )
    .await?;

    let plan = context
        .sql(
            "SELECT o.id, o.customer_name, o.region \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("HashJoinExec"), "{display}");
    assert!(display.contains("DeltaDataFusionExec"), "{display}");

    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].registration_name(), Some("orders"));
    assert_eq!(metrics[0].snapshot().reader.files_planned, 2);
    assert_eq!(metrics[0].snapshot().reader.file_tasks_started, 0);

    let batches = collect_plan(&context, plan).await?;
    assert_eq!(ids(&batches), [1, 2]);
    assert_eq!(regions(&batches), ["us-west", "us-west"]);
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.file_tasks_started, 1);
    assert_eq!(metrics.reader.file_tasks_completed, 1);
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_filters_unsupported, 0);
    assert_eq!(metrics.dynamic_filter_snapshot_attempts, 2);
    assert_eq!(metrics.dynamic_partition_tasks_pruned, 1);
    assert_eq!(metrics.dynamic_partition_tasks_kept, 1);
    assert_eq!(metrics.dynamic_partition_tasks_kept_missing_metadata, 0);
    assert_eq!(
        metrics.dynamic_partition_tasks_kept_unsupported_expression,
        0
    );
    Ok(())
}

#[tokio::test]
async fn dynamic_join_kept_file_still_applies_deletion_vector() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
        "provider-dynamic-dv",
        "us-west",
        &[1],
    )?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )
    .await?;

    let plan = context
        .sql(
            "SELECT o.region, o.id \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    let batches = collect_plan(&context, plan).await?;
    assert_eq!(ids(&batches), [1, 3]);
    assert_eq!(regions(&batches), ["us-west", "us-west"]);

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_partition_tasks_pruned, 0);
    assert_eq!(metrics.dynamic_partition_tasks_kept, 1);
    assert_eq!(metrics.reader.deletion_vector_payloads_loaded, 1);
    assert_eq!(metrics.reader.deletion_vectors_applied, 1);
    assert_eq!(metrics.reader.deletion_vector_rows_deleted, 1);
    assert_eq!(metrics.reader.deletion_vector_failures, 0);
    assert_eq!(metrics.reader.deletion_vector_rejections, 0);
    Ok(())
}

#[tokio::test]
async fn direct_exact_filter_applies_before_deletion_vector_masking() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_row_groups_and_deletion_vector(
        "provider-dv-predicate-pruning",
        3,
        &[4],
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let filter = col("id").gt(lit(3_i32));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter])?,
        [TableProviderFilterPushDown::Exact]
    );

    let context = SessionContext::new();
    context.register_table("orders", Arc::new(provider))?;
    let plan = context
        .sql("SELECT id FROM orders WHERE id > 3 ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!display.contains("FilterExec"), "{display}");
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(ids(&collect_plan(&context, plan).await?), [4, 6]);
    let metrics = metrics[0].snapshot().reader;
    assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
    assert_eq!(metrics.deletion_vectors_applied, 1);
    assert_eq!(metrics.deletion_vector_rows_deleted, 1);
    Ok(())
}

#[tokio::test]
async fn execution_records_batch_size_and_rejects_invalid_partition() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-execution-options")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let provider = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(13),
    );
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].snapshot().output_batch_size, None);

    let error = plan
        .execute(1, context.task_ctx())
        .err()
        .ok_or("out-of-range partition unexpectedly executed")?;
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::DataFusion);
    assert!(
        reader
            .to_string()
            .contains("reason=scan_partition_index_out_of_range")
    );

    assert_eq!(ids(&collect_plan(&context, plan).await?), [1, 2, 3]);
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.output_batch_size, Some(13));
    assert_eq!(metrics.reader.scan_partitions_started, 1);
    assert_eq!(metrics.reader.scan_partitions_completed, 1);
    assert_eq!(metrics.reader.file_tasks_started, 1);
    assert_eq!(metrics.reader.file_tasks_completed, 1);
    Ok(())
}

#[tokio::test]
async fn empty_scan_has_no_partitions_rows_or_execution_metrics() -> TestResult {
    let fixture = TestTable::empty("provider-empty-scan")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    assert_eq!(plan.properties().output_partitioning().partition_count(), 0);
    assert!(
        displayable(plan.as_ref())
            .indent(true)
            .to_string()
            .contains("partitions=0")
    );

    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert!(collect_plan(&context, plan).await?.is_empty());
    let metrics = metrics[0].snapshot().reader;
    assert_eq!(metrics.scan_partitions_planned, 0);
    assert_eq!(metrics.files_planned, 0);
    assert_eq!(metrics.estimated_input_rows, Some(0));
    assert_eq!(metrics.estimated_input_bytes, Some(0));
    assert_eq!(metrics.scan_partitions_started, 0);
    assert_eq!(metrics.scan_partitions_completed, 0);
    assert_eq!(metrics.file_tasks_started, 0);
    assert_eq!(metrics.file_tasks_completed, 0);
    assert_eq!(metrics.batches_produced, 0);
    assert_eq!(metrics.rows_produced, 0);
    assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
    assert_eq!(metrics.deletion_vectors_applied, 0);
    assert_eq!(metrics.deletion_vector_rows_deleted, 0);
    assert_eq!(metrics.deletion_vector_failures, 0);
    assert_eq!(metrics.deletion_vector_rejections, 0);
    Ok(())
}

#[tokio::test]
async fn execution_error_preserves_reader_source_and_partial_metrics() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-missing-file")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    fs::remove_file(fixture.path().join(fixture.data_file_path()))?;
    let provider = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )?;
    let context = SessionContext::new();
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    let metrics = collect_metrics(plan.as_ref());
    let mut stream = plan.execute(0, context.task_ctx())?;
    let error = stream
        .next()
        .await
        .ok_or("missing file returned no stream item")?
        .expect_err("missing file unexpectedly succeeded");
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::DataFileRead);
    assert_eq!(reader.code(), "data_file_read");
    assert!(reader.source().is_some());
    assert!(stream.next().await.is_none());

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.file_tasks_started, 1);
    assert_eq!(metrics.reader.file_tasks_completed, 0);
    assert_eq!(metrics.reader.batches_produced, 0);
    assert_eq!(metrics.reader.rows_produced, 0);
    Ok(())
}

#[tokio::test]
async fn execution_stream_drop_preserves_bounded_partial_metrics() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_large_files("provider-stream-drop", 20_000)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let execution_options = DeltaReaderExecutionOptions::new()
        .with_prefetch_file_count_per_partition(0)
        .with_max_concurrent_file_reads_per_partition(1)?
        .with_max_concurrent_file_reads_per_scan(Some(1))?
        .with_output_buffer_capacity_per_partition(1)?;
    let provider = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            execution_options,
            target_partitions: Some(1),
            intra_file_repartitioning: Default::default(),
            use_arrow_view_types: true,
        },
    )?;
    let context = SessionContext::new();
    let projection = vec![0];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await?;
    let metrics = collect_metrics(plan.as_ref());
    let mut stream = plan.execute(0, context.task_ctx())?;
    let first = stream.next().await.ok_or("expected first batch")??;
    assert_eq!(ids(std::slice::from_ref(&first)).first().copied(), Some(1));
    drop(stream);

    for _ in 0..1000 {
        if metrics[0].snapshot().reader.batches_produced > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.scan_partitions_started, 1);
    assert_eq!(metrics.reader.scan_partitions_completed, 0);
    assert_eq!(metrics.reader.file_tasks_started, 1);
    assert_eq!(metrics.reader.file_tasks_completed, 0);
    assert!((1..=2).contains(&metrics.reader.batches_produced));
    assert!((1..=16_384).contains(&metrics.reader.rows_produced));
    Ok(())
}

#[tokio::test]
async fn direct_metadata_size_hint_bytes_preserves_rows_and_request_fallback() -> TestResult {
    let fixture =
        RealParquetDeltaTable::new_with_two_large_files("provider-parquet-metadata-hint", 20_000)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let mut outputs = Vec::new();
    let mut requests = Vec::new();

    for hint in [None, Some(64 * 1024), Some(9)] {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let execution_options =
            DeltaReaderExecutionOptions::new().with_parquet_metadata_size_hint_bytes(hint)?;
        register_table(
            &context,
            "orders",
            table.clone(),
            ScanOptions {
                execution_options,
                target_partitions: Some(1),
                intra_file_repartitioning: Default::default(),
                use_arrow_view_types: true,
            },
        )?;
        let plan = context
            .sql("SELECT count(*) AS row_count, sum(id) AS id_sum FROM orders")
            .await?
            .create_physical_plan()
            .await?;
        let metrics = collect_metrics(plan.as_ref());
        let batches = collect_plan(&context, plan).await?;
        let batch = batches.first().ok_or("aggregate returned no batch")?;
        assert_eq!(batch.num_rows(), 1);
        let row_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("count was not Int64")?
            .value(0);
        let id_sum = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("sum was not Int64")?
            .value(0);
        outputs.push((row_count, id_sum));
        let snapshot = metrics[0].snapshot().reader;
        assert_eq!(snapshot.file_tasks_started, 2);
        requests.push(
            snapshot
                .parquet_data_file_range_get_operations
                .ok_or("missing direct Parquet range GET metric")?,
        );
    }

    assert_eq!(outputs[1], outputs[0]);
    assert_eq!(outputs[2], outputs[0]);
    assert_eq!(outputs[0], (40_000, 800_020_000));
    assert_eq!(requests[0].checked_sub(requests[1]), Some(2));
    assert_eq!(requests[2], requests[0]);
    Ok(())
}

#[tokio::test]
async fn dynamic_join_pruning_preserves_the_sql_residual() -> TestResult {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
            .set_bool(
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                true,
            ),
    );
    register_allowed_regions(&context, vec!["us-west"])?;
    let fixture =
        RealParquetDeltaTable::new_with_two_partition_values("provider-dynamic-residual")?;
    register_fixture(
        &context,
        "orders",
        &fixture,
        ScanOptions {
            target_partitions: Some(1),
            ..Default::default()
        },
    )
    .await?;

    let plan = context
        .sql(
            "SELECT o.id, o.customer_name, o.region \
             FROM allowed_regions r JOIN orders o ON r.region = o.region \
             WHERE o.customer_name LIKE 'west-1%' ORDER BY o.id",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("FilterExec"), "{display}");
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(ids(&collect_plan(&context, plan).await?), [1]);

    let metrics = metrics[0].snapshot();
    assert_eq!(metrics.reader.files_planned, 2);
    assert_eq!(metrics.reader.file_tasks_started, 1);
    assert_eq!(metrics.reader.file_tasks_completed, 1);
    assert_eq!(metrics.reader.rows_produced, 2);
    assert_eq!(metrics.dynamic_filters_received, 1);
    assert_eq!(metrics.dynamic_filters_accepted, 1);
    assert_eq!(metrics.dynamic_filters_unsupported, 0);
    assert_eq!(metrics.dynamic_partition_tasks_pruned, 1);
    assert_eq!(metrics.dynamic_partition_tasks_kept, 1);
    assert_eq!(
        metrics.reader.files_planned,
        metrics
            .reader
            .file_tasks_started
            .saturating_add(metrics.dynamic_partition_tasks_pruned)
    );
    Ok(())
}

#[tokio::test]
async fn optimizer_keeps_limit_above_delta_kernel_residual() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-residual-limit")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let execution_options =
        DeltaReaderExecutionOptions::new().with_reader_backend(ParquetReaderBackend::DeltaKernel);
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_table(
        &context,
        "orders",
        table.clone(),
        ScanOptions {
            execution_options,
            target_partitions: Some(1),
            intra_file_repartitioning: Default::default(),
            use_arrow_view_types: true,
        },
    )?;

    let plan = context
        .sql("SELECT customer_name FROM orders WHERE id > 1 LIMIT 1")
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("fetch=1"), "{display}");
    assert!(display.contains("FilterExec"), "{display}");
    assert!(display.contains("DeltaDataFusionExec"), "{display}");
    let filter = display.find("FilterExec").ok_or("missing FilterExec")?;
    let scan = display
        .find("DeltaDataFusionExec")
        .ok_or("missing DeltaDataFusionExec")?;
    assert!(filter < scan, "{display}");

    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 1);
    assert!(metrics[0].snapshot().use_arrow_view_types);
    assert_eq!(metrics[0].snapshot().reader.estimated_input_rows, Some(3));
    let batches = collect_plan(&context, plan).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("Delta Kernel did not preserve the DataFusion view schema")?;
    assert_eq!(names.iter().collect::<Vec<_>>(), [Some("bob")]);
    assert_eq!(metrics[0].snapshot().reader.rows_produced, 3);

    register_table(
        &context,
        "orders_standard",
        table,
        ScanOptions {
            execution_options,
            target_partitions: Some(1),
            intra_file_repartitioning: Default::default(),
            use_arrow_view_types: false,
        },
    )?;
    let plan = context
        .sql("SELECT customer_name FROM orders_standard WHERE id > 1 LIMIT 1")
        .await?
        .create_physical_plan()
        .await?;
    let metrics = collect_metrics(plan.as_ref());
    let batches = collect_plan(&context, plan).await?;
    assert!(!metrics[0].snapshot().use_arrow_view_types);
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Delta Kernel did not preserve the standard Utf8 schema")?;
    assert_eq!(names.iter().collect::<Vec<_>>(), [Some("bob")]);
    Ok(())
}

#[tokio::test]
async fn direct_scan_decodes_both_string_representations_with_exact_values() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_supported_types("provider-view-values")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_table(&context, "typed", table.clone(), ScanOptions::default())?;

    let plan = context
        .sql("SELECT customer_name, payload, attributes FROM typed")
        .await?
        .create_physical_plan()
        .await?;
    let metrics = collect_metrics(plan.as_ref());
    let batches = collect_plan(&context, plan).await?;
    assert!(metrics[0].snapshot().use_arrow_view_types);
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("customer_name was not Utf8View")?;
    assert_eq!(
        names.iter().collect::<Vec<_>>(),
        [Some("alice"), Some("bob"), None]
    );
    let payloads = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or("payload was not BinaryView")?;
    assert_eq!(
        payloads.iter().collect::<Vec<_>>(),
        [Some(b"alpha".as_slice()), Some(b"beta".as_slice()), None]
    );
    let attributes = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("attributes was not Struct")?;
    let labels = attributes
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("attributes.label was not Utf8View")?;
    assert_eq!(
        labels.iter().collect::<Vec<_>>(),
        [Some("low"), Some("high"), None]
    );

    let standard = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            use_arrow_view_types: false,
            ..Default::default()
        },
    )?;
    assert_eq!(
        standard
            .schema()
            .field_with_name("customer_name")?
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        standard.schema().field_with_name("payload")?.data_type(),
        &DataType::Binary
    );
    context.register_table("typed_standard", Arc::new(standard))?;

    let plan = context
        .sql("SELECT customer_name, payload, attributes FROM typed_standard")
        .await?
        .create_physical_plan()
        .await?;
    let metrics = collect_metrics(plan.as_ref());
    let batches = collect_plan(&context, plan).await?;
    assert!(!metrics[0].snapshot().use_arrow_view_types);
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("customer_name was not Utf8")?;
    assert_eq!(
        names.iter().collect::<Vec<_>>(),
        [Some("alice"), Some("bob"), None]
    );
    let payloads = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or("payload was not Binary")?;
    assert_eq!(
        payloads.iter().collect::<Vec<_>>(),
        [Some(b"alpha".as_slice()), Some(b"beta".as_slice()), None]
    );
    let attributes = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("attributes was not Struct")?;
    let labels = attributes
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("attributes.label was not Utf8")?;
    assert_eq!(
        labels.iter().collect::<Vec<_>>(),
        [Some("low"), Some("high"), None]
    );
    Ok(())
}

#[tokio::test]
async fn direct_scan_preserves_views_through_nested_map_reordering() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
        "provider-reordered-map-views",
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_table(&context, "mapped", table, ScanOptions::default())?;

    let batches = context
        .sql("SELECT attributes FROM mapped")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let attributes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or("attributes was not Map")?;
    let keys = attributes
        .keys()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("map keys were not Utf8View")?;
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        [Some("home"), Some("work"), Some("mailing")]
    );
    let values = attributes
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("map values were not Struct")?;
    assert_eq!(values.fields()[0].name(), "zip");
    assert_eq!(values.fields()[1].name(), "city");
    let zips = values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("map value zip was not Int32")?;
    assert_eq!(
        zips.iter().collect::<Vec<_>>(),
        [Some(94110), Some(10001), None]
    );
    let cities = values
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or("map value city was not Utf8View")?;
    assert_eq!(
        cities.iter().collect::<Vec<_>>(),
        [Some("san francisco"), Some("new york"), Some("phoenix")]
    );
    Ok(())
}

#[tokio::test]
async fn joined_delta_scans_keep_distinct_metrics_and_limit_above_join() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("provider-joined-scans")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy().into_owned())
        .load_table()
        .await?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    register_table(&context, "orders", table.clone(), ScanOptions::default())?;
    register_table(&context, "customers", table, ScanOptions::default())?;

    let star = context.sql("SELECT * FROM orders").await?;
    assert_eq!(star.schema().fields().len(), 2);
    assert_eq!(star.schema().field(0).name(), "id");
    assert_eq!(star.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(star.schema().field(1).name(), "customer_name");
    assert_eq!(star.schema().field(1).data_type(), &DataType::Utf8View);
    let projected = context
        .sql("SELECT customer_name FROM orders")
        .await?
        .into_optimized_plan()?;
    assert_eq!(projected.schema().fields().len(), 1);
    assert_eq!(projected.schema().field(0).name(), "customer_name");
    assert_eq!(projected.schema().field(0).data_type(), &DataType::Utf8View);

    let plan = context
        .sql(
            "SELECT orders.id FROM orders \
             JOIN customers ON orders.id = customers.id LIMIT 1",
        )
        .await?
        .create_physical_plan()
        .await?;
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("HashJoinExec"), "{display}");
    assert!(display.contains("fetch=1"), "{display}");
    let metrics = collect_metrics(plan.as_ref());
    assert_eq!(metrics.len(), 2);
    let mut names = metrics
        .iter()
        .map(|metrics| metrics.registration_name().unwrap_or_default())
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["customers", "orders"]);
    assert_eq!(ids(&collect_plan(&context, plan).await?), [1]);
    assert!(
        metrics
            .iter()
            .all(|metrics| metrics.snapshot().reader.file_tasks_started == 1)
    );
    Ok(())
}
