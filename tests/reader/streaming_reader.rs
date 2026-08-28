//! Integration tests for the streaming reader and its Parquet backends.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{Float64Array, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use delta_arrow_reader::ParquetReaderBackend;
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaReaderPhase, DeltaScalar, DeltaSnapshotSelection,
    DeltaStorageOptions,
};
use delta_arrow_reader::{
    DeltaScan, DeltaScanExecutionOptions, DeltaScanMetrics, DeltaTableBuilder,
};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct TestTable(PathBuf);

impl TestTable {
    fn two_versions(name: &str) -> TestResult<Self> {
        let table = Self::empty(name)?;
        let first = table.write_parquet(
            "part-0.parquet",
            &[1, 2, 3, 4],
            &[Some("a"), Some("b"), None, Some("d")],
            &[-0.0, 0.0, 1.5, 2.5],
        )?;
        let second = table.write_parquet(
            "part-1.parquet",
            &[5, 6, 7, 8],
            &[Some("e"), Some("f"), Some("g"), Some("h")],
            &[3.5, 4.5, 5.5, 6.5],
        )?;
        table.write_log(
            0,
            &[protocol(1), metadata(), add("part-0.parquet", first, 1, 4)],
        )?;
        table.write_log(1, &[add("part-1.parquet", second, 5, 8)])?;
        Ok(table)
    }

    fn unsupported(name: &str) -> TestResult<Self> {
        let table = Self::empty(name)?;
        table.write_log(0, &[protocol(4), metadata()])?;
        Ok(table)
    }

    fn missing_data_file(name: &str) -> TestResult<Self> {
        let table = Self::empty(name)?;
        table.write_log(
            0,
            &[protocol(1), metadata(), add("missing.parquet", 100, 1, 1)],
        )?;
        Ok(table)
    }

    fn malformed_add(name: &str, invalid_size: &str) -> TestResult<Self> {
        let table = Self::empty(name)?;
        table.write_log(
            0,
            &[
                protocol(1),
                metadata(),
                json!({
                    "add": {
                        "path": "secret.parquet",
                        "partitionValues": {},
                        "size": invalid_size,
                        "modificationTime": 1587968586000_i64,
                        "dataChange": true
                    }
                }),
            ],
        )?;
        Ok(table)
    }

    fn empty(name: &str) -> TestResult<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = Path::new("target")
            .join("delta-arrow-reader-streaming-tests")
            .join(format!("{}-{name}-{nonce}", std::process::id()));
        fs::create_dir_all(path.join("_delta_log"))?;
        Ok(Self(path))
    }

    fn uri(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }

    fn normalized_uri(&self) -> TestResult<String> {
        url::Url::from_directory_path(fs::canonicalize(&self.0)?)
            .map(|url| url.into())
            .map_err(|()| "test path cannot become a file URL".into())
    }

    fn disable_delta_log(&self) -> TestResult {
        fs::rename(self.0.join("_delta_log"), self.0.join("disabled-log"))?;
        Ok(())
    }

    fn write_parquet(
        &self,
        name: &str,
        ids: &[i32],
        labels: &[Option<&str>],
        scores: &[f64],
    ) -> TestResult<u64> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("score", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(labels.to_vec())),
                Arc::new(Float64Array::from(scores.to_vec())),
            ],
        )?;
        let path = self.0.join(name);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(fs::metadata(path)?.len())
    }

    fn write_log(&self, version: u64, actions: &[Value]) -> TestResult {
        let contents = actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            self.0
                .join("_delta_log")
                .join(format!("{version:020}.json")),
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
            {"name": "label", "type": "string", "nullable": true, "metadata": {}},
            {"name": "score", "type": "double", "nullable": false, "metadata": {}}
        ]
    });
    json!({
        "metaData": {
            "id": "delta-arrow-reader-streaming-test",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495_i64
        }
    })
}

fn add(path: &str, size: u64, min_id: i32, max_id: i32) -> Value {
    let stats = json!({
        "numRecords": 4,
        "minValues": {"id": min_id},
        "maxValues": {"id": max_id},
        "nullCount": {"id": 0}
    });
    json!({
        "add": {
            "path": path,
            "partitionValues": {},
            "size": size,
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "stats": stats.to_string()
        }
    })
}

fn runtime() -> TestResult<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

async fn collect_scan(scan: DeltaScan) -> TestResult<(Vec<RecordBatch>, DeltaScanMetrics)> {
    let stream = scan.into_stream();
    let metrics = stream.metrics();
    let batches = stream.try_collect().await?;
    Ok((batches, metrics))
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

fn sorted_ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut values = ids(batches);
    values.sort_unstable();
    values
}

fn labels(batches: &[RecordBatch]) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(batch.schema().index_of("label").expect("label column"))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 label")
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn table_loads_versions_and_public_state_is_redacted() -> TestResult {
    let fixture = TestTable::two_versions("load")?;
    let secret_uri = fixture.uri();
    let mut storage_options = DeltaStorageOptions::new();
    storage_options.insert("secret-token".into(), "never-print-this".into());
    let builder_debug = format!(
        "{:?}",
        DeltaTableBuilder::new(&secret_uri).with_storage_options(storage_options)
    );
    assert!(!builder_debug.contains(&secret_uri));
    assert!(!builder_debug.contains("secret-token"));
    assert!(!builder_debug.contains("never-print-this"));

    let runtime = runtime()?;
    let latest = runtime.block_on(DeltaTableBuilder::new(&secret_uri).load_table())?;
    let fixed_snapshot = runtime.block_on(
        DeltaTableBuilder::new(&secret_uri)
            .with_snapshot_selection(DeltaSnapshotSelection::Version(0))
            .load_snapshot(),
    )?;
    assert_eq!(fixed_snapshot.version(), 0);
    assert_eq!(fixed_snapshot.table_url(), fixture.normalized_uri()?);
    fixed_snapshot.validate_protocol()?;
    assert!(!format!("{fixed_snapshot:?}").contains(&secret_uri));

    let fixed = fixed_snapshot.into_table()?;
    let cloned = latest.clone();

    assert_eq!(latest.version(), 1);
    assert_eq!(fixed.version(), 0);
    assert_eq!(latest.table_url(), fixture.normalized_uri()?);
    assert!(Arc::ptr_eq(&latest.schema(), &cloned.schema()));
    latest.validate_protocol()?;
    assert!(!format!("{latest:?}").contains(&secret_uri));
    Ok(())
}

#[test]
fn local_end_to_end_example_reads_without_sql() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("local-example")?;
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_snapshot_selection(DeltaSnapshotSelection::Version(0))
            .load_table()
            .await?;
        let scan = table
            .scan()
            .with_projection(["id", "label"])
            .with_limit(3)
            .build()
            .await?;
        let (batches, _) = collect_scan(scan).await?;

        assert_eq!(ids(&batches), [1, 2, 3]);
        println!("read 3 rows from deterministic Delta snapshot 0");
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn eager_scan_metadata_plans_repeated_queries_without_the_delta_log() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("eager-scan-metadata")?;
        let eager = DeltaTableBuilder::new(fixture.uri())
            .load_table_with_eager_scan_metadata()
            .await?;
        let lazy = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let low_ids = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::LtEq,
            value: DeltaScalar::Int32(4),
        };
        let high_ids = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(4),
        };

        let (expected_low, _) =
            collect_scan(lazy.scan().with_predicate(low_ids.clone()).build().await?).await?;
        let (expected_high, _) =
            collect_scan(lazy.scan().with_predicate(high_ids.clone()).build().await?).await?;

        fixture.disable_delta_log()?;

        let (actual_low, low_metrics) =
            collect_scan(eager.scan().with_predicate(low_ids).build().await?).await?;
        let (actual_high, high_metrics) =
            collect_scan(eager.scan().with_predicate(high_ids).build().await?).await?;

        assert_eq!(sorted_ids(&actual_low), sorted_ids(&expected_low));
        assert_eq!(sorted_ids(&actual_high), sorted_ids(&expected_high));
        assert_eq!(low_metrics.snapshot().files_planned, 1);
        assert_eq!(high_metrics.snapshot().files_planned, 1);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn unsupported_protocol_is_inspectable_but_never_scannable() -> TestResult {
    let fixture = TestTable::unsupported("unsupported")?;
    let runtime = runtime()?;
    let table = runtime.block_on(DeltaTableBuilder::new(fixture.uri()).load_table())?;

    assert_eq!(table.version(), 0);
    assert_eq!(table.protocol().min_reader_version(), 4);
    let validation = table.validate_protocol().expect_err("protocol must fail");
    assert_eq!(validation.phase(), DeltaReaderPhase::Protocol);
    let build = runtime.block_on(table.scan().build());
    let error = match build {
        Ok(_) => panic!("unsupported protocol built a scan"),
        Err(error) => error,
    };
    assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
    let build = runtime.block_on(table.scan().with_projection(["missing"]).build());
    let error = match build {
        Ok(_) => panic!("unsupported protocol built another scan"),
        Err(error) => error,
    };
    assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
    Ok(())
}

#[test]
fn eager_metadata_failure_returns_no_table_and_redacts_the_source() -> TestResult {
    const INVALID_SIZE: &str = "secret-not-a-file-size";
    let fixture = TestTable::malformed_add("eager-failure", INVALID_SIZE)?;
    let runtime = runtime()?;

    let lazy = runtime.block_on(DeltaTableBuilder::new(fixture.uri()).load_table())?;
    assert_eq!(lazy.version(), 0);
    let error = match runtime
        .block_on(DeltaTableBuilder::new(fixture.uri()).load_table_with_eager_scan_metadata())
    {
        Ok(_) => return Err("malformed eager metadata should not return a table".into()),
        Err(error) => error,
    };

    assert_eq!(error.code(), "scan_planning");
    assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
    assert!(
        error
            .to_string()
            .contains("eager_scan_metadata_materialization_failed")
    );
    assert!(error.source().is_some());
    assert!(!error.to_string().contains(INVALID_SIZE));
    assert!(!format!("{error:?}").contains(INVALID_SIZE));
    Ok(())
}

#[test]
fn projection_predicate_limit_partition_and_metrics_contracts_hold() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("scan")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;

        let full_scan = table.scan().with_target_partitions(2)?.build().await?;
        assert_eq!(full_scan.partition_count(), 2);
        assert_eq!(
            full_scan
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "label", "score"]
        );
        let (full_batches, full_metrics) = collect_scan(full_scan).await?;
        let full_ids = ids(&full_batches);
        assert_eq!(full_ids.len(), 8);
        assert_eq!(full_metrics.snapshot().files_planned, 2);
        assert_eq!(full_metrics.snapshot().file_tasks_completed, 2);
        assert_eq!(full_metrics.snapshot().scheduler_rows_emitted, 8);

        let (repeat_batches, _) =
            collect_scan(table.scan().with_target_partitions(2)?.build().await?).await?;
        assert_eq!(ids(&repeat_batches), full_ids);

        let ordered = table
            .scan()
            .with_projection(["label", "id"])
            .build()
            .await?;
        assert_eq!(
            ordered
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["label", "id"]
        );
        let (ordered_batches, _) = collect_scan(ordered).await?;
        assert_eq!(ids(&ordered_batches), full_ids);

        let empty = table
            .scan()
            .with_projection(Vec::<&str>::new())
            .build()
            .await?;
        assert!(empty.schema().fields().is_empty());
        let (empty_batches, _) = collect_scan(empty).await?;
        assert!(empty_batches.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(
            empty_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            8
        );

        for invalid in [vec!["id", "id"], vec!["missing"]] {
            let result = table.scan().with_projection(invalid).build().await;
            let error = match result {
                Ok(_) => panic!("invalid projection built a scan"),
                Err(error) => error,
            };
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert_eq!(error.code(), "invalid_projection");
        }

        let hidden_predicate = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(4),
        };
        let hidden = table
            .scan()
            .with_projection(["label"])
            .with_predicate(hidden_predicate.clone())
            .build()
            .await?;
        assert_eq!(hidden.schema().fields().len(), 1);
        let (hidden_batches, _) = collect_scan(hidden).await?;
        assert_eq!(
            labels(&hidden_batches),
            ["e", "f", "g", "h"]
                .into_iter()
                .map(|value| Some(value.to_owned()))
                .collect::<Vec<_>>()
        );

        let empty_filtered = table
            .scan()
            .with_projection(Vec::<&str>::new())
            .with_predicate(hidden_predicate)
            .build()
            .await?;
        let (empty_filtered_batches, _) = collect_scan(empty_filtered).await?;
        assert!(
            empty_filtered_batches
                .iter()
                .all(|batch| batch.num_columns() == 0)
        );
        assert_eq!(
            empty_filtered_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let signed_zero = table
            .scan()
            .with_projection(["id"])
            .with_predicate(DeltaPredicate::Compare {
                column: "score".into(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Float64(-0.0),
            })
            .build()
            .await?;
        let (signed_zero_batches, signed_zero_metrics) = collect_scan(signed_zero).await?;
        assert_eq!(ids(&signed_zero_batches), [1]);
        assert_eq!(signed_zero_metrics.snapshot().scheduler_rows_emitted, 8);

        for limit in [1, 3, 5, 8, 20] {
            let (batches, _) = collect_scan(
                table
                    .scan()
                    .with_target_partitions(2)?
                    .with_limit(limit)
                    .build()
                    .await?,
            )
            .await?;
            assert_eq!(ids(&batches), full_ids[..full_ids.len().min(limit)]);
        }

        let zero = table.scan().with_limit(0).build().await?;
        let stream = zero.into_stream();
        let zero_metrics = stream.metrics();
        let zero_batches: Vec<RecordBatch> = stream.try_collect().await?;
        assert!(zero_batches.is_empty());
        let zero_snapshot = zero_metrics.snapshot();
        assert_eq!(zero_snapshot.file_tasks_started, 0);
        assert_eq!(zero_snapshot.scheduler_batches_emitted, 0);

        let early_options = DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_batches_per_partition(1)?;
        let early = table
            .scan()
            .with_target_partitions(1)?
            .with_execution_options(early_options)
            .with_limit(1)
            .build()
            .await?;
        let (early_batches, early_metrics) = collect_scan(early).await?;
        assert_eq!(ids(&early_batches), full_ids[..1]);
        let early_snapshot = early_metrics.snapshot();
        assert_eq!(early_snapshot.files_planned, 2);
        assert_eq!(early_snapshot.file_tasks_started, 1);
        assert_eq!(early_snapshot.file_tasks_completed, 0);
        assert_eq!(early_snapshot.scheduler_batches_emitted, 1);
        assert_eq!(early_snapshot.scheduler_rows_emitted, 2);
        tokio::task::yield_now().await;
        let after_yield = early_metrics.snapshot();
        assert_eq!(
            after_yield.file_tasks_started,
            early_snapshot.file_tasks_started
        );
        assert_eq!(
            after_yield.scheduler_batches_emitted,
            early_snapshot.scheduler_batches_emitted
        );
        assert_eq!(
            after_yield.scheduler_rows_emitted,
            early_snapshot.scheduler_rows_emitted
        );

        let error = match table.scan().with_target_partitions(0) {
            Ok(_) => panic!("zero partition target was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn stream_is_pull_driven_reports_one_error_and_retains_drop_metrics() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("drop")?;
        let options = DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(1)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_batches_per_partition(1)?;
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_execution_options(options)
            .load_table()
            .await?;

        let idle = table.scan().with_target_partitions(1)?.build().await?;
        let idle_stream = idle.into_stream();
        let idle_metrics = idle_stream.metrics();
        assert_eq!(idle_metrics.snapshot().file_tasks_started, 0);
        drop(idle_stream);
        tokio::task::yield_now().await;
        assert_eq!(idle_metrics.snapshot().file_tasks_started, 0);

        let partial = table.scan().with_target_partitions(1)?.build().await?;
        let mut partial_stream = partial.into_stream();
        let partial_metrics = partial_stream.metrics();
        let first = partial_stream.next().await.expect("first batch")?;
        assert!(first.num_rows() > 0);
        drop(partial_stream);
        tokio::task::yield_now().await;
        let snapshot = partial_metrics.snapshot();
        assert!(snapshot.file_tasks_started >= 1);
        assert!(snapshot.scheduler_rows_emitted >= u64::try_from(first.num_rows())?);

        let missing = TestTable::missing_data_file("error")?;
        let table = DeltaTableBuilder::new(missing.uri()).load_table().await?;
        let scan = table.scan().with_target_partitions(1)?.build().await?;
        let mut stream = scan.into_stream();
        let metrics = stream.metrics();
        let error = stream
            .next()
            .await
            .expect("one error item")
            .expect_err("missing file must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::DataFileRead);
        assert!(stream.next().await.is_none());
        assert_eq!(metrics.snapshot().file_tasks_started, 1);
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn eager_metadata_preserves_direct_and_delta_kernel_results_without_the_log() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("backend-parity")?;
        let direct = DeltaTableBuilder::new(fixture.uri())
            .load_table_with_eager_scan_metadata()
            .await?;
        let kernel = DeltaTableBuilder::new(fixture.uri())
            .with_execution_options(
                DeltaScanExecutionOptions::new()
                    .with_parquet_backend(ParquetReaderBackend::DeltaKernel),
            )
            .load_table_with_eager_scan_metadata()
            .await?;
        fixture.disable_delta_log()?;
        let kernel_options = DeltaScanExecutionOptions::new()
            .with_parquet_backend(ParquetReaderBackend::DeltaKernel);
        let predicate = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::GtEq,
            value: DeltaScalar::Int32(5),
        };
        let direct_scan = direct
            .scan()
            .with_projection(["id"])
            .with_predicate(predicate.clone())
            .with_target_partitions(2)?
            .build()
            .await?;
        let kernel_scan = kernel
            .scan()
            .with_projection(["id"])
            .with_predicate(predicate)
            .with_target_partitions(2)?
            .build()
            .await?;
        let (direct_batches, direct_metrics) = collect_scan(direct_scan).await?;
        let (kernel_batches, kernel_metrics) = collect_scan(kernel_scan).await?;

        assert_eq!(sorted_ids(&direct_batches), [5, 6, 7, 8]);
        assert_eq!(sorted_ids(&kernel_batches), sorted_ids(&direct_batches));
        assert_eq!(direct_metrics.snapshot().files_planned, 1);
        assert_eq!(kernel_metrics.snapshot().files_planned, 1);
        assert_eq!(
            direct_metrics.snapshot().parquet_backend,
            ParquetReaderBackend::Direct
        );
        assert_eq!(
            kernel_metrics.snapshot().parquet_backend,
            ParquetReaderBackend::DeltaKernel
        );

        let residual = DeltaPredicate::Compare {
            column: "score".into(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Float64(-0.0),
        };
        let direct_residual = direct
            .scan()
            .with_projection(["id"])
            .with_predicate(residual.clone())
            .build()
            .await?;
        let kernel_residual = kernel
            .scan()
            .with_projection(["id"])
            .with_predicate(residual)
            .build()
            .await?;
        let (direct_residual, direct_residual_metrics) = collect_scan(direct_residual).await?;
        let (kernel_residual, kernel_residual_metrics) = collect_scan(kernel_residual).await?;
        assert_eq!(sorted_ids(&direct_residual), [1]);
        assert_eq!(sorted_ids(&kernel_residual), sorted_ids(&direct_residual));
        assert_eq!(direct_residual_metrics.snapshot().scheduler_rows_emitted, 8);
        assert_eq!(kernel_residual_metrics.snapshot().scheduler_rows_emitted, 8);

        let per_scan_override = direct
            .scan()
            .with_projection(["id"])
            .with_execution_options(kernel_options)
            .build()
            .await?;
        let (override_batches, override_metrics) = collect_scan(per_scan_override).await?;
        assert_eq!(sorted_ids(&override_batches), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            override_metrics.snapshot().parquet_backend,
            ParquetReaderBackend::DeltaKernel
        );
        Ok::<_, Box<dyn Error>>(())
    })
}

#[test]
fn delta_kernel_reads_through_the_streaming_surface() -> TestResult {
    runtime()?.block_on(async {
        let fixture = TestTable::two_versions("kernel-streaming")?;
        let options = DeltaScanExecutionOptions::new()
            .with_parquet_backend(ParquetReaderBackend::DeltaKernel);
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_execution_options(options)
            .load_table()
            .await?;
        let scan = table
            .scan()
            .with_projection(["id"])
            .with_target_partitions(2)?
            .build()
            .await?;
        let (batches, metrics) = collect_scan(scan).await?;

        assert_eq!(ids(&batches).len(), 8);
        assert_eq!(
            metrics.snapshot().parquet_backend,
            ParquetReaderBackend::DeltaKernel
        );
        Ok::<_, Box<dyn Error>>(())
    })
}
