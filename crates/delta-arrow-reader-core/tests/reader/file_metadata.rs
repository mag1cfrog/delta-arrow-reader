//! Source-file metadata stays aligned with rows after the existing reader transforms.

use super::*;
use datafusion::physical_plan::collect;
use delta_arrow_reader::datafusion::DeltaFileMetadataProvider;
use url::Url;

fn metadata_error(error: datafusion::error::DataFusionError) -> Box<dyn Error> {
    std::iter::successors(Some(&error as &dyn Error), |&error| error.source())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
        .into()
}

fn strings(batch: &RecordBatch, column: usize) -> TestResult<&StringViewArray> {
    batch
        .column(column)
        .as_any()
        .downcast_ref()
        .ok_or("expected string views".into())
}

#[tokio::test]
async fn file_metadata_partitioned_multi_file_deletion_vectors_filter_and_group() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_partition_values_and_deletion_vector(
        "origin-partitioned-dv",
    )?;
    let base_uri =
        Url::from_directory_path(fs::canonicalize(fixture.path())?).map_err(|()| "file URI")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for views in [false, true] {
            let base = DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    target_partitions: Some(1),
                    use_arrow_view_types: views,
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend),
                    ..Default::default()
                },
            )?;
            let metadata =
                DeltaFileMetadataProvider::try_new(base.clone(), "source_path", "source_name")?;
            assert_eq!(
                metadata.schema().fields().len(),
                base.schema().fields().len() + 2
            );
            let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
            ctx.register_table("files", Arc::new(metadata))?;
            ctx.register_table("plain", Arc::new(base))?;
            let rows = ctx
                .sql("SELECT source_name, id, source_path FROM files WHERE id >= 2 ORDER BY id")
                .await?
                .collect()
                .await
                .map_err(metadata_error)?;
            let mut actual_ids = Vec::new();
            for batch in &rows {
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("id type")?;
                for row in 0..batch.num_rows() {
                    let id = ids.value(row);
                    actual_ids.push(id);
                    let name = if id == 2 {
                        "part-00001.parquet"
                    } else {
                        "part-00002.parquet"
                    };
                    assert_eq!(strings(batch, 0)?.value(row), name);
                    assert_eq!(strings(batch, 2)?.value(row), base_uri.join(name)?.as_str());
                }
            }
            assert_eq!(actual_ids, [2, 3, 4], "{backend:?}, views={views}");
            let filtered = ctx.sql(&format!("SELECT id FROM files WHERE source_path = '{}' AND region = 'us-east' ORDER BY id", base_uri.join("part-00002.parquet")?)).await?.collect().await?;
            assert_eq!(ids(&filtered), [3, 4]);
            let grouped = ctx.sql("SELECT source_path, count(*) AS n FROM files GROUP BY source_path ORDER BY source_path").await?.collect().await?;
            let groups: Vec<_> = grouped
                .iter()
                .flat_map(|batch| {
                    let counts = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("count type");
                    (0..batch.num_rows()).map(move |i| {
                        (
                            strings(batch, 0).expect("path type").value(i).to_owned(),
                            counts.value(i),
                        )
                    })
                })
                .collect();
            assert_eq!(
                groups,
                [
                    (base_uri.join("part-00001.parquet")?.to_string(), 1),
                    (base_uri.join("part-00002.parquet")?.to_string(), 2)
                ]
            );
            for sql in [
                "SELECT * FROM plain ORDER BY id",
                "SELECT id, region FROM plain WHERE id > 2 ORDER BY id",
                "SELECT count(*) FROM plain",
            ] {
                let expected = ctx.sql(sql).await?.collect().await?;
                let actual = ctx
                    .sql(&sql.replace("plain", "files"))
                    .await?
                    .collect()
                    .await?;
                if sql.starts_with("SELECT *") {
                    let projected: Vec<_> = actual
                        .iter()
                        .map(|batch| batch.project(&[0, 1, 2]))
                        .collect::<Result<_, _>>()?;
                    assert_eq!(
                        arrow::compute::concat_batches(&expected[0].schema(), &projected)?,
                        arrow::compute::concat_batches(&expected[0].schema(), &expected)?
                    );
                } else {
                    assert_eq!(actual, expected);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn file_metadata_mapping_projection_and_empty_tables() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_column_mapping("origin-mapping")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let uri = Url::from_file_path(fs::canonicalize(
        fixture.path().join(fixture.data_file_path()),
    )?)
    .map_err(|()| "file URI")?
    .to_string();
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let base = DeltaTableProvider::try_new(
            table.clone(),
            ScanOptions {
                execution_options: DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                ..Default::default()
            },
        )?;
        for names in [
            ("id", "filename"),
            ("path", "customer_name"),
            ("same", "same"),
            ("", "name"),
        ] {
            assert!(DeltaFileMetadataProvider::try_new(base.clone(), names.0, names.1).is_err());
        }
        let metadata = DeltaFileMetadataProvider::try_new(base, "path", "name")?;
        let ctx = SessionContext::new();
        for projection in [
            vec![3],
            vec![2],
            vec![3, 2],
            vec![3, 0, 2, 1],
            vec![],
            vec![2, 2, 0],
        ] {
            let plan = metadata
                .scan(&ctx.state(), Some(&projection), &[], None)
                .await?;
            assert_eq!(
                plan.schema().as_ref(),
                &metadata.schema().project(&projection)?
            );
            let batches = collect(plan, ctx.task_ctx())
                .await
                .map_err(metadata_error)?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            for batch in &batches {
                for (i, column) in projection.iter().enumerate() {
                    if *column >= 2 {
                        assert!(strings(batch, i)?.iter().all(|v| v
                            == Some(if *column == 2 {
                                &uri
                            } else {
                                fixture.data_file_path()
                            })));
                    }
                }
            }
        }
        assert!(
            metadata
                .scan(&ctx.state(), Some(&vec![4]), &[], None)
                .await
                .is_err()
        );
        ctx.register_table("mapped", Arc::new(metadata))?;
        let result = ctx
            .sql("SELECT id, path FROM mapped WHERE customer_name = 'alice' ORDER BY id")
            .await?
            .collect()
            .await
            .map_err(metadata_error)?;
        assert_eq!(ids(&result), [1]);
    }
    let fixture = TestTable::empty("origin-empty")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let provider = DeltaFileMetadataProvider::try_new(
        DeltaTableProvider::try_new(table, ScanOptions::default())?,
        "path",
        "name",
    )?;
    let ctx = SessionContext::new();
    ctx.register_table("empty", Arc::new(provider))?;
    for sql in [
        "SELECT * FROM empty",
        "SELECT path FROM empty",
        "SELECT name FROM empty",
        "SELECT path FROM empty WHERE id > 0",
    ] {
        assert_eq!(
            ctx.sql(sql)
                .await?
                .collect()
                .await?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }
    Ok(())
}
