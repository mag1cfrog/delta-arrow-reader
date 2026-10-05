//! Public API regression for a foreign URL aliasing a readable local Parquet file.

use std::{
    error::Error,
    fmt, fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{array::Int32Array, record_batch::RecordBatch};
use async_trait::async_trait;
use delta_arrow_reader::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaTableBuilder, ParquetReaderBackend,
};
use futures_util::{TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    local::LocalFileSystem, path::Path as StorePath,
};
use serde_json::Value;
use url::Url;

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn fixture(name: &str, foreign: bool) -> TestResult<RealParquetDeltaTable> {
    let table = RealParquetDeltaTable::new_default(name)?;
    let file = fs::canonicalize(table.path().join(table.data_file_path()))?;
    let local = Url::from_file_path(file).map_err(|()| "invalid file URL")?;
    let location = if foreign {
        format!("s3://secret-bucket{}?secret-token", local.path())
    } else {
        local.to_string()
    };
    set_data_file_path(&table, &location)?;
    Ok(table)
}

fn set_data_file_path(table: &RealParquetDeltaTable, location: &str) -> TestResult {
    let log = table.path().join("_delta_log/00000000000000000001.json");
    let mut actions = fs::read_to_string(&log)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let add = actions
        .iter_mut()
        .find_map(|action| action.get_mut("add"))
        .ok_or("missing add action")?;
    add["path"] = Value::String(location.to_owned());
    fs::write(
        log,
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    Ok(())
}

fn assert_ids(batches: &[RecordBatch]) -> TestResult {
    let mut ids = Vec::new();
    for batch in batches {
        ids.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("id type")?
                .values(),
        );
    }
    assert_eq!(ids, [1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn data_file_location_streaming_rejects_foreign_url_with_readable_local_key() -> TestResult {
    for (backend, buffered) in [
        (ParquetReaderBackend::Direct, false),
        (ParquetReaderBackend::Direct, true),
        (ParquetReaderBackend::DeltaKernel, false),
    ] {
        for foreign in [false, true] {
            let fixture = fixture(&format!("streaming-location-{buffered}-{foreign}"), foreign)?;
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_backend(backend)
                .with_parquet_full_file_read_threshold_bytes(
                    buffered.then_some(usize::try_from(fixture.data_file_size())?),
                )?;
            let scan = table.scan().with_execution_options(options).build().await?;
            let stream = scan.into_stream();
            let metrics = stream.metrics();
            let result = stream.try_collect::<Vec<_>>().await;
            if foreign {
                let error = result.err().ok_or("foreign URL returned local data")?;
                assert!(matches!(
                    error,
                    DeltaReaderError::DataFileRead {
                        reason: "data_file_store_mismatch",
                        ..
                    }
                ));
                assert!(!format!("{error} {error:?}").contains("secret"));
                let metrics = metrics.snapshot();
                if backend == ParquetReaderBackend::Direct {
                    assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
                    assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
                }
                assert_eq!(metrics.scheduler_rows_emitted, 0);
            } else {
                assert_ids(&result?)?;
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn data_file_location_datafusion_rejects_foreign_url_with_readable_local_key() -> TestResult {
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader::datafusion::{ScanOptions, register_table};

    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for foreign in [false, true] {
            let fixture = fixture(&format!("datafusion-location-{foreign}"), foreign)?;
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            let context = SessionContext::new();
            register_table(
                &context,
                "orders",
                table,
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend),
                    ..Default::default()
                },
            )?;
            let result = context.sql("SELECT id FROM orders").await?.collect().await;
            if foreign {
                let error = result
                    .err()
                    .ok_or("foreign URL returned local data through DataFusion")?;
                assert!(error.to_string().contains("data_file_store_mismatch"));
                assert!(!format!("{error} {error:?}").contains("secret"));
            } else {
                assert_ids(&result?)?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn data_file_location_checks_the_store_before_data_io_in_both_backends() -> TestResult {
    let fixture = RealParquetDeltaTable::new_default("location-store-spy")?;
    let store = Arc::new(DataReadStore {
        inner: LocalFileSystem::new_with_prefix(fixture.path())?,
        data_gets: AtomicUsize::new(0),
    });
    let scheme = format!("darlocation{}", std::process::id());
    let weak = Arc::downgrade(&store);
    delta_kernel_default_engine::storage::insert_url_handler(
        &scheme,
        Arc::new(move |_, _| {
            let store = weak.upgrade().ok_or_else(|| object_store::Error::Generic {
                store: "data-file-location-test-store",
                source: std::io::Error::other("store was released").into(),
            })?;
            Ok((Box::new(store), StorePath::ROOT))
        }),
    )?;
    let table_url = format!("{scheme}://table/");
    let file = fixture.data_file_path();
    fs::create_dir(fixture.path().join("a b"))?;
    fs::copy(
        fixture.path().join(file),
        fixture.path().join("a b/%2F#.parquet"),
    )?;
    let cases = [
        (file.to_owned(), true),
        (
            format!("{table_url}{file}?secret-token#secret-fragment"),
            true,
        ),
        ("a%20b/%252F%23.parquet".to_owned(), true),
        (format!("{table_url}a%20b/%252F%23.parquet"), true),
        (
            format!("{scheme}://secret-other/{file}?secret-token"),
            false,
        ),
        (format!("s3://secret-bucket/{file}?secret-token"), false),
        (
            format!("{scheme}://secret-user:secret-password@table/{file}"),
            false,
        ),
    ];
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (path, accepted) in &cases {
            set_data_file_path(&fixture, path)?;
            let table = DeltaTableBuilder::new(&table_url).load_table().await?;
            let scan = table
                .scan()
                .with_execution_options(
                    DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                )
                .build()
                .await?;
            let before = store.data_gets.load(Ordering::Relaxed);
            let result = scan.into_stream().try_collect::<Vec<_>>().await;
            let after = store.data_gets.load(Ordering::Relaxed);
            if *accepted {
                assert_ids(&result?)?;
                assert!(after > before, "spy must observe {backend:?} data reads");
            } else {
                assert_eq!(before, after, "{backend:?} read a foreign data file");
                let error = result.err().ok_or("foreign URL returned local data")?;
                assert!(matches!(
                    error,
                    DeltaReaderError::DataFileRead {
                        reason: "data_file_store_mismatch",
                        ..
                    }
                ));
                let mut source: Option<&dyn Error> = Some(&error);
                while let Some(error) = source {
                    assert!(!format!("{error} {error:?}").contains("secret"));
                    source = error.source();
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct DataReadStore {
    inner: LocalFileSystem,
    data_gets: AtomicUsize,
}

impl fmt::Display for DataReadStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("data-file-location-test-store")
    }
}

#[async_trait]
impl ObjectStore for DataReadStore {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        if path.as_ref().ends_with(".parquet") {
            self.data_gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
