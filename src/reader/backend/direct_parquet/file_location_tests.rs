//! Regression tests for data-file storage identity, before any table-store I/O.

use std::{error::Error, sync::Arc};

use arrow::{
    array::Int32Array,
    datatypes::{DataType, Field, Schema},
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use url::Url;

use super::{
    DirectParquetReader, MeteredParquetObjectStore, MultiRangeReadStrategy,
    tests::{metrics, task},
};
use crate::{DeltaReaderError, DeltaScanExecutionOptions, delta::kernel::DeltaKernelEngineContext};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test]
async fn data_file_location_alias_reads_the_declared_object_not_a_prefixed_decoy() -> TestResult {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let correct = super::tests::parquet_bytes_for(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![777]))],
    )?;
    let decoy = super::tests::parquet_bytes_for(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![888]))],
    )?;
    assert_eq!(
        correct.len(),
        decoy.len(),
        "decoy must also pass the size check"
    );
    let size = u64::try_from(correct.len())?;
    let cases = [
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            "https://account.blob.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
            "container/other/part.parquet",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            "https://account.dfs.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
            "container/other/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "https://account.blob.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
            "container/other/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "//account.blob.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
            "container/other/part.parquet",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            "abfs://container@account.blob.core.windows.net/other/part.parquet",
            "other/part.parquet",
            "part.parquet",
        ),
        (
            "abfss://workspace@account.dfs.fabric.microsoft.com/table/",
            "https://account.blob.fabric.microsoft.com/workspace/other/part.parquet",
            "other/part.parquet",
            "workspace/other/part.parquet",
        ),
        (
            "https://s3.us-east-1.amazonaws.com/bucket/table/",
            "https://s3.us-east-1.amazonaws.com/bucket/other/part.parquet",
            "other/part.parquet",
            "bucket/other/part.parquet",
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/table/",
            "https://account.r2.cloudflarestorage.com/bucket/other/part.parquet",
            "other/part.parquet",
            "bucket/other/part.parquet",
        ),
        (
            "https://s3.s3.us-east-1.amazonaws.com/table/",
            "https://s3.s3.us-east-1.amazonaws.com/other/part.parquet",
            "other/part.parquet",
            "part.parquet",
        ),
        (
            "https://example.com/table/",
            "https://example.com/other/part.parquet",
            "other/part.parquet",
            "part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "part.parquet",
            "container/table/part.parquet",
            "table/part.parquet",
        ),
        (
            "https://s3.us-east-1.amazonaws.com/bucket/table/",
            "part.parquet",
            "bucket/table/part.parquet",
            "table/part.parquet",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            "https://account.blob.core.windows.net/container/a%20b/%252F.parquet",
            "a b/%2F.parquet",
            "container/a b/%2F.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "/container/other/part.parquet",
            "container/other/part.parquet",
            "other/part.parquet",
        ),
    ];
    for (table, file, expected_key, decoy_key) in cases {
        for mode in ["ordinary", "ranged", "cached", "buffered"] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            store
                .put(&Path::parse(expected_key)?, correct.clone().into())
                .await?;
            store
                .put(&Path::parse(decoy_key)?, decoy.clone().into())
                .await?;
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(
                    (mode == "buffered").then_some(correct.len()),
                )?;
            let mut reader = reader(table, options)?;
            reader.store = store;
            if mode == "cached" {
                reader = reader.with_metadata_cache(Arc::default());
                // Prime metadata for the wrong object as well. The URL must not
                // reuse that entry just because its unnormalized path collides.
                let mut control = Url::parse("memory:///")?;
                control
                    .path_segments_mut()
                    .map_err(|()| "control URL has no path")?
                    .extend(decoy_key.split('/'));
                let mut control = task(control.path(), Some(size))?;
                control.parquet_byte_range = Some(0..size);
                let mut stream = reader
                    .open_physical_parquet_stream(&control, &schema, Default::default())
                    .await?;
                let batch = stream.next_batch().await?.ok_or("missing decoy batch")?;
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("id type")?
                        .values(),
                    &[888]
                );
            }
            let mut task = task(file, Some(size))?;
            if mode == "ranged" || mode == "cached" {
                task.parquet_byte_range = Some(0..size);
            }
            let mut stream = reader
                .open_physical_parquet_stream(&task, &schema, Default::default())
                .await
                .map_err(|error| {
                    format!(
                        "{table} + {file}, mode={mode}: {error}, source={:?}",
                        error.source()
                    )
                })?;
            let batch = stream.next_batch().await?.ok_or("missing batch")?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("id type")?;
            assert_eq!(ids.values(), &[777], "{table} + {file}, mode={mode}");
            assert!(stream.next_batch().await?.is_none());
        }
    }
    Ok(())
}

fn reader(table: &str, options: DeltaScanExecutionOptions) -> TestResult<DirectParquetReader> {
    // Store construction is offline; the reader's store is replaced below for I/O tests.
    let storage_options = [
        ("skip_signature".to_owned(), "true".to_owned()),
        ("region".to_owned(), "us-east-1".to_owned()),
        ("account_name".to_owned(), "account".to_owned()),
    ]
    .into_iter()
    .collect();
    Ok(DirectParquetReader::new(
        Arc::new(DeltaKernelEngineContext::try_new(
            Url::parse(table)?,
            &storage_options,
        )?),
        options,
        metrics(),
        Arc::default(),
    ))
}

fn assert_foreign(error: &DeltaReaderError) {
    assert!(
        matches!(
            error,
            DeltaReaderError::DataFileRead {
                reason: "data_file_store_mismatch",
                ..
            }
        ),
        "{error}"
    );
    // Check both the public redacted boundary and the diagnostic source chain.
    let mut source: Option<&dyn Error> = Some(error);
    while let Some(error) = source {
        assert!(!format!("{error} {error:?}").contains("secret"));
        source = error.source();
    }
}

#[test]
fn data_file_location_handles_normalization_and_redacts_invalid_paths() -> TestResult {
    use super::file_location::resolve_data_file_path;

    for (table, file, expected) in [
        (
            "https://account.blob.core.windows.net/",
            "part.parquet",
            "part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "../../other/part.parquet",
            "other/part.parquet",
        ),
        (
            "file:///table/",
            "file://localhost/other/a%20b.parquet",
            "other/a b.parquet",
        ),
        (
            "s3://bucket/table/",
            "s3://bucket/other/%252F.parquet",
            "other/%2F.parquet",
        ),
        (
            "s3://bucket/table/",
            "s3://bucket/other%2Fpart.parquet",
            "other/part.parquet",
        ),
        (
            "https://s3bucket.s3.us-east-1.amazonaws.com/table/",
            "../other/part.parquet",
            "other/part.parquet",
        ),
        (
            "custom://account/table/",
            "custom://account/other/part.parquet",
            "other/part.parquet",
        ),
    ] {
        assert_eq!(
            resolve_data_file_path(&Url::parse(table)?, file)?.as_ref(),
            expected
        );
    }

    for (table, file) in [
        (
            "https://account.blob.core.windows.net/container/table/",
            "//account.blob.core.windows.net/secret-container/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            " \t/\n/account.blob.core.windows.net/secret-container/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            r"\\account.blob.core.windows.net/secret-container/part.parquet",
        ),
        ("https://example.com/table/", r"/\secret-host/part.parquet"),
        ("s3://bucket/table/", "s3://bucket-prefix/part.parquet"),
        ("custom://account/table/", "custom://other/part.parquet"),
        ("file:///table/", "file://secret-host/table/part.parquet"),
        (
            "https://account.blob.core.windows.net/container/table/",
            "https://account.blob.core.windows.net//container/part.parquet",
        ),
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "abfss://container@account.dfs.fabric.microsoft.com/table/part.parquet",
        ),
    ] {
        assert_foreign(
            &resolve_data_file_path(&Url::parse(table)?, file)
                .err()
                .ok_or("foreign namespace was accepted")?,
        );
    }

    for file in ["s3://secret-user:secret-password@[", "secret/%00.parquet"] {
        let error = resolve_data_file_path(&Url::parse("s3://bucket/table/")?, file)
            .err()
            .ok_or("invalid URL/key was accepted")?;
        assert!(matches!(
            error,
            DeltaReaderError::DataFileRead {
                reason: "data_file_path_resolution_failed",
                ..
            }
        ));
        let mut source: Option<&dyn Error> = Some(&error);
        while let Some(error) = source {
            assert!(!format!("{error} {error:?}").contains("secret"));
            source = error.source();
        }
    }
    Ok(())
}

#[test]
fn data_file_location_preserves_same_store_paths_and_aliases() -> TestResult {
    for (table, file, expected) in [
        ("s3://bucket/table/", "part.parquet", "table/part.parquet"),
        (
            "https://s3.s3.us-east-1.amazonaws.com/table/",
            "https://s3.s3.us-east-1.amazonaws.com/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "https://account.blob.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "https://account.dfs.core.windows.net/container/table/",
            "abfss://container@account.blob.core.windows.net:443/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "abfss://workspace@account.dfs.fabric.microsoft.com/table/",
            "https://account.blob.fabric.microsoft.com/workspace/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "s3://bucket/table/",
            "../other/part.parquet",
            "other/part.parquet",
        ),
        (
            "s3://bucket/table/",
            "s3a://bucket/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "s3a://bucket/table/",
            "s3://bucket/a%20b/%23%25%3F.parquet",
            "a b/#%?.parquet",
        ),
        (
            "s3://bucket/table/",
            "//bucket/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "gs://bucket/table/",
            "gs://bucket/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "memory:///table/",
            "memory:///other/part.parquet",
            "other/part.parquet",
        ),
        (
            "az://container/table/",
            "azure://container/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "adl://container/table/",
            "abfss://container/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "abfss://container@account.dfs.core.windows.net/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "https://account.blob.core.windows.net/container/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "https://account.dfs.fabric.microsoft.com/workspace/table/",
            "../other/part.parquet",
            "workspace/other/part.parquet",
        ),
        (
            "https://s3.us-east-1.amazonaws.com/bucket/table/",
            "../other/part.parquet",
            "bucket/other/part.parquet",
        ),
        (
            "https://bucket.s3.us-east-1.amazonaws.com/table/",
            "/other/part.parquet",
            "other/part.parquet",
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/table/",
            "../other/part.parquet",
            "bucket/other/part.parquet",
        ),
        (
            "https://EXAMPLE.com:443/table/",
            "https://example.com/other/part.parquet",
            "other/part.parquet",
        ),
    ] {
        let reader = reader(table, DeltaScanExecutionOptions::new())?;
        let object = reader.resolve_parquet_object(&task(file, Some(1))?)?;
        assert_eq!(object.path.as_ref(), expected, "{table} + {file}");
    }
    Ok(())
}

#[tokio::test]
async fn data_file_location_rejects_foreign_stores_before_any_get() -> TestResult {
    let cases = [
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "https://account.blob.core.windows.net/secret-container/table/part.parquet",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            "https://secret-account.blob.core.windows.net/container/table/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "abfss://secret-container@account.dfs.core.windows.net/table/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "https://account.blob.fabric.microsoft.com/container/table/part.parquet",
        ),
        (
            "s3://bucket/table/",
            "s3://secret-bucket/table/part.parquet",
        ),
        ("s3://bucket/table/", "gs://bucket/table/part.parquet"),
        (
            "s3://bucket/table/",
            "s3a://secret-bucket/table/part.parquet",
        ),
        ("s3://bucket/table/", "//secret-bucket/table/part.parquet"),
        (
            "s3://bucket/table/",
            "s3://secret-user:secret-password@secret-bucket/table/part.parquet?secret-token#secret-fragment",
        ),
        (
            "gs://bucket/table/",
            "gs://secret-bucket/table/part.parquet",
        ),
        ("memory:///table/", "s3://secret-bucket/table/part.parquet"),
        (
            "az://container/table/",
            "abfs://secret-container/table/part.parquet",
        ),
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "abfss://secret-container@account.dfs.core.windows.net/table/part.parquet",
        ),
        (
            "abfs://container@account.dfs.core.windows.net/table/",
            "abfss://container@secret-account.dfs.core.windows.net/table/part.parquet",
        ),
        (
            "https://account.blob.core.windows.net/container/table/",
            "https://account.blob.core.windows.net/secret-container/table/part.parquet",
        ),
        (
            "https://account.dfs.fabric.microsoft.com/workspace/table/",
            "https://account.dfs.fabric.microsoft.com/secret-workspace/table/part.parquet",
        ),
        (
            "https://s3.us-east-1.amazonaws.com/bucket/table/",
            "https://s3.us-east-1.amazonaws.com/secret-bucket/table/part.parquet",
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/table/",
            "https://account.r2.cloudflarestorage.com/secret-bucket/table/part.parquet",
        ),
        (
            "https://example.com/table/",
            "https://secret.example.com/table/part.parquet",
        ),
        (
            "https://example.com/table/",
            "http://example.com/table/part.parquet",
        ),
        (
            "https://example.com/table/",
            "https://example.com:444/table/part.parquet",
        ),
    ];
    let bytes = super::tests::parquet_bytes()?;
    let size = u64::try_from(bytes.len())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    for (table, foreign) in cases {
        for mode in ["ordinary", "ranged", "cached", "buffered"] {
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(
                    (mode == "buffered").then_some(bytes.len()),
                )?;
            let mut reader = reader(table, options)?;
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            // Populate exactly the key the broken implementation would read. A missing object
            // must never make this regression pass by accident.
            let old_key = Path::from_url_path(Url::parse(table)?.join(foreign)?.path())?;
            store.put(&old_key, bytes.clone().into()).await?;
            reader.store = Arc::new(MeteredParquetObjectStore::new(
                store,
                reader.metrics.clone(),
                MultiRangeReadStrategy::ChooseAutomatically,
            ));
            let mut file = task(foreign, Some(size))?;
            if mode == "ranged" || mode == "cached" {
                file.parquet_byte_range = Some(0..size);
            }
            if mode == "cached" {
                reader = reader.with_metadata_cache(Arc::default());
                // Prime the cache at the colliding object key, through a legitimate URL.
                let mut control = task(&format!("/{old_key}"), Some(size))?;
                control.parquet_byte_range = Some(0..size);
                let mut stream = reader
                    .open_physical_parquet_stream(&control, &schema, Default::default())
                    .await?;
                let batch = stream.next_batch().await?.ok_or("missing control batch")?;
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("id type")?
                        .values(),
                    &[1, 2, 3]
                );
            }
            let before = reader.metrics.snapshot();
            let result = reader
                .open_physical_parquet_stream(&file, &schema, Default::default())
                .await;
            let after = reader.metrics.snapshot();
            assert_eq!(
                after.parquet_data_file_full_get_operations,
                before.parquet_data_file_full_get_operations,
                "{table} {foreign} {mode}"
            );
            assert_eq!(
                after.parquet_data_file_range_get_operations,
                before.parquet_data_file_range_get_operations,
                "{table} {foreign} {mode}"
            );
            assert_eq!(
                after.parquet_data_file_bytes_received, before.parquet_data_file_bytes_received,
                "{table} {foreign} {mode}"
            );
            let error = result
                .err()
                .ok_or("foreign URL read a same-key object from the table store")?;
            assert_foreign(&error);
        }
    }
    Ok(())
}
