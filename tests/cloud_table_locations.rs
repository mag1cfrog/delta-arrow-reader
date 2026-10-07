//! Cloud URL regressions run in their own process because the kernel URL registry
//! is global and these tests replace the built-in cloud handlers.

use std::{error::Error, fs, sync::Arc};

use arrow::array::Int32Array;
use delta_arrow_reader::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaTableBuilder, ParquetReaderBackend,
};
use futures_util::TryStreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, ObjectStoreScheme, local::LocalFileSystem, memory::InMemory,
    path::Path as StorePath,
};
use serde_json::Value;

#[allow(dead_code)]
#[path = "reader/support.rs"]
mod support;
use support::RealParquetDeltaTable;

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn cloud_table_locations_use_bucket_relative_keys_in_both_backends() -> TestResult {
    let store = Arc::new(InMemory::new());
    for scheme in ["https", "s3", "abfss"] {
        let store = Arc::clone(&store);
        delta_kernel_default_engine::storage::insert_url_handler(
            scheme,
            Arc::new(move |url, _| {
                // Use the dependency's URL parser, replacing only cloud I/O.
                let (_, path) = ObjectStoreScheme::parse(url)?;
                Ok((Box::new(Arc::clone(&store)), path))
            }),
        )?;
    }
    let fixture = RealParquetDeltaTable::new_with_deletion_vector("cloud-url-keys", &[1])?;
    let local = LocalFileSystem::new_with_prefix(fixture.path())?;
    let objects = local.list(None).try_collect::<Vec<_>>().await?;
    let dv_file = objects
        .iter()
        .find(|object| object.location.as_ref().ends_with(".bin"))
        .ok_or("missing deletion vector file")?
        .location
        .as_ref();
    // Only correct keys exist. A duplicated namespace or double decoding fails.
    for object in &objects {
        let key = if object.location.as_ref() == fixture.data_file_path() {
            "a b/%2F#.parquet"
        } else {
            object.location.as_ref()
        };
        store
            .put(
                &StorePath::parse(format!("table/{key}"))?,
                local.get(&object.location).await?.bytes().await?.into(),
            )
            .await?;
    }
    let log_key = StorePath::from("table/_delta_log/00000000000000000001.json");
    let original = fs::read_to_string(fixture.path().join("_delta_log/00000000000000000001.json"))?;
    for (table_url, ambiguous_namespace) in [
        ("s3://bucket/table/", false),
        (
            "abfss://container@account.dfs.core.windows.net/table/",
            false,
        ),
        ("https://s3.us-east-1.amazonaws.com/bucket/table/", false),
        ("https://bucket.s3.us-east-1.amazonaws.com/table/", false),
        ("https://s3bucket.s3.us-east-1.amazonaws.com/table/", false),
        ("https://s3.s3.us-east-1.amazonaws.com/table/", false),
        (
            "https://account.blob.core.windows.net/container/table/",
            false,
        ),
        (
            "https://account.dfs.core.windows.net/container/table/",
            false,
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/table/",
            false,
        ),
        ("https://example.com/table/", false),
        ("https://s3.us-east-1.amazonaws.com/bucket//table/", true),
        ("https://s3.us-east-1.amazonaws.com/bucket/%2Ftable/", true),
        (
            "https://account.blob.core.windows.net/container//table/",
            true,
        ),
        (
            "https://account.blob.core.windows.net/container/%2Ftable/",
            true,
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket//table/",
            true,
        ),
        (
            "https://account.r2.cloudflarestorage.com/bucket/%2Ftable/",
            true,
        ),
    ] {
        if ambiguous_namespace {
            let error = DeltaTableBuilder::new(table_url)
                .load_table()
                .await
                .err()
                .ok_or_else(|| format!("ambiguous table URL was accepted: {table_url}"))?;
            assert_eq!(error.code(), "invalid_table_location", "{table_url}");
            continue;
        }
        // Preserve the endpoint for path-style URLs: the bucket/container alone
        // must distinguish a foreign store.
        let foreign_table_url = table_url
            .replace("/bucket/", "/secret-bucket/")
            .replace("/container/", "/secret-container/");
        let foreign_table_url = if foreign_table_url == table_url {
            "s3://secret-bucket/table/".to_owned()
        } else {
            foreign_table_url
        };
        for kind in ["relative", "absolute", "foreign-file", "foreign-dv"] {
            let absolute = kind != "relative";
            let actions = original
                .lines()
                .map(|line| {
                    let mut action: Value = serde_json::from_str(line)?;
                    if let Some(add) = action.get_mut("add") {
                        let file = "a%20b/%252F%23.parquet";
                        add["path"] = Value::String(if kind == "foreign-file" {
                            format!("{foreign_table_url}{file}")
                        } else if absolute {
                            format!("{table_url}{file}?X-Amz-Signature=unused#fragment")
                        } else {
                            file.to_owned()
                        });
                        if absolute {
                            add["deletionVector"]["storageType"] = Value::String("p".to_owned());
                            add["deletionVector"]["pathOrInlineDv"] =
                                Value::String(if kind == "foreign-dv" {
                                    format!("{foreign_table_url}{dv_file}")
                                } else {
                                    format!("{table_url}{dv_file}")
                                });
                        }
                    }
                    Ok(action.to_string())
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()?
                .join("\n");
            store.put(&log_key, actions.into()).await?;
            for backend in [
                ParquetReaderBackend::Direct,
                ParquetReaderBackend::DeltaKernel,
            ] {
                let table = DeltaTableBuilder::new(table_url).load_table().await?;
                assert_eq!(table.table_url(), table_url);
                let table = table.refresh().await?;
                let result = table
                    .scan()
                    .with_projection(["id"])
                    .with_execution_options(
                        DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                    )
                    .build()
                    .await?
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await;
                if kind == "foreign-file" {
                    let error = result.err().ok_or_else(|| {
                        format!("invalid identity returned rows: {table_url}, {backend:?}, {kind}")
                    })?;
                    assert!(
                        matches!(
                            error,
                            DeltaReaderError::DataFileRead {
                                reason: "data_file_store_mismatch",
                                ..
                            }
                        ),
                        "{table_url}, {backend:?}, {kind}: {error:?}"
                    );
                    continue;
                }
                if kind == "foreign-dv" {
                    let error = result.err().ok_or_else(|| {
                        format!("foreign DV returned rows: {table_url}, {backend:?}, {kind}")
                    })?;
                    assert!(
                        matches!(error, DeltaReaderError::DeletionVectorRead { .. }),
                        "{table_url}, {backend:?}, {kind}: {error:?}"
                    );
                    let source = error.source().ok_or_else(|| {
                        format!(
                            "missing DV error source: {table_url}, {backend:?}, {kind}: {error:?}"
                        )
                    })?;
                    assert!(
                        source.to_string().contains(
                            "deletion vector URL does not identify the configured table store"
                        ),
                        "{table_url}, {backend:?}, {kind}: {error:?}"
                    );
                    continue;
                }
                let batches = result?;
                let ids: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                    })
                    .copied()
                    .collect();
                assert_eq!(ids, [1, 3], "{table_url}, {backend:?}, absolute={absolute}");
            }
        }
    }
    Ok(())
}
