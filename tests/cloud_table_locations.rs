//! S3 uses a local endpoint; other cloud URLs replace Kernel's global handlers,
//! so these regressions run in their own process.

use std::{error::Error, fs, sync::Arc};

use arrow::array::Int32Array;
use delta_arrow_reader::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaStorageOptions, DeltaTableBuilder,
    ParquetReaderBackend,
};
use futures_util::TryStreamExt;
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use object_store::{
    GetRange, ObjectStore, ObjectStoreExt, ObjectStoreScheme, client::HttpResponseBody,
    local::LocalFileSystem, memory::InMemory, path::Path as StorePath,
};
use serde_json::Value;
use tokio::{net::TcpListener, task::JoinSet};
use url::Url;

#[allow(dead_code)]
#[path = "reader/support.rs"]
mod support;
use support::RealParquetDeltaTable;

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn cloud_table_locations_use_bucket_relative_keys_in_both_backends() -> TestResult {
    // Keep all cloud handler replacements in one test to avoid registry races.
    cloud_table_locations_with_explicit_namespaces().await?;
    azure_tables_with_container_options().await
}

async fn cloud_table_locations_with_explicit_namespaces() -> TestResult {
    let store = Arc::new(InMemory::new());
    for scheme in ["https", "abfss"] {
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
    // Exercise the SDK-owned S3 path, including its URL encoding and options.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let s3_options = DeltaStorageOptions::from([
        (
            "aws_endpoint".into(),
            format!("http://{}", listener.local_addr()?),
        ),
        ("aws_region".into(), "us-east-1".into()),
        ("aws_allow_http".into(), "true".into()),
        ("aws_skip_signature".into(), "true".into()),
    ]);
    // Dropping the set also stops the listener and its connection tasks on failure.
    let mut server = JoinSet::new();
    server.spawn(serve_s3(listener, Arc::clone(&store)));
    let log_key = StorePath::from("table/_delta_log/00000000000000000001.json");
    let original = fs::read_to_string(fixture.path().join("_delta_log/00000000000000000001.json"))?;
    for (table_url, ambiguous_namespace) in [
        ("s3://bucket/table/", false),
        ("s3a://bucket/table/", false),
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
                let mut builder = DeltaTableBuilder::new(table_url);
                if matches!(Url::parse(table_url)?.scheme(), "s3" | "s3a") {
                    builder = builder.with_storage_options(s3_options.clone());
                }
                let table = builder.load_table().await?;
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
                let mut ids = Vec::new();
                for batch in &batches {
                    let column = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("id column is not Int32")?;
                    ids.extend_from_slice(column.values());
                }
                assert_eq!(ids, [1, 3], "{table_url}, {backend:?}, absolute={absolute}");
            }
        }
    }
    assert!(server.try_join_next().is_none(), "S3 test server stopped");
    Ok(())
}

async fn serve_s3(
    listener: TcpListener,
    store: Arc<InMemory>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                let store = Arc::clone(&store);
                connections.spawn(http1::Builder::new().serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |request| s3_response(request, Arc::clone(&store))),
                ));
            }
            Some(result) = connections.join_next() => { result??; }
        }
    }
}

// Only the read operations exercised by this fixture are needed, not an S3 emulator.
async fn s3_response(
    request: Request<Incoming>,
    store: Arc<InMemory>,
) -> Result<Response<HttpResponseBody>, Box<dyn Error + Send + Sync>> {
    let url = Url::parse(&format!("http://localhost{}", request.uri()))?;
    if url
        .query_pairs()
        .any(|(key, value)| key == "list-type" && value == "2")
    {
        assert_eq!(url.path().trim_end_matches('/'), "/bucket");
        let query = url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let prefix = query.get("prefix").map_or("", |s| s.as_ref());
        let offset = query.get("start-after").map_or("", |s| s.as_ref());
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        let mut xml = String::from("<ListBucketResult><IsTruncated>false</IsTruncated>");
        for object in objects {
            let key = object.location.as_ref();
            if key.starts_with(prefix) && key > offset {
                let key = key
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;");
                xml.push_str(&format!(
                    "<Contents><Key>{key}</Key><LastModified>{}</LastModified><Size>{}</Size></Contents>",
                    object.last_modified.to_rfc3339(), object.size,
                ));
            }
        }
        xml.push_str("</ListBucketResult>");
        return Ok(Response::builder()
            .header("Content-Type", "application/xml")
            .body(xml.into())?);
    }
    let key = StorePath::from_url_path(url.path().strip_prefix("/bucket/").ok_or("wrong bucket")?)?;
    let object = match store.get(&key).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(404)
                .body(bytes::Bytes::new().into())?);
        }
        Err(error) => return Err(error.into()),
    };
    let mut response = Response::builder()
        .header("ETag", object.meta.e_tag.as_deref().unwrap_or_default())
        .header(
            "Last-Modified",
            object
                .meta
                .last_modified
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );
    let mut body = object.bytes().await?;
    if let Some(range) = request.headers().get("Range") {
        let (start, end) = range
            .to_str()?
            .strip_prefix("bytes=")
            .and_then(|s| s.split_once('-'))
            .ok_or("invalid range")?;
        let range = match (start, end) {
            ("", end) => GetRange::Suffix(end.parse()?),
            (start, "") => GetRange::Offset(start.parse()?),
            (start, end) => GetRange::Bounded(
                start.parse()?..end.parse::<u64>()?.checked_add(1).ok_or("range overflow")?,
            ),
        }
        .as_range(body.len() as u64)?;
        response = response.status(206).header(
            "Content-Range",
            format!("bytes {}-{}/{}", range.start, range.end - 1, body.len()),
        );
        body = body.slice(range.start as usize..range.end as usize);
    }
    response = response.header("Content-Length", body.len());
    if request.method() == "HEAD" {
        body = bytes::Bytes::new();
    } else {
        assert_eq!(request.method(), "GET");
    }
    Ok(response.body(body.into())?)
}

async fn azure_tables_with_container_options() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_deletion_vector("azure-container-option", &[1])?;
    let local = LocalFileSystem::new_with_prefix(fixture.path())?;
    let store = Arc::new(InMemory::new());
    let objects = local.list(None).try_collect::<Vec<_>>().await?;
    let dv_file = objects
        .iter()
        .find(|object| object.location.as_ref().ends_with(".bin"))
        .ok_or("missing deletion vector file")?
        .location
        .as_ref();
    for object in &objects {
        store
            .put(
                &object.location,
                local.get(&object.location).await?.bytes().await?.into(),
            )
            .await?;
    }
    let handler_store = Arc::clone(&store);
    delta_kernel_default_engine::storage::insert_url_handler(
        "https",
        Arc::new(move |url, options| {
            // Build the real Azure store to validate URL and option handling;
            // replace cloud I/O with a store containing only container-relative keys.
            let (_, path) = object_store::parse_url_opts(
                url,
                options.into_iter().collect::<DeltaStorageOptions>(),
            )?;
            Ok((Box::new(Arc::clone(&handler_store)), path))
        }),
    )?;
    let log_key = StorePath::from("_delta_log/00000000000000000001.json");
    let original = fs::read_to_string(fixture.path().join(log_key.as_ref()))?;
    for (table_url, container_option) in [
        ("https://account.blob.core.windows.net/", "container_name"),
        (
            "https://account.dfs.core.windows.net/",
            "AZURE_CONTAINER_NAME",
        ),
        (
            "https://account.blob.fabric.microsoft.com/",
            "azure_container_name",
        ),
        (
            "https://account.dfs.fabric.microsoft.com/",
            "container_name",
        ),
    ] {
        let options = DeltaStorageOptions::from([
            ("AZURE_CONTAINER_NAME".to_owned(), "ignored".to_owned()),
            (container_option.to_owned(), "container".to_owned()),
            ("skip_signature".to_owned(), "true".to_owned()),
        ]);
        for kind in [
            "relative",
            "root-relative",
            "absolute",
            "foreign-file",
            "foreign-root-file",
            "foreign-host-file",
            "foreign-dv",
        ] {
            let actions = original
                .lines()
                .map(|line| {
                    let mut action: Value = serde_json::from_str(line)?;
                    if let Some(add) = action.get_mut("add") {
                        let file = fixture.data_file_path();
                        add["path"] = Value::String(match kind {
                            "relative" => file.to_owned(),
                            "root-relative" => format!("/container/{file}"),
                            "foreign-file" => format!("{table_url}secret-container/{file}"),
                            "foreign-root-file" => format!("/secret-container/{file}"),
                            "foreign-host-file" => {
                                format!(r"\\secret-account.blob.core.windows.net\container\{file}")
                            }
                            _ => format!("{table_url}container/{file}"),
                        });
                        if !matches!(kind, "relative" | "root-relative") {
                            let container = if kind == "foreign-dv" {
                                "secret-container"
                            } else {
                                "container"
                            };
                            add["deletionVector"]["storageType"] = Value::String("p".to_owned());
                            add["deletionVector"]["pathOrInlineDv"] =
                                Value::String(format!("{table_url}{container}/{dv_file}"));
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
                let case = format!("{table_url}, {container_option}, {backend:?}, {kind}");
                let table = DeltaTableBuilder::new(table_url)
                    .with_storage_options(options.clone())
                    .load_table()
                    .await?
                    .refresh()
                    .await?;
                assert_eq!(table.table_url(), table_url, "{case}");
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
                match kind {
                    "foreign-file" | "foreign-root-file" | "foreign-host-file" => assert!(
                        matches!(
                            result,
                            Err(DeltaReaderError::DataFileRead {
                                reason: "data_file_store_mismatch",
                                ..
                            })
                        ),
                        "{case}: {result:?}"
                    ),
                    "foreign-dv" => {
                        let error = result
                            .err()
                            .ok_or_else(|| format!("{case}: foreign DV returned rows"))?;
                        assert!(
                            matches!(error, DeltaReaderError::DeletionVectorRead { .. }),
                            "{case}: {error:?}"
                        );
                        let source = error
                            .source()
                            .ok_or_else(|| format!("{case}: missing DV error source: {error:?}"))?;
                        assert!(
                            source.to_string().contains(
                                "deletion vector URL does not identify the configured table store"
                            ),
                            "{case}: {error:?}"
                        );
                    }
                    _ => {
                        let batches = result.map_err(|error| format!("{case}: {error:?}"))?;
                        let mut ids = Vec::new();
                        for batch in &batches {
                            let column = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .ok_or("id column is not Int32")?;
                            ids.extend_from_slice(column.values());
                        }
                        assert_eq!(ids, [1, 3], "{case}");
                    }
                }
            }
        }
    }
    Ok(())
}
