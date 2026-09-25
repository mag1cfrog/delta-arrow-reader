//! Controlled HTTPS checks, launched by https.py with isolated trust/proxy settings.

use std::{error::Error, fs, path::Path};

use arrow::array::Int32Array;
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaStorageOptions, DeltaTableBuilder, ParquetReaderBackend,
};
use delta_kernel::Engine;
use delta_kernel_default_engine::{DefaultEngineBuilder, storage::store_from_url_opts};
use futures_util::TryStreamExt;
use serde_json::Value;
use url::Url;

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn options() -> DeltaStorageOptions {
    let mut options = DeltaStorageOptions::new();
    if let Ok(proxy) = std::env::var("DAR_TLS_PROXY") {
        options.insert("proxy_url".into(), proxy);
    }
    options
}

fn read_presigned(url: Url) -> TestResult<bytes::Bytes> {
    let store = store_from_url_opts(&url, options())?;
    let engine = DefaultEngineBuilder::new(store).build();
    let mut files = engine.storage_handler().read_files(vec![(url, None)])?;
    let bytes = files.next().ok_or("missing HTTPS result")??;
    assert!(files.next().is_none());
    Ok(bytes)
}

fn assert_certificate_error(error: &(dyn Error + 'static)) {
    // An arbitrary failure (404, timeout, proxy failure) must not pass this check.
    let mut messages = format!("{error:?}");
    let mut source = error.source();
    while let Some(error) = source {
        messages.push_str(&format!(" | {error:?}"));
        source = error.source();
    }
    assert!(
        messages.to_ascii_lowercase().contains("certificate"),
        "expected a certificate validation failure, got: {messages}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run with python3 tests/reader/https.py; requires a local TLS server and test CA"]
async fn verified_storage_paths() -> TestResult {
    let endpoint = std::env::var("DAR_TLS_ENDPOINT")?;
    let directory = std::env::var("DAR_TLS_DIRECTORY")?;
    let fixture = RealParquetDeltaTable::new_default("https")?;

    for presigned in [false, true] {
        let name = if presigned { "presigned" } else { "plain" };
        let root = Path::new(&directory).join(name);
        fs::create_dir_all(root.join("_delta_log"))?;
        fs::copy(
            fixture.path().join(fixture.data_file_path()),
            root.join(fixture.data_file_path()),
        )?;
        for version in 0..=1 {
            let log = format!("_delta_log/{version:020}.json");
            let actions = fs::read_to_string(fixture.path().join(&log))?
                .lines()
                .map(|line| {
                    let mut action: Value = serde_json::from_str(line)?;
                    if presigned && let Some(add) = action.get_mut("add") {
                        add["path"] = Value::String(format!(
                            "{endpoint}/{name}/{}?X-Amz-Signature=test",
                            fixture.data_file_path()
                        ));
                    }
                    Ok(action.to_string())
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()?;
            fs::write(root.join(log), actions.join("\n"))?;
        }

        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let table = DeltaTableBuilder::new(format!("{endpoint}/{name}/"))
                .with_storage_options(options())
                .load_table()
                .await?;
            let scan = table
                .scan()
                .with_execution_options(
                    DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                )
                .build()
                .await?;
            let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
            let mut ids = Vec::new();
            for batch in batches {
                let column = batch.column_by_name("id").ok_or("missing id column")?;
                let column = column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("wrong id type")?;
                ids.extend(column.iter());
            }
            ids.sort();
            assert_eq!(ids, [Some(1), Some(2), Some(3)], "{backend:?}, {name}");
        }
    }

    // This handler has a separate reqwest path from Kernel's Parquet opener.
    let path = format!("plain/{}?X-Amz-Signature=test", fixture.data_file_path());
    assert_eq!(
        read_presigned(Url::parse(&format!("{endpoint}/{path}"))?)?.as_ref(),
        fs::read(fixture.path().join(fixture.data_file_path()))?
    );

    for endpoint in [
        std::env::var("DAR_TLS_UNTRUSTED_ENDPOINT")?,
        endpoint.replace("localhost", "127.0.0.1"),
    ] {
        // Snapshot reads use object_store/reqwest 0.12 for either backend.
        let error = DeltaTableBuilder::new(format!("{endpoint}/plain/"))
            .with_storage_options(options())
            .load_table()
            .await
            .err()
            .ok_or("invalid certificate was accepted")?;
        assert_certificate_error(&error);

        // A presigned read must also fail through Kernel/reqwest 0.13.
        let error = read_presigned(Url::parse(&format!("{endpoint}/{path}"))?)
            .err()
            .ok_or("Kernel accepted an invalid certificate")?;
        assert_certificate_error(error.as_ref());
    }
    Ok(())
}
