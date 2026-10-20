//! Native cloud clients retain caller-owned credentials across reads and refreshes.
#![allow(clippy::expect_used)]

use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::array::Int32Array;
use async_trait::async_trait;
use delta_arrow_reader::{
    DeltaReaderPhase, DeltaScan, DeltaScanExecutionOptions, DeltaSnapshotSelection,
    DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder, ParquetReaderBackend,
    WarmupMode,
};
use futures_util::TryStreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AwsCredential, azure::AzureCredential,
    client::CredentialProvider, gcp::GcpCredential, local::LocalFileSystem, memory::InMemory,
    path::Path,
};
use tokio::{net::TcpListener, task::JoinSet};
use url::Url;

#[path = "support/cloud_http.rs"]
mod cloud_http;
#[allow(dead_code)]
#[path = "reader/support.rs"]
mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// A shared generation switches credentials without replacing the provider.
struct RotatingCredentials<T> {
    credentials: [Arc<T>; 2],
    generation: Arc<AtomicUsize>,
}

impl<T> fmt::Debug for RotatingCredentials<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("secret-provider-debug")
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> CredentialProvider for RotatingCredentials<T> {
    type Credential = T;

    async fn get_credential(&self) -> object_store::Result<Arc<T>> {
        self.credentials
            .get(self.generation.load(Ordering::SeqCst))
            .cloned()
            .ok_or_else(|| object_store::Error::Generic {
                store: "test-provider",
                source: "secret-provider-failure".into(),
            })
    }
}

#[derive(Clone, Copy, Debug)]
enum CredentialKind {
    S3,
    R2,
    AzureSas,
    AzureBearer,
    Gcs,
}

impl CredentialKind {
    const ALL: [Self; 5] = [
        Self::S3,
        Self::R2,
        Self::AzureSas,
        Self::AzureBearer,
        Self::Gcs,
    ];

    fn table_url(self) -> &'static str {
        match self {
            Self::S3 => "s3://bucket/table/",
            Self::R2 => "https://account.r2.cloudflarestorage.com/bucket/table/",
            Self::AzureSas => "abfss://bucket@account.dfs.core.windows.net/table/",
            Self::AzureBearer => "https://account.blob.core.windows.net/bucket/table/",
            Self::Gcs => "gs://bucket/table/",
        }
    }

    fn options(self, endpoint: &str) -> DeltaStorageOptions {
        let endpoint_key = match self {
            Self::S3 | Self::R2 => "aws_endpoint_url_s3",
            Self::AzureSas | Self::AzureBearer => "azure_endpoint",
            Self::Gcs => "google_base_url",
        };
        let mut options = DeltaStorageOptions::from([
            (endpoint_key.into(), endpoint.into()),
            ("allow_http".into(), "true".into()),
        ]);
        if matches!(self, Self::S3 | Self::R2) {
            // R2 must replace this with the region derived from its URL.
            options.insert("aws_region".into(), "us-west-2".into());
        }
        options
    }

    fn provider(self, generation: Arc<AtomicUsize>) -> DeltaStorageCredentialProvider {
        match self {
            Self::S3 | Self::R2 => {
                DeltaStorageCredentialProvider::Aws(Arc::new(RotatingCredentials {
                    credentials: [0, 1].map(|version| {
                        Arc::new(AwsCredential {
                            key_id: format!("secret-key-{version}"),
                            secret_key: format!("secret-signing-key-{version}"),
                            token: Some(format!("secret-session-{version}")),
                        })
                    }),
                    generation,
                }))
            }
            Self::AzureSas | Self::AzureBearer => {
                DeltaStorageCredentialProvider::Azure(Arc::new(RotatingCredentials {
                    credentials: [0, 1].map(|version| {
                        Arc::new(if matches!(self, Self::AzureSas) {
                            AzureCredential::SASToken(vec![
                                ("sig".into(), format!("secret-sas-{version}")),
                                ("sp".into(), "rl".into()),
                            ])
                        } else {
                            AzureCredential::BearerToken(format!("secret-bearer-{version}"))
                        })
                    }),
                    generation,
                }))
            }
            Self::Gcs => DeltaStorageCredentialProvider::Gcp(Arc::new(RotatingCredentials {
                credentials: [0, 1].map(|version| {
                    Arc::new(GcpCredential {
                        bearer: format!("secret-bearer-{version}"),
                    })
                }),
                generation,
            })),
        }
    }

    fn assert_authorization(self, requests: &[cloud_http::CapturedRequest], generation: usize) {
        assert!(!requests.is_empty(), "{self:?}: no storage requests");
        for request in requests {
            let authorization = request
                .headers
                .get("authorization")
                .map(|header| header.to_str())
                .transpose()
                .expect("valid authorization header");
            match self {
                Self::S3 | Self::R2 => {
                    let authorization = authorization.expect("signed S3 request");
                    assert!(
                        authorization.contains(&format!("Credential=secret-key-{generation}/"))
                    );
                    let region = if matches!(self, Self::R2) {
                        "auto"
                    } else {
                        "us-west-2"
                    };
                    assert!(authorization.contains(&format!("/{region}/s3/aws4_request")));
                    assert_eq!(
                        request.headers["x-amz-security-token"],
                        format!("secret-session-{generation}")
                    );
                }
                Self::AzureSas => {
                    assert!(authorization.is_none());
                    let url = Url::parse(&format!("http://localhost{}", request.uri))
                        .expect("captured request URL");
                    assert!(url.query_pairs().any(|(key, value)| {
                        key == "sig" && value == format!("secret-sas-{generation}")
                    }));
                }
                Self::AzureBearer | Self::Gcs => {
                    assert_eq!(
                        authorization,
                        Some(format!("Bearer secret-bearer-{generation}").as_str())
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn rotating_credentials_reach_logs_data_vectors_and_refreshed_tables() -> TestResult {
    let fixture = support::RealParquetDeltaTable::new_with_deletion_vector("credentials", &[1])?;
    let local = LocalFileSystem::new_with_prefix(fixture.path())?;
    for kind in CredentialKind::ALL {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let store = Arc::new(InMemory::new());
            for object in local.list(None).try_collect::<Vec<_>>().await? {
                store
                    .put(
                        &Path::from(format!("table/{}", object.location)),
                        local.get(&object.location).await?.bytes().await?.into(),
                    )
                    .await?;
            }
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let requests = cloud_http::CapturedRequests::default();
            let mut server = JoinSet::new();
            server.spawn(cloud_http::serve(
                listener,
                Arc::clone(&store),
                Some(Arc::clone(&requests)),
            ));
            let generation = Arc::new(AtomicUsize::new(0));
            let builder = DeltaTableBuilder::new(kind.table_url())
                .with_storage_options(kind.options(&endpoint))
                .with_credential_provider(kind.provider(Arc::clone(&generation)))
                .with_execution_options(
                    DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                )
                .with_warmup(WarmupMode::None);
            assert!(!format!("{builder:?}").contains("secret-"));
            // Exercise both public loading paths with every credential form.
            let table = if backend == ParquetReaderBackend::Direct {
                builder.load_table().await?
            } else {
                builder.load_snapshot().await?.into_table()?
            };
            assert_eq!(table.table_url(), kind.table_url());
            assert_eq!(table.version(), 1);
            assert_rows(table.scan().build().await?).await?;
            {
                let captured = requests.lock().map_err(|_| "request lock poisoned")?;
                kind.assert_authorization(&captured, 0);
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().contains("_delta_log"))
                );
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().ends_with(".parquet"))
                );
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().ends_with(".bin"))
                );
            }
            let clone = table.clone();
            let scan = clone.scan().build().await?;
            requests
                .lock()
                .map_err(|_| "request lock poisoned")?
                .clear();
            generation.store(1, Ordering::SeqCst);
            assert_rows(scan).await?;
            assert_eq!(table.version(), 1);
            {
                let captured = requests.lock().map_err(|_| "request lock poisoned")?;
                kind.assert_authorization(&captured, 1);
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().ends_with(".parquet"))
                );
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().ends_with(".bin"))
                );
            }
            store
                .put(
                    &Path::from("table/_delta_log/00000000000000000002.json"),
                    "{\"commitInfo\":{\"timestamp\":1}}\n".into(),
                )
                .await?;
            let refreshed = clone.refresh().await?;
            assert_eq!(refreshed.version(), 2);
            assert_eq!(table.version(), 1);
            assert_rows(refreshed.scan().build().await?).await?;
            {
                let captured = requests.lock().map_err(|_| "request lock poisoned")?;
                kind.assert_authorization(&captured, 1);
                assert!(
                    captured
                        .iter()
                        .any(|request| request.decoded_path().as_ref().contains("_delta_log"))
                );
            }
            let scan = table.scan().build().await?;
            requests
                .lock()
                .map_err(|_| "request lock poisoned")?
                .clear();
            generation.store(2, Ordering::SeqCst);
            let error = clone
                .refresh()
                .await
                .expect_err("provider failure must fail refresh");
            assert_eq!(error.phase(), DeltaReaderPhase::Snapshot);
            assert_eq!(error.reason(), "snapshot_refresh_failed");
            assert!(!format!("{error:?} {error}").contains("secret-"));
            let error = scan
                .into_stream()
                .try_collect::<Vec<_>>()
                .await
                .expect_err("provider failure must fail an existing scan");
            assert!(matches!(
                error.phase(),
                DeltaReaderPhase::DataFileRead | DeltaReaderPhase::DeletionVector
            ));
            assert!(!format!("{error:?} {error}").contains("secret-"));
            assert!(
                requests
                    .lock()
                    .map_err(|_| "request lock poisoned")?
                    .is_empty()
            );
            assert!(server.try_join_next().is_none(), "fixture server stopped");
        }
    }
    Ok(())
}

async fn assert_rows(scan: DeltaScan) -> TestResult {
    let mut ids = Vec::new();
    for batch in scan.into_stream().try_collect::<Vec<_>>().await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("id column type")?;
        ids.extend_from_slice(column.values());
    }
    assert_eq!(ids, [1, 3]);
    Ok(())
}

#[tokio::test]
async fn conflicting_inputs_fail_before_any_storage_read() -> TestResult {
    let local_url = Url::from_directory_path(std::env::current_dir()?)
        .map_err(|()| "invalid local fixture URL")?;
    for (kind, conflicts) in [
        (
            CredentialKind::S3,
            &[
                "AWS_ACCESS_KEY_ID",
                "secret_access_key",
                "session_token",
                "skip_signature",
                "web_identity_token_file",
                "aws_role_arn",
                "aws_container_credentials_full_uri",
                "metadata_endpoint",
            ][..],
        ),
        (
            CredentialKind::AzureSas,
            &[
                "AZURE_STORAGE_SAS_TOKEN",
                "token",
                "azure_storage_master_key",
                "use_azure_cli",
                "use_emulator",
                "federated_token_file",
                "azure_client_id",
                "azure_fabric_session_token",
            ][..],
        ),
        (
            CredentialKind::Gcs,
            &[
                "GOOGLE_SERVICE_ACCOUNT",
                "service_account_key",
                "google_application_credentials",
                "skip_signature",
            ][..],
        ),
    ] {
        let provider = kind.provider(Arc::new(AtomicUsize::new(2)));
        assert!(!format!("{provider:?}").contains("secret-"));
        for conflict in conflicts {
            let error = DeltaTableBuilder::new(kind.table_url())
                .with_credential_provider(provider.clone())
                .with_storage_options(DeltaStorageOptions::from([(
                    conflict.to_string(),
                    "secret-value".into(),
                )]))
                .load_table()
                .await
                .expect_err("conflicting credential input");
            assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
            assert_eq!(error.reason(), "credential_provider_conflicting_options");
            assert!(!format!("{error:?} {error}").contains("secret-"));
        }
        for wrong_url in [
            local_url.as_str(),
            "https://example.com/secret-table",
            match kind {
                CredentialKind::S3 => "gs://bucket/table",
                _ => "s3://bucket/table",
            },
        ] {
            let error = DeltaTableBuilder::new(wrong_url)
                .with_credential_provider(provider.clone())
                .with_snapshot_selection(DeltaSnapshotSelection::Version(0))
                .load_snapshot()
                .await
                .expect_err("provider/store mismatch");
            assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
            assert_eq!(error.reason(), "credential_provider_store_mismatch");
            assert!(!format!("{error:?} {error}").contains("secret-"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicit_provider_ignores_ambient_authentication_settings() -> TestResult {
    const CHILD: &str = "DAR_TEST_EXPLICIT_PROVIDER_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Keep environment changes out of the other tests and the developer's shell.
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "explicit_provider_ignores_ambient_authentication_settings",
            ])
            .env(CHILD, "1")
            .env("AWS_ACCESS_KEY_ID", "ambient-key")
            .env("AWS_SECRET_ACCESS_KEY", "ambient-secret")
            .env("AWS_SKIP_SIGNATURE", "true")
            .env("AZURE_STORAGE_TOKEN", "ambient-token")
            .env("AZURE_STORAGE_USE_EMULATOR", "true")
            .env(
                "GOOGLE_SERVICE_ACCOUNT",
                "secret-missing-service-account.json",
            )
            .env("GOOGLE_APPLICATION_CREDENTIALS", "secret-missing-adc.json")
            .output()?;
        assert!(output.status.success(), "{output:?}");
        return Ok(());
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let requests = cloud_http::CapturedRequests::default();
    let mut server = JoinSet::new();
    server.spawn(cloud_http::serve(
        listener,
        Arc::new(InMemory::new()),
        Some(Arc::clone(&requests)),
    ));
    for kind in CredentialKind::ALL {
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            DeltaTableBuilder::new(kind.table_url())
                .with_storage_options(kind.options(&endpoint))
                .with_credential_provider(kind.provider(Arc::new(AtomicUsize::new(2))))
                .load_snapshot(),
        )
        .await?
        .expect_err("provider failure must fail loading");
        assert_eq!(error.phase(), DeltaReaderPhase::Snapshot);
        assert_eq!(error.reason(), "snapshot_load_failed");
        assert!(!format!("{error:?} {error}").contains("secret-"));
        // The raw source is retained for callers, while the public error stays redacted.
        assert!(
            error
                .source()
                .ok_or("missing provider error")?
                .to_string()
                .contains("secret-provider-failure")
        );
        assert!(
            requests
                .lock()
                .map_err(|_| "request lock poisoned")?
                .is_empty()
        );
    }
    assert!(server.try_join_next().is_none(), "fixture server stopped");
    Ok(())
}
