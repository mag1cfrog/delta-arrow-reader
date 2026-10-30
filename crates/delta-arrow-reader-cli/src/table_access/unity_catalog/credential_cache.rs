//! On-demand renewal of one table's credentials, shared by all of its storage clients.

use std::{
    fmt,
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use delta_arrow_reader::DeltaStorageCredentialProvider;
use object_store::{
    CredentialProvider, aws::AwsCredential, azure::AzureCredential, gcp::GcpCredential,
};
use tokio::{runtime::Handle, sync::Mutex, task::JoinSet, time::Instant};

use super::{
    Error, databricks_cli,
    storage_location::{StorageBackend, TableStorageLocation},
    temporary_credentials::{
        CloudCredentials, TemporaryTableCredentials, parse_temporary_credentials,
    },
};

/// Renews credentials on the command runtime using the original profile, table ID, and root.
struct TableCredentialRefresher {
    profile: String,
    table_id: String,
    storage_location: TableStorageLocation,
    /// Child processes belong to the command's runtime, even when Core calls from its own workers.
    command_runtime: Handle,
}

impl TableCredentialRefresher {
    async fn refresh(self: &Arc<Self>) -> Result<TemporaryTableCredentials, Error> {
        let refresher = Arc::clone(self);
        let mut task = JoinSet::new();
        // Dropping the waiter aborts response parsing; command cleanup owns child termination.
        task.spawn_on(
            async move {
                let bytes = databricks_cli::generate_read_credentials(
                    &refresher.profile,
                    &refresher.table_id,
                )
                .await?;
                parse_temporary_credentials(&bytes, &refresher.storage_location)
            },
            &self.command_runtime,
        );
        task.join_next()
            .await
            .ok_or(Error::DatabricksCommandFailed)?
            .map_err(|_| Error::DatabricksCommandFailed)?
    }
}

/// A grant and its refresh deadline, measured monotonically to survive clock adjustments.
struct CachedTableCredentials {
    grant: TemporaryTableCredentials,
    refresh_at: Instant,
}

impl CachedTableCredentials {
    fn new(grant: TemporaryTableCredentials) -> Result<Self, Error> {
        let lifetime = grant
            .expires_at
            .duration_since(SystemTime::now())
            .ok()
            .filter(|lifetime| !lifetime.is_zero())
            .ok_or(Error::CredentialsExpired)?;
        // Short grants keep most of their useful lifetime instead of refreshing on every request.
        let margin = Duration::from_secs(30).min(lifetime / 10);
        let refresh_at = Instant::now()
            .checked_add(lifetime - margin)
            .ok_or(Error::InvalidDatabricksResponse)?;
        Ok(Self { grant, refresh_at })
    }

    fn is_fresh(&self) -> bool {
        Instant::now() < self.refresh_at && SystemTime::now() < self.grant.expires_at
    }
}

/// One invocation's in-memory grant or terminal refresh failure.
///
/// The mutex coalesces concurrent refreshes. A failure is retained so waiting reads do not
/// launch more subprocesses or fall back to a grant whose authorization could have been revoked.
pub(super) struct TableCredentialCache {
    refresher: Arc<TableCredentialRefresher>,
    state: Mutex<Result<CachedTableCredentials, Error>>,
}

impl TableCredentialCache {
    pub(super) fn new(
        profile: String,
        table_id: String,
        storage_location: TableStorageLocation,
        credentials: TemporaryTableCredentials,
    ) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            refresher: Arc::new(TableCredentialRefresher {
                profile,
                table_id,
                storage_location,
                command_runtime: Handle::current(),
            }),
            state: Mutex::new(Ok(CachedTableCredentials::new(credentials)?)),
        }))
    }

    /// Native clients share this cache without changing Core's credential-provider interface.
    pub(super) fn credential_provider(self: &Arc<Self>) -> DeltaStorageCredentialProvider {
        match self.refresher.storage_location.backend {
            StorageBackend::S3 | StorageBackend::R2 => DeltaStorageCredentialProvider::Aws(
                Arc::new(AwsTableCredentialProvider(Arc::clone(self))),
            ),
            StorageBackend::AzureBlob => DeltaStorageCredentialProvider::Azure(Arc::new(
                AzureTableCredentialProvider(Arc::clone(self)),
            )),
            StorageBackend::Gcs => DeltaStorageCredentialProvider::Gcp(Arc::new(
                GcpTableCredentialProvider(Arc::clone(self)),
            )),
        }
    }

    /// Recover the UC category if a dependency has converted the provider error to a string.
    pub(super) async fn refresh_failure(&self) -> Option<Error> {
        self.state.lock().await.as_ref().err().cloned()
    }

    async fn get_credentials(&self) -> object_store::Result<CloudCredentials> {
        self.get_or_refresh(|| self.refresher.refresh())
            .await
            .map_err(storage_error)
    }

    async fn get_or_refresh<F, Fut>(&self, refresh: F) -> Result<CloudCredentials, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<TemporaryTableCredentials, Error>>,
    {
        let mut state = self.state.lock().await;
        let cached = state.as_ref().map_err(Clone::clone)?;
        if cached.is_fresh() {
            return Ok(cached.grant.credentials.clone());
        }
        let replacement = refresh().await.and_then(|grant| {
            if !cached.grant.credentials.same_kind(&grant.credentials) {
                return Err(Error::UnsupportedDirectRead);
            }
            CachedTableCredentials::new(grant)
        });
        *state = replacement;
        state
            .as_ref()
            .map(|cached| cached.grant.credentials.clone())
            .map_err(Clone::clone)
    }
}

impl fmt::Debug for TableCredentialCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TableCredentialCache")
            .finish_non_exhaustive()
    }
}

fn storage_error(source: Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "Unity Catalog",
        source: Box::new(source),
    }
}

/// S3 and R2 request signing through the shared UC cache.
#[derive(Debug)]
struct AwsTableCredentialProvider(Arc<TableCredentialCache>);

#[async_trait]
impl CredentialProvider for AwsTableCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        match self.0.get_credentials().await? {
            CloudCredentials::Aws(credentials) => Ok(credentials),
            _ => Err(storage_error(Error::UnsupportedDirectRead)),
        }
    }
}

/// Azure SAS or bearer authorization through the shared UC cache.
#[derive(Debug)]
struct AzureTableCredentialProvider(Arc<TableCredentialCache>);

#[async_trait]
impl CredentialProvider for AzureTableCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        match self.0.get_credentials().await? {
            CloudCredentials::Azure(credentials) => Ok(credentials),
            _ => Err(storage_error(Error::UnsupportedDirectRead)),
        }
    }
}

/// GCS bearer authorization through the shared UC cache.
#[derive(Debug)]
struct GcpTableCredentialProvider(Arc<TableCredentialCache>);

#[async_trait]
impl CredentialProvider for GcpTableCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        match self.0.get_credentials().await? {
            CloudCredentials::Gcp(credentials) => Ok(credentials),
            _ => Err(storage_error(Error::UnsupportedDirectRead)),
        }
    }
}

#[cfg(test)]
mod tests;
