//! S3 stores for ordinary reads and partial-page reads.
//!
//! Reading small pieces of many Parquet pages can send thousands of requests.
//! The SDK's usual HTTP pool may start a new connection while waiting for an
//! existing one. Even if it reuses a connection, the new one can keep opening
//! in the background. Limiting active requests alone therefore does not prevent
//! these extra connections from exhausting file descriptors.
//!
//! Partial reads use a pool that also limits connection setup. We construct
//! the stores here because Kernel's URL handlers return stores whose HTTP
//! clients we cannot replace. Ordinary reads keep the SDK's usual client.
//! Both stores use the SDK for signing and share one credential provider,
//! including its cache and refresh logic.
//!
//! Small-request calibration and later partial reads use the same client, so
//! the measured costs reflect how those reads will run. Unsupported network
//! settings or failed calibration leave queries on ordinary reads.

mod http_pool;

use std::sync::Arc;

use object_store::{
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsCredentialProvider},
    client::{ClientConfigKey, ClientOptions, HttpClient, HttpConnector},
    path::Path,
};
use url::Url;

use crate::DeltaStorageOptions;

pub(super) fn build(
    url: &Url,
    options: &DeltaStorageOptions,
    credential_provider: Option<&AwsCredentialProvider>,
) -> object_store::Result<(AmazonS3, Option<AmazonS3Builder>)> {
    // Preserve the URL path validation performed by parse_url_opts.
    Path::from_url_path(url.path())?;
    let mut builder = AmazonS3Builder::new().with_url(url.as_str());
    let mut partial_reads_supported = true;
    for (key, value) in options {
        // Match object_store::parse_url_opts: case-insensitive, ignore unknown keys.
        if let Ok(key) = key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            partial_reads_supported &= match key {
                AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp) => true,
                // ponytail: custom network settings retain the SDK transport. Add
                // support here only alongside equivalent transport behavior/tests.
                AmazonS3ConfigKey::Client(_) | AmazonS3ConfigKey::S3Express => false,
                _ => true,
            };
            builder = builder.with_config(key, value);
        }
    }
    if let Some(provider) = credential_provider {
        builder = builder.with_credentials(Arc::clone(provider));
    }
    let store = builder.clone().build()?;
    let partial = partial_reads_supported.then(|| {
        builder
            // Keep refresh/caching/credential HTTP requests on the original SDK client.
            .with_credentials(Arc::clone(store.credentials()))
            .with_http_connector(PartialReadConnector)
    });
    Ok((store, partial))
}

#[derive(Debug)]
struct PartialReadConnector;

impl HttpConnector for PartialReadConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        // The ordinary SDK builder validates the value first. Preserve all of
        // its accepted boolean spellings when selecting the partial transport.
        let allow_http = matches!(
            options
                .get_config_value(&ClientConfigKey::AllowHttp)
                .map(|value| value.to_ascii_lowercase())
                .as_deref(),
            Some("1" | "true" | "on" | "yes" | "y")
        );
        http_pool::client(allow_http).map_err(|source| object_store::Error::Generic {
            store: "S3 partial reads",
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use object_store::{aws::AwsCredential, client::CredentialProvider};

    use super::*;

    #[derive(Debug, Default)]
    struct RotatingAwsCredentials(AtomicUsize);

    #[async_trait]
    impl CredentialProvider for RotatingAwsCredentials {
        type Credential = AwsCredential;

        async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
            let generation = self.0.load(Ordering::SeqCst);
            Ok(Arc::new(AwsCredential {
                key_id: format!("key-{generation}"),
                secret_key: format!("secret-{generation}"),
                token: Some(format!("token-{generation}")),
            }))
        }
    }

    #[tokio::test]
    async fn ordinary_and_partial_stores_share_renewable_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        for location in [
            "s3://bucket/table",
            "https://account.r2.cloudflarestorage.com/bucket/table",
        ] {
            let provider = Arc::new(RotatingAwsCredentials::default());
            let credentials: AwsCredentialProvider = provider.clone();
            let (ordinary, partial) = build(
                &Url::parse(location)?,
                &DeltaStorageOptions::new(),
                Some(&credentials),
            )?;
            let partial = partial.ok_or("partial reads unavailable")?.build()?;
            assert!(Arc::ptr_eq(ordinary.credentials(), &credentials));
            assert!(Arc::ptr_eq(partial.credentials(), &credentials));
            for generation in 0..2 {
                provider.0.store(generation, Ordering::SeqCst);
                for store in [&ordinary, &partial] {
                    let credential = store.credentials().get_credential().await?;
                    assert_eq!(credential.key_id, format!("key-{generation}"));
                    assert_eq!(credential.token, Some(format!("token-{generation}")));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn preserve_options_credentials_and_unsupported_network_settings()
    -> Result<(), Box<dyn std::error::Error>> {
        let url = Url::parse("s3://bucket/root")?;
        let base = DeltaStorageOptions::from([
            ("AWS_ACCESS_KEY_ID".into(), "test-key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "test-secret".into()),
            ("AWS_SESSION_TOKEN".into(), "test-token".into()),
            ("AWS_REGION".into(), "us-west-2".into()),
            ("unrecognized_option".into(), "ignored".into()),
        ]);
        let (ordinary, partial) = build(&url, &base, None)?;
        let partial = partial
            .ok_or("default S3 configuration should support partial reads")?
            .build()?;
        assert!(Arc::ptr_eq(ordinary.credentials(), partial.credentials()));
        let credentials = partial.credentials().get_credential().await?;
        assert_eq!(credentials.key_id, "test-key");
        assert_eq!(credentials.token.as_deref(), Some("test-token"));

        for (key, value) in [
            ("timeout", "17s"),
            ("AWS_CONNECT_TIMEOUT", "2s"),
            ("aws_allow_invalid_certificates", "true"),
            ("http1_only", "false"),
            ("proxy_url", "http://127.0.0.1:1234"),
            ("pool_max_idle_per_host", "0"),
            ("user_agent", "custom-agent"),
            ("randomize_addresses", "false"),
            ("s3_express", "false"),
        ] {
            let mut options = base.clone();
            options.insert(key.into(), value.into());
            let (_, partial) = build(&url, &options, None)?;
            assert!(
                partial.is_none(),
                "unsupported option {key} must keep ordinary reads"
            );
        }
        let mut http = base.clone();
        http.insert("AWS_ALLOW_HTTP".into(), "true".into());
        http.insert("AWS_ENDPOINT".into(), "http://127.0.0.1:1234".into());
        assert!(build(&url, &http, None)?.1.is_some());
        http.insert("AWS_CONNECT_TIMEOUT".into(), "invalid-duration".into());
        assert!(build(&url, &http, None).is_err());
        Ok(())
    }
}
