//! SDK-owned S3 stores. Other schemes keep Kernel's registered URL handlers.

mod http_pool;

use std::sync::Arc;

use object_store::{
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey},
    client::{ClientConfigKey, ClientOptions, HttpClient, HttpConnector},
    path::Path,
};
use url::Url;

use crate::DeltaStorageOptions;

pub(super) fn build(
    url: &Url,
    options: &DeltaStorageOptions,
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
    use super::*;

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
        let (ordinary, partial) = build(&url, &base)?;
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
            let (_, partial) = build(&url, &options)?;
            assert!(
                partial.is_none(),
                "unsupported option {key} must keep ordinary reads"
            );
        }
        let mut http = base.clone();
        http.insert("AWS_ALLOW_HTTP".into(), "true".into());
        http.insert("AWS_ENDPOINT".into(), "http://127.0.0.1:1234".into());
        assert!(build(&url, &http)?.1.is_some());
        http.insert("AWS_CONNECT_TIMEOUT".into(), "invalid-duration".into());
        assert!(build(&url, &http).is_err());
        Ok(())
    }
}
