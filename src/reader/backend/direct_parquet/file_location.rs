//! Resolve a data URL within the table's already configured object store.

use object_store::{ObjectStoreScheme, path::Path};
use url::{Position, Url};

use super::data_file_error;
use crate::DeltaReaderError;

pub(super) fn resolve_data_file_path(
    table_url: &Url,
    file_path: &str,
) -> Result<Path, DeltaReaderError> {
    let location = table_url
        .join(file_path)
        .map_err(|error| data_file_error("data_file_path_resolution_failed", error))?;
    let mut key = location.path();
    // Compare the full authority, not just the host (ABFS puts its container in
    // the username). Do not infer aliases across endpoints: storage options may
    // point an s3:// or az:// URL at a private service with the same bucket name.
    if !is_path_reference(file_path) {
        let namespace = path_namespace(&location);
        if storage_identity(table_url, path_namespace(table_url))
            != storage_identity(&location, namespace)
        {
            return Err(data_file_error(
                "data_file_store_mismatch",
                std::io::Error::other("data file URL does not identify the configured table store"),
            ));
        }
        // In an explicit cloud HTTPS URL the first segment identifies the
        // bucket/container. It must not become part of the key inside that store.
        if !namespace.is_empty() {
            key = key
                .strip_prefix('/')
                .unwrap_or(key)
                .split_once('/')
                .map_or("", |(_, key)| key);
        }
    }

    // Path-only references keep Kernel's table directory convention. Changing
    // their prefix here alone would separate relative data files from the log.
    Path::from_url_path(key).map_err(|_| {
        // object_store path errors can contain the complete, unredacted input.
        data_file_error(
            "data_file_path_resolution_failed",
            std::io::Error::other("invalid data file object key"),
        )
    })
}

fn is_path_reference(path: &str) -> bool {
    if Url::parse(path).is_ok() {
        return false;
    }
    // URL parsing ignores leading C0 controls/space and embedded tabs/newlines.
    // A network-path reference declares an authority even without a scheme.
    // Backslashes also introduce an authority for special schemes such as HTTPS.
    let mut bytes = path
        .trim_start_matches(|ch| ch <= ' ')
        .bytes()
        .filter(|byte| !matches!(byte, b'\t' | b'\r' | b'\n'));
    !matches!(
        (bytes.next(), bytes.next()),
        (Some(b'/' | b'\\'), Some(b'/' | b'\\'))
    )
}

#[derive(PartialEq, Eq)]
enum StorageIdentity<'a> {
    Azure {
        account: &'a str,
        service: &'a str,
        container: &'a str,
    },
    Url {
        scheme: &'a str,
        authority: &'a str,
        namespace: &'a str,
    },
}

fn storage_identity<'a>(url: &'a Url, namespace: &'a str) -> StorageIdentity<'a> {
    // object_store maps qualified ABFS and HTTPS Azure URLs to the account's
    // blob endpoint. Both spellings identify the same store, including dfs/blob
    // aliases. Core Azure and Fabric remain distinct services.
    let container = match url.scheme() {
        "https" if url.username().is_empty() => Some(namespace),
        "az" | "abfs" | "abfss" if !url.username().is_empty() => Some(url.username()),
        _ => None,
    };
    if let Some(container) = container
        && url.password().is_none()
        && url.port().is_none_or(|port| port == 443)
        && let Some((account, suffix)) = url.host_str().and_then(|host| host.split_once('.'))
    {
        let service = match suffix {
            "dfs.core.windows.net" | "blob.core.windows.net" => Some("core"),
            "dfs.fabric.microsoft.com" | "blob.fabric.microsoft.com" => Some("fabric"),
            _ => None,
        };
        if let Some(service) = service {
            return StorageIdentity::Azure {
                account,
                service,
                container,
            };
        }
    }
    StorageIdentity::Url {
        scheme: storage_scheme(url),
        authority: &url[Position::BeforeUsername..Position::AfterPort],
        namespace,
    }
}

fn storage_scheme(url: &Url) -> &str {
    match url.scheme() {
        "s3a" => "s3",
        // These short forms all take the container from the host and use the
        // configured account. Qualified ABFS URLs are handled separately above.
        "az" | "adl" | "azure" | "abfs" | "abfss" if url.username().is_empty() => "az",
        "az" | "abfs" | "abfss" => "abfs",
        scheme => scheme,
    }
}

fn path_namespace(url: &Url) -> &str {
    // HTTPS cloud URLs can carry a bucket/container in their first path segment.
    // It is part of the store's identity. Use the installed object_store
    // classifier so ordinary HTTP stores do not acquire a container boundary.
    let namespace_in_path = url.scheme() == "https"
        && match ObjectStoreScheme::parse(url) {
            Ok((ObjectStoreScheme::MicrosoftAzure, _)) => true,
            Ok((ObjectStoreScheme::AmazonS3, _)) => url.host_str().is_some_and(|host| {
                // Match s3.<region>.amazonaws.com, not the virtual-hosted URL
                // s3.s3.<region>.amazonaws.com for a bucket named "s3".
                host.strip_prefix("s3.")
                    .and_then(|host| host.strip_suffix(".amazonaws.com"))
                    .is_some_and(|region| !region.contains('.'))
                    || host.ends_with(".r2.cloudflarestorage.com")
            }),
            _ => false,
        };
    if namespace_in_path {
        let path = url.path().strip_prefix('/').unwrap_or(url.path());
        path.split('/').next().unwrap_or("")
    } else {
        ""
    }
}
