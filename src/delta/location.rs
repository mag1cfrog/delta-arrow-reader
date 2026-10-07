//! Delta table location normalization.

use object_store::{ObjectStoreScheme, path::Path};
use url::{Position, Url};

use super::kernel::parse_table_location;
use crate::{DeltaReaderError, error::InvalidTableLocationSnafu};

pub(crate) fn object_store_path(url: &Url) -> object_store::Result<Path> {
    if let Some(path) = s3_virtual_host_path_workaround(url)? {
        return Ok(path);
    }
    match ObjectStoreScheme::parse(url) {
        Ok((_, path)) => Ok(path),
        // Registered custom stores use the URL's path. Invalid keys still fail
        // Path validation here; object_store's parse error type is private.
        Err(_) => Ok(Path::from_url_path(url.path())?),
    }
}

// object_store mistakes virtual-hosted buckets starting with "s3" for path-style URLs.
// Remove this helper and its call once our minimum object_store version fixes
// https://github.com/mag1cfrog/delta-arrow-reader/issues/399.
fn s3_virtual_host_path_workaround(url: &Url) -> object_store::Result<Option<Path>> {
    if url.scheme() == "https"
        && url.host_str().is_some_and(|host| {
            host.starts_with("s3") && host.contains(".s3.") && host.ends_with(".amazonaws.com")
        })
    {
        return Ok(Some(Path::from_url_path(url.path())?));
    }
    Ok(None)
}

/// Replaces the URL path with a store-relative object key for Kernel I/O.
/// Encodes the key once, preserves the URL's trailing directory slash, and
/// removes query strings and fragments.
pub(crate) fn with_object_store_path(mut url: Url, path: &Path) -> delta_kernel::DeltaResult<Url> {
    let directory = url.path().ends_with('/');
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| delta_kernel::Error::generic("object store URL cannot contain a path"))?;
        segments.clear().extend(path.parts());
        if directory {
            segments.push("");
        }
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// Normalizes a Delta table path or URL for snapshot loading.
pub(crate) fn normalize_table_location(table_location: &str) -> Result<url::Url, DeltaReaderError> {
    if table_location.trim().is_empty() {
        return InvalidTableLocationSnafu {
            reason: "empty_table_location",
        }
        .fail();
    }

    parse_table_location(table_location)
        .ok()
        .filter(|url| path_namespace(url).is_some())
        .ok_or_else(|| {
            InvalidTableLocationSnafu {
                reason: "invalid_table_location",
            }
            .build()
        })
}

// Compare the full authority, including an ABFS container in the username.
// Endpoint aliases cannot be inferred from bucket names: options may select a
// private store. Azure's documented account/container aliases are handled below.
pub(crate) fn same_store(table: &Url, file: &Url) -> bool {
    let (Some(table_namespace), Some(file_namespace)) =
        (path_namespace(table), path_namespace(file))
    else {
        return false;
    };
    storage_identity(table, table_namespace) == storage_identity(file, file_namespace)
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

fn path_namespace(url: &Url) -> Option<&str> {
    // A prefix removed by object_store identifies the bucket/container, not
    // part of the object key. Keep it in identity checks for relative paths too.
    // Invalid paths have no identity; two failures must never compare equal.
    let full = Path::from_url_path(url.path()).ok()?;
    let key = object_store_path(url).ok()?;
    Some(if full != key {
        let path = url.path().strip_prefix('/').unwrap_or(url.path());
        path.split('/').next().unwrap_or("")
    } else {
        ""
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::normalize_table_location;
    use crate::DeltaReaderPhase;

    struct TestDir(PathBuf);

    impl TestDir {
        fn absolute(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = std::env::temp_dir().join(unique_name(name)?);
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        fn relative(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-location-tests")
                .join(unique_name(name)?);
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    #[test]
    fn normalizes_absolute_and_relative_local_paths() -> Result<(), Box<dyn std::error::Error>> {
        let absolute = TestDir::absolute("absolute")?;
        let relative = TestDir::relative("relative")?;

        let absolute_uri = normalize_table_location(&absolute.0.to_string_lossy())?;
        let relative_uri = normalize_table_location(&relative.0.to_string_lossy())?;
        let relative_path = relative_uri
            .to_file_path()
            .map_err(|()| std::io::Error::other("expected a local file URI"))?;

        assert!(absolute_uri.as_str().starts_with("file://"));
        assert!(absolute_uri.as_str().ends_with('/'));
        assert_eq!(
            fs::canonicalize(relative_path)?,
            fs::canonicalize(&relative.0)?
        );
        assert_eq!(
            normalize_table_location(absolute_uri.as_str())?,
            absolute_uri
        );
        Ok(())
    }

    #[test]
    fn preserves_remote_url_semantics_without_opening_a_store()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            normalize_table_location("s3://bucket/path/to/table")?.as_str(),
            "s3://bucket/path/to/table/"
        );
        Ok(())
    }

    #[test]
    fn rejects_empty_missing_and_hostile_locations_without_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let missing = std::env::temp_dir()
            .join("sensitive-missing-table")
            .join(unique_name("missing")?);
        let parent = TestDir::absolute("regular-file")?;
        let regular_file = parent.0.join("not-a-directory");
        fs::write(&regular_file, "not a table")?;

        for (table_location, expected_reason) in [
            ("", "empty_table_location"),
            (" \t\n", "empty_table_location"),
            (&missing.to_string_lossy(), "invalid_table_location"),
            (&regular_file.to_string_lossy(), "invalid_table_location"),
            (
                "s3://secret-user:secret-password@[",
                "invalid_table_location",
            ),
        ] {
            let error =
                normalize_table_location(table_location).expect_err("location should be rejected");
            assert_eq!(error.code(), "invalid_table_location");
            assert_eq!(error.phase(), DeltaReaderPhase::TableLocation);
            assert!(error.to_string().contains(expected_reason));
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
        }

        Ok(())
    }
}
