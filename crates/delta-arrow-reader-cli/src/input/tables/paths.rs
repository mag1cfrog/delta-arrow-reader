//! Local paths and storage URLs resolved relative to a table manifest.

use std::path::Path;

use object_store::ObjectStoreScheme;
use url::Url;

use super::manifest_error;
use crate::{diagnostics::Error, table_access::direct_credentials::validate_table_url};

pub(super) fn resolve_authorization_file_path(
    path: &str,
    manifest_directory: &Path,
    index: usize,
    field: &str,
) -> Result<String, Error> {
    let invalid_path = || {
        manifest_error(
            &format!("/tables/{index}/authorization/{field}"),
            "invalid_authorization_reference",
            "a nonempty UTF-8 local file path without control characters",
        )
    };
    if path.trim().is_empty()
        || path.chars().any(char::is_control)
        || (!Path::new(path).is_absolute()
            && !matches!(
                Url::parse(path),
                Err(url::ParseError::RelativeUrlWithoutBase)
            ))
    {
        return Err(invalid_path());
    }
    manifest_directory
        .join(path)
        .into_os_string()
        .into_string()
        .map_err(|_| invalid_path())
}

pub(super) fn resolve_location(
    location: &str,
    directory: &Path,
    index: usize,
) -> Result<String, Error> {
    let invalid_location = || {
        manifest_error(
            &format!("/tables/{index}/location"),
            "invalid_table_location",
            "a nonempty UTF-8 path or storage URL without embedded credentials or control characters",
        )
    };
    if location.trim().is_empty() || location.chars().any(char::is_control) {
        return Err(invalid_location());
    }
    if location.starts_with("uc://") {
        // UC names and their profile are validated together after authorization is decoded.
        return Ok(location.to_owned());
    }
    if !Path::new(location).is_absolute() {
        match Url::parse(location) {
            Ok(url) => {
                let (scheme, _) = ObjectStoreScheme::parse(&url).map_err(|_| invalid_location())?;
                validate_table_url(&url, &scheme).map_err(|_| invalid_location())?;
                return Ok(url.into());
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => {}
            Err(_) => return Err(invalid_location()),
        }
    }
    directory
        .join(location)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(invalid_location)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_resolution_distinguishes_explicit_paths_from_storage_urls()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::path::absolute("manifest")?;
        for location in ["../table with spaces", "./name://table", "./-table"] {
            assert_eq!(
                Path::new(&resolve_location(location, &directory, 0)?),
                directory.join(location)
            );
        }
        let absolute_table = directory.join("table");
        let location = absolute_table.to_str().ok_or("non-UTF-8 test directory")?;
        assert_eq!(resolve_location(location, &directory, 0)?, location);
        for location in [
            "file:///tmp/table",
            "s3://bucket/table",
            "gs://bucket/table",
            "abfss://container@account.dfs.core.windows.net/table",
        ] {
            assert_eq!(resolve_location(location, &directory, 0)?, location);
        }
        assert!(resolve_location("https://", &directory, 0).is_err());
        Ok(())
    }
}
