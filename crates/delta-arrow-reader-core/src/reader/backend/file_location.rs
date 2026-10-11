//! Resolve a data URL within the table's already configured object store.

use object_store::path::Path;
use url::Url;

use super::data_file_error;
use crate::{
    DeltaReaderError,
    delta::location::{object_store_path, same_store, with_object_store_path},
};

pub(super) fn resolve_data_file_url(
    table_url: &Url,
    file_path: &str,
) -> Result<Url, DeltaReaderError> {
    let (location, path) = resolve_data_file(table_url, file_path)?;
    with_object_store_path(location, &path)
        .map_err(|error| data_file_error("data_file_path_resolution_failed", error))
}

pub(super) fn resolve_data_file_path(
    table_url: &Url,
    file_path: &str,
) -> Result<Path, DeltaReaderError> {
    resolve_data_file(table_url, file_path).map(|(_, path)| path)
}

fn resolve_data_file(table_url: &Url, file_path: &str) -> Result<(Url, Path), DeltaReaderError> {
    let location = table_url
        .join(file_path)
        .map_err(|error| data_file_error("data_file_path_resolution_failed", error))?;
    let path = object_store_path(&location).map_err(|_| {
        // object_store path errors can contain the complete, unredacted input.
        data_file_error(
            "data_file_path_resolution_failed",
            std::io::Error::other("invalid data file object key"),
        )
    })?;
    if !same_store(table_url, &location) {
        return Err(data_file_error(
            "data_file_store_mismatch",
            std::io::Error::other("data file URL does not identify the configured table store"),
        ));
    }

    Ok((location, path))
}
