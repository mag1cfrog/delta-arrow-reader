//! Parquet data-file backend implementations.

pub(crate) mod direct_parquet;
mod file_location;
pub(crate) mod kernel_reader;

use snafu::IntoError;

use crate::{DeltaReaderError, error::DataFileReadSnafu};

fn data_file_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    DataFileReadSnafu { reason }.into_error(Box::new(source))
}
