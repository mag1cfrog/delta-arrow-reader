//! File inputs for table declarations, storage settings, credentials, and row filters.
//!
//! Each file is associated with a CLI option and has its own accepted structure
//! and value types. This module loads the files and associates read and format
//! errors with the corresponding option. The predicate and execution-options
//! submodules define how their file contents become reader settings.
//!
//! For `--execution-options-file scan.json`, these failures have different causes:
//!
//! - A missing `scan.json` is a file-read error.
//! - A file containing only `{` has incomplete JSON syntax.
//! - `{"prefetch_files_per_partition":"none"}` has valid JSON syntax but gives a
//!   string where this option requires an integer.
//!
//! Each diagnostic identifies `--execution-options-file` as the input to correct.

use std::{fs::File, io::Read};

use serde::de::DeserializeOwned;
use snafu::{ResultExt, ensure};

use crate::diagnostics::{Error, InputCorrection, InputIoSnafu, InputValidationSnafu};

mod env_file;
mod execution_options;
mod json;
mod predicate;
mod schema_errors;
mod schemas;
mod storage_options;
mod tables;

pub(crate) use env_file::{MAX_ENV_FILE_RECORDS, read_env_file};
pub(crate) use execution_options::{ExecutionOptionsInput, execution_options_correction};
pub(crate) use json::{
    JsonObject, deserialize_integer_string, deserialize_present_field, parse_json,
};
pub(crate) use predicate::{PredicateInput, predicate_type_mismatch_correction};
pub(crate) use storage_options::StorageOptionsInput;
pub(crate) use tables::{MAX_TABLES, read_table_requests};

pub(crate) const MAX_INPUT_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum JsonInputKind {
    StorageOptions,
    Predicate,
    ExecutionOptions,
    Tables,
}

impl JsonInputKind {
    pub(crate) fn argument(self) -> &'static str {
        match self {
            Self::StorageOptions => "--storage-options-file",
            Self::Predicate => "--predicate-file",
            Self::ExecutionOptions => "--execution-options-file",
            Self::Tables => "--tables-file",
        }
    }

    pub(crate) fn correction(self, hint: &'static str) -> InputCorrection {
        let expected = match self {
            Self::StorageOptions => "a JSON object with unique keys and string values",
            Self::Predicate => "a predicate object with an 'op' tag and typed comparison values",
            Self::ExecutionOptions => "a JSON object containing supported execution options",
            Self::Tables => "a versioned JSON object with a nonempty tables array",
        };
        InputCorrection {
            argument: self.argument().to_owned(),
            field_path: None,
            expected: expected.to_owned(),
            supplied: None,
            hint,
        }
    }

    pub(crate) fn schema_hint(self) -> &'static str {
        match self {
            Self::StorageOptions => {
                "Use a JSON object; keep every key unique and every value a string."
            }
            Self::Predicate => {
                "Use a documented 'op', its required fields, and a supported scalar 'type'; do not repeat fields."
            }
            Self::ExecutionOptions => {
                "Use only documented execution-option fields, once each, with their declared types."
            }
            Self::Tables => {
                "Use format_version 1 and a tables array; each object has alias, location, and optional table_version and authorization. Authorization requires source and may reference storage_options_file, env_file, or databricks_profile; inline credentials are invalid. Do not repeat fields."
            }
        }
    }
}

pub(crate) fn read_json_file<T: DeserializeOwned>(
    path: &str,
    input_kind: JsonInputKind,
) -> Result<T, Error> {
    read_json_file_with_bytes(path, input_kind).map(|(value, _)| value)
}

pub(crate) fn read_json_file_with_bytes<T: DeserializeOwned>(
    path: &str,
    input_kind: JsonInputKind,
) -> Result<(T, Vec<u8>), Error> {
    let bytes = read_bounded_input_bytes(path, input_kind.argument())?;
    ensure!(
        bytes.len() as u64 <= MAX_INPUT_BYTES,
        InputValidationSnafu {
            reason: "input_file_too_large",
            correction: input_kind.correction("Keep the JSON input at or below 1 MiB."),
        }
    );
    Ok((parse_json(&bytes, input_kind)?, bytes))
}

fn read_bounded_input_bytes(path: &str, argument: &'static str) -> Result<Vec<u8>, Error> {
    let file = File::open(path).context(InputIoSnafu { argument })?;
    let mut bytes = Vec::new();
    // One extra byte lets each format report its own size-limit correction.
    file.take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .context(InputIoSnafu { argument })?;
    Ok(bytes)
}
