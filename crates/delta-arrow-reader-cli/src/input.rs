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

use std::{fmt, fs::File, io::Read, marker::PhantomData, str::FromStr};

use delta_arrow_reader::DeltaStorageOptions;
use num_traits::Bounded;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeOwned, MapAccess, Visitor, value::MapAccessDeserializer},
};
use snafu::{ResultExt, ensure};

use crate::{Error, InputIoSnafu, InputValidationSnafu, diagnostics::InputCorrection};

mod env_file;
mod execution_options;
mod predicate;
mod schema_errors;
mod schemas;

pub(crate) use env_file::{MAX_ENV_FILE_RECORDS, read_env_file};
pub(crate) use execution_options::{ExecutionOptionsInput, execution_options_correction};
pub(crate) use predicate::{PredicateInput, predicate_type_mismatch_correction};

pub(crate) const MAX_INPUT_BYTES: u64 = 1024 * 1024;

const CANONICAL_INTEGER_EXPECTATION_PREFIX: &str = "canonical decimal integer string";

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

pub(crate) fn parse_json<T: DeserializeOwned>(
    bytes: &[u8],
    input_kind: JsonInputKind,
) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|source| {
        let correction = if source.is_data() {
            schema_errors::diagnose_schema_error(bytes, input_kind)
        } else {
            None
        };
        Error::InputJson {
            source,
            input_kind,
            correction,
        }
    })
}

pub(crate) fn deserialize_integer_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr + Bounded + fmt::Display,
{
    struct CanonicalIntegerStringVisitor<T>(PhantomData<T>);

    impl<'de, T: FromStr + Bounded + fmt::Display> de::Visitor<'de>
        for CanonicalIntegerStringVisitor<T>
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "{CANONICAL_INTEGER_EXPECTATION_PREFIX} from {} to {}",
                T::min_value(),
                T::max_value()
            )
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
            let digits = value.strip_prefix('-').unwrap_or(value).as_bytes();
            if value != "0"
                && !(matches!(digits.first(), Some(b'1'..=b'9'))
                    && digits.iter().all(u8::is_ascii_digit))
            {
                return Err(E::invalid_value(de::Unexpected::Str(value), &self));
            }
            value
                .parse()
                .map_err(|_| E::invalid_value(de::Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_str(CanonicalIntegerStringVisitor(PhantomData))
}

// The outer Option records presence, keeping omission separate from explicit null.
pub(crate) fn deserialize_present_field<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

// Serde also accepts arrays for structs and tagged enums; require objects.
pub(crate) struct JsonObject<T>(pub(crate) T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for JsonObjectVisitor<T> {
            type Value = JsonObject<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }

        deserializer.deserialize_map(JsonObjectVisitor(PhantomData))
    }
}

pub(crate) struct StorageOptionsInput(pub(crate) DeltaStorageOptions);

impl<'de> Deserialize<'de> for StorageOptionsInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StorageOptionsVisitor;

        impl<'de> Visitor<'de> for StorageOptionsVisitor {
            type Value = StorageOptionsInput;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string map with unique keys")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut options = DeltaStorageOptions::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    if options.insert(key, value).is_some() {
                        return Err(serde::de::Error::duplicate_field("storage option key"));
                    }
                }
                Ok(StorageOptionsInput(options))
            }
        }

        deserializer.deserialize_map(StorageOptionsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageOptionsInput;

    #[test]
    fn storage_options_preserve_strings() {
        let options: StorageOptionsInput = serde_json::from_str(
            r#"{"AWS_ACCESS_KEY_ID":" secret, value ","MixedCase":"unchanged"}"#,
        )
        .unwrap();
        assert_eq!(options.0["AWS_ACCESS_KEY_ID"], " secret, value ");
        assert_eq!(options.0["MixedCase"], "unchanged");
        assert_eq!(options.0.len(), 2);
    }
}
