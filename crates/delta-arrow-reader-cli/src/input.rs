use std::{fmt, fs::File, io::Read, marker::PhantomData};

use delta_arrow_reader::DeltaStorageOptions;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, MapAccess, Visitor, value::MapAccessDeserializer},
};
use snafu::{ResultExt, ensure};

use crate::{Error, InputIoSnafu, InputValidationSnafu, diagnostics::InputCorrection};

pub(crate) mod schema_errors;

const MAX_JSON_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) enum JsonInputKind {
    StorageOptions,
    Predicate,
    ExecutionOptions,
}

impl JsonInputKind {
    pub(crate) fn argument(self) -> &'static str {
        match self {
            Self::StorageOptions => "--storage-options-file",
            Self::Predicate => "--predicate-file",
            Self::ExecutionOptions => "--execution-options-file",
        }
    }

    pub(crate) fn correction(self, hint: &'static str) -> InputCorrection {
        let expected = match self {
            Self::StorageOptions => "a JSON object with unique keys and string values",
            Self::Predicate => "a predicate object with an 'op' tag and typed comparison values",
            Self::ExecutionOptions => "a JSON object containing supported execution options",
        };
        InputCorrection {
            argument: self.argument().to_owned(),
            field_path: None,
            expected: expected.to_owned(),
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
        }
    }
}

pub(crate) fn read_json_file<T: DeserializeOwned>(
    path: &str,
    input_kind: JsonInputKind,
) -> Result<T, Error> {
    let file = File::open(path).context(InputIoSnafu { input_kind })?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON_BYTES + 1)
        .read_to_end(&mut bytes)
        .context(InputIoSnafu { input_kind })?;
    ensure!(
        bytes.len() as u64 <= MAX_JSON_BYTES,
        InputValidationSnafu {
            reason: "input_file_too_large",
            correction: input_kind.correction("Keep the JSON input at or below 1 MiB."),
        }
    );
    parse_json(&bytes, input_kind)
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

// Serde also accepts arrays for structs and tagged enums; require objects.
pub(crate) struct JsonObject<T>(pub(crate) T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = JsonObject<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }

        deserializer.deserialize_map(ObjectVisitor(PhantomData))
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
