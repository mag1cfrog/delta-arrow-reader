use std::{fmt, fs::File, io::Read};

use delta_arrow_reader::DeltaStorageOptions;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, MapAccess, Visitor},
};
use snafu::{ResultExt, ensure};

use crate::{Error, InputIoSnafu, InputJsonSnafu};

const MAX_JSON_BYTES: u64 = 1024 * 1024;

pub(crate) fn read_json_file<T: DeserializeOwned>(path: &str) -> Result<T, Error> {
    let file = File::open(path).context(InputIoSnafu)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON_BYTES + 1)
        .read_to_end(&mut bytes)
        .context(InputIoSnafu)?;
    ensure!(bytes.len() as u64 <= MAX_JSON_BYTES, InputJsonSnafu);
    serde_json::from_slice(&bytes).map_err(|_| InputJsonSnafu.build())
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
                        return Err(serde::de::Error::custom("duplicate object key"));
                    }
                }
                Ok(StorageOptionsInput(options))
            }
        }

        deserializer.deserialize_map(StorageOptionsVisitor)
    }
}
