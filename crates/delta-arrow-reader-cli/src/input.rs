use std::{fmt, fs::File, io::Read};

use delta_arrow_reader::DeltaStorageOptions;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, MapAccess, Visitor},
};

use crate::Error;

const MAX_JSON_BYTES: u64 = 1024 * 1024;

pub(crate) fn read_json<T: DeserializeOwned>(path: &str) -> Result<T, Error> {
    let file = File::open(path).map_err(|_| Error::InputIo)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::InputIo)?;
    if bytes.len() as u64 > MAX_JSON_BYTES {
        return Err(Error::InputJson);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InputJson)
}

pub(crate) struct StorageOptions(pub(crate) DeltaStorageOptions);

impl<'de> Deserialize<'de> for StorageOptions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringMap;

        impl<'de> Visitor<'de> for StringMap {
            type Value = StorageOptions;

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
                Ok(StorageOptions(options))
            }
        }

        deserializer.deserialize_map(StringMap)
    }
}
