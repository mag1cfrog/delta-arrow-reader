//! Storage option strings retained exactly as supplied, with duplicate keys rejected.

use std::fmt;

use delta_arrow_reader::DeltaStorageOptions;
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};

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
