//! JSON decoding rules shared by input formats, with errors tied to their CLI option.

use std::{fmt, marker::PhantomData, str::FromStr};

use num_traits::Bounded;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeOwned, MapAccess, Visitor, value::MapAccessDeserializer},
};

use super::{JsonInputKind, schema_errors};
use crate::diagnostics::Error;

pub(super) const CANONICAL_INTEGER_EXPECTATION_PREFIX: &str = "canonical decimal integer string";

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
