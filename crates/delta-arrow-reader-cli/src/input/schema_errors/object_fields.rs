//! Keep object entries in order, including duplicates that a JSON map would discard.

use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer,
    de::{
        DeserializeOwned, MapAccess, Visitor,
        value::{MapDeserializer, StringDeserializer},
    },
};
use serde_json::value::RawValue;
use serde_value::DeserializerError;
use strum::VariantNames;

use super::{FieldLocation, correction_from_serde_error};
use crate::diagnostics::InputCorrection;

pub(in crate::input) struct ObjectFields<T>(Vec<(String, T)>);

impl<'de> ObjectFields<&'de RawValue> {
    pub(in crate::input) fn required_field(
        &self,
        name: &'static str,
        location: &FieldLocation,
    ) -> Result<&'de RawValue, InputCorrection> {
        self.0
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| *value)
            .ok_or_else(|| {
                location
                    .field(name)
                    .invalid("a required field", "Supply the missing field.")
            })
    }

    pub(in crate::input) fn deserialize_tag<T: DeserializeOwned + VariantNames>(
        &self,
        name: &'static str,
        location: &FieldLocation,
    ) -> Result<T, InputCorrection> {
        let expected = format!("one of: {}", T::VARIANTS.join(", "));
        let value = self
            .required_field(name, location)
            .map_err(|mut correction| {
                correction.expected.clone_from(&expected);
                correction
            })?;
        if self.0.iter().filter(|(field, _)| field == name).count() > 1 {
            return Err(location.field(name).invalid(
                "one occurrence of this field",
                "Remove the duplicate field.",
            ));
        }
        let tag = serde_json::from_str::<String>(value.get()).ok();
        tag.and_then(|tag| T::deserialize(StringDeserializer::<DeserializerError>::new(tag)).ok())
            .ok_or_else(|| {
                location
                    .field(name)
                    .invalid(expected, "Use one of the listed values.")
            })
    }

    pub(in crate::input) fn check_variant_fields<T: DeserializeOwned>(
        &self,
        tag_field: &'static str,
        location: &FieldLocation,
    ) -> Result<(), InputCorrection> {
        let fields = self
            .0
            .iter()
            .filter(|(name, _)| name != tag_field)
            .map(|(name, raw)| {
                serde_json::from_str::<serde_value::Value>(raw.get())
                    .map(|value| (name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| location.generic_correction())?;
        ObjectFields(fields).check_schema::<T>(location, Some(tag_field))
    }
}

impl ObjectFields<serde_value::Value> {
    pub(in crate::input) fn check_schema<T: DeserializeOwned>(
        self,
        location: &FieldLocation,
        tag_field: Option<&'static str>,
    ) -> Result<(), InputCorrection> {
        serde_path_to_error::deserialize::<_, T>(MapDeserializer::<_, DeserializerError>::new(
            self.0.into_iter(),
        ))
        .map(|_| ())
        .map_err(|error| correction_from_serde_error(error, location, tag_field))
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ObjectFields<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = ObjectFields<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    fields.push(entry);
                }
                Ok(ObjectFields(fields))
            }
        }
        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}
