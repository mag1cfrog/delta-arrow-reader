//! Shared object checks for JSON input diagnostics.
//!
//! The caller selects the expected object schema. This module reports missing,
//! repeated, or invalid fields within that object. Traversal through nested
//! predicates is handled by the predicate schema diagnostics.
//!
//! For example, this condition supplies `value` twice:
//!
//! ```json
//! {"op":"constant","value":true,"value":false}
//! ```
//!
//! The correction points to `/value` and expects `one occurrence of this field`.
//! The duplicate is an error; neither occurrence takes precedence.

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

use super::{InputFieldLocation, correction_from_serde_error};
use crate::diagnostics::InputCorrection;

pub(in crate::input) struct ObjectFields<T> {
    // A map would discard duplicate fields before diagnostics could report them.
    entries: Vec<(String, T)>,
}

impl<'de> ObjectFields<&'de RawValue> {
    pub(in crate::input) fn field_value(&self, name: &str) -> Option<&'de RawValue> {
        for (field, value) in &self.entries {
            if field == name {
                return Some(*value);
            }
        }
        None
    }

    pub(in crate::input) fn required_field(
        &self,
        name: &'static str,
        location: &InputFieldLocation,
    ) -> Result<&'de RawValue, InputCorrection> {
        match self.field_value(name) {
            Some(value) => Ok(value),
            None => Err(location
                .child_field(name)
                .build_correction("a required field", "Supply the missing field.")),
        }
    }

    pub(in crate::input) fn deserialize_tag<T: DeserializeOwned + VariantNames>(
        &self,
        name: &'static str,
        location: &InputFieldLocation,
    ) -> Result<T, InputCorrection> {
        let expected = format!("one of: {}", T::VARIANTS.join(", "));
        let Some(value) = self.field_value(name) else {
            return Err(location
                .child_field(name)
                .build_correction(expected, "Supply the missing field."));
        };
        let occurrences = self
            .entries
            .iter()
            .filter(|(field, _)| field == name)
            .count();
        if occurrences > 1 {
            return Err(location.child_field(name).build_correction(
                "one occurrence of this field",
                "Remove the duplicate field.",
            ));
        }
        let tag_text = match serde_json::from_str::<String>(value.get()) {
            Ok(text) => text,
            Err(_) => {
                return Err(location
                    .child_field(name)
                    .build_correction(expected, "Use one of the listed values."));
            }
        };
        T::deserialize(StringDeserializer::<DeserializerError>::new(tag_text)).map_err(|_| {
            location
                .child_field(name)
                .build_correction(expected, "Use one of the listed values.")
        })
    }

    pub(in crate::input) fn check_variant_fields<T: DeserializeOwned>(
        &self,
        tag_field: &'static str,
        location: &InputFieldLocation,
    ) -> Result<(), InputCorrection> {
        let mut entries = Vec::new();
        for (name, raw_value) in &self.entries {
            if name == tag_field {
                continue;
            }
            let value = serde_json::from_str::<serde_value::Value>(raw_value.get())
                .map_err(|_| location.input_format_correction())?;
            entries.push((name.clone(), value));
        }
        let fields = ObjectFields { entries };
        match fields.deserialize_as::<T>() {
            Ok(_) => Ok(()),
            Err(error) => Err(correction_from_serde_error(
                error,
                location,
                Some(tag_field),
            )),
        }
    }
}

impl ObjectFields<serde_value::Value> {
    pub(super) fn deserialize_as<T: DeserializeOwned>(
        self,
    ) -> Result<T, serde_path_to_error::Error<DeserializerError>> {
        // Typed errors let callers describe a failure without formatting input values.
        let deserializer = MapDeserializer::<_, DeserializerError>::new(self.entries.into_iter());
        serde_path_to_error::deserialize(deserializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ObjectFields<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectFieldsVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectFieldsVisitor<T> {
            type Value = ObjectFields<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(ObjectFields { entries })
            }
        }
        deserializer.deserialize_map(ObjectFieldsVisitor(PhantomData))
    }
}
