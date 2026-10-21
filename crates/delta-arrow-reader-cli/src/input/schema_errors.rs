//! Add safe corrections to JSON inputs that normal deserialization has already rejected.
//! serde_json's error text can include supplied values. Replaying the same Deserialize
//! implementations gives typed errors and paths, so corrections can omit those values.

use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use serde_value::DeserializerError;

use super::{JsonInputKind, StorageOptionsInput, execution_options, predicate};
use crate::diagnostics::InputCorrection;

mod object_fields;

pub(super) use object_fields::ObjectFields;

pub(super) fn diagnose_schema_error(
    bytes: &[u8],
    input_kind: JsonInputKind,
) -> Option<InputCorrection> {
    let raw = serde_json::from_slice::<&RawValue>(bytes).ok()?;
    let location = FieldLocation {
        input_kind,
        path: String::new(),
    };
    match input_kind {
        JsonInputKind::StorageOptions => diagnose_flat_input::<StorageOptionsInput>(raw, &location),
        JsonInputKind::ExecutionOptions => {
            diagnose_flat_input::<execution_options::ExecutionOptionsInput>(raw, &location)
        }
        JsonInputKind::Predicate => predicate::diagnose_schema_error(raw, &location),
    }
}

fn diagnose_flat_input<T: DeserializeOwned>(
    raw: &RawValue,
    location: &FieldLocation,
) -> Option<InputCorrection> {
    // Keep duplicate keys and integer/float token types intact for the schema check.
    let fields = match serde_json::from_str::<ObjectFields<serde_value::Value>>(raw.get()) {
        Ok(fields) => fields,
        Err(error) if error.is_data() => {
            return Some(location.invalid(
                "a JSON object",
                "Supply a value matching this field's expected format.",
            ));
        }
        // shortcut: deeply nested invalid values keep the generic hint; revisit for nested options.
        Err(_) => return None,
    };
    fields.check_schema::<T>(location, None).err()
}

fn correction_from_serde_error(
    error: serde_path_to_error::Error<DeserializerError>,
    location: &FieldLocation,
    tag_field: Option<&'static str>,
) -> InputCorrection {
    let storage = matches!(location.input_kind, JsonInputKind::StorageOptions);
    let (mut expected, hint) = match error.inner() {
        DeserializerError::UnknownField(_, fields) => {
            let fields = tag_field
                .into_iter()
                .chain(fields.iter().copied())
                .collect::<Vec<_>>();
            return location.invalid(
                format!("object fields: {}", fields.join(", ")),
                "Remove unsupported fields; use only the listed fields.",
            );
        }
        DeserializerError::DuplicateField(name) => {
            return if storage {
                location.invalid("unique object keys", "Remove duplicate object keys.")
            } else {
                location.field(name).invalid(
                    "one occurrence of this field",
                    "Remove the duplicate field.",
                )
            };
        }
        DeserializerError::MissingField(name) => {
            return location
                .field(name)
                .invalid("a required field", "Supply the missing field.");
        }
        DeserializerError::UnknownVariant(_, choices) => (
            format!("one of: {}", choices.join(", ")),
            "Use one of the listed values.",
        ),
        DeserializerError::InvalidType(_, expected)
        | DeserializerError::InvalidValue(_, expected) => (
            expected.clone(),
            if expected.starts_with(predicate::CANONICAL_INTEGER_EXPECTATION_PREFIX) {
                "Use a decimal string within the allowed range, with no '+', leading zeros, or negative zero."
            } else if expected == predicate::TIMEZONE_EXPECTATION {
                "Supply null or a nonempty timezone string matching the column's timezone."
            } else {
                "Supply a value matching this field's expected format."
            },
        ),
        _ => return location.generic_correction(),
    };
    if storage {
        return location.invalid("string storage option values", hint);
    }
    // Unknown keys were handled above; these strict input structs expose only declared fields.
    let mut path = location.path.clone();
    for segment in error.path() {
        use serde_path_to_error::Segment;
        match segment {
            Segment::Map { key } => path.push_str(&format!("/{key}")),
            Segment::Seq { index } => path.push_str(&format!("/{index}")),
            _ => return location.generic_correction(),
        }
    }
    if matches!(location.input_kind, JsonInputKind::ExecutionOptions) {
        expected = execution_options::describe_integer_requirement(path.trim_start_matches('/'))
            .unwrap_or(expected);
    } else {
        expected = match expected.as_str() {
            "u8" => "a JSON integer from 0 to 255".to_owned(),
            "i8" => "a JSON integer from -128 to 127".to_owned(),
            "i32" => "a JSON integer fitting i32".to_owned(),
            _ => expected,
        };
    }
    InputCorrection {
        field_path: Some(path),
        ..location.invalid(expected, hint)
    }
}

pub(super) struct FieldLocation {
    input_kind: JsonInputKind,
    path: String,
}

impl FieldLocation {
    pub(super) fn field(&self, name: &'static str) -> Self {
        Self {
            input_kind: self.input_kind,
            path: format!("{}/{name}", self.path),
        }
    }

    pub(super) fn index(&self, index: usize) -> Self {
        Self {
            input_kind: self.input_kind,
            path: format!("{}/{index}", self.path),
        }
    }

    pub(super) fn invalid(
        &self,
        expected: impl Into<String>,
        hint: &'static str,
    ) -> InputCorrection {
        InputCorrection {
            argument: self.input_kind.argument().to_owned(),
            field_path: Some(self.path.clone()),
            expected: expected.into(),
            hint,
        }
    }

    fn generic_correction(&self) -> InputCorrection {
        self.input_kind.correction(self.input_kind.schema_hint())
    }

    pub(super) fn parse<'de, T: serde::Deserialize<'de>>(
        &self,
        raw: &'de RawValue,
        expected: &str,
    ) -> Result<T, InputCorrection> {
        serde_json::from_str(raw.get()).map_err(|_| {
            self.invalid(
                expected,
                "Supply a value matching this field's expected format.",
            )
        })
    }
}
