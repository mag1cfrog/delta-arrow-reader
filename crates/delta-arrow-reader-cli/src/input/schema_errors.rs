//! Diagnostic details for JSON inputs that do not match the CLI's expected format.
//!
//! A schema error means that a JSON value does not satisfy the requirements of an
//! input option. For example, `{"op":"and"}` is valid JSON, but the predicate format
//! also requires an `args` field.
//!
//! This module supplies the field location, expected format, and correction hint
//! for those failures. The input parser reports JSON syntax errors; the reader
//! checks filters against the selected table's columns and types.
//!
//! With `--predicate-file filter.json` and `{"op":"and"}` in that file,
//! the diagnostic includes these correction fields:
//!
//! ```json
//! {
//!   "argument": "--predicate-file",
//!   "field_path": "/args",
//!   "expected": "a required field",
//!   "hint": "Supply the missing field."
//! }
//! ```
//!
//! `/args` identifies the missing field. An incomplete JSON document such as `{`
//! is a syntax error instead and has no schema field path.

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
    match fields.deserialize_as::<T>() {
        Ok(_) => None,
        Err(error) => Some(correction_from_serde_error(error, location, None)),
    }
}

fn correction_from_serde_error(
    error: serde_path_to_error::Error<DeserializerError>,
    location: &FieldLocation,
    tag_field: Option<&'static str>,
) -> InputCorrection {
    // Storage keys are user input; never include them in a correction or its field path.
    if matches!(location.input_kind, JsonInputKind::StorageOptions) {
        return storage_options_correction(error.inner(), location);
    }

    match error.inner() {
        DeserializerError::UnknownField(_, fields) => {
            let mut allowed_fields = Vec::new();
            if let Some(tag_field) = tag_field {
                allowed_fields.push(tag_field);
            }
            allowed_fields.extend_from_slice(fields);
            location.invalid(
                format!("object fields: {}", allowed_fields.join(", ")),
                "Remove unsupported fields; use only the listed fields.",
            )
        }
        DeserializerError::DuplicateField(name) => location.field(name).invalid(
            "one occurrence of this field",
            "Remove the duplicate field.",
        ),
        DeserializerError::MissingField(name) => location
            .field(name)
            .invalid("a required field", "Supply the missing field."),
        _ => correction_at_error_path(&error, location),
    }
}

fn storage_options_correction(
    error: &DeserializerError,
    location: &FieldLocation,
) -> InputCorrection {
    match error {
        DeserializerError::DuplicateField(_) => {
            location.invalid("unique object keys", "Remove duplicate object keys.")
        }
        DeserializerError::InvalidType(..) | DeserializerError::InvalidValue(..) => location
            .invalid(
                "string storage option values",
                "Supply a value matching this field's expected format.",
            ),
        _ => location.generic_correction(),
    }
}

fn correction_at_error_path(
    error: &serde_path_to_error::Error<DeserializerError>,
    object_location: &FieldLocation,
) -> InputCorrection {
    // Unknown fields and storage keys are handled before reaching this path.
    let Some(field_location) = object_location.at_serde_path(error.path()) else {
        return object_location.generic_correction();
    };
    match error.inner() {
        DeserializerError::UnknownVariant(_, choices) => field_location.invalid(
            format!("one of: {}", choices.join(", ")),
            "Use one of the listed values.",
        ),
        DeserializerError::InvalidType(_, expected)
        | DeserializerError::InvalidValue(_, expected) => {
            invalid_value_correction(expected, &field_location)
        }
        _ => object_location.generic_correction(),
    }
}

fn invalid_value_correction(serde_expected: &str, location: &FieldLocation) -> InputCorrection {
    let expected = if matches!(location.input_kind, JsonInputKind::ExecutionOptions) {
        execution_options::describe_integer_requirement(location.path.trim_start_matches('/'))
            .unwrap_or_else(|| serde_expected.to_owned())
    } else {
        match serde_expected {
            "u8" => "a JSON integer from 0 to 255",
            "i8" => "a JSON integer from -128 to 127",
            "i32" => "a JSON integer fitting i32",
            _ => serde_expected,
        }
        .to_owned()
    };
    let hint = if serde_expected.starts_with(predicate::CANONICAL_INTEGER_EXPECTATION_PREFIX) {
        "Match the literal type to the inspected column (e.g. Int32 uses int32); encode the value as a decimal string within that type's range, with no '+', leading zeros, or negative zero."
    } else if serde_expected == predicate::TIMEZONE_EXPECTATION {
        "Supply null or a nonempty timezone string matching the column's timezone."
    } else {
        "Supply a value matching this field's expected format."
    };
    location.invalid(expected, hint)
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

    fn at_serde_path(&self, relative_path: &serde_path_to_error::Path) -> Option<Self> {
        use serde_path_to_error::Segment;

        let mut path = self.path.clone();
        for segment in relative_path {
            match segment {
                Segment::Map { key } => path.push_str(&format!("/{key}")),
                Segment::Seq { index } => path.push_str(&format!("/{index}")),
                _ => return None,
            }
        }
        Some(Self {
            input_kind: self.input_kind,
            path,
        })
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
            supplied: None,
            hint,
        }
    }

    fn generic_correction(&self) -> InputCorrection {
        self.input_kind.correction(self.input_kind.schema_hint())
    }
}
