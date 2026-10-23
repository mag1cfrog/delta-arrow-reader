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
    let location = InputFieldLocation {
        input_kind,
        json_pointer: String::new(),
    };
    match input_kind {
        JsonInputKind::StorageOptions => {
            diagnose_object_schema::<StorageOptionsInput>(raw, &location)
        }
        JsonInputKind::ExecutionOptions => {
            diagnose_object_schema::<execution_options::ExecutionOptionsInput>(raw, &location)
        }
        JsonInputKind::Predicate => predicate::diagnose_schema_error(raw, &location),
        JsonInputKind::Tables => diagnose_manifest_schema_error(raw, &location),
    }
}

fn diagnose_manifest_schema_error(
    raw: &RawValue,
    location: &InputFieldLocation,
) -> Option<InputCorrection> {
    use crate::query::tables_manifest::{ManifestAuthorization, ManifestTable, TablesManifest};

    if let Some(correction) = diagnose_object_schema::<TablesManifest>(raw, location) {
        return Some(correction);
    }
    // Nested Value maps discard duplicate keys; recheck each object from the raw input.
    let fields: ObjectFields<&RawValue> = serde_json::from_str(raw.get()).ok()?;
    let tables: Vec<&RawValue> = serde_json::from_str(fields.field_value("tables")?.get()).ok()?;
    for (index, table) in tables.into_iter().enumerate() {
        let table_location = location.child_field("tables").array_element(index);
        if let Some(correction) = diagnose_object_schema::<ManifestTable>(table, &table_location) {
            return Some(correction);
        }
        let fields: ObjectFields<&RawValue> = serde_json::from_str(table.get()).ok()?;
        if let Some(authorization) = fields.field_value("authorization")
            && let Some(correction) = diagnose_object_schema::<ManifestAuthorization>(
                authorization,
                &table_location.child_field("authorization"),
            )
        {
            return Some(correction);
        }
    }
    None
}

fn diagnose_object_schema<T: DeserializeOwned>(
    raw: &RawValue,
    location: &InputFieldLocation,
) -> Option<InputCorrection> {
    // Keep duplicate keys and integer/float token types intact for the schema check.
    let fields = match serde_json::from_str::<ObjectFields<serde_value::Value>>(raw.get()) {
        Ok(fields) => fields,
        Err(error) if error.is_data() => {
            return Some(location.build_correction(
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
    location: &InputFieldLocation,
    tag_field: Option<&'static str>,
) -> InputCorrection {
    // Storage keys are user input; never include them in a correction or its field path.
    if matches!(location.input_kind, JsonInputKind::StorageOptions) {
        return storage_options_correction(error.inner(), location);
    }

    let nested_location = if matches!(location.input_kind, JsonInputKind::Tables) {
        // UnknownField includes the rejected key; the correction belongs to its object.
        let known_path_len = error
            .path()
            .iter()
            .count()
            .saturating_sub(usize::from(matches!(
                error.inner(),
                DeserializerError::UnknownField(..)
            )));
        location.with_relative_serde_path(error.path().iter().take(known_path_len))
    } else {
        None
    };
    let field_container = nested_location.as_ref().unwrap_or(location);
    match error.inner() {
        DeserializerError::UnknownField(_, fields) => {
            let mut allowed_fields = Vec::new();
            if let Some(tag_field) = tag_field {
                allowed_fields.push(tag_field);
            }
            allowed_fields.extend_from_slice(fields);
            field_container.build_correction(
                format!("object fields: {}", allowed_fields.join(", ")),
                "Remove unsupported fields; use only the listed fields.",
            )
        }
        DeserializerError::DuplicateField(name) => {
            field_container.child_field(name).build_correction(
                "one occurrence of this field",
                "Remove the duplicate field.",
            )
        }
        DeserializerError::MissingField(name) => field_container
            .child_field(name)
            .build_correction("a required field", "Supply the missing field."),
        _ => correction_at_error_path(&error, location),
    }
}

fn storage_options_correction(
    error: &DeserializerError,
    location: &InputFieldLocation,
) -> InputCorrection {
    match error {
        DeserializerError::DuplicateField(_) => {
            location.build_correction("unique object keys", "Remove duplicate object keys.")
        }
        DeserializerError::InvalidType(..) | DeserializerError::InvalidValue(..) => location
            .build_correction(
                "string storage option values",
                "Supply a value matching this field's expected format.",
            ),
        _ => location.input_format_correction(),
    }
}

fn correction_at_error_path(
    error: &serde_path_to_error::Error<DeserializerError>,
    object_location: &InputFieldLocation,
) -> InputCorrection {
    // Unknown fields and storage keys are handled before reaching this path.
    let Some(field_location) = object_location.with_relative_serde_path(error.path()) else {
        return object_location.input_format_correction();
    };
    match error.inner() {
        DeserializerError::UnknownVariant(_, choices) => field_location.build_correction(
            format!("one of: {}", choices.join(", ")),
            "Use one of the listed values.",
        ),
        DeserializerError::InvalidType(_, expected)
        | DeserializerError::InvalidValue(_, expected) => {
            invalid_value_correction(expected, &field_location)
        }
        _ => object_location.input_format_correction(),
    }
}

fn invalid_value_correction(
    serde_expected: &str,
    location: &InputFieldLocation,
) -> InputCorrection {
    let expected = if matches!(location.input_kind, JsonInputKind::ExecutionOptions) {
        execution_options::describe_integer_requirement(
            location.json_pointer.trim_start_matches('/'),
        )
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
    let hint = if serde_expected.starts_with(super::CANONICAL_INTEGER_EXPECTATION_PREFIX)
        && matches!(location.input_kind, JsonInputKind::Tables)
    {
        "Use a decimal string within the stated range, with no '+', leading zeros, or negative zero."
    } else if serde_expected.starts_with(super::CANONICAL_INTEGER_EXPECTATION_PREFIX) {
        "Match the literal type to the inspected column (e.g. Int32 uses int32); encode the value as a decimal string within that type's range, with no '+', leading zeros, or negative zero."
    } else if serde_expected == predicate::TIMEZONE_EXPECTATION {
        "Supply null or a nonempty timezone string matching the column's timezone."
    } else {
        "Supply a value matching this field's expected format."
    };
    location.build_correction(expected, hint)
}

pub(super) struct InputFieldLocation {
    input_kind: JsonInputKind,
    json_pointer: String,
}

impl InputFieldLocation {
    pub(super) fn child_field(&self, name: &'static str) -> Self {
        Self {
            input_kind: self.input_kind,
            json_pointer: format!("{}/{name}", self.json_pointer),
        }
    }

    pub(super) fn array_element(&self, index: usize) -> Self {
        Self {
            input_kind: self.input_kind,
            json_pointer: format!("{}/{index}", self.json_pointer),
        }
    }

    fn with_relative_serde_path<'a>(
        &self,
        relative_path: impl IntoIterator<Item = &'a serde_path_to_error::Segment>,
    ) -> Option<Self> {
        use serde_path_to_error::Segment;

        let mut json_pointer = self.json_pointer.clone();
        for segment in relative_path {
            match segment {
                Segment::Map { key } => json_pointer.push_str(&format!("/{key}")),
                Segment::Seq { index } => json_pointer.push_str(&format!("/{index}")),
                _ => return None,
            }
        }
        Some(Self {
            input_kind: self.input_kind,
            json_pointer,
        })
    }

    pub(super) fn build_correction(
        &self,
        expected: impl Into<String>,
        hint: &'static str,
    ) -> InputCorrection {
        InputCorrection {
            argument: self.input_kind.argument().to_owned(),
            field_path: Some(self.json_pointer.clone()),
            expected: expected.into(),
            supplied: None,
            hint,
        }
    }

    fn input_format_correction(&self) -> InputCorrection {
        self.input_kind.correction(self.input_kind.schema_hint())
    }
}
