use std::{collections::HashSet, fmt, marker::PhantomData, str::FromStr};

use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, MapAccess, Visitor, value::MapDeserializer},
};
use serde_json::value::RawValue;
use serde_value::DeserializerError;

use super::{JsonInputSource, StorageOptionsInput};
use crate::{diagnostics::InputCorrection, execution_options, predicate};

const PREDICATE_OPERATORS: &[&str] = &[
    "constant",
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "is_null",
    "is_not_null",
    "and",
    "or",
    "not",
];
const SCALAR_TYPES: &[&str] = &[
    "boolean",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "utf8",
    "large_utf8",
    "binary",
    "large_binary",
    "fixed_size_binary",
    "date32",
    "timestamp_us",
    "decimal128",
];

pub(crate) fn correction(bytes: &[u8], input: JsonInputSource) -> Option<InputCorrection> {
    // Analyze only rejected input; raw values preserve duplicate keys and numeric token types.
    let raw = serde_json::from_slice::<&RawValue>(bytes).ok()?;
    let location = FieldLocation {
        input,
        path: String::new(),
    };
    match input {
        JsonInputSource::StorageOptions => {
            deserialize_flat_input::<StorageOptionsInput>(raw, &location)
        }
        JsonInputSource::ExecutionOptions => {
            deserialize_flat_input::<execution_options::ExecutionOptionsInput>(raw, &location)
        }
        JsonInputSource::Predicate => predicate_node(raw, &location, 1),
    }
    .err()
}

struct FieldLocation {
    input: JsonInputSource,
    path: String,
}

impl FieldLocation {
    fn field(&self, name: &'static str) -> Self {
        Self {
            input: self.input,
            path: format!("{}/{name}", self.path),
        }
    }

    fn index(&self, index: usize) -> Self {
        Self {
            input: self.input,
            path: format!("{}/{index}", self.path),
        }
    }

    fn invalid(&self, expected: impl Into<String>, hint: &'static str) -> InputCorrection {
        InputCorrection {
            argument: self.input.argument().to_owned(),
            field_path: Some(self.path.clone()),
            expected: expected.into(),
            hint,
        }
    }

    fn parse<'de, T: Deserialize<'de>>(
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

struct ObjectFields<T>(Vec<(String, T)>);

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

impl<'de> ObjectFields<&'de RawValue> {
    fn check_fields(
        &self,
        allowed: &[&'static str],
        location: &FieldLocation,
    ) -> Result<(), InputCorrection> {
        let mut seen = HashSet::new();
        for (name, _) in &self.0 {
            let Some(known) = allowed.iter().copied().find(|field| field == name) else {
                return Err(location.invalid(
                    format!("object fields: {}", allowed.join(", ")),
                    "Remove unsupported fields; use only the listed fields.",
                ));
            };
            if !seen.insert(known) {
                return Err(location.field(known).invalid(
                    "one occurrence of this field",
                    "Remove the duplicate field.",
                ));
            }
        }
        Ok(())
    }

    fn required(
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

    fn tag(
        &self,
        name: &'static str,
        choices: &[&'static str],
        location: &FieldLocation,
    ) -> Result<&'static str, InputCorrection> {
        let expected = format!("one of: {}", choices.join(", "));
        let value = self.required(name, location).map_err(|mut correction| {
            correction.expected.clone_from(&expected);
            correction
        })?;
        let tag = serde_json::from_str::<String>(value.get()).ok();
        let selected = choices
            .iter()
            .copied()
            .find(|choice| Some(*choice) == tag.as_deref());
        selected.ok_or_else(|| {
            location
                .field(name)
                .invalid(expected, "Use one of the listed values.")
        })
    }
}

fn deserialize_flat_input<T: DeserializeOwned>(
    raw: &RawValue,
    location: &FieldLocation,
) -> Result<(), InputCorrection> {
    // Preserve duplicate keys and integer/float token types while reusing the input's Deserialize.
    let fields = match serde_json::from_str::<ObjectFields<serde_value::Value>>(raw.get()) {
        Ok(fields) => fields,
        Err(error) if error.is_data() => {
            return Err(location.invalid(
                "a JSON object",
                "Supply a value matching this field's expected format.",
            ));
        }
        // shortcut: deeply nested invalid values keep the generic hint; revisit for nested options.
        Err(_) => return Ok(()),
    };
    let mut field = String::new();
    let entries = fields
        .0
        .into_iter()
        .inspect(|(name, _)| field.clone_from(name));
    let error = match T::deserialize(MapDeserializer::<_, DeserializerError>::new(entries)) {
        Ok(_) => return Ok(()),
        Err(error) => error,
    };
    let storage = matches!(location.input, JsonInputSource::StorageOptions);
    let (expected, hint) = match error {
        DeserializerError::UnknownField(_, fields) => {
            return Err(location.invalid(
                format!("object fields: {}", fields.join(", ")),
                "Remove unsupported fields; use only the listed fields.",
            ));
        }
        DeserializerError::DuplicateField(name) => {
            return Err(if storage {
                location.invalid("unique object keys", "Remove duplicate object keys.")
            } else {
                location.field(name).invalid(
                    "one occurrence of this field",
                    "Remove the duplicate field.",
                )
            });
        }
        DeserializerError::UnknownVariant(_, choices) => (
            format!("one of: {}", choices.join(", ")),
            "Use one of the listed values.",
        ),
        DeserializerError::InvalidType(_, expected)
        | DeserializerError::InvalidValue(_, expected) => (
            if storage {
                "string storage option values".to_owned()
            } else {
                execution_options::expected_integer(&field).unwrap_or(expected)
            },
            "Supply a value matching this field's expected format.",
        ),
        _ => return Err(location.input.correction(location.input.schema_hint())),
    };
    let mut correction = location.invalid(expected, hint);
    if !storage {
        // Execution options reject unknown keys before deserializing their values.
        correction.field_path = Some(format!("/{field}"));
    }
    Err(correction)
}

fn predicate_node(
    raw: &RawValue,
    location: &FieldLocation,
    depth: usize,
) -> Result<(), InputCorrection> {
    if depth > predicate::MAX_PREDICATE_DEPTH {
        return Err(location.invalid(
            format!(
                "at most {} predicate levels, including the root",
                predicate::MAX_PREDICATE_DEPTH
            ),
            "Reduce predicate nesting.",
        ));
    }
    let fields: ObjectFields<&RawValue> = location.parse(raw, "a JSON predicate object")?;
    let op = fields.tag("op", PREDICATE_OPERATORS, location)?;
    let allowed: &[&str] = match op {
        "constant" => &["op", "value"],
        "is_null" | "is_not_null" => &["op", "column"],
        "and" | "or" => &["op", "args"],
        "not" => &["op", "arg"],
        _ => &["op", "column", "value"],
    };
    fields.check_fields(allowed, location)?;
    match op {
        "constant" => {
            location
                .field("value")
                .parse::<bool>(fields.required("value", location)?, "a JSON boolean")?;
        }
        "and" | "or" => {
            let args = location.field("args");
            let children: Vec<&RawValue> = args.parse(
                fields.required("args", location)?,
                "an array of predicate objects",
            )?;
            for (index, child) in children.into_iter().enumerate() {
                predicate_node(child, &args.index(index), depth + 1)?;
            }
        }
        "not" => predicate_node(
            fields.required("arg", location)?,
            &location.field("arg"),
            depth + 1,
        )?,
        _ => {
            location.field("column").parse::<String>(
                fields.required("column", location)?,
                "a logical column name as a JSON string",
            )?;
            if !matches!(op, "is_null" | "is_not_null") {
                scalar(
                    fields.required("value", location)?,
                    &location.field("value"),
                )?;
            }
        }
    }
    Ok(())
}

fn integer_string<T: FromStr + fmt::Display>(
    raw: &RawValue,
    location: &FieldLocation,
    minimum: T,
    maximum: T,
) -> Result<(), InputCorrection> {
    predicate::deserialize_integer_string::<_, T>(&mut serde_json::Deserializer::from_str(raw.get()))
        .map(|_| ())
        .map_err(|_| location.invalid(
            format!("canonical decimal integer string from {minimum} to {maximum}"),
            "Use a decimal string within the allowed range, with no '+', leading zeros, or negative zero.",
        ))
}

fn scalar(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    let fields: ObjectFields<&RawValue> = location.parse(raw, "a typed JSON scalar object")?;
    let kind = fields.tag("type", SCALAR_TYPES, location)?;
    let allowed: &[&str] = match kind {
        "fixed_size_binary" => &["type", "value", "size"],
        "timestamp_us" => &["type", "value", "timezone"],
        "decimal128" => &["type", "value", "precision", "scale"],
        _ => &["type", "value"],
    };
    fields.check_fields(allowed, location)?;
    let raw_value = fields.required("value", location)?;
    let value = location.field("value");
    match kind {
        "boolean" => {
            value.parse::<bool>(raw_value, "a JSON boolean")?;
        }
        "int8" => integer_string(raw_value, &value, i8::MIN, i8::MAX)?,
        "int16" => integer_string(raw_value, &value, i16::MIN, i16::MAX)?,
        "int32" | "date32" => integer_string(raw_value, &value, i32::MIN, i32::MAX)?,
        "int64" | "timestamp_us" => integer_string(raw_value, &value, i64::MIN, i64::MAX)?,
        "decimal128" => integer_string(raw_value, &value, i128::MIN, i128::MAX)?,
        "float32" | "float64" => {
            value.parse::<f64>(raw_value, "a JSON number")?;
        }
        "utf8" | "large_utf8" => {
            value.parse::<String>(raw_value, "a JSON string")?;
        }
        "binary" | "large_binary" | "fixed_size_binary" => {
            let bytes: Vec<&RawValue> = value.parse(raw_value, "an array of JSON byte integers")?;
            for (index, byte) in bytes.into_iter().enumerate() {
                value
                    .index(index)
                    .parse::<u8>(byte, "a JSON integer from 0 to 255")?;
            }
        }
        _ => {}
    }
    match kind {
        "fixed_size_binary" => {
            location.field("size").parse::<i32>(
                fields.required("size", location)?,
                "a JSON integer fitting i32",
            )?;
        }
        "timestamp_us" => {
            let timezone = fields.required("timezone", location)?;
            predicate::deserialize_timezone(&mut serde_json::Deserializer::from_str(
                timezone.get(),
            ))
            .map_err(|_| {
                location.field("timezone").invalid(
                    "null or a nonempty JSON string",
                    "Supply null or a nonempty timezone string matching the column's timezone.",
                )
            })?;
        }
        "decimal128" => {
            location.field("precision").parse::<u8>(
                fields.required("precision", location)?,
                "a JSON integer from 0 to 255",
            )?;
            location.field("scale").parse::<i8>(
                fields.required("scale", location)?,
                "a JSON integer from -128 to 127",
            )?;
        }
        _ => {}
    }
    Ok(())
}
