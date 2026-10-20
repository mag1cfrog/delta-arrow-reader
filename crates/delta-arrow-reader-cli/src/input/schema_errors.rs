use std::{collections::HashSet, fmt, str::FromStr};

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use serde_json::value::RawValue;

use super::JsonInputSource;
use crate::{diagnostics::InputCorrection, execution_options, predicate};

const EXECUTION_FIELDS: &[&str] = &[
    "parquet_backend",
    "max_concurrent_file_reads_per_scan",
    "max_concurrent_file_reads_per_partition",
    "output_buffer_batches_per_partition",
    "prefetch_files_per_partition",
    "parquet_metadata_size_hint_bytes",
    "parquet_full_file_read_threshold_bytes",
];
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
        JsonInputSource::StorageOptions => storage_options(raw, &location),
        JsonInputSource::ExecutionOptions => execution_options(raw, &location),
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

struct ObjectFields<'de>(Vec<(String, &'de RawValue)>);

impl<'de> Deserialize<'de> for ObjectFields<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = ObjectFields<'de>;

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
        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl<'de> ObjectFields<'de> {
    fn check_fields(
        &self,
        allowed: Option<&[&'static str]>,
        location: &FieldLocation,
    ) -> Result<(), InputCorrection> {
        let mut seen = HashSet::new();
        for (name, _) in &self.0 {
            let known =
                allowed.and_then(|fields| fields.iter().copied().find(|field| field == name));
            if let Some(fields) = allowed
                && known.is_none()
            {
                return Err(location.invalid(
                    format!("object fields: {}", fields.join(", ")),
                    "Remove unsupported fields; use only the listed fields.",
                ));
            }
            if !seen.insert(name) {
                return Err(match known {
                    Some(field) => location.field(field).invalid(
                        "one occurrence of this field",
                        "Remove the duplicate field.",
                    ),
                    None => location.invalid("unique object keys", "Remove duplicate object keys."),
                });
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

fn storage_options(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    let fields: ObjectFields<'_> = location.parse(raw, "a JSON object with string values")?;
    fields.check_fields(None, location)?;
    for (_, value) in &fields.0 {
        location.parse::<String>(value, "string storage option values")?;
    }
    Ok(())
}

fn execution_options(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    let fields: ObjectFields<'_> =
        location.parse(raw, "a JSON object with execution option fields")?;
    fields.check_fields(Some(EXECUTION_FIELDS), location)?;
    for &name in EXECUTION_FIELDS {
        let Some((_, value)) = fields.0.iter().find(|(field, _)| field == name) else {
            continue;
        };
        let field = location.field(name);
        if name == "parquet_backend" {
            fields.tag(name, &["direct", "delta_kernel"], location)?;
            continue;
        }
        let Some(expected) = execution_options::expected_integer(name) else {
            continue;
        };
        match name {
            "max_concurrent_file_reads_per_scan"
            | "parquet_metadata_size_hint_bytes"
            | "parquet_full_file_read_threshold_bytes" => {
                field.parse::<Option<usize>>(value, &expected)?;
            }
            _ => {
                field.parse::<usize>(value, &expected)?;
            }
        }
    }
    Ok(())
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
    let fields: ObjectFields<'_> = location.parse(raw, "a JSON predicate object")?;
    let op = fields.tag("op", PREDICATE_OPERATORS, location)?;
    let allowed: &[&str] = match op {
        "constant" => &["op", "value"],
        "is_null" | "is_not_null" => &["op", "column"],
        "and" | "or" => &["op", "args"],
        "not" => &["op", "arg"],
        _ => &["op", "column", "value"],
    };
    fields.check_fields(Some(allowed), location)?;
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
    let fields: ObjectFields<'_> = location.parse(raw, "a typed JSON scalar object")?;
    let kind = fields.tag("type", SCALAR_TYPES, location)?;
    let allowed: &[&str] = match kind {
        "fixed_size_binary" => &["type", "value", "size"],
        "timestamp_us" => &["type", "value", "timezone"],
        "decimal128" => &["type", "value", "precision", "scale"],
        _ => &["type", "value"],
    };
    fields.check_fields(Some(allowed), location)?;
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
