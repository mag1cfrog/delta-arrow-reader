use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer,
    de::{
        DeserializeOwned, IgnoredAny, MapAccess, Visitor,
        value::{MapDeserializer, StringDeserializer},
    },
};
use serde_json::value::RawValue;
use serde_value::DeserializerError;
use strum::VariantNames;

use super::{JsonInputSource, StorageOptionsInput};
use crate::{diagnostics::InputCorrection, execution_options, predicate};

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

    fn tag<T: DeserializeOwned + VariantNames>(
        &self,
        name: &'static str,
        location: &FieldLocation,
    ) -> Result<T, InputCorrection> {
        let expected = format!("one of: {}", T::VARIANTS.join(", "));
        let value = self.required(name, location).map_err(|mut correction| {
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

    fn deserialize_payload<T: DeserializeOwned>(
        &self,
        tag: &'static str,
        location: &FieldLocation,
    ) -> Result<(), InputCorrection> {
        let fields = self
            .0
            .iter()
            .filter(|(name, _)| name != tag)
            .map(|(name, raw)| {
                serde_json::from_str::<serde_value::Value>(raw.get())
                    .map(|value| (name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| location.input.correction(location.input.schema_hint()))?;
        deserialize_fields::<T>(ObjectFields(fields), location, Some(tag))
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
    deserialize_fields::<T>(fields, location, None)
}

fn deserialize_fields<T: DeserializeOwned>(
    fields: ObjectFields<serde_value::Value>,
    location: &FieldLocation,
    tag: Option<&'static str>,
) -> Result<(), InputCorrection> {
    let error =
        match serde_path_to_error::deserialize::<_, T>(
            MapDeserializer::<_, DeserializerError>::new(fields.0.into_iter()),
        ) {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
    let storage = matches!(location.input, JsonInputSource::StorageOptions);
    let (mut expected, hint) = match error.inner() {
        DeserializerError::UnknownField(_, fields) => {
            let fields = tag
                .into_iter()
                .chain(fields.iter().copied())
                .collect::<Vec<_>>();
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
        DeserializerError::MissingField(name) => {
            return Err(location
                .field(name)
                .invalid("a required field", "Supply the missing field."));
        }
        DeserializerError::UnknownVariant(_, choices) => (
            format!("one of: {}", choices.join(", ")),
            "Use one of the listed values.",
        ),
        DeserializerError::InvalidType(_, expected)
        | DeserializerError::InvalidValue(_, expected) => (
            expected.clone(),
            if expected.starts_with("canonical decimal integer string") {
                "Use a decimal string within the allowed range, with no '+', leading zeros, or negative zero."
            } else if expected == "null or a nonempty JSON string" {
                "Supply null or a nonempty timezone string matching the column's timezone."
            } else {
                "Supply a value matching this field's expected format."
            },
        ),
        _ => return Err(location.input.correction(location.input.schema_hint())),
    };
    if storage {
        return Err(location.invalid("string storage option values", hint));
    }
    // Unknown keys were handled above; these strict input structs expose only declared fields.
    let mut path = location.path.clone();
    for segment in error.path() {
        use serde_path_to_error::Segment;
        match segment {
            Segment::Map { key } => path.push_str(&format!("/{key}")),
            Segment::Seq { index } => path.push_str(&format!("/{index}")),
            _ => return Err(location.input.correction(location.input.schema_hint())),
        }
    }
    if matches!(location.input, JsonInputSource::ExecutionOptions) {
        expected =
            execution_options::expected_integer(path.trim_start_matches('/')).unwrap_or(expected);
    } else {
        expected = match expected.as_str() {
            "u8" => "a JSON integer from 0 to 255".to_owned(),
            "i8" => "a JSON integer from -128 to 127".to_owned(),
            "i32" => "a JSON integer fitting i32".to_owned(),
            _ => expected,
        };
    }
    Err(InputCorrection {
        field_path: Some(path),
        ..location.invalid(expected, hint)
    })
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
    use predicate::{
        ColumnInput, ComparisonInput, PredicateChildInput, PredicateChildrenInput,
        PredicateOperator, ScalarValueInput,
    };
    let op = fields.tag::<PredicateOperator>("op", location)?;
    // Decode one node at a time; internally tagged enums otherwise erase nested error paths.
    match op {
        PredicateOperator::Constant => {
            fields.deserialize_payload::<ScalarValueInput<bool>>("op", location)?
        }
        PredicateOperator::And | PredicateOperator::Or => {
            fields.deserialize_payload::<PredicateChildrenInput<IgnoredAny>>("op", location)?;
            let args = location.field("args");
            let children: Vec<&RawValue> = args.parse(
                fields.required("args", location)?,
                "an array of predicate objects",
            )?;
            for (index, child) in children.into_iter().enumerate() {
                predicate_node(child, &args.index(index), depth + 1)?;
            }
        }
        PredicateOperator::Not => {
            fields.deserialize_payload::<PredicateChildInput<IgnoredAny>>("op", location)?;
            predicate_node(
                fields.required("arg", location)?,
                &location.field("arg"),
                depth + 1,
            )?;
        }
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
            fields.deserialize_payload::<ColumnInput>("op", location)?
        }
        PredicateOperator::Eq
        | PredicateOperator::Ne
        | PredicateOperator::Lt
        | PredicateOperator::Le
        | PredicateOperator::Gt
        | PredicateOperator::Ge => {
            fields.deserialize_payload::<ComparisonInput<IgnoredAny>>("op", location)?;
            scalar(
                fields.required("value", location)?,
                &location.field("value"),
            )?;
        }
    }
    Ok(())
}

fn scalar(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    use predicate::{
        DecimalScalarInput, FixedSizeBinaryScalarInput, IntegerScalarInput, ScalarType,
        ScalarValueInput, TimestampScalarInput,
    };
    let fields: ObjectFields<&RawValue> = location.parse(raw, "a typed JSON scalar object")?;
    match fields.tag::<ScalarType>("type", location)? {
        ScalarType::Boolean => {
            fields.deserialize_payload::<ScalarValueInput<bool>>("type", location)
        }
        ScalarType::Int8 => fields.deserialize_payload::<IntegerScalarInput<i8>>("type", location),
        ScalarType::Int16 => {
            fields.deserialize_payload::<IntegerScalarInput<i16>>("type", location)
        }
        ScalarType::Int32 | ScalarType::Date32 => {
            fields.deserialize_payload::<IntegerScalarInput<i32>>("type", location)
        }
        ScalarType::Int64 => {
            fields.deserialize_payload::<IntegerScalarInput<i64>>("type", location)
        }
        ScalarType::Float32 | ScalarType::Float64 => {
            fields.deserialize_payload::<ScalarValueInput<f64>>("type", location)
        }
        ScalarType::Utf8 | ScalarType::LargeUtf8 => {
            fields.deserialize_payload::<ScalarValueInput<String>>("type", location)
        }
        ScalarType::Binary | ScalarType::LargeBinary => {
            fields.deserialize_payload::<ScalarValueInput<Vec<u8>>>("type", location)
        }
        ScalarType::FixedSizeBinary => {
            fields.deserialize_payload::<FixedSizeBinaryScalarInput>("type", location)
        }
        ScalarType::TimestampUs => {
            fields.deserialize_payload::<TimestampScalarInput>("type", location)
        }
        ScalarType::Decimal128 => {
            fields.deserialize_payload::<DecimalScalarInput>("type", location)
        }
    }
}
