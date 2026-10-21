//! Best-effort diagnostics after normal deserialization rejects a JSON input.
//! Helpers returning `Result<(), InputCorrection>` propagate corrections through `Err`.
//! `Ok(())` means no correction was found, including when analysis was skipped;
//! it does not establish that the input is valid.

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

use super::{JsonInputKind, StorageOptionsInput};
use crate::{diagnostics::InputCorrection, execution_options, predicate};

pub(crate) fn diagnose_schema_error(
    bytes: &[u8],
    input_kind: JsonInputKind,
) -> Option<InputCorrection> {
    // Analyze only rejected input; raw values preserve duplicate keys and numeric token types.
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
        JsonInputKind::Predicate => diagnose_predicate_node(raw, &location, 1),
    }
    .err()
}

struct FieldLocation {
    input_kind: JsonInputKind,
    path: String,
}

impl FieldLocation {
    fn field(&self, name: &'static str) -> Self {
        Self {
            input_kind: self.input_kind,
            path: format!("{}/{name}", self.path),
        }
    }

    fn index(&self, index: usize) -> Self {
        Self {
            input_kind: self.input_kind,
            path: format!("{}/{index}", self.path),
        }
    }

    fn invalid(&self, expected: impl Into<String>, hint: &'static str) -> InputCorrection {
        InputCorrection {
            argument: self.input_kind.argument().to_owned(),
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

    fn diagnose_variant_fields<T: DeserializeOwned>(
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
            .map_err(|_| {
                location
                    .input_kind
                    .correction(location.input_kind.schema_hint())
            })?;
        diagnose_fields::<T>(ObjectFields(fields), location, Some(tag))
    }
}

fn diagnose_flat_input<T: DeserializeOwned>(
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
    diagnose_fields::<T>(fields, location, None)
}

fn diagnose_fields<T: DeserializeOwned>(
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
    let storage = matches!(location.input_kind, JsonInputKind::StorageOptions);
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
            if expected.starts_with(predicate::CANONICAL_INTEGER_EXPECTATION_PREFIX) {
                "Use a decimal string within the allowed range, with no '+', leading zeros, or negative zero."
            } else if expected == predicate::TIMEZONE_EXPECTATION {
                "Supply null or a nonempty timezone string matching the column's timezone."
            } else {
                "Supply a value matching this field's expected format."
            },
        ),
        _ => {
            return Err(location
                .input_kind
                .correction(location.input_kind.schema_hint()));
        }
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
            _ => {
                return Err(location
                    .input_kind
                    .correction(location.input_kind.schema_hint()));
            }
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
    Err(InputCorrection {
        field_path: Some(path),
        ..location.invalid(expected, hint)
    })
}

fn diagnose_predicate_node(
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
        ColumnReferenceFields, ComparisonFields, LogicalGroupFields, NegationFields,
        PredicateOperator, ValueFields,
    };
    let op = fields.tag::<PredicateOperator>("op", location)?;
    // Decode one node at a time; internally tagged enums otherwise erase nested error paths.
    match op {
        PredicateOperator::Constant => {
            fields.diagnose_variant_fields::<ValueFields<bool>>("op", location)?
        }
        PredicateOperator::And | PredicateOperator::Or => {
            fields.diagnose_variant_fields::<LogicalGroupFields<IgnoredAny>>("op", location)?;
            let args = location.field("args");
            let children: Vec<&RawValue> = args.parse(
                fields.required("args", location)?,
                "an array of predicate objects",
            )?;
            for (index, child) in children.into_iter().enumerate() {
                diagnose_predicate_node(child, &args.index(index), depth + 1)?;
            }
        }
        PredicateOperator::Not => {
            fields.diagnose_variant_fields::<NegationFields<IgnoredAny>>("op", location)?;
            diagnose_predicate_node(
                fields.required("arg", location)?,
                &location.field("arg"),
                depth + 1,
            )?;
        }
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
            fields.diagnose_variant_fields::<ColumnReferenceFields>("op", location)?
        }
        PredicateOperator::Eq
        | PredicateOperator::Ne
        | PredicateOperator::Lt
        | PredicateOperator::Le
        | PredicateOperator::Gt
        | PredicateOperator::Ge => {
            fields.diagnose_variant_fields::<ComparisonFields<IgnoredAny>>("op", location)?;
            diagnose_scalar(
                fields.required("value", location)?,
                &location.field("value"),
            )?;
        }
    }
    Ok(())
}

fn diagnose_scalar(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    use predicate::{
        Decimal128Fields, FixedSizeBinaryFields, IntegerStringFields, ScalarType,
        TimestampMicrosecondFields, ValueFields,
    };
    let fields: ObjectFields<&RawValue> = location.parse(raw, "a typed JSON scalar object")?;
    match fields.tag::<ScalarType>("type", location)? {
        ScalarType::Boolean => {
            fields.diagnose_variant_fields::<ValueFields<bool>>("type", location)
        }
        ScalarType::Int8 => {
            fields.diagnose_variant_fields::<IntegerStringFields<i8>>("type", location)
        }
        ScalarType::Int16 => {
            fields.diagnose_variant_fields::<IntegerStringFields<i16>>("type", location)
        }
        ScalarType::Int32 | ScalarType::Date32 => {
            fields.diagnose_variant_fields::<IntegerStringFields<i32>>("type", location)
        }
        ScalarType::Int64 => {
            fields.diagnose_variant_fields::<IntegerStringFields<i64>>("type", location)
        }
        ScalarType::Float32 | ScalarType::Float64 => {
            fields.diagnose_variant_fields::<ValueFields<f64>>("type", location)
        }
        ScalarType::Utf8 | ScalarType::LargeUtf8 => {
            fields.diagnose_variant_fields::<ValueFields<String>>("type", location)
        }
        ScalarType::Binary | ScalarType::LargeBinary => {
            fields.diagnose_variant_fields::<ValueFields<Vec<u8>>>("type", location)
        }
        ScalarType::FixedSizeBinary => {
            fields.diagnose_variant_fields::<FixedSizeBinaryFields>("type", location)
        }
        ScalarType::TimestampUs => {
            fields.diagnose_variant_fields::<TimestampMicrosecondFields>("type", location)
        }
        ScalarType::Decimal128 => {
            fields.diagnose_variant_fields::<Decimal128Fields>("type", location)
        }
    }
}
