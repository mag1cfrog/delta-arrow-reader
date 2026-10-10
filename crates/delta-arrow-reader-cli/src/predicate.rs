use std::{fmt, marker::PhantomData, str::FromStr};

use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor, value::MapAccessDeserializer},
};
use snafu::ensure;

use crate::{Error, InputJsonSnafu};

const MAX_PREDICATE_NODES: usize = 1024;
const MAX_PREDICATE_DEPTH: usize = 32;

#[derive(Deserialize)]
#[serde(try_from = "JsonObject<PredicateNode>")]
pub(crate) struct PredicateInput(pub(crate) DeltaPredicate);

// Serde's tagged enums also accept arrays; require objects at every JSON node.
struct JsonObject<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = JsonObject<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }

        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum PredicateNode {
    Constant {
        value: bool,
    },
    Eq {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    Ne {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    Lt {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    Le {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    Gt {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    Ge {
        column: String,
        value: JsonObject<ScalarInput>,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And {
        args: Vec<JsonObject<Self>>,
    },
    Or {
        args: Vec<JsonObject<Self>>,
    },
    Not {
        arg: Box<JsonObject<Self>>,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ScalarInput {
    Boolean {
        value: bool,
    },
    Int8 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i8,
    },
    Int16 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i16,
    },
    Int32 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i32,
    },
    Int64 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i64,
    },
    Float32 {
        value: f64,
    },
    Float64 {
        value: f64,
    },
    Utf8 {
        value: String,
    },
    LargeUtf8 {
        value: String,
    },
    Binary {
        value: Vec<u8>,
    },
    LargeBinary {
        value: Vec<u8>,
    },
    FixedSizeBinary {
        size: i32,
        value: Vec<u8>,
    },
    Date32 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i32,
    },
    TimestampUs {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i64,
        #[serde(deserialize_with = "deserialize_timezone")]
        timezone: Option<String>,
    },
    Decimal128 {
        #[serde(deserialize_with = "deserialize_integer_string")]
        value: i128,
        precision: u8,
        scale: i8,
    },
}

fn deserialize_integer_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
{
    let value = String::deserialize(deserializer)?;
    let digits = value.strip_prefix('-').unwrap_or(&value).as_bytes();
    if value != "0"
        && !(matches!(digits.first(), Some(b'1'..=b'9')) && digits.iter().all(u8::is_ascii_digit))
    {
        return Err(de::Error::custom("expected a canonical integer string"));
    }
    value
        .parse()
        .map_err(|_| de::Error::custom("integer out of range"))
}

fn deserialize_timezone<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let timezone = Option::<String>::deserialize(deserializer)?;
    if timezone.as_ref().is_some_and(String::is_empty) {
        return Err(de::Error::custom("expected null or a nonempty timezone"));
    }
    Ok(timezone)
}

impl TryFrom<JsonObject<PredicateNode>> for PredicateInput {
    type Error = Error;

    fn try_from(node: JsonObject<PredicateNode>) -> Result<Self, Error> {
        let mut node_count = 0;
        node.0.into_predicate(1, &mut node_count).map(Self)
    }
}

impl PredicateNode {
    fn into_predicate(self, depth: usize, node_count: &mut usize) -> Result<DeltaPredicate, Error> {
        ensure!(
            depth <= MAX_PREDICATE_DEPTH && *node_count < MAX_PREDICATE_NODES,
            InputJsonSnafu
        );
        *node_count += 1;
        Ok(match self {
            Self::Constant { value } => DeltaPredicate::Constant(value),
            Self::Eq { column, value } => convert_comparison(column, DeltaComparison::Eq, value)?,
            Self::Ne { column, value } => {
                convert_comparison(column, DeltaComparison::NotEq, value)?
            }
            Self::Lt { column, value } => convert_comparison(column, DeltaComparison::Lt, value)?,
            Self::Le { column, value } => convert_comparison(column, DeltaComparison::LtEq, value)?,
            Self::Gt { column, value } => convert_comparison(column, DeltaComparison::Gt, value)?,
            Self::Ge { column, value } => convert_comparison(column, DeltaComparison::GtEq, value)?,
            Self::IsNull { column } => DeltaPredicate::IsNull { column },
            Self::IsNotNull { column } => DeltaPredicate::IsNotNull { column },
            Self::And { args } => DeltaPredicate::And(
                args.into_iter()
                    .map(|arg| arg.0.into_predicate(depth + 1, node_count))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or { args } => DeltaPredicate::Or(
                args.into_iter()
                    .map(|arg| arg.0.into_predicate(depth + 1, node_count))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not { arg } => {
                DeltaPredicate::Not(Box::new(arg.0.into_predicate(depth + 1, node_count)?))
            }
        })
    }
}

fn convert_comparison(
    column: String,
    op: DeltaComparison,
    value: JsonObject<ScalarInput>,
) -> Result<DeltaPredicate, Error> {
    Ok(DeltaPredicate::Compare {
        column,
        op,
        value: value.0.into_scalar()?,
    })
}

impl ScalarInput {
    fn into_scalar(self) -> Result<DeltaScalar, Error> {
        Ok(match self {
            Self::Boolean { value } => DeltaScalar::Boolean(value),
            Self::Int8 { value } => DeltaScalar::Int8(value),
            Self::Int16 { value } => DeltaScalar::Int16(value),
            Self::Int32 { value } => DeltaScalar::Int32(value),
            Self::Int64 { value } => DeltaScalar::Int64(value),
            Self::Float32 { value } => {
                let value = value as f32;
                ensure!(value.is_finite(), InputJsonSnafu);
                DeltaScalar::Float32(value)
            }
            Self::Float64 { value } => {
                ensure!(value.is_finite(), InputJsonSnafu);
                DeltaScalar::Float64(value)
            }
            Self::Utf8 { value } => DeltaScalar::Utf8(value),
            Self::LargeUtf8 { value } => DeltaScalar::LargeUtf8(value),
            Self::Binary { value } => DeltaScalar::Binary(value),
            Self::LargeBinary { value } => DeltaScalar::LargeBinary(value),
            Self::FixedSizeBinary { size, value } => {
                ensure!(size > 0 && size as usize == value.len(), InputJsonSnafu);
                DeltaScalar::FixedSizeBinary { size, value }
            }
            Self::Date32 { value } => DeltaScalar::Date32(value),
            Self::TimestampUs { value, timezone } => {
                DeltaScalar::TimestampMicrosecond { value, timezone }
            }
            Self::Decimal128 {
                value,
                precision,
                scale,
            } => DeltaScalar::Decimal128 {
                value,
                precision,
                scale,
            },
        })
    }
}

#[cfg(test)]
mod tests;
