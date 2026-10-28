//! Typed predicate literals and their conversion to reader scalars.

use std::{fmt, str::FromStr};

use delta_arrow_reader::DeltaScalar;
use num_traits::Bounded;
use serde::{Deserialize, Deserializer, de};
use snafu::ensure;

use super::{ValueFields, diagnostics::validation_correction};
use crate::{
    diagnostics::{Error, InputValidationSnafu},
    input::deserialize_integer_string,
};

// Schema diagnostics match this custom Serde expectation to select a correction hint.
pub(in crate::input) const TIMEZONE_EXPECTATION: &str = "null or a nonempty JSON string";

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(ScalarType))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
pub(super) enum ScalarInput {
    Boolean(ValueFields<bool>),
    Int8(IntegerStringFields<i8>),
    Int16(IntegerStringFields<i16>),
    Int32(IntegerStringFields<i32>),
    Int64(IntegerStringFields<i64>),
    Float32(ValueFields<f64>),
    Float64(ValueFields<f64>),
    Utf8(ValueFields<String>),
    LargeUtf8(ValueFields<String>),
    Binary(ValueFields<Vec<u8>>),
    LargeBinary(ValueFields<Vec<u8>>),
    FixedSizeBinary(FixedSizeBinaryFields),
    Date32(IntegerStringFields<i32>),
    TimestampUs(TimestampMicrosecondFields),
    Decimal128(Decimal128Fields),
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "T: FromStr + Bounded + fmt::Display")
)]
pub(super) struct IntegerStringFields<T> {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixedSizeBinaryFields {
    size: i32,
    value: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TimestampMicrosecondFields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i64,
    #[serde(deserialize_with = "deserialize_timezone")]
    timezone: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decimal128Fields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i128,
    precision: u8,
    scale: i8,
}

impl ScalarInput {
    pub(super) fn into_scalar(self) -> Result<DeltaScalar, Error> {
        Ok(match self {
            Self::Boolean(ValueFields { value }) => DeltaScalar::Boolean(value),
            Self::Int8(IntegerStringFields { value }) => DeltaScalar::Int8(value),
            Self::Int16(IntegerStringFields { value }) => DeltaScalar::Int16(value),
            Self::Int32(IntegerStringFields { value }) => DeltaScalar::Int32(value),
            Self::Int64(IntegerStringFields { value }) => DeltaScalar::Int64(value),
            Self::Float32(ValueFields { value }) => {
                let value = value as f32;
                ensure!(
                    value.is_finite(),
                    InputValidationSnafu {
                        reason: "predicate_non_finite_float",
                        correction: validation_correction(
                            "/value",
                            "JSON number that remains finite after conversion to float32"
                                .to_owned(),
                            "Use a finite number within the float32 range.",
                        ),
                    }
                );
                DeltaScalar::Float32(value)
            }
            Self::Float64(ValueFields { value }) => {
                ensure!(
                    value.is_finite(),
                    InputValidationSnafu {
                        reason: "predicate_non_finite_float",
                        correction: validation_correction(
                            "/value",
                            "finite float64 JSON number".to_owned(),
                            "Use a finite number within the float64 range.",
                        ),
                    }
                );
                DeltaScalar::Float64(value)
            }
            Self::Utf8(ValueFields { value }) => DeltaScalar::Utf8(value),
            Self::LargeUtf8(ValueFields { value }) => DeltaScalar::LargeUtf8(value),
            Self::Binary(ValueFields { value }) => DeltaScalar::Binary(value),
            Self::LargeBinary(ValueFields { value }) => DeltaScalar::LargeBinary(value),
            Self::FixedSizeBinary(FixedSizeBinaryFields { size, value }) => {
                ensure!(
                    size > 0 && size as usize == value.len(),
                    InputValidationSnafu {
                        reason: "predicate_binary_size_mismatch",
                        correction: validation_correction(
                            "/size",
                            "positive i32 JSON integer equal to the byte array length".to_owned(),
                            "Use a positive fixed binary size matching the number of bytes.",
                        ),
                    }
                );
                DeltaScalar::FixedSizeBinary { size, value }
            }
            Self::Date32(IntegerStringFields { value }) => DeltaScalar::Date32(value),
            Self::TimestampUs(TimestampMicrosecondFields { value, timezone }) => {
                DeltaScalar::TimestampMicrosecond { value, timezone }
            }
            Self::Decimal128(Decimal128Fields {
                value,
                precision,
                scale,
            }) => DeltaScalar::Decimal128 {
                value,
                precision,
                scale,
            },
        })
    }
}

fn deserialize_timezone<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let timezone = Option::<String>::deserialize(deserializer)?;
    if timezone.as_ref().is_some_and(String::is_empty) {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(""),
            &TIMEZONE_EXPECTATION,
        ));
    }
    Ok(timezone)
}
