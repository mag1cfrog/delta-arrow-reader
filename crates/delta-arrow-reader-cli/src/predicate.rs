use std::{fmt, marker::PhantomData, str::FromStr};

use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar};
use num_traits::Bounded;
use serde::{Deserialize, Deserializer, de};
use snafu::ensure;

use crate::{
    Error, InputValidationSnafu,
    diagnostics::InputCorrection,
    input::{JsonInputKind, JsonObject},
};

const MAX_PREDICATE_NODES: usize = 1024;
pub(crate) const MAX_PREDICATE_DEPTH: usize = 32;

// Schema diagnostics match these custom Serde expectations to select correction hints.
pub(crate) const CANONICAL_INTEGER_EXPECTATION_PREFIX: &str = "canonical decimal integer string";
pub(crate) const TIMEZONE_EXPECTATION: &str = "null or a nonempty JSON string";

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct PredicateInput {
    root: JsonObject<PredicateNode>,
}

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(PredicateOperator), vis(pub(crate)))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
pub(crate) enum PredicateNode {
    Constant(ValueFields<bool>),
    Eq(ComparisonFields),
    Ne(ComparisonFields),
    Lt(ComparisonFields),
    Le(ComparisonFields),
    Gt(ComparisonFields),
    Ge(ComparisonFields),
    IsNull(ColumnReferenceFields),
    IsNotNull(ColumnReferenceFields),
    And(LogicalGroupFields),
    Or(LogicalGroupFields),
    Not(NegationFields),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComparisonFields<T = JsonObject<ScalarInput>> {
    column: String,
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColumnReferenceFields {
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LogicalGroupFields<T = JsonObject<PredicateNode>> {
    args: Vec<T>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NegationFields<T = JsonObject<PredicateNode>> {
    arg: Box<T>,
}

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(ScalarType), vis(pub(crate)))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
pub(crate) enum ScalarInput {
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
#[serde(deny_unknown_fields)]
pub(crate) struct ValueFields<T> {
    value: T,
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "T: FromStr + Bounded + fmt::Display")
)]
pub(crate) struct IntegerStringFields<T> {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixedSizeBinaryFields {
    size: i32,
    value: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampMicrosecondFields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i64,
    #[serde(deserialize_with = "deserialize_timezone")]
    timezone: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Decimal128Fields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i128,
    precision: u8,
    scale: i8,
}

fn deserialize_integer_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr + Bounded + fmt::Display,
{
    struct CanonicalIntegerStringVisitor<T>(PhantomData<T>);

    impl<'de, T: FromStr + Bounded + fmt::Display> de::Visitor<'de>
        for CanonicalIntegerStringVisitor<T>
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "{CANONICAL_INTEGER_EXPECTATION_PREFIX} from {} to {}",
                T::min_value(),
                T::max_value()
            )
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
            let digits = value.strip_prefix('-').unwrap_or(value).as_bytes();
            if value != "0"
                && !(matches!(digits.first(), Some(b'1'..=b'9'))
                    && digits.iter().all(u8::is_ascii_digit))
            {
                return Err(E::invalid_value(de::Unexpected::Str(value), &self));
            }
            value
                .parse()
                .map_err(|_| E::invalid_value(de::Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_str(CanonicalIntegerStringVisitor(PhantomData))
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

impl PredicateInput {
    pub(crate) fn into_predicate(self) -> Result<DeltaPredicate, Error> {
        let mut node_count = 0;
        self.root.0.into_predicate(1, &mut node_count)
    }
}

fn validation_correction(
    field_path: &str,
    expected: String,
    hint: &'static str,
) -> InputCorrection {
    InputCorrection {
        argument: JsonInputKind::Predicate.argument().to_owned(),
        field_path: Some(field_path.to_owned()),
        expected,
        hint,
    }
}

fn prepend_field_path(mut error: Error, prefix: &str) -> Error {
    if let Error::InputValidation { correction, .. } = &mut error
        && let Some(path) = &mut correction.field_path
    {
        path.insert_str(0, prefix);
    }
    error
}

impl PredicateNode {
    fn into_predicate(self, depth: usize, node_count: &mut usize) -> Result<DeltaPredicate, Error> {
        ensure!(
            depth <= MAX_PREDICATE_DEPTH,
            InputValidationSnafu {
                reason: "predicate_depth_limit_exceeded",
                correction: validation_correction(
                    "",
                    format!(
                        "predicate nesting of at most {MAX_PREDICATE_DEPTH} levels, including the root"
                    ),
                    "Reduce nesting at this predicate node.",
                ),
            }
        );
        ensure!(
            *node_count < MAX_PREDICATE_NODES,
            InputValidationSnafu {
                reason: "predicate_node_limit_exceeded",
                correction: validation_correction(
                    "",
                    format!("at most {MAX_PREDICATE_NODES} predicate nodes in the whole tree"),
                    "Remove or combine predicate nodes; this node exceeds the tree's total limit.",
                ),
            }
        );
        *node_count += 1;
        Ok(match self {
            Self::Constant(ValueFields { value }) => DeltaPredicate::Constant(value),
            Self::Eq(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Eq, value)?
            }
            Self::Ne(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::NotEq, value)?
            }
            Self::Lt(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Lt, value)?
            }
            Self::Le(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::LtEq, value)?
            }
            Self::Gt(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Gt, value)?
            }
            Self::Ge(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::GtEq, value)?
            }
            Self::IsNull(ColumnReferenceFields { column }) => DeltaPredicate::IsNull { column },
            Self::IsNotNull(ColumnReferenceFields { column }) => {
                DeltaPredicate::IsNotNull { column }
            }
            Self::And(LogicalGroupFields { args }) => DeltaPredicate::And(
                args.into_iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| prepend_field_path(error, &format!("/args/{index}")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or(LogicalGroupFields { args }) => DeltaPredicate::Or(
                args.into_iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| prepend_field_path(error, &format!("/args/{index}")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not(NegationFields { arg }) => DeltaPredicate::Not(Box::new(
                arg.0
                    .into_predicate(depth + 1, node_count)
                    .map_err(|error| prepend_field_path(error, "/arg"))?,
            )),
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
        value: value
            .0
            .into_scalar()
            .map_err(|error| prepend_field_path(error, "/value"))?,
    })
}

impl ScalarInput {
    fn into_scalar(self) -> Result<DeltaScalar, Error> {
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

#[cfg(test)]
mod tests;
