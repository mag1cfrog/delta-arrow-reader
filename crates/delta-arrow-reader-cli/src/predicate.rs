use std::str::FromStr;

use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar};
use serde::{Deserialize, Deserializer, de};
use snafu::ensure;

use crate::{
    Error, InputValidationSnafu,
    diagnostics::InputCorrection,
    input::{JsonInputSource, JsonObject},
};

const MAX_PREDICATE_NODES: usize = 1024;
pub(crate) const MAX_PREDICATE_DEPTH: usize = 32;

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct PredicateInput {
    root: JsonObject<PredicateNode>,
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

pub(crate) fn deserialize_integer_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
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

pub(crate) fn deserialize_timezone<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let timezone = Option::<String>::deserialize(deserializer)?;
    if timezone.as_ref().is_some_and(String::is_empty) {
        return Err(de::Error::custom("expected null or a nonempty timezone"));
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
        argument: JsonInputSource::Predicate.argument().to_owned(),
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
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| prepend_field_path(error, &format!("/args/{index}")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or { args } => DeltaPredicate::Or(
                args.into_iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| prepend_field_path(error, &format!("/args/{index}")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not { arg } => DeltaPredicate::Not(Box::new(
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
            Self::Boolean { value } => DeltaScalar::Boolean(value),
            Self::Int8 { value } => DeltaScalar::Int8(value),
            Self::Int16 { value } => DeltaScalar::Int16(value),
            Self::Int32 { value } => DeltaScalar::Int32(value),
            Self::Int64 { value } => DeltaScalar::Int64(value),
            Self::Float32 { value } => {
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
            Self::Float64 { value } => {
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
            Self::Utf8 { value } => DeltaScalar::Utf8(value),
            Self::LargeUtf8 { value } => DeltaScalar::LargeUtf8(value),
            Self::Binary { value } => DeltaScalar::Binary(value),
            Self::LargeBinary { value } => DeltaScalar::LargeBinary(value),
            Self::FixedSizeBinary { size, value } => {
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
