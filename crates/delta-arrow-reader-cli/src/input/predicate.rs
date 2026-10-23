//! Row filters accepted by `--predicate-file`.
//!
//! Predicates combine column comparisons and null checks with boolean operations.
//! Each literal includes its data type. This module defines the file format and
//! limits the number and nesting depth of predicate nodes. The reader resolves
//! referenced columns and checks the predicate against the selected table.
//!
//! For a table whose `id` column is `Int32`, this file represents `id >= 10`:
//!
//! ```json
//! {
//!   "op": "ge",
//!   "column": "id",
//!   "value": {"type": "int32", "value": "10"}
//! }
//! ```
//!
//! Pass the file to `--predicate-file`. Integer literals use decimal strings;
//! the `type` field determines their width.

use std::{fmt, marker::PhantomData, str::FromStr};

use arrow_schema::{DataType, TimeUnit};
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaPredicatePathStep, DeltaScalar};
use num_traits::Bounded;
use serde::{Deserialize, Deserializer, de};
use snafu::ensure;

use crate::{
    Error, InputValidationSnafu,
    diagnostics::InputCorrection,
    input::{JsonInputKind, JsonObject},
};

mod json_schema;
mod schema_errors;

pub(super) use json_schema::predicate_schema;
pub(super) use schema_errors::diagnose_schema_error;

pub(super) const MAX_PREDICATE_NODES: usize = 1024;
pub(super) const MAX_PREDICATE_DEPTH: usize = 32;

// Schema diagnostics match these custom Serde expectations to select correction hints.
pub(super) const CANONICAL_INTEGER_EXPECTATION_PREFIX: &str = "canonical decimal integer string";
pub(super) const TIMEZONE_EXPECTATION: &str = "null or a nonempty JSON string";

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct PredicateInput {
    root: JsonObject<PredicateNode>,
}

impl PredicateInput {
    pub(crate) fn into_predicate(self) -> Result<DeltaPredicate, Error> {
        let mut node_count = 0;
        self.root.0.into_predicate(1, &mut node_count)
    }
}

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(PredicateOperator))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
enum PredicateNode {
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
struct ComparisonFields<T = JsonObject<ScalarInput>> {
    column: String,
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnReferenceFields {
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalGroupFields<T = JsonObject<PredicateNode>> {
    args: Vec<T>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NegationFields<T = JsonObject<PredicateNode>> {
    arg: Box<T>,
}

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(ScalarType))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
enum ScalarInput {
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
struct ValueFields<T> {
    value: T,
}

#[derive(Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "T: FromStr + Bounded + fmt::Display")
)]
struct IntegerStringFields<T> {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedSizeBinaryFields {
    size: i32,
    value: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimestampMicrosecondFields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i64,
    #[serde(deserialize_with = "deserialize_timezone")]
    timezone: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decimal128Fields {
    #[serde(deserialize_with = "deserialize_integer_string")]
    value: i128,
    precision: u8,
    scale: i8,
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

fn validation_correction(
    field_path: &str,
    expected: String,
    hint: &'static str,
) -> InputCorrection {
    InputCorrection {
        argument: JsonInputKind::Predicate.argument().to_owned(),
        field_path: Some(field_path.to_owned()),
        expected,
        supplied: None,
        hint,
    }
}

pub(crate) fn predicate_type_mismatch_correction(
    expected: &DataType,
    supplied: &DataType,
    path: &[DeltaPredicatePathStep],
) -> InputCorrection {
    let mut field_path = String::new();
    for step in path {
        match step {
            DeltaPredicatePathStep::AndChild(index) | DeltaPredicatePathStep::OrChild(index) => {
                field_path.push_str(&format!("/args/{index}"));
            }
            DeltaPredicatePathStep::NotChild => field_path.push_str("/arg"),
        }
    }
    let field = match (expected, supplied) {
        (
            DataType::Decimal128(expected_precision, _),
            DataType::Decimal128(supplied_precision, _),
        ) => {
            if expected_precision != supplied_precision {
                "precision"
            } else {
                "scale"
            }
        }
        (DataType::FixedSizeBinary(_), DataType::FixedSizeBinary(_)) => "size",
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => "timezone",
        _ => "type",
    };
    field_path.push_str(&format!("/value/{field}"));
    let expected_description = predicate_type_description(expected);
    let hint = if expected_description.is_none() {
        "This column type cannot be compared by a predicate scalar; use a supported column or a SQL query."
    } else if field == "timezone" {
        "Use inspect TABLE --view summary --column NAME with the same authorization and --table-version N for the exact timezone; timezone text is omitted here."
    } else {
        "Match the scalar type and parameters to expected; use capabilities --schema predicate --detail TYPE for its encoding."
    };
    InputCorrection {
        argument: JsonInputKind::Predicate.argument().to_owned(),
        field_path: Some(field_path),
        expected: expected_description
            .unwrap_or_else(|| "a column type supported by predicate comparisons".to_owned()),
        supplied: predicate_type_description(supplied).map(String::into_boxed_str),
        hint,
    }
}

fn predicate_type_description(data_type: &DataType) -> Option<String> {
    // Arrow's Display includes untrusted nested names and timezone strings.
    Some(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int8 => "int8".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Float32 => "float32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::LargeUtf8 => "large_utf8".to_owned(),
        DataType::Binary => "binary".to_owned(),
        DataType::LargeBinary => "large_binary".to_owned(),
        DataType::FixedSizeBinary(size) => format!("fixed_size_binary (size {size})"),
        DataType::Decimal128(precision, scale) => {
            format!("decimal128 (precision {precision}, scale {scale})")
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            if timezone.is_some() {
                "timestamp_us (timezone set)".to_owned()
            } else {
                "timestamp_us (timezone null)".to_owned()
            }
        }
        _ => return None,
    })
}

fn prepend_field_path(mut error: Error, prefix: &str) -> Error {
    if let Error::InputValidation { correction, .. } = &mut error
        && let Some(path) = &mut correction.field_path
    {
        path.insert_str(0, prefix);
    }
    error
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn type_corrections_identify_parameters_without_disclosing_schema_text() {
        for (expected, supplied, field, expected_text, supplied_text) in [
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(11, 2),
                "precision",
                "decimal128 (precision 10, scale 2)",
                "decimal128 (precision 11, scale 2)",
            ),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(10, 3),
                "scale",
                "decimal128 (precision 10, scale 2)",
                "decimal128 (precision 10, scale 3)",
            ),
            (
                DataType::FixedSizeBinary(3),
                DataType::FixedSizeBinary(2),
                "size",
                "fixed_size_binary (size 3)",
                "fixed_size_binary (size 2)",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("secret-column-timezone".into())),
                DataType::Timestamp(TimeUnit::Microsecond, Some("secret-scalar-timezone".into())),
                "timezone",
                "timestamp_us (timezone set)",
                "timestamp_us (timezone set)",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("secret-scalar-timezone".into())),
                "timezone",
                "timestamp_us (timezone null)",
                "timestamp_us (timezone set)",
            ),
            (
                DataType::Struct(
                    vec![arrow_schema::Field::new(
                        "secret-name",
                        DataType::Int32,
                        false,
                    )]
                    .into(),
                ),
                DataType::Utf8,
                "type",
                "a column type supported by predicate comparisons",
                "utf8",
            ),
        ] {
            let correction = predicate_type_mismatch_correction(&expected, &supplied, &[]);
            assert_eq!(
                correction.field_path.as_deref(),
                Some(format!("/value/{field}").as_str())
            );
            assert_eq!(correction.expected, expected_text);
            assert_eq!(correction.supplied.as_deref(), Some(supplied_text));
            let record = Error::InputValidation {
                reason: "test_type_mismatch",
                correction,
            }
            .to_json_diagnostic();
            assert!(!record.to_string().contains("secret"));
            if field == "timezone" {
                assert!(
                    record["hint"]
                        .as_str()
                        .unwrap()
                        .contains("timezone text is omitted")
                );
            }
        }
    }

    fn parse_with_schema_assertions(text: &str) -> Result<DeltaPredicate, Error> {
        let result =
            crate::input::parse_json::<PredicateInput>(text.as_bytes(), JsonInputKind::Predicate)?
                .into_predicate();
        if result.is_ok() {
            static SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
                std::sync::LazyLock::new(|| {
                    jsonschema::validator_for(&JsonInputKind::Predicate.schema_document()["schema"])
                        .unwrap()
                });
            SCHEMA
                .validate(&serde_json::from_str(text).unwrap())
                .unwrap();
            assert!(
                crate::input::schema_errors::diagnose_schema_error(
                    text.as_bytes(),
                    JsonInputKind::Predicate
                )
                .is_none(),
                "{text}"
            );
        }
        result
    }

    #[test]
    fn schema_lists_every_operator_and_scalar_type() {
        use strum::VariantNames;

        let document = JsonInputKind::Predicate.schema_document();
        for (definition, tag, variants) in [
            ("predicate", "op", PredicateOperator::VARIANTS),
            ("scalar", "type", ScalarType::VARIANTS),
        ] {
            let mut advertised: Vec<_> = document["schema"]["$defs"][definition]["oneOf"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|schema| schema["properties"][tag]["enum"].as_array().unwrap())
                .map(|name| name.as_str().unwrap())
                .collect();
            let mut expected = variants.to_vec();
            advertised.sort_unstable();
            expected.sort_unstable();
            assert_eq!(advertised, expected);
        }
    }

    fn parse_valid_predicate(value: Value) -> DeltaPredicate {
        parse_with_schema_assertions(&value.to_string()).unwrap()
    }

    fn parse_valid_scalar(value: Value) -> DeltaScalar {
        match parse_valid_predicate(json!({"op": "eq", "column": "id", "value": value})) {
            DeltaPredicate::Compare { value, .. } => value,
            other => panic!("expected comparison, got {other:?}"),
        }
    }

    #[test]
    fn integer_schema_errors_keep_nested_paths_and_the_declared_width() {
        for (kind, value, range) in [
            ("int8", "128", "from -128 to 127"),
            ("int16", "32768", "from -32768 to 32767"),
            ("int32", "2147483648", "from -2147483648 to 2147483647"),
            ("date32", "2147483648", "from -2147483648 to 2147483647"),
            (
                "int64",
                "9223372036854775808",
                "from -9223372036854775808 to 9223372036854775807",
            ),
            (
                "timestamp_us",
                "9223372036854775808",
                "from -9223372036854775808 to 9223372036854775807",
            ),
            (
                "decimal128",
                "170141183460469231731687303715884105728",
                "from -170141183460469231731687303715884105728 to 170141183460469231731687303715884105727",
            ),
        ] {
            let mut scalar = json!({"type": kind, "value": value});
            if kind == "timestamp_us" {
                scalar["timezone"] = Value::Null;
            } else if kind == "decimal128" {
                scalar["precision"] = json!(38);
                scalar["scale"] = json!(0);
            }
            let text = json!({"op": "not", "arg": {
                "op": "eq", "column": "secret-column", "value": scalar,
            }})
            .to_string();
            let record = parse_with_schema_assertions(&text)
                .unwrap_err()
                .to_json_diagnostic();
            assert_eq!(record["field_path"], "/arg/value/value", "{kind}: {record}");
            assert!(
                record["expected"].as_str().unwrap().ends_with(range),
                "{kind}: {record}"
            );
            assert!(!record.to_string().contains("secret"));
        }
    }

    #[test]
    fn maps_every_operator_and_preserves_columns() {
        for (op, expected) in [
            ("eq", DeltaComparison::Eq),
            ("ne", DeltaComparison::NotEq),
            ("lt", DeltaComparison::Lt),
            ("le", DeltaComparison::LtEq),
            ("gt", DeltaComparison::Gt),
            ("ge", DeltaComparison::GtEq),
        ] {
            for column in ["id", "", " a,b.c ", "ID"] {
                assert_eq!(
                    parse_valid_predicate(json!({"op": op, "column": column,
                        "value": {"type": "int32", "value": "10"}})),
                    DeltaPredicate::Compare {
                        column: column.into(),
                        op: expected,
                        value: DeltaScalar::Int32(10),
                    }
                );
            }
        }
        for value in [false, true] {
            assert_eq!(
                parse_valid_predicate(json!({"op": "constant", "value": value})),
                DeltaPredicate::Constant(value)
            );
        }
        assert_eq!(
            parse_valid_predicate(json!({"op": "and", "args": []})),
            DeltaPredicate::And(vec![])
        );
        assert_eq!(
            parse_valid_predicate(json!({"op": "or", "args": []})),
            DeltaPredicate::Or(vec![])
        );
        assert_eq!(
            parse_valid_predicate(json!({"op": "and", "args": [
                {"op": "is_null", "column": " a.b "},
                {"op": "or", "args": [
                    {"op": "is_not_null", "column": ""},
                    {"op": "not", "arg": {"op": "constant", "value": false}}
                ]}
            ]})),
            DeltaPredicate::And(vec![
                DeltaPredicate::IsNull {
                    column: " a.b ".into()
                },
                DeltaPredicate::Or(vec![
                    DeltaPredicate::IsNotNull { column: "".into() },
                    DeltaPredicate::Not(Box::new(DeltaPredicate::Constant(false))),
                ]),
            ])
        );
    }

    #[test]
    fn maps_every_scalar_without_losing_integer_precision() {
        for (input, expected) in [
            (
                json!({"type": "boolean", "value": false}),
                DeltaScalar::Boolean(false),
            ),
            (
                json!({"type": "int8", "value": "-128"}),
                DeltaScalar::Int8(i8::MIN),
            ),
            (
                json!({"type": "int8", "value": "127"}),
                DeltaScalar::Int8(i8::MAX),
            ),
            (
                json!({"type": "int16", "value": "-32768"}),
                DeltaScalar::Int16(i16::MIN),
            ),
            (
                json!({"type": "int16", "value": "32767"}),
                DeltaScalar::Int16(i16::MAX),
            ),
            (
                json!({"type": "int32", "value": "-2147483648"}),
                DeltaScalar::Int32(i32::MIN),
            ),
            (
                json!({"type": "int32", "value": "2147483647"}),
                DeltaScalar::Int32(i32::MAX),
            ),
            (
                json!({"type": "int64", "value": "-9223372036854775808"}),
                DeltaScalar::Int64(i64::MIN),
            ),
            (
                json!({"type": "int64", "value": "9223372036854775807"}),
                DeltaScalar::Int64(i64::MAX),
            ),
            (
                json!({"type": "int64", "value": "9007199254740993"}),
                DeltaScalar::Int64(9_007_199_254_740_993),
            ),
            (
                json!({"type": "int64", "value": "0"}),
                DeltaScalar::Int64(0),
            ),
            (
                json!({"type": "float32", "value": 0.1}),
                DeltaScalar::Float32(0.1),
            ),
            (
                json!({"type": "float32", "value": f32::MAX as f64}),
                DeltaScalar::Float32(f32::MAX),
            ),
            (
                json!({"type": "float32", "value": 1e-100}),
                DeltaScalar::Float32(0.0),
            ),
            (
                json!({"type": "float64", "value": -1.25e100}),
                DeltaScalar::Float64(-1.25e100),
            ),
            (
                json!({"type": "float64", "value": 10}),
                DeltaScalar::Float64(10.0),
            ),
            (
                json!({"type": "utf8", "value": " a\n\u{0}b "}),
                DeltaScalar::Utf8(" a\n\u{0}b ".into()),
            ),
            (
                json!({"type": "large_utf8", "value": ""}),
                DeltaScalar::LargeUtf8("".into()),
            ),
            (
                json!({"type": "binary", "value": []}),
                DeltaScalar::Binary(vec![]),
            ),
            (
                json!({"type": "large_binary", "value": [0, 255]}),
                DeltaScalar::LargeBinary(vec![0, 255]),
            ),
            (
                json!({"type": "fixed_size_binary", "size": 2, "value": [0, 255]}),
                DeltaScalar::FixedSizeBinary {
                    size: 2,
                    value: vec![0, 255],
                },
            ),
            (
                json!({"type": "date32", "value": "-1"}),
                DeltaScalar::Date32(-1),
            ),
            (
                json!({"type": "date32", "value": "-2147483648"}),
                DeltaScalar::Date32(i32::MIN),
            ),
            (
                json!({"type": "date32", "value": "2147483647"}),
                DeltaScalar::Date32(i32::MAX),
            ),
            (
                json!({"type": "timestamp_us", "value": "-9223372036854775808", "timezone": null}),
                DeltaScalar::TimestampMicrosecond {
                    value: i64::MIN,
                    timezone: None,
                },
            ),
            (
                json!({"type": "timestamp_us", "value": "9223372036854775807", "timezone": "UTC"}),
                DeltaScalar::TimestampMicrosecond {
                    value: i64::MAX,
                    timezone: Some("UTC".into()),
                },
            ),
            (
                json!({"type": "timestamp_us", "value": "-1", "timezone": " Custom/Zone "}),
                DeltaScalar::TimestampMicrosecond {
                    value: -1,
                    timezone: Some(" Custom/Zone ".into()),
                },
            ),
            (
                json!({"type": "decimal128", "value": "123456789012345678901234567890", "precision": 30, "scale": -2}),
                DeltaScalar::Decimal128 {
                    value: 123_456_789_012_345_678_901_234_567_890,
                    precision: 30,
                    scale: -2,
                },
            ),
            // Precision, scale, and value compatibility belong to the core planner.
            (
                json!({"type": "decimal128", "value": i128::MIN.to_string(), "precision": 0, "scale": 127}),
                DeltaScalar::Decimal128 {
                    value: i128::MIN,
                    precision: 0,
                    scale: 127,
                },
            ),
            (
                json!({"type": "decimal128", "value": i128::MAX.to_string(), "precision": 255, "scale": -128}),
                DeltaScalar::Decimal128 {
                    value: i128::MAX,
                    precision: 255,
                    scale: -128,
                },
            ),
        ] {
            assert_eq!(parse_valid_scalar(input.clone()), expected, "{input}");
        }
    }

    #[test]
    fn preserves_float64_bits() {
        for expected in [
            1.9651349465042103,
            -1.9651349465042103,
            2.3318716180463287e-130,
            -7.386266683291869e223,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            0.0,
            -0.0,
        ] {
            let actual = parse_valid_scalar(json!({"type": "float64", "value": expected}));
            let DeltaScalar::Float64(actual) = actual else {
                panic!("expected Float64, got {actual:?}");
            };
            assert_eq!(actual.to_bits(), expected.to_bits(), "{expected:?}");
        }
    }

    #[test]
    fn rejects_wrong_shapes_unknown_fields_and_duplicate_keys() {
        for text in [
            "",
            "{}",
            "[]",
            "null",
            "true",
            r#""constant""#,
            "1",
            r#"{"op":"constant","value":true} {}"#,
            r#"["constant",true]"#,
            r#"{"op":"Constant","value":true}"#,
            r#"{"op":"constant","value":true,"extra":0}"#,
            r#"{"op":"constant","value":null}"#,
            r#"{"op":"constant","value":"true"}"#,
            r#"{"op":"constant"}"#,
            r#"{"op":"constant","op":"constant","value":true}"#,
            r#"{"op":"constant","value":true,"\u0076alue":false}"#,
            r#"{"op":"constant","value":true,"value":true}"#,
            r#"{"op":"is_null","column":null}"#,
            r#"{"op":"is_null","column":"id","value":null}"#,
            r#"{"op":"is_null","column":"id","column":"id"}"#,
            r#"{"op":"is_not_null"}"#,
            r#"{"op":"and","args":null}"#,
            r#"{"op":"and","args":{}}"#,
            r#"{"op":"and","args":[null]}"#,
            r#"{"op":"and","args":[["constant",true]]}"#,
            r#"{"op":"and","args":[],"args":[]}"#,
            r#"{"op":"or","args":[{"op":"constant","value":true,"extra":0}]}"#,
            r#"{"op":"not","args":[]}"#,
            r#"{"op":"not","arg":[]}"#,
            r#"{"op":"not","arg":["constant",true]}"#,
            r#"{"op":"not","arg":{"op":"constant","value":true},"arg":{}}"#,
            r#"{"op":"eq","column":"id","value":null}"#,
            r#"{"op":"eq","column":"id","value":["int32","1"]}"#,
            r#"{"op":"eq","column":"id","value":{"type":"int32","value":"1"},"value":null}"#,
        ] {
            assert!(parse_with_schema_assertions(text).is_err(), "{text}");
        }
        for scalar in [
            "{}",
            "[]",
            "null",
            "true",
            "1",
            r#""int32""#,
            r#"{"type":"int32"}"#,
            r#"{"type":"int32","value":"1","extra":0}"#,
            r#"{"type":"int32","type":"int32","value":"1"}"#,
            r#"{"type":"int32","\u0074ype":"int32","value":"1"}"#,
            r#"{"type":"int32","value":"1","value":"2"}"#,
            r#"{"type":"uint32","value":"1"}"#,
            r#"{"type":"boolean","value":1}"#,
            r#"{"type":"utf8","value":null}"#,
            r#"{"type":"utf8","value":1}"#,
            r#"{"type":"float64","value":"1"}"#,
            r#"{"type":"float32","value":null}"#,
            r#"{"type":"float32","value":3.5e38}"#,
            r#"{"type":"float32","value":-3.5e38}"#,
            r#"{"type":"float64","value":1e400}"#,
            r#"{"type":"float64","value":NaN}"#,
            r#"{"type":"binary","value":"AA=="}"#,
            r#"{"type":"binary","value":[-1]}"#,
            r#"{"type":"binary","value":[256]}"#,
            r#"{"type":"binary","value":[1.0]}"#,
            r#"{"type":"binary","value":[1e0]}"#,
            r#"{"type":"large_binary","value":["1"]}"#,
            r#"{"type":"fixed_size_binary","value":[],"size":0}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":-1}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":2}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":2147483648}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":1.0}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":1e0}"#,
            r#"{"type":"fixed_size_binary","value":[1],"size":1,"size":1}"#,
            r#"{"type":"timestamp_us","value":"0"}"#,
            r#"{"type":"timestamp_us","value":"0","timezone":""}"#,
            r#"{"type":"timestamp_us","value":"0","timezone":0}"#,
            r#"{"type":"timestamp_us","value":"0","timezone":null,"timezone":null}"#,
            r#"{"type":"decimal128","value":"1","precision":256,"scale":0}"#,
            r#"{"type":"decimal128","value":"1","precision":-1,"scale":0}"#,
            r#"{"type":"decimal128","value":"1","precision":1.0,"scale":0}"#,
            r#"{"type":"decimal128","value":"1","precision":1e0,"scale":0}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"scale":128}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"scale":-129}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"scale":0.0}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"scale":0e0}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"scale":0,"scale":0}"#,
            r#"{"type":"decimal128","value":"1","precision":1,"precision":1,"scale":0}"#,
        ] {
            let text = format!(r#"{{"op":"eq","column":"id","value":{scalar}}}"#);
            assert!(parse_with_schema_assertions(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn rejects_noncanonical_integers_and_width_overflow() {
        for (kind, underflow, overflow) in [
            ("int8", "-129", "128"),
            ("int16", "-32769", "32768"),
            ("int32", "-2147483649", "2147483648"),
            ("int64", "-9223372036854775809", "9223372036854775808"),
            ("date32", "-2147483649", "2147483648"),
            (
                "timestamp_us",
                "-9223372036854775809",
                "9223372036854775808",
            ),
            (
                "decimal128",
                "-170141183460469231731687303715884105729",
                "170141183460469231731687303715884105728",
            ),
        ] {
            for value in [
                "", "-", "+1", "-0", "00", "01", "-01", " 1", "1 ", "1\n", "1.0", "1e2", "0x1",
                "\u{661}", underflow, overflow,
            ]
            .map(Value::from)
            .into_iter()
            .chain([
                json!(0),
                json!(1.0),
                json!(true),
                json!(null),
                json!([]),
                json!({}),
            ]) {
                let mut scalar = json!({"type": kind, "value": value});
                if kind == "timestamp_us" {
                    scalar["timezone"] = Value::Null;
                }
                if kind == "decimal128" {
                    scalar["precision"] = json!(38);
                    scalar["scale"] = json!(0);
                }
                let predicate = json!({"op": "eq", "column": "id", "value": scalar});
                assert!(
                    parse_with_schema_assertions(&predicate.to_string()).is_err(),
                    "{predicate}"
                );
            }
        }
    }

    #[test]
    fn enforces_depth_and_node_limits_including_the_root() {
        let mut value = json!({"op": "constant", "value": true});
        for depth in 1..=33 {
            let result = parse_with_schema_assertions(&value.to_string());
            assert_eq!(result.is_ok(), depth <= 32, "depth {depth}");
            value = match depth % 3 {
                0 => json!({"op": "not", "arg": value}),
                1 => json!({"op": "and", "args": [value]}),
                _ => json!({"op": "or", "args": [value]}),
            };
        }
        for nodes in [1024, 1025] {
            let args = vec![json!({"op": "constant", "value": true}); nodes - 1];
            let value = json!({"op": "and", "args": args});
            assert_eq!(
                parse_with_schema_assertions(&value.to_string()).is_ok(),
                nodes == 1024
            );
        }
        // Count across sibling subtrees, not just within each branch.
        let args = vec![json!({"op": "not", "arg": {"op": "constant", "value": true}}); 512];
        let value = json!({"op": "or", "args": args});
        assert!(parse_with_schema_assertions(&value.to_string()).is_err());
        let deeply_nested = format!(
            "{}{}{}",
            r#"{"op":"not","arg":"#.repeat(256),
            r#"{"op":"constant","value":true}"#,
            "}".repeat(256)
        );
        assert!(parse_with_schema_assertions(&deeply_nested).is_err());
    }
}
