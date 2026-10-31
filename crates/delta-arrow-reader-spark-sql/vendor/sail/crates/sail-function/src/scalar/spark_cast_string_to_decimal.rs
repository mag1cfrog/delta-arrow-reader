// Local Spark CAST adapter. Parsing and HALF_UP rounding use bigdecimal 0.4.10.
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, new_null_array};
use arrow::datatypes::{DataType, Field, FieldRef, is_validate_decimal_precision};
use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive, Zero};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToDecimal {
    precision: u8,
    scale: i8,
    safe: bool,
    signature: Signature,
}

impl SparkCastStringToDecimal {
    pub fn new(precision: u8, scale: i8, safe: bool) -> Self {
        Self {
            precision,
            scale,
            safe,
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
        }
    }

    fn convert<'a>(&self, values: impl Iterator<Item = Option<&'a str>>) -> Result<ArrayRef> {
        let mut output = Decimal128Array::builder(values.size_hint().0)
            .with_precision_and_scale(self.precision, self.scale)?;
        for value in values {
            let result = value
                .map(|s| parse_decimal(s, self.precision, self.scale))
                .transpose()?
                .flatten();
            if value.is_some() && result.is_none() && !self.safe {
                return exec_err!(
                    "Cannot cast {value:?} to DECIMAL({},{})",
                    self.precision,
                    self.scale
                );
            }
            output.append_option(result);
        }
        Ok(Arc::new(output.finish()))
    }

    fn invoke_array(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [array] = args else {
            return exec_err!("string-to-Decimal CAST expects one argument");
        };
        match array.data_type() {
            DataType::Null => Ok(new_null_array(&self.return_type(&[])?, array.len())),
            DataType::Utf8 => self.convert(as_string_array(array)?.iter()),
            DataType::LargeUtf8 => self.convert(as_large_string_array(array)?.iter()),
            DataType::Utf8View => self.convert(as_string_view_array(array)?.iter()),
            other => exec_err!("string-to-Decimal CAST does not accept {other}"),
        }
    }
}

impl ScalarUDFImpl for SparkCastStringToDecimal {
    fn name(&self) -> &str {
        "spark_cast_string_to_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Preserve literal CAST errors before empty-plan pruning. Conditional
        // reachability is handled by the shared expression simplifier.
        if let [
            Expr::Literal(
                ScalarValue::Utf8(value)
                | ScalarValue::LargeUtf8(value)
                | ScalarValue::Utf8View(value),
                _,
            ),
        ] = args.as_slice()
        {
            let array = self.convert(std::iter::once(value.as_deref()))?;
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::try_from_array(&array, 0)?,
                None,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let udf = Self::new(self.precision, self.scale, self.safe);
        make_scalar_function(move |a| udf.invoke_array(a), vec![])(&args.args)
    }
}

// Java 21 Character.digit(char, 10): each non-ASCII BMP range has ten digits.
// Captured with the Spark 4.2.0 oracle. Supplementary characters stay invalid.
const JAVA_DECIMAL_ZEROS: [u32; 36] = [
    0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66, 0x0ce6, 0x0d66,
    0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90,
    0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10,
];

fn decimal_digit_to_ascii(c: char) -> Option<char> {
    if c.is_ascii() {
        return Some(c);
    }
    let code = u32::from(c);
    let i = JAVA_DECIMAL_ZEROS
        .partition_point(|&zero| zero <= code)
        .checked_sub(1)?;
    let digit = code - JAVA_DECIMAL_ZEROS[i];
    (digit < 10).then(|| char::from(b'0' + digit as u8))
}

fn parse_input(input: &str) -> Option<BigDecimal> {
    // Spark Decimal.fromString uses Java String.trim, not Unicode whitespace trimming.
    let input = input.trim_matches(|c| c <= '\u{20}');
    let unsigned = input.strip_prefix(['+', '-']).unwrap_or(input);
    let mantissa = if let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E']) {
        // Java BigDecimal limits the exponent magnitude and effective scale to i32.
        if exponent.parse::<i32>().ok()? == i32::MIN {
            return None;
        }
        mantissa
    } else {
        unsigned
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    // bigdecimal accepts underscores and a sign after '.', which Spark rejects.
    if integer.is_empty() && fraction.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let decimal = input.parse::<BigDecimal>().ok()?;
    let source_scale = decimal.as_bigint_and_scale().1;
    i32::try_from(source_scale).ok()?;
    Some(decimal)
}

// Keep ordinary input at one conversion call, without a separate wrapper call.
#[inline(always)]
fn parse_decimal(input: &str, precision: u8, scale: i8) -> Result<Option<i128>> {
    let value = parse_ascii_decimal(input, precision, scale)?;
    if value.is_some() {
        return Ok(value);
    }
    parse_unicode_decimal(input, precision, scale)
}

// Keep allocation and normalization outside the ordinary ASCII conversion.
#[cold]
#[inline(never)]
fn parse_unicode_decimal(input: &str, precision: u8, scale: i8) -> Result<Option<i128>> {
    if input.is_ascii() {
        return Ok(None);
    }
    let Some(normalized) = input
        .chars()
        .map(decimal_digit_to_ascii)
        .collect::<Option<String>>()
    else {
        return Ok(None);
    };
    parse_ascii_decimal(&normalized, precision, scale)
}

fn parse_ascii_decimal(input: &str, precision: u8, scale: i8) -> Result<Option<i128>> {
    let Some(decimal) = parse_input(input) else {
        return Ok(None);
    };
    let source_scale = decimal.as_bigint_and_scale().1 as i32;
    // Spark computes this subtraction as a Java/Scala Int, including wraparound.
    if (decimal.digits() as i32).wrapping_sub(source_scale) > 38 {
        return Ok(None);
    }
    // BigInteger.TEN.pow checks 4 * exponent / 32 > MAX_MAG_LENGTH (2^26).
    // Spark propagates this JVM range error even from TRY_CAST. Reject it without
    // allocating the enormous power of ten; zero needs no rescaling work.
    let rescale = (i64::from(scale) - i64::from(source_scale)).abs();
    if !decimal.is_zero() && 4 * rescale / 32 > (1 << 26) {
        return exec_err!("BigInteger would overflow supported range");
    }
    let value = decimal
        .with_scale_round(i64::from(scale), RoundingMode::HalfUp)
        .into_bigint_and_scale()
        .0
        .to_i128();
    Ok(value.filter(|v| is_validate_decimal_precision(*v, precision)))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn decimal_string_values_arrays_and_scalar() -> Result<()> {
        let cases = [
            ("1.235", Some(124)),
            ("\u{ff11}.\u{ff12}\u{ff13}\u{ff15}", Some(124)),
            ("-\u{661}.\u{662}\u{663}\u{665}", Some(-124)),
            ("\u{967}.\u{968}\u{96b}e+\u{662}", Some(12500)),
            ("1e\u{ff12}", Some(10000)),
            ("\u{1d7d9}.25", None),
            ("1e\u{1d7da}", None),
            ("\u{00b2}", None),
            ("\u{2460}", None),
            ("\u{ff0b}\u{ff11}", None),
            ("\u{ff11}\u{ff0e}\u{ff12}", None),
            ("-1.235", Some(-124)),
            ("1e2", Some(10000)),
            (" +1.25\0", Some(125)),
            (".5", Some(50)),
            ("1.", Some(100)),
            ("0e37", Some(0)),
            ("0e38", None),
            ("1e-100", Some(0)),
            ("1e-2147483648", None),
            ("0e2147483647", Some(0)),
            ("", None),
            (".", None),
            ("+", None),
            ("bad", None),
            ("NaN", None),
            ("Infinity", None),
            ("1_2", None),
            (".+2", None),
            ("1e", None),
            ("1e2-3", None),
            ("1.2.3", None),
            ("1\0.25", None),
            ("\u{7f}1.25", None),
            ("\u{a0}1.25", None),
            ("99999999.995", None),
            (
                "1.23499999999999999999999999999999999999999999999999",
                Some(123),
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(parse_decimal(value, 10, 2)?, expected, "{value:?}");
        }
        let info = SimplifyContext::builder().build();
        for (text, _) in &cases {
            for safe in [true, false] {
                let udf = SparkCastStringToDecimal::new(10, 2, safe);
                let invoked = udf.convert(std::iter::once(Some(*text)));
                for value in [
                    ScalarValue::Utf8(Some((*text).into())),
                    ScalarValue::LargeUtf8(Some((*text).into())),
                    ScalarValue::Utf8View(Some((*text).into())),
                ] {
                    let folded = udf.simplify(vec![Expr::Literal(value, None)], &info);
                    match &invoked {
                        Ok(array) => {
                            let ExprSimplifyResult::Simplified(Expr::Literal(actual, _)) = folded?
                            else {
                                panic!("literal was not folded");
                            };
                            assert_eq!(actual, ScalarValue::try_from_array(array, 0)?);
                        }
                        Err(error) => {
                            assert_eq!(folded.unwrap_err().to_string(), error.to_string())
                        }
                    }
                }
            }
        }
        for value in ["1e2147483647", "1e-2147483647"] {
            assert!(parse_decimal(value, 10, 2).is_err());
            for safe in [true, false] {
                assert!(
                    SparkCastStringToDecimal::new(10, 2, safe)
                        .invoke_array(&[Arc::new(StringArray::from(vec![value]))])
                        .is_err()
                );
            }
        }
        let nulls = new_null_array(&DataType::Null, 3);
        for safe in [true, false] {
            let result =
                SparkCastStringToDecimal::new(10, 2, safe).invoke_array(&[nulls.clone()])?;
            assert_eq!(result.data_type(), &DataType::Decimal128(10, 2));
            assert_eq!((result.len(), result.null_count()), (3, 3));
        }
        let values = cases
            .iter()
            .map(|c| Some(c.0))
            .chain([None])
            .collect::<Vec<_>>();
        let expected = cases.iter().map(|c| c.1).chain([None]).collect::<Vec<_>>();
        for input in [
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
            Arc::new(StringViewArray::from(values)) as ArrayRef,
        ] {
            let udf = SparkCastStringToDecimal::new(10, 2, true);
            let output = udf.invoke_array(&[input.clone()])?;
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                SparkCastStringToDecimal::new(10, 2, false)
                    .invoke_array(&[input.clone()])
                    .is_err()
            );
            assert_eq!(udf.invoke_array(&[input.slice(0, 0)])?.len(), 0);
        }
        let udf = SparkCastStringToDecimal::new(10, 2, true);
        let result =
            make_scalar_function(move |a| udf.invoke_array(a), vec![])(&[ColumnarValue::Scalar(
                ScalarValue::Utf8(Some("1e2".into())),
            )])?;
        let ColumnarValue::Scalar(result) = result else {
            return exec_err!("expected scalar result");
        };
        assert_eq!(result, ScalarValue::Decimal128(Some(10000), 10, 2));
        Ok(())
    }
}
