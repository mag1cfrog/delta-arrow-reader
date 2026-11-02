use lexical_core::{FromLexicalWithOptions, NumberFormatBuilder, ParseFloatOptions};
use std::num::NonZeroU8;

fn parse_spark_float<T: FromLexicalWithOptions<Options = ParseFloatOptions>>(
    value: &str,
) -> Option<T> {
    const DECIMAL: u128 = NumberFormatBuilder::new().no_special(true).build_strict();
    const HEX: u128 = NumberFormatBuilder::new()
        .no_special(true)
        .mantissa_radix(16)
        .exponent_base(NonZeroU8::new(2))
        .exponent_radix(NonZeroU8::new(10))
        .base_prefix(NonZeroU8::new(b'x'))
        .required_exponent_notation(true)
        .build_strict();
    const DECIMAL_OPTIONS: ParseFloatOptions = ParseFloatOptions::new();
    const HEX_OPTIONS: ParseFloatOptions =
        ParseFloatOptions::builder().exponent(b'p').build_strict();
    let value = value.trim_matches(|c: char| c <= ' ');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    // Java accepts signed "NaN"; Spark's case-insensitive fallback accepts only unsigned NaN.
    if value.eq_ignore_ascii_case("nan") || matches!(value, "+NaN" | "-NaN") {
        return lexical_core::parse_with_options::<T, { lexical_core::format::STANDARD }>(
            b"NaN",
            &DECIMAL_OPTIONS,
        )
        .ok();
    }
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return lexical_core::parse_with_options::<T, { lexical_core::format::STANDARD }>(
            value.as_bytes(),
            &DECIMAL_OPTIONS,
        )
        .ok();
    }
    let value = value.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(value);
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        lexical_core::parse_with_options::<T, HEX>(value.as_bytes(), &HEX_OPTIONS).ok()
    } else {
        lexical_core::parse_with_options::<T, DECIMAL>(value.as_bytes(), &DECIMAL_OPTIONS).ok()
    }
}

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Float32Type, Float64Type,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToFloat {
    signature: Signature,
    data_type: DataType,
    null_on_error: bool,
}

impl SparkCastStringToFloat {
    pub fn try_new(data_type: DataType, null_on_error: bool) -> Result<Self> {
        if !matches!(data_type, DataType::Float32 | DataType::Float64) {
            return plan_err!("string floating cast requires FLOAT or DOUBLE, got {data_type}");
        }
        Ok(Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            data_type,
            null_on_error,
        })
    }
}

impl ScalarUDFImpl for SparkCastStringToFloat {
    fn name(&self) -> &str {
        if self.data_type == DataType::Float32 {
            "spark_cast_string_to_float32"
        } else {
            "spark_cast_string_to_float64"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Native literal CAST errors survive constant folding before parent
        // NULL/empty-result pruning. Keep that behavior when CAST uses this UDF.
        if let [
            Expr::Literal(
                ScalarValue::Utf8(value)
                | ScalarValue::LargeUtf8(value)
                | ScalarValue::Utf8View(value),
                _,
            ),
        ] = args.as_slice()
        {
            let value = match self.data_type {
                DataType::Float32 => ScalarValue::Float32(parse_value::<Float32Type>(
                    value.as_deref(),
                    self.null_on_error,
                )?),
                DataType::Float64 => ScalarValue::Float64(parse_value::<Float64Type>(
                    value.as_deref(),
                    self.null_on_error,
                )?),
                _ => unreachable!("constructor validates the target type"),
            };
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(value, None)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark keeps string casts nullable even in ANSI mode, preserving NULL predicate evaluation.
        Ok(Arc::new(Field::new(
            self.name(),
            self.data_type.clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (data_type, null_on_error) = (self.data_type.clone(), self.null_on_error);
        make_scalar_function(
            move |arrays| {
                let [array] = arrays else {
                    return Err(invalid_arg_count_exec_err(
                        "spark_cast_string_to_float",
                        (1, 1),
                        arrays.len(),
                    ));
                };
                match data_type {
                    DataType::Float32 => cast_array::<Float32Type>(array, null_on_error),
                    DataType::Float64 => cast_array::<Float64Type>(array, null_on_error),
                    _ => unreachable!("constructor validates the target type"),
                }
            },
            vec![],
        )(&args.args)
    }
}

fn cast_array<T: ArrowPrimitiveType>(array: &ArrayRef, null_on_error: bool) -> Result<ArrayRef>
where
    T::Native: FromLexicalWithOptions<Options = ParseFloatOptions>,
{
    let values = match array.data_type() {
        DataType::Utf8 => parse_values::<T>(as_string_array(array)?.iter(), null_on_error),
        DataType::LargeUtf8 => {
            parse_values::<T>(as_large_string_array(array)?.iter(), null_on_error)
        }
        DataType::Utf8View => parse_values::<T>(as_string_view_array(array)?.iter(), null_on_error),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_cast_string_to_float",
                "STRING",
                other,
            ));
        }
    }?;
    Ok(Arc::new(values))
}

fn parse_values<'a, T: ArrowPrimitiveType>(
    values: impl Iterator<Item = Option<&'a str>>,
    null_on_error: bool,
) -> Result<PrimitiveArray<T>>
where
    T::Native: FromLexicalWithOptions<Options = ParseFloatOptions>,
{
    values.map(|v| parse_value::<T>(v, null_on_error)).collect()
}

fn parse_value<T: ArrowPrimitiveType>(
    value: Option<&str>,
    null_on_error: bool,
) -> Result<Option<T::Native>>
where
    T::Native: FromLexicalWithOptions<Options = ParseFloatOptions>,
{
    let Some(value) = value else { return Ok(None) };
    match parse_spark_float::<T::Native>(value) {
        Some(number) => Ok(Some(number)),
        None if null_on_error => Ok(None),
        None => exec_err!(
            "[CAST_INVALID_INPUT] Cannot cast string {value:?} to {}",
            T::DATA_TYPE
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{LargeStringArray, StringArray, StringViewArray};

    #[test]
    fn floating_string_modes_arrays_and_rounding() -> Result<()> {
        let values = vec![
            Some(" \t1.25f\n"),
            Some("0X1.8P2"),
            Some("-0.0D"),
            None,
            Some("bad"),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        for input in arrays {
            let f = cast_array::<Float32Type>(&input, true)?;
            let f = f
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()
                .unwrap();
            assert_eq!(
                f.iter().map(|v| v.map(f32::to_bits)).collect::<Vec<_>>(),
                vec![
                    Some(1.25f32.to_bits()),
                    Some(6f32.to_bits()),
                    Some((-0f32).to_bits()),
                    None,
                    None
                ]
            );
            let d = cast_array::<Float64Type>(&input, true)?;
            let d = d
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()
                .unwrap();
            assert_eq!(
                d.iter().map(|v| v.map(f64::to_bits)).collect::<Vec<_>>(),
                vec![
                    Some(1.25f64.to_bits()),
                    Some(6f64.to_bits()),
                    Some((-0f64).to_bits()),
                    None,
                    None
                ]
            );
            assert!(cast_array::<Float32Type>(&input, false).is_err());
            assert!(cast_array::<Float64Type>(&input, false).is_err());
        }
        assert_eq!(
            parse_spark_float::<f32>("1.000000059604644775390626")
                .unwrap()
                .to_bits(),
            0x3f800001
        );
        assert_eq!(
            parse_spark_float::<f32>("0x1.000001p-150")
                .unwrap()
                .to_bits(),
            1
        );
        assert_eq!(
            parse_spark_float::<f64>("0x1.0000000000001p-1075")
                .unwrap()
                .to_bits(),
            1
        );
        assert_eq!(
            parse_spark_float::<f32>("-0x0p999999").unwrap().to_bits(),
            0x80000000
        );
        assert_eq!(
            parse_spark_float::<f64>("-0x0p999999").unwrap().to_bits(),
            0x8000000000000000
        );
        for text in [
            "+nan",
            "NaNf",
            "Infinityd",
            "0x1",
            "\u{7f}1.0",
            "\u{a0}1.0",
            "1_0",
        ] {
            assert!(parse_spark_float::<f32>(text).is_none(), "{text:?}");
            assert!(parse_spark_float::<f64>(text).is_none(), "{text:?}");
        }
        Ok(())
    }
}
