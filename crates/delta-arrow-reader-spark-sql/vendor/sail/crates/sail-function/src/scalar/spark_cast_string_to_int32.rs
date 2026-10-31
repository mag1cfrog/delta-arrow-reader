use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::{PrimInt, Signed};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToInteger {
    signature: Signature,
    data_type: DataType,
}

impl SparkCastStringToInteger {
    pub fn try_new(data_type: DataType) -> Result<Self> {
        if !matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ) {
            return plan_err!("legacy string cast requires a signed integer, got {data_type}");
        }
        Ok(Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            data_type,
        })
    }
}

impl ScalarUDFImpl for SparkCastStringToInteger {
    fn name(&self) -> &str {
        match self.data_type {
            DataType::Int8 => "spark_cast_string_to_int8",
            DataType::Int16 => "spark_cast_string_to_int16",
            DataType::Int32 => "spark_cast_string_to_int32",
            _ => "spark_cast_string_to_int64",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.data_type.clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_type = self.data_type.clone();
        make_scalar_function(
            move |arrays| cast_string_array_to_integer(arrays, &data_type),
            vec![],
        )(&args.args)
    }
}

fn cast_string_array_to_integer(args: &[ArrayRef], data_type: &DataType) -> Result<ArrayRef> {
    let [array] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_cast_string_to_integer",
            (1, 1),
            args.len(),
        ));
    };
    match data_type {
        DataType::Int8 => cast_string_array::<Int8Type>(array),
        DataType::Int16 => cast_string_array::<Int16Type>(array),
        DataType::Int32 => cast_string_array::<Int32Type>(array),
        DataType::Int64 => cast_string_array::<Int64Type>(array),
        other => Err(unsupported_data_type_exec_err(
            "spark_cast_string_to_integer",
            "signed integer",
            other,
        )),
    }
}

fn cast_string_array<T: ArrowPrimitiveType>(array: &ArrayRef) -> Result<ArrayRef>
where
    T::Native: PrimInt + Signed,
{
    let result = match array.data_type() {
        DataType::Utf8 => parse_string_values::<T>(as_string_array(array)?.iter()),
        DataType::LargeUtf8 => parse_string_values::<T>(as_large_string_array(array)?.iter()),
        DataType::Utf8View => parse_string_values::<T>(as_string_view_array(array)?.iter()),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_cast_string_to_integer",
                "STRING",
                other,
            ));
        }
    };
    Ok(Arc::new(result))
}

fn parse_string_values<'a, T: ArrowPrimitiveType>(
    values: impl Iterator<Item = Option<&'a str>>,
) -> PrimitiveArray<T>
where
    T::Native: PrimInt + Signed,
{
    values
        .map(|value| value.and_then(parse_spark_legacy_string::<T::Native>))
        .collect()
}

fn parse_spark_legacy_string<T: PrimInt + Signed>(value: &str) -> Option<T> {
    let bytes = value.as_bytes();
    let mut offset = 0;
    while offset < bytes.len() && is_spark_trim_byte(bytes[offset]) {
        offset += 1;
    }
    if offset == bytes.len() {
        return None;
    }

    let mut end = bytes.len() - 1;
    while end > offset && is_spark_trim_byte(bytes[end]) {
        end -= 1;
    }

    let negative = bytes[offset] == b'-';
    if negative || bytes[offset] == b'+' {
        if offset == end {
            return None;
        }
        offset += 1;
    }

    let radix = T::from(10)?;
    let stop_value = T::min_value() / radix;
    let mut result = T::zero();
    while offset <= end {
        let byte = bytes[offset];
        offset += 1;
        if byte == b'.' {
            break;
        }
        if !byte.is_ascii_digit() || result < stop_value {
            return None;
        }
        result = result
            .checked_mul(&radix)?
            .checked_sub(&T::from(byte - b'0')?)?;
    }

    while offset <= end {
        if !bytes[offset].is_ascii_digit() {
            return None;
        }
        offset += 1;
    }

    if negative {
        Some(result)
    } else {
        T::zero().checked_sub(&result)
    }
}

fn is_spark_trim_byte(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Array, Int32Array, LargeStringArray, StringArray, StringViewArray,
    };
    use datafusion_common::exec_err;

    use super::*;

    #[test]
    fn test_parse_spark_legacy_string_to_i32() {
        let cases = [
            ("100", Some(100)),
            (" +100 ", Some(100)),
            ("1.23", Some(1)),
            ("-4.56", Some(-4)),
            (".9", Some(0)),
            ("1.", Some(1)),
            (".", Some(0)),
            ("2147483647.999", Some(i32::MAX)),
            ("-2147483648.999", Some(i32::MIN)),
            ("2147483648", None),
            ("2178802287", None),
            ("-2147483649", None),
            ("2147483648.0", None),
            ("123.a", None),
            ("1e2", None),
            ("", None),
            ("+", None),
        ];

        for (input, expected) in cases {
            assert_eq!(
                parse_spark_legacy_string::<i32>(input),
                expected,
                "input: {input:?}"
            );
        }
    }

    #[test]
    fn test_cast_all_string_array_representations_to_nullable_int32() -> Result<()> {
        let values = vec![
            Some("100"),
            Some("1.23"),
            Some("-4.56"),
            Some("2147483647"),
            Some("2178802287"),
            Some("2147483648.0"),
            Some("bad"),
            None,
        ];
        let expected = vec![
            Some(100),
            Some(1),
            Some(-4),
            Some(i32::MAX),
            None,
            None,
            None,
            None,
        ];
        let inputs = [
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
            Arc::new(StringViewArray::from(values)) as ArrayRef,
        ];

        for input in inputs {
            let output = cast_string_array_to_integer(&[input], &DataType::Int32)?;
            assert_eq!(output.data_type(), &DataType::Int32);
            let Some(output) = output.as_any().downcast_ref::<Int32Array>() else {
                return exec_err!("expected Int32Array");
            };
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
        }
        Ok(())
    }

    #[test]
    fn legacy_integer_widths_and_representations() -> Result<()> {
        use datafusion_common::ScalarValue;
        for (data_type, min, max) in [
            (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
            (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
            (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
            (DataType::Int64, i64::MIN, i64::MAX),
        ] {
            let strings = [
                "  +7 ".to_owned(),
                "-1.9".to_owned(),
                format!("{min}.9"),
                format!("{max}.9"),
                (i128::from(min) - 1).to_string(),
                (i128::from(max) + 1).to_string(),
                "bad".to_owned(),
                "1e2".to_owned(),
                ".".to_owned(),
                "\u{7f}7\u{7f}".to_owned(),
            ];
            let values = strings
                .iter()
                .map(|s| Some(s.as_str()))
                .chain([None])
                .collect::<Vec<_>>();
            let expected = [
                Some(7),
                Some(-1),
                Some(min),
                Some(max),
                None,
                None,
                None,
                None,
                Some(0),
                Some(7),
                None,
            ];
            let arrays = [
                Arc::new(StringArray::from(values.clone())) as ArrayRef,
                Arc::new(LargeStringArray::from(values.clone())) as ArrayRef,
                Arc::new(StringViewArray::from(values)) as ArrayRef,
            ];
            let udf = SparkCastStringToInteger::try_new(data_type.clone())?;
            assert_eq!(udf.return_type(&[DataType::Utf8])?, data_type);
            for input in arrays {
                let output = cast_string_array_to_integer(&[input], &data_type)?;
                assert_eq!(output.data_type(), &data_type);
                for (row, value) in expected.iter().enumerate() {
                    assert_eq!(
                        ScalarValue::try_from_array(&output, row)?.cast_to(&DataType::Int64)?,
                        ScalarValue::Int64(*value),
                        "{data_type} row {row}"
                    );
                }
            }
        }
        assert!(SparkCastStringToInteger::try_new(DataType::Float64).is_err());
        Ok(())
    }

    #[test]
    fn test_return_field_is_nullable_for_non_nullable_input() -> Result<()> {
        let udf = SparkCastStringToInteger::try_new(DataType::Int32)?;
        let input = Arc::new(Field::new("value", DataType::Utf8, false));
        let arg_fields = [input];
        let scalar_arguments = [None];
        let field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;

        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());
        Ok(())
    }
}
