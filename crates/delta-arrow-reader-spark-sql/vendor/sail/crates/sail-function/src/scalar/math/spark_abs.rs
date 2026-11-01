// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, new_null_array,
};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::math::expr_fn::abs;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAbs {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkAbs {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkAbs {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }
}

impl ScalarUDFImpl for SparkAbs {
    fn name(&self) -> &str {
        "spark_abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = arg_types else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), arg_types.len()));
        };
        if arg_type.is_numeric()
            || arg_type.is_null()
            || matches!(arg_type, DataType::Interval(_) | DataType::Duration(_))
        {
            Ok(arg_type.clone())
        } else {
            Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, Interval, or Duration type",
                arg_type,
            ))
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg_type] = arg_types else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), arg_types.len()));
        };
        match arg_type {
            t if t.is_numeric() => Ok(vec![t.clone()]),
            DataType::Null => Ok(vec![DataType::Null]),
            DataType::Interval(_) | DataType::Duration(_) => Ok(vec![arg_type.clone()]),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(vec![DataType::Float64])
            }
            other => Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, String, Interval, or Duration type",
                other,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), args.args.len()));
        };
        // Float/Decimal/UInt/Null have no ANSI overflow concerns; delegate to
        // DataFusion's built-in abs so the kernel stays correct even on
        // bypass paths where `simplify` has not rewritten the call.
        if matches!(
            arg.data_type(),
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Null
        ) {
            return datafusion::functions::math::abs::AbsFunc::new().invoke_with_args(args);
        }
        let return_dtype = args.return_field.data_type().clone();
        // Skip the kernel pass when every input row is NULL.
        if let ColumnarValue::Array(array) = arg
            && array.null_count() == array.len()
        {
            return Ok(ColumnarValue::Array(new_null_array(
                &return_dtype,
                array.len(),
            )));
        }
        match arg {
            // Signed integer abs: ANSI=true errors on MIN; ANSI=false wraps
            // (matches Java's Math.abs(int) — abs(MIN) returns MIN).
            ColumnarValue::Scalar(ScalarValue::Int8(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] byte overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int8(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int16(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] short overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int16(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int32(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!(
                                "[ARITHMETIC_OVERFLOW] integer overflow on abs({x})"
                            )
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int64(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] long overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
            },
            // Interval/Duration abs is ALWAYS-checked in Spark — both ANSI=true
            // and ANSI=false raise ARITHMETIC_OVERFLOW on the MIN value, unlike
            // signed integer abs which respects spark.sql.ansi.enabled.
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] integer overflow on abs(interval year-month)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(r)))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(interval day-time)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(r)))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(interval month-day-nano)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationSecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration second)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationSecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMillisecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration millisecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMillisecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration microsecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationNanosecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration nanosecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationNanosecond(r)))
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Int8 => {
                        if self.ansi_mode {
                            let result: Int8Array =
                                array.as_primitive::<Int8Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] byte overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int8Array =
                                array.as_primitive::<Int8Type>().unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int16 => {
                        if self.ansi_mode {
                            let result: Int16Array =
                                array.as_primitive::<Int16Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] short overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int16Array = array
                                .as_primitive::<Int16Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int32 => {
                        if self.ansi_mode {
                            let result: Int32Array =
                                array.as_primitive::<Int32Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] integer overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int32Array = array
                                .as_primitive::<Int32Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int64 => {
                        if self.ansi_mode {
                            let result: Int64Array =
                                array.as_primitive::<Int64Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] long overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int64Array = array
                                .as_primitive::<Int64Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Interval(IntervalUnit::YearMonth) => {
                        let result: IntervalYearMonthArray = array
                            .as_primitive::<IntervalYearMonthType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] integer overflow on abs(interval year-month)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let result: IntervalDayTimeArray = array
                            .as_primitive::<IntervalDayTimeType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(interval day-time)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::DayTime));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let result: IntervalMonthDayNanoArray = array
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(interval month-day-nano)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::MonthDayNano));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Second) => {
                        let result: DurationSecondArray = array
                            .as_primitive::<DurationSecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration second)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Second));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Millisecond) => {
                        let result: DurationMillisecondArray = array
                            .as_primitive::<DurationMillisecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration millisecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Millisecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let result: DurationMicrosecondArray = array
                            .as_primitive::<DurationMicrosecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration microsecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Microsecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Nanosecond) => {
                        let result: DurationNanosecondArray = array
                            .as_primitive::<DurationNanosecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration nanosecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Nanosecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => Err(unsupported_data_type_exec_err(
                        "abs",
                        "Numeric, Interval, or Duration type",
                        other,
                    )),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, Interval, or Duration type",
                &other.data_type(),
            )),
        }
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), args.len()));
        };
        // Idempotence: abs(abs(x)) = abs(x).
        if let Expr::ScalarFunction(inner) = arg
            && let Some(inner_abs) = inner.func.inner().downcast_ref::<Self>()
            && inner_abs.ansi_mode == self.ansi_mode
        {
            return Ok(ExprSimplifyResult::Simplified(arg.clone()));
        }

        let dt = info.get_data_type(arg)?;
        match dt {
            // Keep in invoke_with_args: interval/duration, and signed integers
            // (where invoke branches on self.ansi_mode between wrapping_abs and
            // checked_abs to honour Spark's ANSI semantics).
            DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(ExprSimplifyResult::Original(args)),
            // Floats, decimals, unsigned, null: no ANSI overflow concern — delegate.
            _ => Ok(ExprSimplifyResult::Simplified(abs(args.one()?))),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let [arg] = input else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), input.len()));
        };
        let range = &arg.range;
        if range.lower().data_type() != range.upper().data_type() {
            return internal_err!("Endpoints of an Interval should have the same type");
        }
        let zero_point = Interval::make_zero(&range.lower().data_type())?;

        if range.gt_eq(&zero_point)? == Interval::TRUE {
            // Non-decreasing for x ≥ 0
            Ok(arg.sort_properties)
        } else if range.lt_eq(&zero_point)? == Interval::TRUE {
            // Non-increasing for x ≤ 0. E.g., [-5, -3, -1] -> [5, 3, 1]
            Ok(-arg.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::{DFSchema, exec_err};
    use datafusion_expr::lit;

    fn abs_arguments(args: Vec<ColumnarValue>, data_type: DataType) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            arg_fields: args
                .iter()
                .map(|arg| Arc::new(Field::new("input", arg.data_type(), true)))
                .collect(),
            number_rows: match args.first() {
                Some(ColumnarValue::Array(array)) => array.len(),
                _ => 1,
            },
            args,
            return_field: Arc::new(Field::new("result", data_type, true)),
            config_options: Arc::default(),
        }
    }

    fn assert_abs_argument_error<T>(result: Result<T>, count: usize) -> Result<()> {
        let Err(error) = result else {
            return exec_err!("ABS accepted {count} arguments");
        };
        assert_eq!(
            error.to_string(),
            format!("Execution error: Spark `abs` function requires 1 argument, got {count}")
        );
        Ok(())
    }

    #[test]
    fn abs_checks_arity_at_each_trait_boundary() -> Result<()> {
        let info = SimplifyContext::builder()
            .with_schema(Arc::new(DFSchema::empty()))
            .build();
        for ansi in [false, true] {
            let udf = SparkAbs::new(ansi);
            for count in [0, 2] {
                assert_abs_argument_error(udf.return_type(&vec![DataType::Int32; count]), count)?;
                assert_abs_argument_error(udf.coerce_types(&vec![DataType::Int32; count]), count)?;
                assert_abs_argument_error(udf.simplify(vec![lit(-1i32); count], &info), count)?;
                assert_abs_argument_error(
                    udf.invoke_with_args(abs_arguments(
                        vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.5))); count],
                        DataType::Float64,
                    )),
                    count,
                )?;
                let properties = (0..count)
                    .map(|_| ExprProperties::new_unknown())
                    .collect::<Vec<_>>();
                assert_abs_argument_error(udf.output_ordering(&properties), count)?;
            }
        }
        Ok(())
    }

    #[test]
    fn abs_valid_arguments_keep_types_values_and_ansi_overflow() -> Result<()> {
        for ansi in [false, true] {
            let udf = SparkAbs::new(ansi);
            for (input, expected) in [
                (ScalarValue::Int32(Some(-7)), ScalarValue::Int32(Some(7))),
                (ScalarValue::Int32(None), ScalarValue::Int32(None)),
                (
                    ScalarValue::Float64(Some(-1.5)),
                    ScalarValue::Float64(Some(1.5)),
                ),
                (
                    ScalarValue::Decimal128(Some(-125), 10, 2),
                    ScalarValue::Decimal128(Some(125), 10, 2),
                ),
                (
                    ScalarValue::IntervalYearMonth(Some(-7)),
                    ScalarValue::IntervalYearMonth(Some(7)),
                ),
                (
                    ScalarValue::DurationMicrosecond(Some(-20)),
                    ScalarValue::DurationMicrosecond(Some(20)),
                ),
            ] {
                let data_type = input.data_type();
                assert_eq!(
                    udf.return_type(std::slice::from_ref(&data_type))?,
                    data_type
                );
                let result = udf.invoke_with_args(abs_arguments(
                    vec![ColumnarValue::Scalar(input)],
                    data_type,
                ))?;
                let actual = match result {
                    ColumnarValue::Scalar(value) => value,
                    ColumnarValue::Array(array) => {
                        assert_eq!(array.len(), 1);
                        ScalarValue::try_from_array(&array, 0)?
                    }
                };
                assert_eq!(actual, expected);
            }
            let values = Arc::new(Int32Array::from(vec![Some(-1), None, Some(2)]));
            let result = udf.invoke_with_args(abs_arguments(
                vec![ColumnarValue::Array(values)],
                DataType::Int32,
            ))?;
            let ColumnarValue::Array(actual) = result else {
                return exec_err!("ABS returned a scalar for an array input");
            };
            assert_eq!(
                actual.as_primitive::<Int32Type>(),
                &Int32Array::from(vec![Some(1), None, Some(2)])
            );
            let overflow = udf.invoke_with_args(abs_arguments(
                vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(i32::MIN)))],
                DataType::Int32,
            ));
            if ansi {
                let Err(error) = overflow else {
                    return exec_err!("ANSI ABS accepted the minimum integer");
                };
                assert!(error.to_string().contains("[ARITHMETIC_OVERFLOW]"));
            } else {
                let ColumnarValue::Scalar(actual) = overflow? else {
                    return exec_err!("ABS changed scalar representation");
                };
                assert_eq!(actual, ScalarValue::Int32(Some(i32::MIN)));
            }
            let properties = ExprProperties::new_unknown()
                .with_range(Interval::make_unbounded(&DataType::Int32)?);
            assert_eq!(
                udf.output_ordering(&[properties])?,
                SortProperties::Unordered
            );
        }
        Ok(())
    }
}
