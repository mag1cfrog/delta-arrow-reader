// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, as_primitive_array};
use datafusion::arrow::compute::binary;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DecimalType, Field, FieldRef, Float32Type, Float64Type, Int32Type,
    Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::utils::decimal::round_decimal_base;
use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBRound {
    signature: Signature,
}

impl Default for SparkBRound {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBRound {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBRound {
    fn name(&self) -> &str {
        "spark_bround"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `RoundBase.nullable = true`, unconditional; `BRound` inherits it and declares no
    /// `nullable` of its own.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1563>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        if !(1..=2).contains(&arg_types.len()) {
            return Err(invalid_arg_count_exec_err(
                "spark_bround",
                (1, 2),
                arg_types.len(),
            ));
        }
        let t = &arg_types[0];
        let data_type = match t {
            DataType::Decimal128(precision, scale) => {
                let target = match args.scalar_arguments.get(1) {
                    None if arg_types.len() == 1 => 0,
                    Some(Some(ScalarValue::Int32(value))) => value.unwrap_or(0),
                    _ => {
                        return exec_err!(
                            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] BROUND scale must be a constant INT"
                        );
                    }
                };
                let (precision, scale) =
                    round_decimal_base(i32::from(*precision), i32::from(*scale), target, true);
                DataType::Decimal128(precision, scale)
            }
            DataType::Float32 => DataType::Float32,
            DataType::Float64 | DataType::Decimal256(_, _) => DataType::Float64,
            DataType::Int32 => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            _ => {
                return Err(unsupported_data_type_exec_err(
                    "spark_bround",
                    "Float64, Float32, Decimal128, Decimal256, Int32, Int64",
                    t,
                ));
            }
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args
            .args
            .first()
            .is_some_and(|x| matches!(x.data_type(), DataType::Decimal128(_, _)))
        {
            return decimal_bround(&args);
        }
        let ScalarFunctionArgs { args, .. } = args;
        let args: Vec<ColumnarValue> = match args.len() {
            1 => vec![
                args[0].clone(),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
            ],
            2 => args.to_vec(),
            _ => {
                return Err(invalid_arg_count_exec_err(
                    "spark_bround",
                    (1, 2),
                    args.len(),
                ));
            }
        };
        let [x, d] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_bround",
                (2, 2),
                args.len(),
            ));
        };
        let len = match (x, d) {
            (ColumnarValue::Array(arr), _) => arr.len(),
            (_, ColumnarValue::Array(arr)) => arr.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
        };

        let x_array = match x {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        let d_array = match d {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        match (x_array.data_type(), d_array.data_type()) {
            (DataType::Float64, DataType::Int32) => {
                let x = as_primitive_array::<Float64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result = binary::<Float64Type, Int32Type, _, Float64Type>(x, d, |x, s| {
                    spark_bround_f64(x, s)
                })?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Float32, DataType::Int32) => {
                let x = as_primitive_array::<Float32Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result =
                    binary::<Float32Type, Int32Type, _, Float32Type>(x, d, spark_bround_f32)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int32, DataType::Int32) => {
                let x = as_primitive_array::<Int32Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result = binary::<Int32Type, Int32Type, _, Int32Type>(x, d, |x, s| {
                    let pow = 10f64.powi(s);
                    (round_half_to_even_f64(x as f64 * pow) / pow) as i32
                })?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int64, DataType::Int32) => {
                let x = as_primitive_array::<Int64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result = binary::<Int64Type, Int32Type, _, Int64Type>(x, d, |x, s| {
                    let pow = 10f64.powi(s);
                    (round_half_to_even_f64(x as f64 * pow).round() / pow).round() as i64
                })?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (dt_x, dt_d) => Err(unsupported_data_types_exec_err(
                "spark_bround",
                "Float32 | Float64 | Int32 | Int64 with Int32/Float64",
                &[dt_x.clone(), dt_d.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        // FIXME check if need delete coerce
        match types.len() {
            1 => {
                let x_type: &DataType = &types[0];
                let valid_x: bool = matches!(
                    x_type,
                    DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                        | DataType::Int32
                        | DataType::Int64
                );

                if valid_x {
                    Ok(vec![x_type.clone(), DataType::Int32])
                } else {
                    Err(unsupported_data_types_exec_err(
                        "spark_bround",
                        "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64",
                        types,
                    ))
                }
            }
            2 => {
                let x_type: &DataType = &types[0];
                let scale_type: &DataType = &types[1];

                let valid_x: bool = matches!(
                    x_type,
                    DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                        | DataType::Int32
                        | DataType::Int64
                );
                let valid_scale: bool = matches!(scale_type, DataType::Int32);

                if valid_x && valid_scale {
                    Ok(vec![x_type.clone(), scale_type.clone()])
                } else {
                    Err(unsupported_data_types_exec_err(
                        "spark_bround",
                        "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64, Int32",
                        types,
                    ))
                }
            }
            _ => Err(invalid_arg_count_exec_err(
                "spark_bround",
                (1, 2),
                types.len(),
            )),
        }
    }
}

fn decimal_bround(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    if !(1..=2).contains(&args.args.len()) {
        return Err(invalid_arg_count_exec_err(
            "spark_bround",
            (1, 2),
            args.args.len(),
        ));
    }
    let DataType::Decimal128(_, input_scale) = args.args[0].data_type() else {
        return internal_err!("BROUND requires Decimal128 input");
    };
    if input_scale < 0 {
        return exec_err!("BROUND does not support negative Decimal input scale");
    }
    let DataType::Decimal128(precision, output_scale) = *args.return_field.data_type() else {
        return internal_err!("BROUND requires a Decimal128 return field");
    };
    let target = match args.args.get(1) {
        None => 0,
        Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(value)))) => *value,
        Some(ColumnarValue::Scalar(ScalarValue::Int32(None))) => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                None,
                precision,
                output_scale,
            )));
        }
        _ => {
            return exec_err!(
                "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] BROUND scale must be a constant INT"
            );
        }
    };
    // Powers are computed once per batch. All valid Decimal128 payloads fit below 10^38.
    let drop = (i64::from(input_scale) - i64::from(target)).max(0);
    let factor = (drop <= 38).then(|| 10_i128.pow(drop as u32));
    let multiplier = if factor.is_some() && target < 0 {
        10_i128.pow((i64::from(output_scale) - i64::from(target)) as u32)
    } else {
        1
    };
    let round = |value: i128| -> std::result::Result<i128, ArrowError> {
        if value != 0 && drop > i64::from(i32::MAX) {
            return Err(ArrowError::ComputeError(
                "Underflow: BROUND scale difference exceeds INT".into(),
            ));
        }
        let Some(factor) = factor else { return Ok(0) };
        let quotient = value / factor;
        let remainder = (value % factor).abs();
        let half = factor / 2;
        let increment =
            factor > 1 && (remainder > half || (remainder == half && quotient % 2 != 0));
        let rounded = (quotient + if increment { value.signum() } else { 0 }) * multiplier;
        Decimal128Type::validate_decimal_precision(rounded, precision, output_scale).map_err(
            |e| {
                ArrowError::ComputeError(format!(
                    "[NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION] {e}"
                ))
            },
        )?;
        Ok(rounded)
    };
    match &args.args[0] {
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, _, _)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Decimal128(value.map(round).transpose()?, precision, output_scale),
        )),
        ColumnarValue::Array(array) => {
            let input = as_primitive_array::<Decimal128Type>(array);
            let output = if target >= i32::from(input_scale) {
                input.clone()
            } else {
                input.try_unary::<_, Decimal128Type, _>(round)?
            };
            Ok(ColumnarValue::Array(Arc::new(
                output.with_precision_and_scale(precision, output_scale)?,
            )))
        }
        _ => internal_err!("BROUND requires Decimal128 input"),
    }
}

fn round_half_to_even_f64(value: f64) -> f64 {
    let rounded: f64 = value.trunc();
    let fraction: f64 = value - rounded;

    if fraction.abs() != 0.5 {
        return value.round();
    }

    if (rounded as i64) % 2 == 0 {
        rounded
    } else if value > 0.0 {
        rounded + 1.0
    } else {
        rounded - 1.0
    }
}

fn spark_bround_f32(value: f32, scale: i32) -> f32 {
    if !value.is_finite() {
        return value;
    }
    // Every finite Float32 is smaller than half of 10^39.
    if value == 0.0 || scale <= -39 {
        return 0.0;
    }
    // At scale 45 the rounding error is below half the smallest Float32 ULP.
    if scale >= 45 {
        return value;
    }
    let factor = 10f64.powi(scale);
    let rounded = ((f64::from(value) * factor).round_ties_even() / factor) as f32;
    // Spark's BigDecimal result has positive zero, including for negative inputs.
    rounded + 0.0
}

fn spark_bround_f64(x: f64, scale: i32) -> f64 {
    let factor: f64 = 10f64.powi(scale);
    let shifted: f64 = x * factor;

    let rounded = if (shifted - shifted.round()).abs() == 0.5 {
        round_half_to_even_f64(shifted)
    } else {
        shifted.round()
    };

    rounded / factor
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, Float64Array, Int32Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn float_bround_zero_scale_boundaries() -> Result<()> {
        use datafusion::arrow::array::Float32Array;

        for (value, scale, expected) in [
            (-0.5, 0, 0.0),
            (-0.0, 45, 0.0),
            (2.5, 39, 2.5),
            (2.5, -46, 0.0),
            (f32::from_bits(1), 44, 0.0),
            (f32::from_bits(1), 45, f32::from_bits(1)),
            (-f32::from_bits(1), 44, 0.0),
            (-f32::from_bits(1), 45, -f32::from_bits(1)),
            (f32::MAX, -39, 0.0),
            (f32::MAX, 39, f32::MAX),
            (f32::INFINITY, 39, f32::INFINITY),
            (f32::NEG_INFINITY, -46, f32::NEG_INFINITY),
        ] {
            assert_eq!(spark_bround_f32(value, scale).to_bits(), expected.to_bits());
        }
        let udf = SparkBRound::new();
        let input = Float32Array::from(vec![
            Some(99.0),
            Some(-0.5),
            Some(2.5),
            None,
            Some(f32::from_bits(1)),
            Some(-f32::from_bits(1)),
            Some(f32::INFINITY),
            Some(f32::NAN),
        ]);
        for (scale, expected) in [
            (
                0,
                vec![
                    Some(0.0),
                    Some(2.0),
                    None,
                    Some(0.0),
                    Some(0.0),
                    Some(f32::INFINITY),
                    Some(f32::NAN),
                ],
            ),
            (
                39,
                vec![
                    Some(-0.5),
                    Some(2.5),
                    None,
                    Some(0.0),
                    Some(0.0),
                    Some(f32::INFINITY),
                    Some(f32::NAN),
                ],
            ),
            (
                -46,
                vec![
                    Some(0.0),
                    Some(0.0),
                    None,
                    Some(0.0),
                    Some(0.0),
                    Some(f32::INFINITY),
                    Some(f32::NAN),
                ],
            ),
        ] {
            for (offset, len) in [(1, 7), (3, 1), (3, 0)] {
                let array = input.slice(offset, len);
                let scale = ScalarValue::Int32(Some(scale));
                let fields = vec![
                    Arc::new(Field::new("a", DataType::Float32, true)),
                    Arc::new(Field::new("scale", DataType::Int32, false)),
                ];
                let field = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[None, Some(&scale)],
                })?;
                let result = udf
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(array)),
                            ColumnarValue::Scalar(scale),
                        ],
                        arg_fields: fields,
                        number_rows: len,
                        return_field: field,
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .into_array(len)?;
                let actual = as_primitive_array::<Float32Type>(&result);
                for (actual, expected) in actual.iter().zip(&expected[offset - 1..offset - 1 + len])
                {
                    match (actual, expected) {
                        (Some(a), Some(b)) if b.is_nan() => assert!(a.is_nan()),
                        (Some(a), Some(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                        (None, None) => {}
                        _ => panic!("BROUND changed validity"),
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn float_bround_return_field_matches_scalar_and_array_results() -> Result<()> {
        use datafusion::arrow::array::Float32Array;

        let udf = SparkBRound::new();
        let invoke = |input: ColumnarValue, scale: Option<ScalarValue>| {
            let len = match &input {
                ColumnarValue::Array(array) => array.len(),
                _ => 1,
            };
            let mut args = vec![input];
            if let Some(scale) = scale {
                args.push(ColumnarValue::Scalar(scale));
            }
            let fields = args
                .iter()
                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                .collect::<Vec<_>>();
            let scalar_arguments = args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Scalar(value) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let field = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalar_arguments,
            })?;
            assert_eq!(field.data_type(), &DataType::Float32);
            assert!(field.is_nullable());
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: fields,
                number_rows: len,
                return_field: field.clone(),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            assert_eq!(result.data_type(), *field.data_type());
            let result = result.into_array(len)?;
            assert_eq!(result.len(), len);
            Ok::<_, datafusion_common::DataFusionError>(
                as_primitive_array::<Float32Type>(&result)
                    .iter()
                    .collect::<Vec<_>>(),
            )
        };
        for (value, scale, expected) in [
            (Some(2.5), None, Some(2.0)),
            (Some(3.5), Some(0), Some(4.0)),
            (Some(-2.5), Some(0), Some(-2.0)),
            (Some(-3.5), None, Some(-4.0)),
            (Some(1.25), Some(1), Some(1.2)),
            (Some(-1.25), Some(1), Some(-1.2)),
            (Some(25.0), Some(-1), Some(20.0)),
            (Some(-35.0), Some(-1), Some(-40.0)),
            (None, None, None),
        ] {
            assert_eq!(
                invoke(
                    ColumnarValue::Scalar(ScalarValue::Float32(value)),
                    scale.map(|s| ScalarValue::Int32(Some(s)))
                )?,
                vec![expected],
            );
        }
        let array = Float32Array::from(vec![
            Some(99.0),
            Some(2.5),
            Some(3.5),
            None,
            Some(-2.5),
            Some(-3.5),
        ]);
        for (offset, len, expected) in [
            (
                1,
                5,
                vec![Some(2.0), Some(4.0), None, Some(-2.0), Some(-4.0)],
            ),
            (3, 1, vec![None]),
            (3, 0, vec![]),
        ] {
            assert_eq!(
                invoke(
                    ColumnarValue::Array(Arc::new(array.slice(offset, len))),
                    None
                )?,
                expected
            );
        }
        assert_eq!(
            invoke(
                ColumnarValue::Array(Arc::new(array)),
                Some(ScalarValue::Int32(None))
            )?,
            vec![None; 6]
        );
        Ok(())
    }

    #[test]
    fn decimal_bround_scalars_arrays_slices_and_nulls() -> Result<()> {
        use datafusion::arrow::array::Decimal128Array;
        use datafusion::arrow::buffer::NullBuffer;

        let udf = SparkBRound::new();
        let invoke = |input: ColumnarValue, target: i32, precision: u8, scale: i8| {
            let fields = vec![
                Arc::new(Field::new("x", input.data_type(), true)),
                Arc::new(Field::new("d", DataType::Int32, false)),
            ];
            let target = ScalarValue::Int32(Some(target));
            let result = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, Some(&target)],
            })?;
            assert_eq!(result.data_type(), &DataType::Decimal128(precision, scale));
            assert!(result.is_nullable());
            let number_rows = match &input {
                ColumnarValue::Array(a) => a.len(),
                _ => 1,
            };
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![input, ColumnarValue::Scalar(target)],
                arg_fields: fields,
                number_rows,
                return_field: result,
                config_options: Arc::new(ConfigOptions::default()),
            })
        };
        for (value, expected) in [
            (2345, 234),
            (2355, 236),
            (-2345, -234),
            (-2355, -236),
            (2344, 234),
            (2346, 235),
            (9995, 1000),
            (0, 0),
        ] {
            let result = invoke(
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), 6, 3)),
                2,
                6,
                2,
            )?;
            let ColumnarValue::Scalar(result) = result else {
                panic!("expected scalar")
            };
            assert_eq!(result, ScalarValue::Decimal128(Some(expected), 6, 2));
        }
        let array = Decimal128Array::new(
            vec![9999, 2345, 2355, i128::MAX, -2345, -2355, 9999].into(),
            Some(NullBuffer::from(vec![
                true, true, true, false, true, true, true,
            ])),
        )
        .with_precision_and_scale(6, 3)?;
        for (offset, len, expected) in [
            (
                1,
                5,
                vec![Some(234), Some(236), None, Some(-234), Some(-236)],
            ),
            (3, 1, vec![None]),
            (3, 0, vec![]),
        ] {
            let result = invoke(
                ColumnarValue::Array(Arc::new(array.slice(offset, len))),
                2,
                6,
                2,
            )?
            .into_array(len)?;
            assert_eq!(
                as_primitive_array::<Decimal128Type>(&result)
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        // An overflowing hidden payload must not turn a NULL row into an error.
        let array = Decimal128Array::new(
            vec![10_i128.pow(38) - 1, 25, 35].into(),
            Some(NullBuffer::from(vec![false, true, true])),
        )
        .with_precision_and_scale(38, 0)?;
        let result = invoke(ColumnarValue::Array(Arc::new(array)), -1, 38, 0)?.into_array(3)?;
        assert_eq!(
            as_primitive_array::<Decimal128Type>(&result)
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(20), Some(40)]
        );
        assert!(
            invoke(
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 0)),
                -1,
                38,
                0
            )
            .unwrap_err()
            .to_string()
            .contains("[NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION]")
        );
        Ok(())
    }

    #[test]
    fn test_bround_null_propagation() -> Result<()> {
        // NULL in `x` on some rows, NULL in `d` (scale) on others.
        let x = Float64Array::from(vec![Some(2.5), None, Some(3.5), Some(1.25)]);
        let d = Int32Array::from(vec![Some(0), Some(0), None, Some(1)]);
        let x_field = Arc::new(Field::new("x", DataType::Float64, true));
        let d_field = Arc::new(Field::new("d", DataType::Int32, true));

        let result = SparkBRound::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(x)),
                ColumnarValue::Array(Arc::new(d)),
            ],
            arg_fields: vec![x_field, d_field.clone()],
            number_rows: 4,
            return_field: Arc::new(Field::new("result", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let output = result.into_array(4)?;
        let output = output.as_primitive::<Float64Type>();

        // Row 0: valid tie-to-even -> 2.0
        assert!(output.is_valid(0));
        assert_eq!(output.value(0), 2.0);
        // Row 1: NULL x -> NULL
        assert!(output.is_null(1));
        // Row 2: NULL scale -> NULL
        assert!(output.is_null(2));
        // Row 3: valid -> 1.2
        assert!(output.is_valid(3));
        assert_eq!(output.value(3), 1.2);
        Ok(())
    }

    #[test]
    fn test_bround_f64_positive_scale() {
        assert_eq!(spark_bround_f64(1.25, 1), 1.2);
        assert_eq!(spark_bround_f64(1.35, 1), 1.4);
    }
    #[test]
    fn test_bround_f64_zero_scale() {
        assert_eq!(spark_bround_f64(2.5, 0), 2.0);
        assert_eq!(spark_bround_f64(3.5, 0), 4.0);
        assert_eq!(spark_bround_f64(-2.5, 0), -2.0);
        assert_eq!(spark_bround_f64(-3.5, 0), -4.0);
    }
    #[test]
    fn test_bround_f64_negative_scale() {
        assert_eq!(spark_bround_f64(25.0, -1), 20.0);
        assert_eq!(spark_bround_f64(35.0, -1), 40.0);
        assert_eq!(spark_bround_f64(-25.0, -1), -20.0);
        assert_eq!(spark_bround_f64(-35.0, -1), -40.0);
    }
    #[test]
    fn test_bround_no_tie() {
        assert_eq!(spark_bround_f64(2.4, 0), 2.0);
        assert_eq!(spark_bround_f64(2.6, 0), 3.0);
        assert_eq!(spark_bround_f64(-2.4, 0), -2.0);
        assert_eq!(spark_bround_f64(-2.6, 0), -3.0);
    }
}
