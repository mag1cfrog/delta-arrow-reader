// Local Spark numeric-to-integer conversions. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{
    DataType, Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
};
use arrow::error::ArrowError;
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CastMode {
    Legacy,
    Ansi,
    Try,
}

/// Mode-aware FLOAT/DOUBLE-to-integer conversion and signed integer narrowing.
/// Spark conservatively declares analyzed floating casts nullable; folding may refine them later.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastNumericToInteger {
    target: DataType,
    signature: Signature,
    mode: CastMode,
}

impl SparkCastNumericToInteger {
    /// Validate a signed target and select CAST or checked NULL-on-failure TRY_CAST.
    pub fn try_new(target: DataType, ansi_mode: bool, is_try: bool) -> Result<Self> {
        if !matches!(
            target,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ) {
            return plan_err!("numeric CAST requires a signed integer target, got {target}");
        }
        Ok(Self {
            target,
            signature: Signature::user_defined(Volatility::Immutable),
            mode: if is_try {
                CastMode::Try
            } else if ansi_mode {
                CastMode::Ansi
            } else {
                CastMode::Legacy
            },
        })
    }

    pub fn target_type(&self) -> &DataType {
        &self.target
    }

    pub fn is_try(&self) -> bool {
        self.mode == CastMode::Try
    }

    /// Convert an already evaluated value without evaluating its producer again.
    pub fn cast_value(&self, value: &ColumnarValue) -> Result<ColumnarValue> {
        self.return_type(&[value.data_type()])?;
        if self.mode != CastMode::Legacy {
            return value
                .cast_to(
                    &self.target,
                    Some(&CastOptions {
                        safe: self.is_try(),
                        ..Default::default()
                    }),
                )
                .map_err(|error| match &error {
                    DataFusionError::ArrowError(cause, _)
                        if self.mode == CastMode::Ansi
                            && matches!(
                                cause.as_ref(),
                                ArrowError::CastError(_) | ArrowError::ArithmeticOverflow(_)
                            ) =>
                    {
                        error.context("[CAST_OVERFLOW]")
                    }
                    _ => error,
                });
        }
        let target = self.target.clone();
        make_scalar_function(
            move |arrays| {
                let input = &arrays[0];
                // JVM floating byte/short conversions first saturate to INT, then keep low bits.
                let result: ArrayRef = match (input.data_type(), &target) {
                    (DataType::Float32, DataType::Int8) => Arc::new(
                        input
                            .as_primitive::<Float32Type>()
                            .unary::<_, Int8Type>(|value| value as i32 as i8),
                    ),
                    (DataType::Float32, DataType::Int16) => Arc::new(
                        input
                            .as_primitive::<Float32Type>()
                            .unary::<_, Int16Type>(|value| value as i32 as i16),
                    ),
                    (DataType::Float32, DataType::Int32) => Arc::new(
                        input
                            .as_primitive::<Float32Type>()
                            .unary::<_, Int32Type>(|value| value as i32),
                    ),
                    (DataType::Float32, DataType::Int64) => Arc::new(
                        input
                            .as_primitive::<Float32Type>()
                            .unary::<_, Int64Type>(|value| value as i64),
                    ),
                    (DataType::Float64, DataType::Int8) => Arc::new(
                        input
                            .as_primitive::<Float64Type>()
                            .unary::<_, Int8Type>(|value| value as i32 as i8),
                    ),
                    (DataType::Float64, DataType::Int16) => Arc::new(
                        input
                            .as_primitive::<Float64Type>()
                            .unary::<_, Int16Type>(|value| value as i32 as i16),
                    ),
                    (DataType::Float64, DataType::Int32) => Arc::new(
                        input
                            .as_primitive::<Float64Type>()
                            .unary::<_, Int32Type>(|value| value as i32),
                    ),
                    (DataType::Float64, DataType::Int64) => Arc::new(
                        input
                            .as_primitive::<Float64Type>()
                            .unary::<_, Int64Type>(|value| value as i64),
                    ),
                    (DataType::Int16, DataType::Int8) => Arc::new(
                        input
                            .as_primitive::<Int16Type>()
                            .unary::<_, Int8Type>(|value| value as i8),
                    ),
                    (DataType::Int32, DataType::Int8) => Arc::new(
                        input
                            .as_primitive::<Int32Type>()
                            .unary::<_, Int8Type>(|value| value as i8),
                    ),
                    (DataType::Int64, DataType::Int8) => Arc::new(
                        input
                            .as_primitive::<Int64Type>()
                            .unary::<_, Int8Type>(|value| value as i8),
                    ),
                    (DataType::Int32, DataType::Int16) => Arc::new(
                        input
                            .as_primitive::<Int32Type>()
                            .unary::<_, Int16Type>(|value| value as i16),
                    ),
                    (DataType::Int64, DataType::Int16) => Arc::new(
                        input
                            .as_primitive::<Int64Type>()
                            .unary::<_, Int16Type>(|value| value as i16),
                    ),
                    (DataType::Int64, DataType::Int32) => Arc::new(
                        input
                            .as_primitive::<Int64Type>()
                            .unary::<_, Int32Type>(|value| value as i32),
                    ),
                    _ => {
                        return exec_err!(
                            "unsupported legacy numeric CAST from {} to {target}",
                            input.data_type()
                        );
                    }
                };
                Ok(result)
            },
            vec![],
        )(std::slice::from_ref(value))
    }
}

impl ScalarUDFImpl for SparkCastNumericToInteger {
    fn name(&self) -> &str {
        "spark_cast_numeric_to_integer"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        self.coerce_types(arguments)?;
        Ok(self.target.clone())
    }

    fn coerce_types(&self, arguments: &[DataType]) -> Result<Vec<DataType>> {
        if !(matches!(arguments, [DataType::Float32 | DataType::Float64])
            || matches!(
                (arguments, &self.target),
                (
                    [DataType::Int16 | DataType::Int32 | DataType::Int64],
                    DataType::Int8
                ) | ([DataType::Int32 | DataType::Int64], DataType::Int16)
                    | ([DataType::Int64], DataType::Int32)
            ))
        {
            return exec_err!(
                "numeric CAST to {} expects FLOAT/DOUBLE or a wider signed integer, got {arguments:?}",
                self.target
            );
        }
        Ok(arguments.to_vec())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = take_function_args(self.name(), args.arg_fields)?;
        self.return_type(std::slice::from_ref(input.data_type()))?;
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            matches!(input.data_type(), DataType::Float32 | DataType::Float64)
                || input.is_nullable()
                || self.is_try(),
        )))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [Expr::Literal(value, _)] = args.as_slice() {
            // Keep literal CAST failures at the native planning boundary.
            let value = self.cast_value(&ColumnarValue::Scalar(value.clone()))?;
            if let ColumnarValue::Scalar(value) = value {
                return Ok(ExprSimplifyResult::Simplified(lit(value)));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    // A parent that keeps Spark's row order of errors needs this cast's first failing row,
    // which only ANSI can have.
    fn invoke_rows_until_error(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<datafusion_expr::EvaluatedRowPrefix> {
        if self.mode != CastMode::Ansi {
            return self
                .invoke_with_args(args)
                .map(datafusion_expr::EvaluatedRowPrefix::complete);
        }
        let [value] = take_function_args(self.name(), args.args.as_slice())?;
        let try_result = Self::try_new(self.target.clone(), true, true)?.cast_value(value)?;
        crate::scalar::row_errors::strict_cast_rows_until_error(
            value,
            try_result,
            args.number_rows,
            |value| self.cast_value(value),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), args.args.as_slice())?;
        self.cast_value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, Int64Array};
    use datafusion_common::{ScalarValue, config::ConfigOptions};
    use datafusion_expr::col;

    #[test]
    fn numeric_integer_cast_rejects_unsupported_pairs_and_preserves_fields() -> Result<()> {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for (ansi, is_try) in [(false, false), (true, false), (true, true)] {
                let cast = SparkCastNumericToInteger::try_new(target.clone(), ansi, is_try)?;
                assert_eq!(cast.target_type(), &target);
                for source in [
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                ] {
                    let allowed = matches!(
                        (&source, &target),
                        (DataType::Float32 | DataType::Float64, _)
                            | (
                                DataType::Int16 | DataType::Int32 | DataType::Int64,
                                DataType::Int8
                            )
                            | (DataType::Int32 | DataType::Int64, DataType::Int16)
                            | (DataType::Int64, DataType::Int32)
                    );
                    if !allowed {
                        assert!(cast.coerce_types(&[source]).is_err());
                        continue;
                    }
                    assert_eq!(
                        cast.coerce_types(std::slice::from_ref(&source))?,
                        vec![source.clone()]
                    );
                    assert_eq!(cast.return_type(std::slice::from_ref(&source))?, target);
                    for nullable in [false, true] {
                        let fields = [Arc::new(Field::new("source", source.clone(), nullable))];
                        let result = cast.return_field_from_args(ReturnFieldArgs {
                            arg_fields: &fields,
                            scalar_arguments: &[None],
                        })?;
                        assert_eq!(result.data_type(), &target);
                        assert_eq!(
                            result.is_nullable(),
                            matches!(source, DataType::Float32 | DataType::Float64)
                                || nullable
                                || is_try
                        );
                        assert!(result.metadata().is_empty());
                    }
                }
                for source in [
                    DataType::UInt64,
                    DataType::Utf8,
                    DataType::Null,
                    DataType::Date32,
                ] {
                    assert!(cast.coerce_types(&[source]).is_err());
                }
                let Err(error) = cast.cast_value(&ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "not a numeric source".into(),
                )))) else {
                    return exec_err!("unsupported numeric CAST source was admitted");
                };
                assert!(!error.to_string().contains("[CAST_OVERFLOW]"));
                assert!(cast.coerce_types(&[]).is_err());
                assert!(
                    cast.coerce_types(&[DataType::Float64, DataType::Float64])
                        .is_err()
                );
            }
        }
        for target in [DataType::UInt32, DataType::Float64, DataType::Utf8] {
            assert!(SparkCastNumericToInteger::try_new(target, false, false).is_err());
        }
        Ok(())
    }

    #[test]
    fn signed_integer_narrowing_keeps_wrapping_checked_and_try_modes() -> Result<()> {
        for (source_type, target, positive, negative, low, high) in [
            (
                DataType::Int16,
                DataType::Int8,
                128_i64,
                -129_i64,
                ScalarValue::Int8(Some(-128)),
                ScalarValue::Int8(Some(127)),
            ),
            (
                DataType::Int32,
                DataType::Int8,
                128,
                -129,
                ScalarValue::Int8(Some(-128)),
                ScalarValue::Int8(Some(127)),
            ),
            (
                DataType::Int64,
                DataType::Int8,
                128,
                -129,
                ScalarValue::Int8(Some(-128)),
                ScalarValue::Int8(Some(127)),
            ),
            (
                DataType::Int32,
                DataType::Int16,
                32768,
                -32769,
                ScalarValue::Int16(Some(-32768)),
                ScalarValue::Int16(Some(32767)),
            ),
            (
                DataType::Int64,
                DataType::Int16,
                32768,
                -32769,
                ScalarValue::Int16(Some(-32768)),
                ScalarValue::Int16(Some(32767)),
            ),
            (
                DataType::Int64,
                DataType::Int32,
                2147483648,
                -2147483649,
                ScalarValue::Int32(Some(-2147483648)),
                ScalarValue::Int32(Some(2147483647)),
            ),
        ] {
            let storage = ScalarValue::iter_to_array(
                [Some(0), Some(positive), Some(negative), None, Some(1)]
                    .into_iter()
                    .map(|value| ScalarValue::Int64(value).cast_to(&source_type))
                    .collect::<Result<Vec<_>>>()?,
            )?;
            let input = storage.slice(1, 4);
            let null = ScalarValue::try_new_null(&target)?;
            let expected = [
                low,
                high,
                null.clone(),
                ScalarValue::Int64(Some(1)).cast_to(&target)?,
            ];
            for (ansi, is_try) in [(false, false), (true, false), (false, true), (true, true)] {
                let cast = SparkCastNumericToInteger::try_new(target.clone(), ansi, is_try)?;
                for (index, legacy) in expected.iter().enumerate() {
                    let value = ScalarValue::try_from_array(&input, index)?;
                    let result = cast.cast_value(&ColumnarValue::Scalar(value));
                    if ansi && !is_try && index < 2 {
                        let Err(error) = result else {
                            return exec_err!("signed narrowing failed to check overflow");
                        };
                        assert!(error.to_string().contains("[CAST_OVERFLOW]"));
                        assert_eq!(
                            error.find_root().to_string(),
                            format!(
                                "Arrow error: Cast error: Can't cast value {} to type {target:?}",
                                if index == 0 { positive } else { negative },
                            )
                        );
                    } else {
                        let expected = if is_try && index < 2 { &null } else { legacy };
                        assert!(
                            matches!(result?, ColumnarValue::Scalar(actual) if &actual == expected)
                        );
                    }
                }
                let result = cast.cast_value(&ColumnarValue::Array(input.clone()));
                if ansi && !is_try {
                    let Err(error) = result else {
                        return exec_err!("array narrowing failed to check overflow");
                    };
                    assert!(error.to_string().contains("[CAST_OVERFLOW]"));
                } else {
                    let ColumnarValue::Array(result) = result? else {
                        return exec_err!("array narrowing changed shape");
                    };
                    assert_eq!(result.len(), 4);
                    for (index, legacy) in expected.iter().enumerate() {
                        let expected = if is_try && index < 2 { &null } else { legacy };
                        assert_eq!(&ScalarValue::try_from_array(&result, index)?, expected);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn numeric_integer_cast_literals_fold_to_the_requested_type() -> Result<()> {
        for (target, positive, negative) in [
            (
                DataType::Int8,
                ScalarValue::Int8(Some(1)),
                ScalarValue::Int8(Some(-1)),
            ),
            (
                DataType::Int16,
                ScalarValue::Int16(Some(1)),
                ScalarValue::Int16(Some(-1)),
            ),
            (
                DataType::Int32,
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(-1)),
            ),
            (
                DataType::Int64,
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(-1)),
            ),
        ] {
            for (ansi, is_try) in [(false, false), (true, false), (false, true), (true, true)] {
                let cast = SparkCastNumericToInteger::try_new(target.clone(), ansi, is_try)?;
                for source in [
                    ScalarValue::Float32(Some(1.9)),
                    ScalarValue::Float64(Some(1.9)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float64(None),
                ] {
                    let expected = if source.is_null() {
                        ScalarValue::try_new_null(&target)?
                    } else {
                        positive.clone()
                    };
                    let result = cast.invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Scalar(source.clone())],
                        arg_fields: vec![Arc::new(Field::new(
                            "source",
                            source.data_type(),
                            source.is_null(),
                        ))],
                        number_rows: 1,
                        return_field: Arc::new(Field::new("result", target.clone(), true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    })?;
                    assert!(matches!(result, ColumnarValue::Scalar(value) if value == expected));
                    let folded = cast.simplify(vec![lit(source)], &SimplifyContext::default())?;
                    assert!(
                        matches!(folded, ExprSimplifyResult::Simplified(Expr::Literal(value, _)) if value == expected)
                    );
                }
                let arguments = vec![col("value")];
                assert!(
                    matches!(cast.simplify(arguments.clone(), &SimplifyContext::default())?,
                    ExprSimplifyResult::Original(actual) if actual == arguments)
                );
                for storage in [
                    Arc::new(Float32Array::from(vec![
                        Some(f32::NAN),
                        Some(1.9),
                        None,
                        Some(-1.9),
                        Some(f32::INFINITY),
                    ])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        Some(1.9),
                        None,
                        Some(-1.9),
                        Some(f64::INFINITY),
                    ])) as ArrayRef,
                ] {
                    let result = cast.cast_value(&ColumnarValue::Array(storage.slice(1, 3)))?;
                    let ColumnarValue::Array(result) = result else {
                        return exec_err!("array CAST changed shape");
                    };
                    assert_eq!(result.len(), 3);
                    assert_eq!(ScalarValue::try_from_array(&result, 0)?, positive);
                    assert_eq!(
                        ScalarValue::try_from_array(&result, 1)?,
                        ScalarValue::try_new_null(&target)?
                    );
                    assert_eq!(ScalarValue::try_from_array(&result, 2)?, negative);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn numeric_integer_cast_preserves_bigint_and_checked_modes() -> Result<()> {
        let bigint = ColumnarValue::Scalar(ScalarValue::Int64(Some(4294967296)));
        for ansi in [false, true] {
            let cast = SparkCastNumericToInteger::try_new(DataType::Int32, ansi, false)?;
            if ansi {
                let Err(error) = cast.cast_value(&bigint) else {
                    return exec_err!("expected ANSI overflow");
                };
                assert_eq!(
                    error.find_root().to_string(),
                    "Arrow error: Cast error: Can't cast value 4294967296 to type Int32"
                );
                assert!(error.to_string().contains("[CAST_OVERFLOW]"));
            } else {
                assert!(matches!(
                    cast.cast_value(&bigint)?,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(0)))
                ));
            }
            for (value, cause, legacy) in [
                (ScalarValue::Float32(Some(f32::NAN)), "NaN", 0),
                (ScalarValue::Float64(Some(f64::INFINITY)), "inf", i32::MAX),
            ] {
                let result = cast.cast_value(&ColumnarValue::Scalar(value));
                if ansi {
                    let Err(error) = result else {
                        return exec_err!("expected existing checked floating error");
                    };
                    assert_eq!(
                        error.find_root().to_string(),
                        format!("Arrow error: Cast error: Can't cast value {cause} to type Int32")
                    );
                    assert!(error.to_string().contains("[CAST_OVERFLOW]"));
                } else {
                    assert!(
                        matches!(result?, ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) if value == legacy)
                    );
                }
            }
            let cast = SparkCastNumericToInteger::try_new(DataType::Int32, ansi, true)?;
            for value in [
                bigint.clone(),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(f32::NAN))),
            ] {
                assert!(matches!(
                    cast.cast_value(&value)?,
                    ColumnarValue::Scalar(ScalarValue::Int32(None))
                ));
            }
        }
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for ansi in [false, true] {
                let cast = SparkCastNumericToInteger::try_new(target.clone(), ansi, true)?;
                for source in [
                    ScalarValue::Float32(Some(f32::NAN)),
                    ScalarValue::Float64(Some(f64::NAN)),
                    ScalarValue::Float32(Some(f32::INFINITY)),
                    ScalarValue::Float64(Some(f64::INFINITY)),
                    ScalarValue::Float32(Some(f32::NEG_INFINITY)),
                    ScalarValue::Float64(Some(f64::NEG_INFINITY)),
                    ScalarValue::Float32(Some(1e30)),
                    ScalarValue::Float64(Some(1e100)),
                ] {
                    let result = cast.cast_value(&ColumnarValue::Scalar(source))?;
                    assert!(
                        matches!(result, ColumnarValue::Scalar(value) if value == ScalarValue::try_new_null(&target)?)
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn ansi_numeric_integer_overflow_preserves_arrow_root_cause() -> Result<()> {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let cast = SparkCastNumericToInteger::try_new(target.clone(), true, false)?;
            for (source, value) in [
                (ScalarValue::Float32(Some(f32::NAN)), "NaN"),
                (ScalarValue::Float64(Some(f64::NAN)), "NaN"),
                (ScalarValue::Float32(Some(f32::INFINITY)), "inf"),
                (ScalarValue::Float64(Some(f64::INFINITY)), "inf"),
                (ScalarValue::Float32(Some(f32::NEG_INFINITY)), "-inf"),
                (ScalarValue::Float64(Some(f64::NEG_INFINITY)), "-inf"),
                (ScalarValue::Float32(Some(1e30)), "1e30"),
                (ScalarValue::Float64(Some(1e100)), "1e100"),
                (ScalarValue::Int64(Some(4294967296)), "4294967296"),
            ] {
                if source.data_type() == DataType::Int64 && target != DataType::Int32 {
                    continue;
                }
                let storage: ArrayRef = match &source {
                    ScalarValue::Float32(value) => Arc::new(Float32Array::from(vec![
                        Some(f32::NAN),
                        None,
                        *value,
                        Some(f32::INFINITY),
                    ])),
                    ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![
                        Some(f64::NAN),
                        None,
                        *value,
                        Some(f64::INFINITY),
                    ])),
                    ScalarValue::Int64(value) => {
                        Arc::new(Int64Array::from(vec![Some(i64::MAX), None, *value]))
                    }
                    _ => return exec_err!("unexpected numeric overflow source"),
                };
                let cause =
                    format!("Arrow error: Cast error: Can't cast value {value} to type {target}");
                for result in [
                    cast.cast_value(&ColumnarValue::Scalar(source.clone()))
                        .map(|_| ()),
                    cast.cast_value(&ColumnarValue::Array(storage.slice(1, 2)))
                        .map(|_| ()),
                    cast.simplify(vec![lit(source)], &SimplifyContext::default())
                        .map(|_| ()),
                ] {
                    let Err(error) = result else {
                        return exec_err!("expected ANSI overflow to {target}");
                    };
                    assert!(matches!(
                        error.find_root(),
                        DataFusionError::ArrowError(_, _)
                    ));
                    assert_eq!(error.find_root().to_string(), cause);
                    assert_eq!(
                        error.to_string(),
                        format!("[CAST_OVERFLOW]\ncaused by\n{cause}")
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn legacy_floating_integer_casts_saturate_before_narrowing() -> Result<()> {
        for (value, expected) in [
            (f64::NAN, [0, 0, 0, 0]),
            (f64::INFINITY, [-1, -1, i32::MAX as i64, i64::MAX]),
            (f64::NEG_INFINITY, [0, 0, i32::MIN as i64, i64::MIN]),
            (257.75, [1, 257, 257, 257]),
            (-257.75, [-1, -257, -257, -257]),
            (65537.75, [1, 1, 65537, 65537]),
            (-65537.75, [-1, -1, -65537, -65537]),
            (2147483648.0, [-1, -1, i32::MAX as i64, 2147483648]),
            (9223372036854775808.0, [-1, -1, i32::MAX as i64, i64::MAX]),
        ] {
            for (target, expected) in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ]
            .into_iter()
            .zip(expected)
            {
                let cast = SparkCastNumericToInteger::try_new(target.clone(), false, false)?;
                for source in [
                    ScalarValue::Float32(Some(value as f32)),
                    ScalarValue::Float64(Some(value)),
                ] {
                    let ColumnarValue::Scalar(result) =
                        cast.cast_value(&ColumnarValue::Scalar(source))?
                    else {
                        return exec_err!("scalar CAST changed shape");
                    };
                    assert_eq!(result.data_type(), target);
                    assert_eq!(
                        result.cast_to(&DataType::Int64)?,
                        ScalarValue::Int64(Some(expected))
                    );
                }
            }
        }
        Ok(())
    }
}
