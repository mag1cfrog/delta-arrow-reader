//! NULLIF returns its original left value when comparison is false or NULL.
//! For example, NULLIF(9007199254740993L, 0D) keeps the exact BIGINT value even
//! though equality uses DOUBLE. NULL left values skip comparison.

use arrow::array::AsArray;
use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::core::nullif::NullIfFunc;
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};
use std::sync::Arc;

use crate::function::common::{SparkComparisonFloat, spark_comparison_widens_to_double};
use crate::function::comparison::{
    comparison_string_type, nanosecond_comparison_cast, string_comparison_cast,
    string_comparison_type, temporal_comparison_type,
};
use crate::function::temporal_coercion::cast_local_timestamp_value_to_ltz;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkNullIf {
    native: NullIfFunc,
    ansi_mode: bool,
    session_timezone: Arc<str>,
    preserve_local_comparison: bool,
}

impl SparkNullIf {
    pub(crate) fn new(ansi_mode: bool, session_timezone: Arc<str>) -> Self {
        Self {
            native: NullIfFunc::new(),
            ansi_mode,
            session_timezone,
            preserve_local_comparison: false,
        }
    }

    // Local VALUES conversion precedes the normal known-NULL-right rewrite.
    pub(crate) fn for_local_values(&self) -> Self {
        let mut function = Self::new(self.ansi_mode, self.session_timezone.clone());
        function.preserve_local_comparison = true;
        function
    }

    fn comparison_type(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        // A dictionary-encoded STRING compares with a value of another type as its values do.
        let (left, right) = (
            &comparison_string_type(left, right).unwrap_or_else(|| left.clone()),
            &comparison_string_type(right, left).unwrap_or_else(|| right.clone()),
        );
        if let Some(conversion) = nanosecond_comparison_cast(left, right, &self.session_timezone) {
            return conversion.inner().return_type(&[left.clone()]);
        }
        if let Some(data_type) = temporal_comparison_type(left, right, &self.session_timezone) {
            return Ok(data_type);
        }
        if let Some(data_type) =
            string_comparison_type(left, right, self.ansi_mode, &self.session_timezone)
        {
            return Ok(data_type);
        }
        if spark_comparison_widens_to_double(left, right, self.ansi_mode) {
            return Ok(DataType::Float64);
        }
        comparison_coercion(left, right).ok_or_else(|| {
            datafusion_common::plan_datafusion_err!(
                "For function 'nullif' {left} and {right} is not comparable"
            )
        })
    }
}

impl ScalarUDFImpl for SparkNullIf {
    fn name(&self) -> &str {
        self.native.name()
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [left, right] = take_function_args(self.name(), args)?;
        self.comparison_type(left, right)?;
        Ok(left.clone())
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn kernel_accepts_null_first_arg(&self, arg_types: &[DataType]) -> bool {
        // Equal DOUBLE inputs skip casts; normalization only changes comparison values.
        matches!(arg_types, [DataType::Float64, DataType::Float64])
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        args.split_first()
            .map(|(first, rest)| (vec![first], rest.iter().collect()))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [left, right] = take_function_args(self.name(), args.as_slice())?;
        let null_left = matches!(left, Expr::Literal(value, _) if value.is_null());
        let null_right = !self.preserve_local_comparison
            && matches!(right, Expr::Literal(value, _) if value.is_null());
        if null_left || null_right {
            // Comparability still applies when equality needs no evaluation.
            let original =
                ScalarUDF::from(Self::new(self.ansi_mode, self.session_timezone.clone()))
                    .call(args.clone());
            let result_type = info.get_data_type(&original)?;
            return Ok(ExprSimplifyResult::Simplified(if null_left {
                lit(ScalarValue::try_new_null(&result_type)?)
            } else {
                left.clone()
            }));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, mut input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = take_function_args(self.name(), input.args.as_slice())?;
        let left = left.clone();
        let same_type = left.data_type() == right.data_type();
        let exact_timestamp_cast = nanosecond_comparison_cast(
            &left.data_type(),
            &right.data_type(),
            &self.session_timezone,
        );
        let uses_spark_temporal_coercion = temporal_comparison_type(
            &left.data_type(),
            &right.data_type(),
            &self.session_timezone,
        )
        .is_some();
        let comparison_type = if let Some(conversion) = &exact_timestamp_cast {
            conversion.inner().return_type(&[left.data_type()])?
        } else {
            self.comparison_type(&left.data_type(), &right.data_type())?
        };
        if !same_type {
            for (value, field) in input.args.iter_mut().zip(&mut input.arg_fields) {
                if let Some(values) = comparison_string_type(&value.data_type(), &comparison_type) {
                    *value = value.cast_to(&values, None)?;
                    *field = Arc::new(field.as_ref().clone().with_data_type(values));
                }
                let comparison_field = std::sync::Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(comparison_type.clone())
                        .with_nullable(field.is_nullable() || !self.ansi_mode),
                );
                *value = if let Some(conversion) = &exact_timestamp_cast {
                    conversion.invoke_with_args(ScalarFunctionArgs {
                        args: vec![value.clone()],
                        arg_fields: vec![field.clone()],
                        number_rows: input.number_rows,
                        return_field: comparison_field.clone(),
                        config_options: input.config_options.clone(),
                    })?
                } else if uses_spark_temporal_coercion
                    && matches!(
                        value.data_type(),
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                    )
                    && let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) =
                        &comparison_type
                {
                    cast_local_timestamp_value_to_ltz(
                        ScalarFunctionArgs {
                            args: vec![value.clone()],
                            arg_fields: vec![field.clone()],
                            number_rows: input.number_rows,
                            return_field: comparison_field.clone(),
                            config_options: input.config_options.clone(),
                        },
                        timezone,
                    )?
                } else if value.data_type().is_string()
                    && string_comparison_type(
                        &value.data_type(),
                        &comparison_type,
                        self.ansi_mode,
                        &self.session_timezone,
                    )
                    .is_some()
                {
                    string_comparison_cast(&comparison_type, self.ansi_mode)?.invoke_with_args(
                        ScalarFunctionArgs {
                            args: vec![value.clone()],
                            arg_fields: vec![field.clone()],
                            number_rows: input.number_rows,
                            return_field: comparison_field.clone(),
                            config_options: input.config_options.clone(),
                        },
                    )?
                } else {
                    value.cast_to(&comparison_type, None)?
                };
                *field = comparison_field;
            }
        }
        // Only equality with zero or NaN can change under float normalization.
        let normalize_floats = matches!(comparison_type, DataType::Float32 | DataType::Float64)
            && !input.args.iter().any(|value| match value {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                    *value != 0.0 && !value.is_nan()
                }
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
                    *value != 0.0 && !value.is_nan()
                }
                _ => false,
            });
        if same_type && !normalize_floats {
            return self.native.invoke_with_args(input);
        }
        if normalize_floats {
            let comparison_float = SparkComparisonFloat { widen: false };
            for (value, field) in input.args.iter_mut().zip(&input.arg_fields) {
                *value = comparison_float.invoke_with_args(ScalarFunctionArgs {
                    args: vec![value.clone()],
                    arg_fields: vec![field.clone()],
                    number_rows: input.number_rows,
                    return_field: field.clone(),
                    config_options: input.config_options.clone(),
                })?;
            }
        }
        // Only copy validity: normalization must not change an unequal value's bits.
        match apply_cmp(Operator::Eq, &input.args[0], &input.args[1])? {
            ColumnarValue::Array(equal) => Ok(ColumnarValue::Array(nullif(
                left.into_array(equal.len())?.as_ref(),
                equal.as_boolean(),
            )?)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_new_null(&left.data_type())?,
            )),
            ColumnarValue::Scalar(ScalarValue::Boolean(_)) => Ok(left),
            _ => datafusion_common::internal_err!("equality returned a non-Boolean result"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float32Type, Float64Type};
    use datafusion_common::{ScalarValue, config::ConfigOptions};
    use std::sync::Arc;

    fn invoke(left: ColumnarValue, right: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let field = Arc::new(Field::new("value", left.data_type(), true));
        let right_field = Arc::new(Field::new("right", right.data_type(), true));
        SparkNullIf::new(true, "UTC".into()).invoke_with_args(ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![field.clone(), right_field],
            number_rows: rows,
            return_field: field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn bits(array: &dyn Array, row: usize) -> u64 {
        match array.data_type() {
            DataType::Float32 => {
                u64::from(array.as_primitive::<Float32Type>().value(row).to_bits())
            }
            DataType::Float64 => array.as_primitive::<Float64Type>().value(row).to_bits(),
            _ => unreachable!("test inputs are FLOAT or DOUBLE"),
        }
    }

    #[test]
    fn empty_batches_skip_nullif_and_arithmetic_operands() -> Result<()> {
        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use datafusion_common::DFSchema;
        use datafusion_expr::{Volatility, col, create_udf};
        use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;

        let operand = create_udf(
            "fail_if_evaluated",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(|_| datafusion_common::exec_err!("unexpected operand evaluation")),
        );
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])));
        let functions = std::iter::once(ScalarUDF::from(SparkNullIf::new(true, "UTC".into())))
            .chain(
                [Operator::Plus, Operator::Minus, Operator::Multiply]
                    .map(|operator| ScalarUDF::from(SparkCheckedArithmetic::new(operator))),
            );
        for function in functions {
            for right in [operand.call(vec![col("a")]), lit(7_i32)] {
                let expression = function.call(vec![operand.call(vec![col("a")]), right]);
                let physical = SessionContext::new()
                    .create_physical_expr(expression, &DFSchema::try_from(batch.schema())?)?;
                let result = physical.evaluate(&batch)?.into_array(0)?;
                assert_eq!(result.len(), 0);
                assert_eq!(result.data_type(), &DataType::Int32);
            }
        }
        Ok(())
    }

    #[test]
    fn equal_zeros_and_nan_payloads_change_only_validity() -> Result<()> {
        let double_nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let negative_double_nan = f64::from_bits(0xfff8_0000_0000_4321);
        let float_nan = f32::from_bits(0x7fc0_1234);
        let negative_float_nan = f32::from_bits(0xffc0_4321);
        let left: [ArrayRef; 2] = [
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(float_nan),
                Some(negative_float_nan),
                Some(float_nan),
                Some(-0.0),
                Some(f32::INFINITY),
                Some(-f32::INFINITY),
                Some(1.0),
                Some(1.0),
                None,
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(double_nan),
                Some(negative_double_nan),
                Some(double_nan),
                Some(-0.0),
                Some(f64::INFINITY),
                Some(-f64::INFINITY),
                Some(1.0),
                Some(1.0),
                None,
                None,
            ])),
        ];
        let right: [ArrayRef; 2] = [
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(negative_float_nan),
                Some(float_nan),
                Some(2.0),
                Some(2.0),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(2.0),
                None,
                Some(1.0),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(negative_double_nan),
                Some(double_nan),
                Some(2.0),
                Some(2.0),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(2.0),
                None,
                Some(1.0),
                None,
            ])),
        ];
        let expected_nulls = [
            true, true, true, true, false, false, true, false, false, false, true, true,
        ];
        for (left, right) in left.into_iter().zip(right) {
            let other_width = if right.data_type() == &DataType::Float32 {
                DataType::Float64
            } else {
                DataType::Float32
            };
            let converted_right = arrow::compute::cast(&right, &other_width)?;
            for right in [right, converted_right] {
                for (offset, len) in [(0, 12), (1, 7), (0, 0)] {
                    let lhs = left.slice(offset, len);
                    let rhs = right.slice(offset, len);
                    let result = invoke(
                        ColumnarValue::Array(lhs.clone()),
                        ColumnarValue::Array(rhs),
                        len,
                    )?
                    .into_array(len)?;
                    assert_eq!(result.data_type(), lhs.data_type());
                    assert_eq!(result.len(), len);
                    for row in 0..len {
                        assert_eq!(result.is_null(row), expected_nulls[offset + row]);
                        if !result.is_null(row) {
                            assert_eq!(bits(result.as_ref(), row), bits(lhs.as_ref(), row));
                        }
                    }
                }
                for (row, expected_null) in expected_nulls.into_iter().enumerate() {
                    let lhs = ScalarValue::try_from_array(&left, row)?;
                    let rhs = ScalarValue::try_from_array(&right, row)?;
                    for rows in [0, 1] {
                        for (a, b) in [
                            (
                                ColumnarValue::Scalar(lhs.clone()),
                                ColumnarValue::Scalar(rhs.clone()),
                            ),
                            (
                                ColumnarValue::Scalar(lhs.clone()),
                                ColumnarValue::Array(right.slice(row, rows)),
                            ),
                            (
                                ColumnarValue::Array(left.slice(row, rows)),
                                ColumnarValue::Scalar(rhs.clone()),
                            ),
                        ] {
                            let result = invoke(a, b, rows)?.into_array(rows)?;
                            assert_eq!(result.data_type(), left.data_type());
                            assert_eq!(result.len(), rows);
                            if rows != 0 {
                                assert_eq!(result.is_null(0), expected_null);
                                if !expected_null {
                                    assert_eq!(bits(result.as_ref(), 0), bits(left.as_ref(), row));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    mod selection_620_tests {
        use super::SparkNullIf;
        use std::cell::RefCell;
        use std::error::Error;
        use std::fmt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use arrow::array::{
            Array, ArrayRef, AsArray, Decimal128Array, DictionaryArray, Float32Array, Float64Array,
            Int8Array, Int64Array, StringArray, TimestampMicrosecondArray,
            TimestampNanosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int8Type, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::physical_expr::expressions::{Column, Literal};
        use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
        use datafusion::prelude::SessionContext;
        use datafusion_common::{DFSchema, Result, ScalarValue, config::ConfigOptions};
        use datafusion_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
            col, create_udf,
        };
        use serde_json::{Value, json};

        type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/nullif-selection-620-original.rs"
        ));

        fn corpus() -> TestResult<Value> {
            Ok(serde_json::from_str(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/nullif-selection-620-inputs.json"
            )))?)
        }

        fn doubles(values: &Value) -> TestResult<ArrayRef> {
            let values = values
                .as_array()
                .ok_or("missing bits")?
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|s| u64::from_str_radix(s, 16).map(f64::from_bits))
                        .transpose()
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }

        fn batch(a: ArrayRef, b: ArrayRef) -> Result<RecordBatch> {
            Ok(RecordBatch::try_from_iter([("a", a), ("b", b)])?)
        }

        fn column(index: usize) -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new(if index == 0 { "a" } else { "b" }, index))
        }

        fn function(
            ansi: bool,
            args: Vec<Arc<dyn PhysicalExpr>>,
            schema: &Schema,
        ) -> Result<ScalarFunctionExpr> {
            ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(SparkNullIf::new(
                    ansi,
                    "America/Phoenix".into(),
                ))),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )
        }

        fn encoded(array: &dyn Array) -> Result<Vec<Value>> {
            if let DataType::Dictionary(_, value_type) = array.data_type() {
                return encoded(arrow::compute::cast(array, value_type)?.as_ref());
            }
            (0..array.len())
                .map(|row| {
                    Ok(if array.is_null(row) {
                        Value::Null
                    } else {
                        json!(match array.data_type() {
                            DataType::Float64 => format!(
                                "{:016x}",
                                array.as_primitive::<Float64Type>().value(row).to_bits()
                            ),
                            DataType::Float32 => format!(
                                "{:08x}",
                                array.as_primitive::<Float32Type>().value(row).to_bits()
                            ),
                            _ => arrow::util::display::array_value_to_string(array, row)?,
                        })
                    })
                })
                .collect()
        }

        fn observed(result: Result<ColumnarValue>, rows: usize) -> Result<Value> {
            match result {
                Ok(value) => {
                    let form = if matches!(value, ColumnarValue::Scalar(_)) {
                        "scalar"
                    } else {
                        "array"
                    };
                    let data_type = format!("{:?}", value.data_type());
                    Ok(
                        json!({"form":form,"type":data_type,"values":encoded(value.into_array(rows)?.as_ref())?}),
                    )
                }
                Err(error) => {
                    Ok(json!({"error":error.to_string(),"root":format!("{:?}",error.find_root())}))
                }
            }
        }

        fn differential(function: &ScalarFunctionExpr, batch: &RecordBatch) -> Result<()> {
            assert_eq!(
                observed(original_selection(function, batch), batch.num_rows())?,
                observed(function.evaluate(batch), batch.num_rows())?
            );
            Ok(())
        }

        #[test]
        fn double_selection_preserves_original_bits_types_nulls_and_forms() -> TestResult {
            let cases = corpus()?;
            let a = doubles(&cases["double_left"])?;
            let b = doubles(&cases["double_right"])?;
            for ansi in [false, true] {
                for left in [
                    a.clone(),
                    Arc::new(Float64Array::from(vec![Some(1.0); a.len()])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![None; a.len()])) as ArrayRef,
                ] {
                    for slice in cases["slices"].as_array().ok_or("missing slices")? {
                        let offset = slice[0].as_u64().ok_or("offset")? as usize;
                        let len = slice[1].as_u64().ok_or("len")? as usize;
                        let input = batch(left.slice(offset, len), b.slice(offset, len))?;
                        differential(
                            &function(ansi, vec![column(0), column(1)], input.schema().as_ref())?,
                            &input,
                        )?;
                        for value in cases["double_scalar_bits"]
                            .as_array()
                            .ok_or("missing scalar bits")?
                        {
                            let value = value
                                .as_str()
                                .map(|s| u64::from_str_radix(s, 16).map(f64::from_bits))
                                .transpose()?;
                            let literal: Arc<dyn PhysicalExpr> =
                                Arc::new(Literal::new(ScalarValue::Float64(value)));
                            for args in [vec![column(0), literal.clone()], vec![literal, column(1)]]
                            {
                                differential(
                                    &function(ansi, args, input.schema().as_ref())?,
                                    &input,
                                )?;
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        #[test]
        fn other_types_and_required_comparison_errors_keep_the_original_selection() -> TestResult {
            let cases = corpus()?;
            let a =
                Arc::new(Float64Array::from(vec![None, Some(1.0), Some(2.0), None])) as ArrayRef;
            let values: Vec<Option<i64>> = serde_json::from_value(cases["large_integers"].clone())?;
            let keys: Vec<Option<i8>> = serde_json::from_value(cases["dictionary_keys"].clone())?;
            let dict_values: Vec<Option<f64>> =
                serde_json::from_value(cases["dictionary_values"].clone())?;
            let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(keys),
                Arc::new(Float64Array::from(dict_values)),
            )?) as ArrayRef;
            let decimal = Arc::new(
                Decimal128Array::from(vec![None, Some(900719925474099300_i128), Some(125), None])
                    .with_precision_and_scale(20, 2)?,
            ) as ArrayRef;
            let mut pairs = vec![
                (
                    a.clone(),
                    Arc::new(Float32Array::from(vec![None, Some(1.0), Some(3.0), None]))
                        as ArrayRef,
                ),
                (Arc::new(Int64Array::from(values)) as ArrayRef, a.clone()),
                (decimal, a.clone()),
                (dictionary.clone(), a.clone()),
                (a.clone(), dictionary),
                (
                    Arc::new(TimestampNanosecondArray::from(vec![
                        None,
                        Some(0),
                        Some(1001),
                        None,
                    ])) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        None,
                        Some(0),
                        Some(1),
                        None,
                    ])) as ArrayRef,
                ),
            ];
            for field in ["skipped_bad_strings", "required_bad_strings"] {
                let strings: Vec<Option<String>> = serde_json::from_value(cases[field].clone())?;
                pairs.push((a.clone(), Arc::new(StringArray::from(strings))));
            }
            for ansi in [false, true] {
                let udf = ScalarUDF::from(SparkNullIf::new(ansi, "America/Phoenix".into()));
                assert!(udf.kernel_accepts_null_first_arg(&[DataType::Float64, DataType::Float64]));
                assert!(
                    udf.with_aliases(["nullif_alias"])
                        .kernel_accepts_null_first_arg(&[DataType::Float64, DataType::Float64])
                );
                for (a, b) in &pairs {
                    assert!(
                        !SparkNullIf::new(ansi, "UTC".into()).kernel_accepts_null_first_arg(&[
                            a.data_type().clone(),
                            b.data_type().clone()
                        ])
                    );
                    let input = batch(a.clone(), b.clone())?;
                    for (offset, len) in [(0, 4), (1, 2), (1, 0)] {
                        let input = input.slice(offset, len);
                        differential(
                            &function(ansi, vec![column(0), column(1)], input.schema().as_ref())?,
                            &input,
                        )?;
                    }
                }
                assert!(
                    !SparkNullIf::new(ansi, "UTC".into())
                        .kernel_accepts_null_first_arg(&[DataType::Float32, DataType::Float32])
                );
                let invalid = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
                assert!(
                    SparkNullIf::new(ansi, "UTC".into())
                        .return_type(&[DataType::Float64, invalid])
                        .is_err()
                );
            }
            assert!(
                !ScalarUDF::from(datafusion::functions::core::nullif::NullIfFunc::new())
                    .kernel_accepts_null_first_arg(&[DataType::Float64, DataType::Float64])
            );
            Ok(())
        }

        fn counted(
            name: &str,
            column_name: &str,
            count: Arc<AtomicUsize>,
            schema: &Schema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            let udf = create_udf(
                name,
                vec![DataType::Float64],
                DataType::Float64,
                Volatility::Volatile,
                Arc::new(move |args| {
                    let arrays = ColumnarValue::values_to_arrays(args)?;
                    count.fetch_add(arrays[0].len(), Ordering::SeqCst);
                    Ok(ColumnarValue::Array(arrays[0].clone()))
                }),
            );
            SessionContext::new().create_physical_expr(
                udf.call(vec![col(column_name)]),
                &DFSchema::try_from(schema.clone())?,
            )
        }

        thread_local! { static KERNEL_ROWS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) }; }

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct SelectionOnlyKernelProbe {
            ansi: bool,
            wrong_rows: bool,
        }

        impl ScalarUDFImpl for SelectionOnlyKernelProbe {
            fn name(&self) -> &str {
                "nullif_probe"
            }
            fn signature(&self) -> &Signature {
                static SIGNATURE: std::sync::LazyLock<Signature> =
                    std::sync::LazyLock::new(|| Signature::any(2, Volatility::Volatile));
                &SIGNATURE
            }
            fn return_type(&self, types: &[DataType]) -> Result<DataType> {
                Ok(types[0].clone())
            }
            fn short_circuits(&self) -> bool {
                true
            }
            fn short_circuits_on_null_first_arg(&self) -> bool {
                true
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                KERNEL_ROWS.with(|rows| rows.borrow_mut().push(args.number_rows));
                if self.wrong_rows {
                    return Ok(ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                        1.0,
                    ]))));
                }
                SparkNullIf::new(self.ansi, "UTC".into()).invoke_with_args(args)
            }
        }

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct NullFirstArgSafeKernelProbe(SelectionOnlyKernelProbe);

        impl ScalarUDFImpl for NullFirstArgSafeKernelProbe {
            fn name(&self) -> &str {
                self.0.name()
            }
            fn signature(&self) -> &Signature {
                self.0.signature()
            }
            fn return_type(&self, types: &[DataType]) -> Result<DataType> {
                self.0.return_type(types)
            }
            fn short_circuits(&self) -> bool {
                true
            }
            fn short_circuits_on_null_first_arg(&self) -> bool {
                true
            }
            fn kernel_accepts_null_first_arg(&self, types: &[DataType]) -> bool {
                matches!(types, [DataType::Float64, DataType::Float64])
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                self.0.invoke_with_args(args)
            }
        }

        fn probe(
            ansi: bool,
            safe: bool,
            args: Vec<Arc<dyn PhysicalExpr>>,
            wrong_rows: bool,
        ) -> ScalarFunctionExpr {
            let kernel = SelectionOnlyKernelProbe { ansi, wrong_rows };
            let udf = if safe {
                ScalarUDF::from(NullFirstArgSafeKernelProbe(kernel))
            } else {
                ScalarUDF::from(kernel)
            };
            ScalarFunctionExpr::new(
                "nullif_probe",
                Arc::new(udf),
                args,
                Arc::new(Field::new("r", DataType::Float64, true)),
                Arc::new(ConfigOptions::default()),
            )
        }

        #[test]
        fn routing_and_effects_distinguish_opt_in_default_and_computed_operands() -> TestResult {
            let input = batch(
                Arc::new(Float64Array::from(vec![None, Some(-0.0), Some(2.0), None])),
                Arc::new(Float64Array::from(vec![1.0, 1.0, 3.0, 4.0])),
            )?;
            for ansi in [false, true] {
                for safe in [false, true] {
                    for literal in [false, true] {
                        let right = if literal {
                            Arc::new(Literal::new(ScalarValue::Float64(Some(1.0))))
                                as Arc<dyn PhysicalExpr>
                        } else {
                            column(1)
                        };
                        let expression = probe(ansi, safe, vec![column(0), right], false);
                        KERNEL_ROWS.with(|rows| rows.borrow_mut().clear());
                        assert_eq!(
                            observed(expression.evaluate(&input), 4)?,
                            observed(original_selection(&expression, &input), 4)?
                        );
                        KERNEL_ROWS
                            .with(|rows| assert_eq!(*rows.borrow(), [if safe { 4 } else { 2 }, 2]));
                    }
                }
                for computed_right in [false, true] {
                    let left_count = Arc::new(AtomicUsize::new(0));
                    let right_count = Arc::new(AtomicUsize::new(0));
                    let left = counted(
                        "count_left",
                        "a",
                        left_count.clone(),
                        input.schema().as_ref(),
                    )?;
                    let right = if computed_right {
                        counted(
                            "count_right",
                            "b",
                            right_count.clone(),
                            input.schema().as_ref(),
                        )?
                    } else {
                        column(1)
                    };
                    let expression = probe(ansi, true, vec![left, right], false);
                    assert_eq!(
                        [
                            left_count.load(Ordering::SeqCst),
                            right_count.load(Ordering::SeqCst)
                        ],
                        [0, 0]
                    );
                    KERNEL_ROWS.with(|rows| rows.borrow_mut().clear());
                    let actual = observed(expression.evaluate(&input), 4)?;
                    assert_eq!(left_count.load(Ordering::SeqCst), 4);
                    assert_eq!(
                        right_count.load(Ordering::SeqCst),
                        if computed_right { 2 } else { 0 }
                    );
                    KERNEL_ROWS.with(|rows| {
                        assert_eq!(*rows.borrow(), [if computed_right { 2 } else { 4 }])
                    });
                    left_count.store(0, Ordering::SeqCst);
                    right_count.store(0, Ordering::SeqCst);
                    assert_eq!(
                        actual,
                        observed(original_selection(&expression, &input), 4)?
                    );
                    assert_eq!(left_count.load(Ordering::SeqCst), 4);
                    assert_eq!(
                        right_count.load(Ordering::SeqCst),
                        if computed_right { 2 } else { 0 }
                    );
                }
                let broken = probe(ansi, true, vec![column(0), column(1)], true);
                let error = broken
                    .evaluate(&input)
                    .err()
                    .ok_or("bad kernel result accepted")?
                    .to_string();
                assert!(error.contains("Expected: 4, Got: 1"), "{error}");
            }
            Ok(())
        }

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct WrongLengthFirstExpr;
        impl fmt::Display for WrongLengthFirstExpr {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "wrong_length_first")
            }
        }
        impl PhysicalExpr for WrongLengthFirstExpr {
            fn data_type(&self, _: &Schema) -> Result<DataType> {
                Ok(DataType::Float64)
            }
            fn nullable(&self, _: &Schema) -> Result<bool> {
                Ok(true)
            }
            fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
                Ok(ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    None,
                    Some(1.0),
                    Some(2.0),
                ]))))
            }
            fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
                vec![]
            }
            fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
            fn with_new_children(
                self: Arc<Self>,
                _: Vec<Arc<dyn PhysicalExpr>>,
            ) -> Result<Arc<dyn PhysicalExpr>> {
                Ok(self)
            }
        }

        #[test]
        fn malformed_left_and_unavailable_right_keep_old_outcomes() -> TestResult {
            let input = batch(
                Arc::new(Float64Array::from(vec![None, Some(1.0), Some(2.0), None])),
                Arc::new(Float64Array::from(vec![1.0; 4])),
            )?;
            for ansi in [false, true] {
                for (args, invocation_rows, succeeds) in [
                    (
                        vec![
                            Arc::new(WrongLengthFirstExpr) as Arc<dyn PhysicalExpr>,
                            column(1),
                        ],
                        vec![2, 2],
                        true,
                    ),
                    (vec![column(0), column(5)], vec![], false),
                ] {
                    let expression = probe(ansi, true, args, false);
                    KERNEL_ROWS.with(|rows| rows.borrow_mut().clear());
                    let old = original_selection(&expression, &input);
                    let actual = expression.evaluate(&input);
                    assert_eq!(old.is_ok(), succeeds);
                    assert_eq!(actual.is_ok(), succeeds);
                    assert_eq!(observed(old, 4)?, observed(actual, 4)?);
                    KERNEL_ROWS.with(|rows| assert_eq!(*rows.borrow(), invocation_rows));
                }
            }
            Ok(())
        }
    }
}
