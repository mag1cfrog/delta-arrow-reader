use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Decimal128Type};
use datafusion::functions::math::round::RoundFunc;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};

use super::utils::decimal::round_decimal_base;

/// Spark's Decimal result type with DataFusion's existing ROUND kernel.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalRound {
    inner: RoundFunc,
    data_type: DataType,
    scale_underflow: bool,
}

impl SparkDecimalRound {
    pub fn new(precision: u8, scale: i8, target_scale: i32) -> Self {
        let scale_underflow = i64::from(scale) - i64::from(target_scale) > i64::from(i32::MAX);
        let (precision, scale) =
            round_decimal_base(i32::from(precision), i32::from(scale), target_scale, true);
        Self {
            inner: RoundFunc::new(),
            data_type: DataType::Decimal128(precision, scale),
            scale_underflow,
        }
    }
}

impl ScalarUDFImpl for SparkDecimalRound {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Match the retained Decimal BROUND check. Ordinary scales do not
        // scan the input; only live, nonzero values can underflow BigDecimal's
        // signed INT scale subtraction. A NULL scale skips value evaluation.
        if self.scale_underflow
            && matches!(
                args.args.get(1),
                Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(_))))
            )
        {
            let nonzero = match args.args.first() {
                Some(ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), _, _))) => {
                    *value != 0
                }
                Some(ColumnarValue::Array(array))
                    if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
                {
                    array
                        .as_primitive::<Decimal128Type>()
                        .iter()
                        .flatten()
                        .any(|value| value != 0)
                }
                _ => false,
            };
            if nonzero {
                return exec_err!("Underflow");
            }
        }
        self.inner.invoke_with_args(args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extreme_scale_underflow_respects_live_values_and_nulls() -> Result<()> {
        use datafusion::arrow::array::{Array, Decimal128Array};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::Field;
        use datafusion_common::config::ConfigOptions;
        use std::sync::Arc;
        let invoke = |input: ColumnarValue, target: i32, null_scale: bool, ansi: bool| {
            let DataType::Decimal128(precision, scale) = input.data_type() else {
                panic!("decimal input")
            };
            let udf = SparkDecimalRound::new(precision, scale, target);
            let len = match &input {
                ColumnarValue::Array(a) => a.len(),
                _ => 1,
            };
            let fields = vec![
                Arc::new(Field::new("a", input.data_type(), true)),
                Arc::new(Field::new("scale", DataType::Int32, null_scale)),
            ];
            let mut config = ConfigOptions::default();
            config.execution.enable_ansi_mode = ansi;
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    input,
                    ColumnarValue::Scalar(ScalarValue::Int32((!null_scale).then_some(target))),
                ],
                arg_fields: fields,
                number_rows: len,
                return_field: Arc::new(Field::new("r", udf.data_type.clone(), true)),
                config_options: Arc::new(config),
            })?
            .into_array(len)
        };
        for ansi in [false, true] {
            for scale in [0, 2, 18, 38] {
                let target = scale as i32 - i32::MAX - 1;
                for value in [Some(1), Some(-1)] {
                    let input = ColumnarValue::Scalar(ScalarValue::Decimal128(value, 38, scale));
                    assert!(
                        invoke(input.clone(), target, false, ansi)
                            .unwrap_err()
                            .to_string()
                            .contains("Underflow")
                    );
                    assert_eq!(invoke(input, target, true, ansi)?.null_count(), 1);
                }
                for value in [None, Some(0)] {
                    let result = invoke(
                        ColumnarValue::Scalar(ScalarValue::Decimal128(value, 38, scale)),
                        target,
                        false,
                        ansi,
                    )?;
                    assert_eq!(
                        result
                            .as_primitive::<Decimal128Type>()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![value]
                    );
                }
                let input = Decimal128Array::new(
                    vec![1, 0, i128::MAX, 0, -1].into(),
                    Some(NullBuffer::from(vec![true, true, false, true, true])),
                )
                .with_precision_and_scale(38, scale)?;
                for (offset, len) in [(1, 3), (2, 1), (2, 0)] {
                    let result = invoke(
                        ColumnarValue::Array(Arc::new(input.slice(offset, len))),
                        target,
                        false,
                        ansi,
                    )?;
                    assert_eq!(result.len(), len);
                    assert!(
                        result
                            .as_primitive::<Decimal128Type>()
                            .iter()
                            .flatten()
                            .all(|v| v == 0)
                    );
                    assert_eq!(result.null_count(), usize::from(len != 0));
                }
                assert!(
                    invoke(ColumnarValue::Array(Arc::new(input)), target, false, ansi)
                        .unwrap_err()
                        .to_string()
                        .contains("Underflow")
                );
            }
            for (value, expected) in [(2345, 235), (-2345, -235)] {
                let result = invoke(
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), 6, 3)),
                    2,
                    false,
                    ansi,
                )?;
                assert_eq!(result.data_type(), &DataType::Decimal128(6, 2));
                assert_eq!(result.as_primitive::<Decimal128Type>().value(0), expected);
            }
            let error = invoke(
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 0)),
                -1,
                false,
                ansi,
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("Decimal overflow"), "{error}");
            assert!(!error.contains("Underflow"));
        }
        Ok(())
    }

    #[test]
    fn spark_decimal_result_types_and_ordering() -> Result<()> {
        for (precision, scale, target, expected_precision, expected_scale) in [
            (23, 13, 2, 13, 2),
            (3, 2, 0, 2, 0),
            (3, 2, -1, 2, 0),
            (3, 2, -255, 38, 0),
            (10, 2, -256, 38, 0),
            (10, 2, -1000, 38, 0),
            (10, 2, i32::MAX, 11, 2),
            (38, 0, -1, 38, 0),
            (38, 38, 0, 1, 0),
            (38, 38, i32::MAX, 38, 38),
        ] {
            let round = SparkDecimalRound::new(precision, scale, target);
            assert_eq!(
                round.return_type(&[DataType::Decimal128(precision, scale), DataType::Int32])?,
                DataType::Decimal128(expected_precision, expected_scale),
                "ROUND(decimal({precision},{scale}), {target})",
            );
        }
        let round = SparkDecimalRound::new(10, 2, 0);
        let order = SortProperties::Ordered(Default::default());
        assert_eq!(
            round.output_ordering(&[
                ExprProperties::new_unknown().with_order(order),
                ExprProperties::new_unknown().with_order(SortProperties::Singleton),
            ])?,
            order,
        );
        Ok(())
    }
}
