//! Reuses the existing NANVL result when the left DOUBLE buffer already holds it.

use arrow::array::{Array, AsArray, Float64Array};
use arrow::datatypes::{DataType, Float64Type};
use datafusion::functions::math::nanvl::NanvlFunc;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature};

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct SparkNanvl {
    native: NanvlFunc,
}

impl ScalarUDFImpl for SparkNanvl {
    fn name(&self) -> &str {
        self.native.name()
    }

    fn signature(&self) -> &Signature {
        self.native.signature()
    }

    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType> {
        self.native.return_type(argument_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.native.documentation()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Array(left), right] = args.args.as_slice()
            && let Some(values) = left.as_primitive_opt::<Float64Type>()
            && right.data_type() == DataType::Float64
            && !matches!(right, ColumnarValue::Array(array) if array.len() != left.len())
            && left_buffer_has_result(values)
        {
            return Ok(ColumnarValue::Array(left.clone()));
        }
        self.native.invoke_with_args(args)
    }
}

fn left_buffer_has_result(left: &Float64Array) -> bool {
    let values = left.values();
    match left.nulls() {
        Some(nulls) if nulls.null_count() != 0 => {
            values.iter().zip(nulls.iter()).all(|(value, valid)| {
                if valid {
                    !value.is_nan()
                } else {
                    // The original output builder writes positive zero under NULL.
                    value.to_bits() == 0
                }
            })
        }
        _ => values.iter().all(|value| !value.is_nan()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use datafusion_common::{ScalarValue, config::ConfigOptions};

    use super::*;

    #[test]
    fn nanvl_preserves_values_bits_and_errors() -> Result<()> {
        let plain = Float64Array::from(vec![
            Some(3.0),
            None,
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        let payload_nan = f64::from_bits(0x7ff8_0000_0000_4321);
        let left_inputs = [
            plain.clone(),
            plain.slice(1, 4),
            plain.slice(0, 0),
            Float64Array::from(vec![Some(payload_nan), None, Some(-0.0)]),
            Float64Array::new(
                vec![9.0, -0.0].into(),
                Some(NullBuffer::from(vec![false, true])),
            ),
        ];
        let native = NanvlFunc::new();
        let candidate = SparkNanvl::default();
        for left in left_inputs {
            let rows = left.len();
            let right = Float64Array::from_iter(
                (0..rows).map(|row| if row % 2 == 0 { Some(-0.0) } else { None }),
            );
            for right in [
                ColumnarValue::Array(Arc::new(right)),
                ColumnarValue::Scalar(ScalarValue::Float64(None)),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(payload_nan))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![1.0; rows + 1]))),
            ] {
                let args = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::new(left.clone())), right],
                    arg_fields: vec![
                        Arc::new(Field::new("left", DataType::Float64, true)),
                        Arc::new(Field::new("right", DataType::Float64, true)),
                    ],
                    number_rows: rows,
                    return_field: Arc::new(Field::new("nanvl", DataType::Float64, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                let actual = candidate.invoke_with_args(args.clone());
                let expected = native.invoke_with_args(args);
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        let actual = actual.into_array(rows)?;
                        let expected = expected.into_array(rows)?;
                        assert_eq!(actual.data_type(), expected.data_type());
                        let actual = actual.as_primitive::<Float64Type>();
                        let expected = expected.as_primitive::<Float64Type>();
                        assert_eq!(actual.len(), expected.len());
                        assert_eq!(actual.null_count(), expected.null_count());
                        for row in 0..rows {
                            assert_eq!(actual.is_null(row), expected.is_null(row));
                            assert_eq!(actual.value(row).to_bits(), expected.value(row).to_bits());
                        }
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string());
                    }
                    (actual, expected) => panic!("actual {actual:?}, expected {expected:?}"),
                }
            }
        }
        Ok(())
    }
}
