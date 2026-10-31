// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Integer HALF_UP arithmetic adapted from datafusion-spark 54.1.0's
// function/math/round.rs. Capture query ANSI and scale during planning, and
// retain the 10^19 boundary that does not fit its signed i64 factor.
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Int8Type, Int16Type, Int32Type, Int64Type};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntegerRound {
    signature: Signature,
    scale: Option<i32>,
    factor: Option<i64>,
    ansi: bool,
}

impl SparkIntegerRound {
    pub fn is_null_scale(&self) -> bool {
        self.scale.is_none()
    }

    pub fn new(scale: Option<i32>, ansi: bool) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
            scale,
            factor: scale
                .filter(|s| *s < 0)
                .and_then(|s| 10_i64.checked_pow(s.unsigned_abs())),
            ansi,
        }
    }

    fn overflow(&self, value: i64) -> DataFusionError {
        exec_datafusion_err!(
            "[ARITHMETIC_OVERFLOW] integer overflow on round({value}, {})",
            self.scale.unwrap_or(0)
        )
    }

    fn round_integer(&self, value: i64) -> Result<i64> {
        let Some(factor) = self.factor else {
            if self.scale == Some(-19) && value.unsigned_abs() >= 5_000_000_000_000_000_000 {
                if self.ansi {
                    return Err(self.overflow(value));
                }
                let wrapped = 10_000_000_000_000_000_000_u64 as i64;
                return Ok(if value < 0 {
                    wrapped.wrapping_neg()
                } else {
                    wrapped
                });
            }
            // ponytail: beyond 19 integral digits the mathematical result is zero;
            // Spark's BigDecimal capacity/scale errors need separate reference review.
            return Ok(0);
        };
        let remainder = value % factor;
        let delta = if remainder >= factor / 2 {
            factor - remainder
        } else if remainder <= -(factor / 2) {
            -factor - remainder
        } else {
            -remainder
        };
        if self.ansi {
            value.checked_add(delta).ok_or_else(|| self.overflow(value))
        } else {
            Ok(value.wrapping_add(delta))
        }
    }
}

impl ScalarUDFImpl for SparkIntegerRound {
    fn name(&self) -> &str {
        "spark_integer_round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [data_type] if data_type.is_signed_integer() => Ok(data_type.clone()),
            _ => internal_err!("integer ROUND requires one signed-integer input"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("integer ROUND requires one input");
        };
        let Some(scale) = self.scale else {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                value.data_type(),
            )?));
        };
        if scale >= 0 {
            return Ok(value.clone());
        }
        macro_rules! apply {
            ($arrow:ty, $native:ty, $scalar:ident) => {{
                let checked = |v: $native| -> Result<$native> {
                    let rounded = self.round_integer(i64::from(v))?;
                    if self.ansi {
                        <$native>::try_from(rounded).map_err(|_| self.overflow(i64::from(v)))
                    } else {
                        Ok(rounded as $native)
                    }
                };
                match value {
                    ColumnarValue::Scalar(ScalarValue::$scalar(v)) => Ok(ColumnarValue::Scalar(
                        ScalarValue::$scalar(v.map(checked).transpose()?),
                    )),
                    ColumnarValue::Array(array) => {
                        let array = array.as_primitive::<$arrow>();
                        let result: PrimitiveArray<$arrow> = if self.ansi {
                            array.try_unary(checked)?
                        } else {
                            array.unary(|v| self.round_integer(i64::from(v)).unwrap() as $native)
                        };
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    _ => internal_err!("unexpected integer ROUND scalar"),
                }
            }};
        }
        match value.data_type() {
            DataType::Int8 => apply!(Int8Type, i8, Int8),
            DataType::Int16 => apply!(Int16Type, i16, Int16),
            DataType::Int32 => apply!(Int32Type, i32, Int32),
            DataType::Int64 => apply!(Int64Type, i64, Int64),
            _ => internal_err!("integer ROUND requires a signed-integer input"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, ArrayRef, Int8Array, Int64Array};
    use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    fn scalar(value: ColumnarValue) -> ScalarValue {
        let ColumnarValue::Scalar(value) = value else {
            panic!("expected a scalar")
        };
        value
    }

    fn invoke(
        udf: &SparkIntegerRound,
        input: ColumnarValue,
        host_ansi: bool,
    ) -> Result<ColumnarValue> {
        let field = Arc::new(Field::new("input", input.data_type(), true));
        let mut config = ConfigOptions::default();
        config.execution.enable_ansi_mode = host_ansi;
        udf.invoke_with_args(ScalarFunctionArgs {
            number_rows: match &input {
                ColumnarValue::Array(a) => a.len(),
                _ => 1,
            },
            args: vec![input],
            arg_fields: vec![field.clone()],
            return_field: field,
            config_options: Arc::new(config),
        })
    }

    #[test]
    fn integer_round_preserves_values_validity_and_query_ansi() -> Result<()> {
        for ansi in [false, true] {
            for (value, scale, expected) in [
                (25, -1, 30),
                (-25, -1, -30),
                (24, -1, 20),
                (-24, -1, -20),
                (9_007_199_254_740_993, 0, 9_007_199_254_740_993),
                (i64::MAX, -18, 9_000_000_000_000_000_000),
                (i64::MIN, -18, -9_000_000_000_000_000_000),
                (4_999_999_999_999_999_999, -19, 0),
                (-4_999_999_999_999_999_999, -19, 0),
                (i64::MAX, -20, 0),
                (i64::MIN, -38, 0),
            ] {
                let udf = SparkIntegerRound::new(Some(scale), ansi);
                let actual = invoke(
                    &udf,
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(value))),
                    !ansi,
                )?;
                assert_eq!(scalar(actual), ScalarValue::Int64(Some(expected)));
                let input = Int64Array::from(vec![Some(i64::MAX), Some(value), None, Some(value)])
                    .slice(1, 3);
                let ColumnarValue::Array(output) =
                    invoke(&udf, ColumnarValue::Array(Arc::new(input)), !ansi)?
                else {
                    panic!("expected an array")
                };
                assert_eq!(
                    output.as_primitive::<Int64Type>(),
                    &Int64Array::from(vec![Some(expected), None, Some(expected)])
                );
            }
            for (value, scale, wrapped) in [
                (i64::MAX, -1, i64::MIN + 2),
                (i64::MIN, -1, i64::MAX - 1),
                (5_000_000_000_000_000_000, -19, -8_446_744_073_709_551_616),
                (-5_000_000_000_000_000_000, -19, 8_446_744_073_709_551_616),
            ] {
                let udf = SparkIntegerRound::new(Some(scale), ansi);
                let actual = invoke(
                    &udf,
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(value))),
                    !ansi,
                );
                if ansi {
                    assert!(
                        actual
                            .unwrap_err()
                            .to_string()
                            .contains("[ARITHMETIC_OVERFLOW]")
                    );
                } else {
                    assert_eq!(scalar(actual?), ScalarValue::Int64(Some(wrapped)));
                }
            }
            let udf = SparkIntegerRound::new(Some(-1), ansi);
            let narrow = invoke(
                &udf,
                ColumnarValue::Scalar(ScalarValue::Int8(Some(127))),
                !ansi,
            );
            if ansi {
                assert!(
                    narrow
                        .unwrap_err()
                        .to_string()
                        .contains("[ARITHMETIC_OVERFLOW]")
                );
            } else {
                assert_eq!(scalar(narrow?), ScalarValue::Int8(Some(-126)));
            }
            // A hidden overflowing value must not fail a NULL row, including slices.
            let input = Int8Array::new(
                ScalarBuffer::from(vec![127, 127, 25]),
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .slice(1, 2);
            let ColumnarValue::Array(output) =
                invoke(&udf, ColumnarValue::Array(Arc::new(input)), !ansi)?
            else {
                panic!("array")
            };
            assert_eq!(
                output.as_primitive::<Int8Type>(),
                &Int8Array::from(vec![None, Some(30)])
            );
            for input in [
                Int8Array::from(Vec::<Option<i8>>::new()),
                Int8Array::from(vec![None, None]),
            ] {
                let ColumnarValue::Array(output) =
                    invoke(&udf, ColumnarValue::Array(Arc::new(input.clone())), !ansi)?
                else {
                    panic!("array")
                };
                assert_eq!(output.as_primitive::<Int8Type>(), &input);
            }
            let input: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(9_007_199_254_740_993), None]));
            let ColumnarValue::Array(output) = invoke(
                &SparkIntegerRound::new(Some(2), ansi),
                ColumnarValue::Array(input.clone()),
                !ansi,
            )?
            else {
                panic!("array")
            };
            assert!(Arc::ptr_eq(&input, &output));
            assert_eq!(
                scalar(invoke(
                    &SparkIntegerRound::new(None, ansi),
                    ColumnarValue::Array(input),
                    !ansi
                )?),
                ScalarValue::Int64(None)
            );
        }
        Ok(())
    }
}
