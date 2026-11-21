// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::types::Time64MicrosecondType;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, Int64Builder, PrimitiveArray, StringArrayType,
    new_null_array,
};
use datafusion::arrow::compute::binary;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::{invalid_time_unit_err, to_time64_array};

/// Returns the divisor in microseconds for a given time_diff unit string.
/// Returns `None` for unsupported units.
fn unit_divisor(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "MICROSECOND" => Some(1),
        "MILLISECOND" => Some(1_000),
        "SECOND" => Some(1_000_000),
        "MINUTE" => Some(60_000_000),
        "HOUR" => Some(3_600_000_000),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeDiff {
    signature: Signature,
}

impl Default for SparkTimeDiff {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeDiff {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimeDiff {
    fn name(&self) -> &str {
        "spark_time_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        let [unit_arg, start_arg, end_arg] = args.as_slice() else {
            return exec_err!(
                "Spark `time_diff` function requires 3 arguments, got {}",
                args.len()
            );
        };

        // All-scalar fast path — return a scalar value directly.
        if let (
            ColumnarValue::Scalar(unit_sv),
            ColumnarValue::Scalar(start_sv),
            ColumnarValue::Scalar(end_sv),
        ) = (unit_arg, start_arg, end_arg)
        {
            let unit_opt = match unit_sv {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => None,
                _ => return exec_err!("time_diff: unit must be a string"),
            };
            let start_opt = match start_sv {
                ScalarValue::Time64Microsecond(v) => *v,
                _ => return exec_err!("time_diff: start must be TIME"),
            };
            let end_opt = match end_sv {
                ScalarValue::Time64Microsecond(v) => *v,
                _ => return exec_err!("time_diff: end must be TIME"),
            };
            let result = match (unit_opt, start_opt, end_opt) {
                (Some(unit), Some(start), Some(end)) => {
                    let divisor = match unit_divisor(unit) {
                        Some(d) => d,
                        None => {
                            return invalid_time_unit_err("time_diff", unit);
                        }
                    };
                    Some((end - start) / divisor)
                }
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)));
        }

        // At least one array arg.
        // Null scalar time args → all-null result without touching the converter.
        if matches!(
            start_arg,
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None))
        ) || matches!(
            end_arg,
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None))
        ) {
            return Ok(ColumnarValue::Array(new_null_array(
                &DataType::Int64,
                number_rows,
            )));
        }

        let starts = to_time64_array(start_arg, "start", "time_diff", number_rows)?;
        if let (
            ColumnarValue::Array(_),
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(unit)) | ScalarValue::LargeUtf8(Some(unit)),
            ),
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(end))),
        ) = (start_arg, unit_arg, end_arg)
            && starts.len() == number_rows
        {
            let Some(divisor) = unit_divisor(unit) else {
                return invalid_time_unit_err("time_diff", unit);
            };
            let result: Int64Array = starts.unary(|start| (*end - start) / divisor);
            return Ok(ColumnarValue::Array(Arc::new(result)));
        }
        let ends = to_time64_array(end_arg, "end", "time_diff", number_rows)?;

        let result: ArrayRef = match unit_arg {
            // Scalar unit — resolve divisor once and apply to all rows.
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(unit)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(unit))) => {
                let divisor = match unit_divisor(unit.as_str()) {
                    Some(d) => d,
                    None => {
                        return invalid_time_unit_err("time_diff", unit);
                    }
                };
                // Rust integer division truncates toward zero, matching Spark behavior.
                // `binary` propagates nulls as the intersection of both inputs' null buffers.
                let result: Int64Array = binary(&ends, &starts, |e, s| (e - s) / divisor)?;
                Arc::new(result)
            }
            // Null scalar unit → all-null result.
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
            | ColumnarValue::Scalar(ScalarValue::Null) => {
                new_null_array(&DataType::Int64, number_rows)
            }
            // Array unit — resolve divisor per row.
            ColumnarValue::Array(unit_array) => match unit_array.data_type() {
                DataType::Utf8 => {
                    time_diff_rows(unit_array.as_string::<i32>(), &starts, &ends, number_rows)?
                }
                DataType::LargeUtf8 => {
                    time_diff_rows(unit_array.as_string::<i64>(), &starts, &ends, number_rows)?
                }
                DataType::Utf8View => {
                    time_diff_rows(unit_array.as_string_view(), &starts, &ends, number_rows)?
                }
                _ => return exec_err!("time_diff: unit must be a string"),
            },
            _ => return exec_err!("time_diff: unit must be a string"),
        };

        Ok(ColumnarValue::Array(result))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return exec_err!(
                "Spark `time_diff` function requires 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Microsecond),
        ])
    }
}

/// Per-row difference computation when unit comes from a string array column.
fn time_diff_rows<'a, S>(
    unit_array: &'a S,
    starts: &PrimitiveArray<Time64MicrosecondType>,
    ends: &PrimitiveArray<Time64MicrosecondType>,
    number_rows: usize,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let mut builder = Int64Builder::with_capacity(number_rows);
    for ((unit_opt, start_opt), end_opt) in unit_array.iter().zip(starts.iter()).zip(ends.iter()) {
        match (unit_opt, start_opt, end_opt) {
            (None, _, _) | (_, None, _) | (_, _, None) => builder.append_null(),
            (Some(unit), Some(start), Some(end)) => match unit_divisor(unit) {
                // Rust integer division truncates toward zero, matching Spark behavior.
                Some(divisor) => builder.append_value((end - start) / divisor),
                None => {
                    return invalid_time_unit_err("time_diff", unit);
                }
            },
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Time64MicrosecondArray;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn literal_time_diff_end_matches_broadcast_end() -> Result<()> {
        let starts = Time64MicrosecondArray::from(vec![
            Some(0),
            None,
            Some(3_600_000_001),
            Some(86_399_999_999),
        ]);
        let function = SparkTimeDiff::new();
        for (length, number_rows) in [(0, 0), (4, 4), (4, 5)] {
            for unit in [
                None,
                Some("microsecond"),
                Some("MILLISECOND"),
                Some("SECOND"),
                Some("MINUTE"),
                Some("HOUR"),
                Some("DAY"),
            ] {
                for end in [0, 3_600_000_000, 86_399_999_999] {
                    let invoke = |end: ColumnarValue| {
                        let args = vec![
                            ColumnarValue::Scalar(ScalarValue::Utf8(unit.map(str::to_owned))),
                            ColumnarValue::Array(Arc::new(starts.slice(0, length))),
                            end,
                        ];
                        function.invoke_with_args(ScalarFunctionArgs {
                            arg_fields: args
                                .iter()
                                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                                .collect(),
                            args,
                            number_rows,
                            return_field: Arc::new(Field::new("difference", DataType::Int64, true)),
                            config_options: Arc::new(Default::default()),
                        })
                    };
                    let scalar = ScalarValue::Time64Microsecond(Some(end));
                    let expected =
                        invoke(ColumnarValue::Array(scalar.to_array_of_size(number_rows)?));
                    let actual = invoke(ColumnarValue::Scalar(scalar));
                    match (actual, expected) {
                        (Ok(ColumnarValue::Array(actual)), Ok(ColumnarValue::Array(expected))) => {
                            assert_eq!(actual.to_data(), expected.to_data());
                        }
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.to_string(), expected.to_string())
                        }
                        outcomes => panic!("different time difference outcomes: {outcomes:?}"),
                    }
                }
            }
        }
        Ok(())
    }
}
