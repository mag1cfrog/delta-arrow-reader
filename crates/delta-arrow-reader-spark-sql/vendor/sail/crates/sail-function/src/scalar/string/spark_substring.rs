//! Spark string substring uses the native implementation; binary ranges retain Java INT wrapping.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, GenericBinaryBuilder, Int64Array, OffsetSizeTrait};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_int64_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::string::substring::SparkSubstring as NativeSparkSubstring;

use crate::functions_utils::make_scalar_function;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkSubstring {
    inner: NativeSparkSubstring,
}

impl SparkSubstring {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ScalarUDFImpl for SparkSubstring {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() == 2 {
            args.args
                .push(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    i32::MAX.into(),
                ))));
            args.arg_fields
                .push(Arc::new(Field::new("length", DataType::Int64, false)));
        }
        if !args.args.first().is_some_and(|value| {
            matches!(
                value.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
        }) {
            return self.inner.invoke_with_args(args);
        }
        make_scalar_function(
            |arrays| {
                let [values, positions, lengths] = take_function_args("substring", arrays)?;
                let positions = as_int64_array(positions)?;
                let lengths = as_int64_array(lengths)?;
                match values.data_type() {
                    DataType::Binary => binary_substring::<i32>(
                        values.as_binary::<i32>().iter(),
                        positions,
                        lengths,
                    ),
                    DataType::LargeBinary => binary_substring::<i64>(
                        values.as_binary::<i64>().iter(),
                        positions,
                        lengths,
                    ),
                    DataType::BinaryView => {
                        let output = binary_substring::<i64>(
                            values.as_binary_view().iter(),
                            positions,
                            lengths,
                        )?;
                        Ok(cast(output.as_ref(), &DataType::BinaryView)?)
                    }
                    other => exec_err!("substring does not accept binary type {other}"),
                }
            },
            vec![],
        )(&args.args)
    }
}

fn binary_substring<'a, O: OffsetSizeTrait>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    positions: &Int64Array,
    lengths: &Int64Array,
) -> Result<ArrayRef> {
    let mut builder = GenericBinaryBuilder::<O>::with_capacity(positions.len(), 0);
    let mut padded = Vec::new();
    for ((value, position), length) in values.zip(positions.iter()).zip(lengths.iter()) {
        let (Some(value), Some(position), Some(length)) = (value, position, length) else {
            builder.append_null();
            continue;
        };
        let size = i32::try_from(value.len())
            .map_err(|_| exec_datafusion_err!("substring binary value exceeds INT length"))?;
        let position = i32::try_from(position)
            .map_err(|_| exec_datafusion_err!("substring position exceeds INT range"))?;
        let length = i32::try_from(length)
            .map_err(|_| exec_datafusion_err!("substring length exceeds INT range"))?;
        if position > size {
            builder.append_value([]);
            continue;
        }

        // Apache Spark ByteArray.subStringSQL uses Java INT arithmetic before clamping.
        // Range arithmetic wraps even for negative lengths.
        let start = if position > 0 {
            position - 1
        } else if position < 0 {
            size.wrapping_add(position)
        } else {
            0
        };
        let end = if size.wrapping_sub(start) < length {
            size
        } else {
            start.wrapping_add(length)
        };
        let start = start.max(0);
        if start >= end {
            builder.append_value([]);
            continue;
        }
        let (start, end) = (start as usize, end as usize);
        let output_len = end - start;
        if builder
            .values_slice()
            .len()
            .checked_add(output_len)
            .and_then(O::from_usize)
            .is_none()
        {
            return exec_err!("substring binary output exceeds Arrow offset capacity");
        }
        if end <= value.len() {
            builder.append_value(&value[start..end]);
        } else {
            // Java Arrays.copyOfRange pads the requested range beyond the input with zeros.
            padded.clear();
            padded.extend_from_slice(&value[start..]);
            padded.resize(output_len, 0);
            builder.append_value(&padded);
        }
    }
    Ok(Arc::new(builder.finish()))
}
