//! Normalize decimal bytes, STRUCT validity and nested dictionaries for Spark XXHASH64.
//! The upstream implementation still supplies the hash kernel.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryBuilder, Decimal128Array, make_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::hash::xxhash64::SparkXxhash64;

use super::utils::minimal_signed_decimal_bytes;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct Xxhash64WithSparkInputEncoding {
    upstream: SparkXxhash64,
}

impl ScalarUDFImpl for Xxhash64WithSparkInputEncoding {
    fn name(&self) -> &str {
        self.upstream.name()
    }

    fn signature(&self) -> &Signature {
        self.upstream.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.upstream.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.upstream.return_field_from_args(args)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        for (index, argument) in args.args.iter_mut().enumerate() {
            if !requires_spark_hash_input_encoding(&argument.data_type()) {
                continue;
            }
            *argument = match argument {
                ColumnarValue::Array(array) => {
                    ColumnarValue::Array(encode_spark_hash_input(array)?)
                }
                ColumnarValue::Scalar(value) => {
                    let array = encode_spark_hash_input(&value.to_array()?)?;
                    ColumnarValue::Scalar(ScalarValue::try_from_array(array.as_ref(), 0)?)
                }
            };
            if let Some(field) = args.arg_fields.get_mut(index) {
                *field = Arc::new(field.as_ref().clone().with_data_type(argument.data_type()));
            }
        }
        self.upstream.invoke_with_args(args)
    }
}

fn requires_spark_hash_input_encoding(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(precision, _) => *precision > 18,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            matches!(field.data_type(), DataType::Dictionary(_, _))
                || requires_spark_hash_input_encoding(field.data_type())
        }
        DataType::Dictionary(_, value) => {
            matches!(value.as_ref(), DataType::Dictionary(_, _))
                || requires_spark_hash_input_encoding(value)
        }
        DataType::Struct(_) => true,
        _ => false,
    }
}

fn encode_spark_hash_input(array: &ArrayRef) -> Result<ArrayRef> {
    if let DataType::Decimal128(precision, _) = array.data_type()
        && *precision > 18
    {
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let mut output = BinaryBuilder::with_capacity(decimals.len(), decimals.len() * 16);
        for value in decimals {
            if let Some(value) = value {
                output.append_value(minimal_signed_decimal_bytes(&value.to_be_bytes()));
            } else {
                output.append_null();
            }
        }
        return Ok(Arc::new(output.finish()));
    }
    if !matches!(
        array.data_type(),
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Dictionary(_, _)
    ) {
        return Ok(Arc::clone(array));
    }
    let data = array.to_data();
    let is_struct = matches!(array.data_type(), DataType::Struct(_));
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            // Nested dictionaries can inherit a seed; the upstream cache starts at 42.
            let child = if let DataType::Dictionary(_, value_type) = child.data_type() {
                let mut value_type = value_type.as_ref();
                while let DataType::Dictionary(_, nested_type) = value_type {
                    value_type = nested_type;
                }
                cast(&make_array(child.clone()), value_type)?.to_data()
            } else {
                child.clone()
            };
            // Upstream hashes STRUCT fields directly, so NULL parents must mask their fields.
            let child = if is_struct && child.data_type() != &DataType::Null {
                child
                    .clone()
                    .into_builder()
                    .nulls(NullBuffer::union(child.nulls(), data.nulls()))
                    .build()?
            } else {
                child.clone()
            };
            Ok(encode_spark_hash_input(&make_array(child))?.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let data_type = match array.data_type() {
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(children[0].data_type().clone()),
        )),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(children[0].data_type().clone()),
        )),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(children[0].data_type().clone()),
            ),
            *size,
        ),
        DataType::Dictionary(key, _) => {
            DataType::Dictionary(key.clone(), Box::new(children[0].data_type().clone()))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .zip(&children)
                .map(|(field, child)| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(child.data_type().clone()),
                    )
                })
                .collect(),
        ),
        _ => return Ok(Arc::clone(array)),
    };
    Ok(make_array(
        data.into_builder()
            .data_type(data_type)
            .child_data(children)
            .build()?,
    ))
}
