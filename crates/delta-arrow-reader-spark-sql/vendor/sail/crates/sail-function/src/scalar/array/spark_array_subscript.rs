use arrow::array::{Array, ArrayRef, ListLikeArray, UInt64Builder};
use arrow::compute::take;
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_fixed_size_list_array, as_int64_array, as_large_list_array, as_large_list_view_array,
    as_list_array, as_list_view_array,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_expr::simplify::SparkArrayIndexError;
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraySubscript {
    ansi_mode: bool,
    signature: Signature,
}

impl SparkArraySubscript {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            ansi_mode,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn subscript(&self, array: &dyn Array, indexes: &dyn Array) -> Result<ArrayRef> {
        let array: &dyn ListLikeArray = match array.data_type() {
            DataType::List(_) => as_list_array(array)?,
            DataType::LargeList(_) => as_large_list_array(array)?,
            DataType::FixedSizeList(..) => as_fixed_size_list_array(array)?,
            DataType::ListView(_) => as_list_view_array(array)?,
            DataType::LargeListView(_) => as_large_list_view_array(array)?,
            other => return exec_err!("array subscript requires an array, got {other}"),
        };
        let indexes = as_int64_array(indexes)?;
        let mut offsets = UInt64Builder::with_capacity(indexes.len());
        let broadcast = array.len() == 1;
        for (row, index) in indexes.iter().enumerate() {
            let row = if broadcast { 0 } else { row };
            let Some(index) = index.filter(|_| array.is_valid(row)) else {
                offsets.append_null();
                continue;
            };
            // Spark narrows BIGINT ordinals to INT before checking bounds in both modes.
            let index = index as i32;
            let range = array.element_range(row);
            match usize::try_from(index).ok().filter(|i| *i < range.len()) {
                Some(index) => offsets.append_value((range.start + index) as u64),
                None if self.ansi_mode => {
                    return Err(DataFusionError::External(Box::new(
                        SparkArrayIndexError::new(format!(
                            "[INVALID_ARRAY_INDEX] The index {index} is out of bounds. The array has {} elements.",
                            range.len()
                        )),
                    )));
                }
                None => offsets.append_null(),
            }
        }
        Ok(take(array.values().as_ref(), &offsets.finish(), None)?)
    }
}

impl ScalarUDFImpl for SparkArraySubscript {
    fn name(&self) -> &str {
        "spark_array_subscript"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _)
                | DataType::ListView(field)
                | DataType::LargeListView(field),
                DataType::Int64,
            ] => Ok(field.data_type().clone()),
            _ => plan_err!("array subscript requires an array and an integer index"),
        }
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        args.split_first()
            .map(|(first, rest)| (vec![first], rest.iter().collect()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(array), ColumnarValue::Array(indexes)] = args.args.as_slice()
        {
            // Share the constant array's child values across all index rows.
            return self
                .subscript(array.to_array()?.as_ref(), indexes.as_ref())
                .map(ColumnarValue::Array);
        }
        make_scalar_function(|args| {
            let [array, indexes] = take_function_args(self.name(), args)?;
            self.subscript(array.as_ref(), indexes.as_ref())
        })(&args.args)
    }
}
