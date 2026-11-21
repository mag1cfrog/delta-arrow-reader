// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, as_large_list_array, as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions_aggregate::min_max;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    Accumulator, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use datafusion_functions_nested::min_max::{array_max_udf, array_min_udf};

use crate::functions_nested_utils::make_scalar_function;

enum ArrayOp {
    Min,
    Max,
}

fn element_return_type(name: &str, data_type: Option<&DataType>) -> Result<DataType> {
    match data_type {
        Some(DataType::List(field) | DataType::LargeList(field)) => Ok(field.data_type().clone()),
        Some(DataType::Null) => Ok(DataType::Null),
        _ => plan_err!("{name} can only accept List or LargeList."),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMin {
    signature: Signature,
}

impl Default for ArrayMin {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMin {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMin {
    fn name(&self) -> &str {
        "array_min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `ArrayMin.nullable = true`, unconditional (an empty array yields NULL).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L2349>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type =
            element_return_type("ArrayMin", args.arg_fields.first().map(|f| f.data_type()))?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_array_extremum(args, ArrayOp::Min)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMax {
    signature: Signature,
}

impl Default for ArrayMax {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMax {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMax {
    fn name(&self) -> &str {
        "array_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `ArrayMax.nullable = true`, unconditional (an empty array yields NULL).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L2422>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type =
            element_return_type("ArrayMax", args.arg_fields.first().map(|f| f.data_type()))?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_array_extremum(args, ArrayOp::Max)
    }
}

fn invoke_array_extremum(mut args: ScalarFunctionArgs, op: ArrayOp) -> Result<ColumnarValue> {
    if args.args[0].data_type() == DataType::Null {
        return Ok(ColumnarValue::Scalar(ScalarValue::Null));
    }

    if let [ColumnarValue::Array(array)] = args.args.as_slice()
        && !array.is_empty()
        && let Some(values) = bigint_extrema_input(array)
    {
        args.args[0] = ColumnarValue::Array(values);
        return match op {
            ArrayOp::Min => array_min_udf().invoke_with_args(args),
            ArrayOp::Max => array_max_udf().invoke_with_args(args),
        };
    }

    match op {
        ArrayOp::Min => make_scalar_function(array_min_inner)(&args.args),
        ArrayOp::Max => make_scalar_function(array_max_inner)(&args.args),
    }
}

fn bigint_extrema_input(array: &ArrayRef) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::List(field) if field.data_type() == &DataType::Int64 => {
            list_with_unmasked_child_ranges(as_list_array(array))
        }
        DataType::LargeList(field) if field.data_type() == &DataType::Int64 => {
            list_with_unmasked_child_ranges(as_large_list_array(array))
        }
        _ => None,
    }
}

fn list_with_unmasked_child_ranges<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Option<ArrayRef> {
    // The original extrema loop reads every offset range, including those under NULL parents.
    let (field, offsets, values, _) = list.clone().into_parts();
    GenericListArray::try_new(field, offsets, values, None)
        .ok()
        .map(|list| Arc::new(list) as ArrayRef)
}

fn array_min_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_min needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Min)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Min)
        }
        _ => exec_err!(
            "array_min does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn array_max_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_max needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Max)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Max)
        }
        _ => exec_err!(
            "array_max does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn compute_min_max_values<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    data_type: &DataType,
    op: ArrayOp,
) -> Result<Vec<ScalarValue>>
where
    i64: TryInto<O>,
{
    let mut result_values = Vec::new();
    for i in 0..array.len() {
        let inner_array = array.value(i);
        let value = match op {
            ArrayOp::Min => {
                let mut min_accumulator = min_max::MinAccumulator::try_new(data_type)?;
                min_accumulator.update_batch(&[inner_array])?;
                min_accumulator.evaluate()?
            }
            ArrayOp::Max => {
                let mut max_accumulator = min_max::MaxAccumulator::try_new(data_type)?;
                max_accumulator.update_batch(&[inner_array])?;
                max_accumulator.evaluate()?
            }
        };
        result_values.push(value);
    }
    Ok(result_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{AsArray, Float64Array, Int64Array};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{Float64Type, Int64Type};

    fn extrema_args(array: ArrayRef, return_type: DataType) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            arg_fields: vec![Arc::new(Field::new(
                "items",
                array.data_type().clone(),
                true,
            ))],
            number_rows: array.len(),
            args: vec![ColumnarValue::Array(array)],
            return_field: Arc::new(Field::new("extremum", return_type, true)),
            config_options: Arc::default(),
        }
    }

    #[test]
    fn typed_bigint_extrema_match_the_row_accumulators() -> Result<()> {
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let offsets = [0, 2, 4, 4, 6, 8];
        let children: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(5),
            Some(-3),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
            None,
            Some(7),
            None,
        ]));
        let parents = NullBuffer::from(vec![false, true, true, true, true]);
        let lists: [ArrayRef; 2] = [
            Arc::new(GenericListArray::<i32>::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.to_vec().into()),
                children.clone(),
                Some(parents.clone()),
            )?),
            Arc::new(GenericListArray::<i64>::try_new(
                field,
                OffsetBuffer::new(
                    offsets
                        .iter()
                        .map(|&offset| i64::from(offset))
                        .collect::<Vec<_>>()
                        .into(),
                ),
                children,
                Some(parents),
            )?),
        ];
        for list in lists {
            for input in [list.clone(), list.slice(1, 4), list.slice(0, 0)] {
                for (op, evaluate) in [
                    (
                        ArrayOp::Min,
                        array_min_inner as fn(&[ArrayRef]) -> Result<ArrayRef>,
                    ),
                    (ArrayOp::Max, array_max_inner),
                ] {
                    let expected = evaluate(std::slice::from_ref(&input));
                    let actual =
                        invoke_array_extremum(extrema_args(input.clone(), DataType::Int64), op)
                            .and_then(|result| result.into_array(input.len()));
                    let values = |result: Result<ArrayRef>| {
                        result
                            .map(|a| a.as_primitive::<Int64Type>().iter().collect::<Vec<_>>())
                            .map_err(|e| e.to_string())
                    };
                    assert_eq!(values(actual), values(expected), "{:?}", input.data_type());
                }
            }
        }
        Ok(())
    }

    #[test]
    fn floating_array_extrema_keep_nan_and_zero_bits() -> Result<()> {
        let input: ArrayRef = Arc::new(GenericListArray::<i32>::try_new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::new(vec![0_i32, 2, 4, 6].into()),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                Some(-1.0),
                Some(f64::from_bits(0xfff8_0000_0000_0001)),
                None,
            ])),
            None,
        )?);
        for (op, evaluate) in [
            (
                ArrayOp::Min,
                array_min_inner as fn(&[ArrayRef]) -> Result<ArrayRef>,
            ),
            (ArrayOp::Max, array_max_inner),
        ] {
            let expected = evaluate(std::slice::from_ref(&input))?;
            let actual = invoke_array_extremum(extrema_args(input.clone(), DataType::Float64), op)?
                .into_array(input.len())?;
            let bits = |array: &ArrayRef| {
                array
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|value| value.map(f64::to_bits))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(&actual), bits(&expected));
        }
        Ok(())
    }
}
