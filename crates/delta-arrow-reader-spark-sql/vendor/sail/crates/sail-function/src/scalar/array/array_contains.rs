//! Spark array membership keeps a missed lookup unknown when an element is NULL.
//! For example, ARRAY_CONTAINS([1, NULL], 2) is NULL, while a lookup for 1 is true.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, lit, when,
};
use datafusion_functions_nested::array_has::array_has_udf;
use datafusion_spark::function::array::spark_array_contains;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayContains {
    spark_function: Arc<ScalarUDF>,
}

impl Default for ArrayContains {
    fn default() -> Self {
        Self {
            spark_function: spark_array_contains(),
        }
    }
}

impl ScalarUDFImpl for ArrayContains {
    fn name(&self) -> &str {
        self.spark_function.name()
    }

    fn signature(&self) -> &Signature {
        self.spark_function.signature()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.spark_function.inner().return_type(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Boolean(None),
                None,
            )));
        }
        match array_has_udf().simplify(args, info)? {
            // An empty IN-list is always false, but an empty array still gives
            // NULL when its lookup is NULL.
            ExprSimplifyResult::Simplified(Expr::InList(list)) if list.list.is_empty() => Ok(
                ExprSimplifyResult::Simplified(when((*list.expr).is_not_null(), lit(false)).end()?),
            ),
            simplified => Ok(simplified),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // DataFusion's Spark function returns a one-row array for these NULL results,
        // even when the inputs contain a full batch of rows.
        if args.args.iter().any(|arg| {
            arg.data_type().is_null()
                || matches!(arg, ColumnarValue::Scalar(value) if value.is_null())
        }) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        self.spark_function.invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, ListArray, NullArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn null_results_cover_the_input_batch() -> Result<()> {
        for rows in [0, 1, 3] {
            let lists = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                (0..rows).map(|_| Some(vec![Some(1), None])),
            )) as ArrayRef;
            for arguments in [
                vec![
                    ColumnarValue::Array(Arc::new(NullArray::new(rows))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ],
                vec![
                    ColumnarValue::Array(lists.clone()),
                    ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ],
                vec![
                    ColumnarValue::Scalar(ScalarValue::Null),
                    ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1); rows]))),
                ],
                vec![
                    ColumnarValue::Scalar(ScalarValue::try_new_null(lists.data_type())?),
                    ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1); rows]))),
                ],
            ] {
                let fields = arguments
                    .iter()
                    .map(|arg| Arc::new(Field::new("argument", arg.data_type(), true)))
                    .collect();
                let result = ArrayContains::default()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: arguments,
                        arg_fields: fields,
                        number_rows: rows,
                        return_field: Arc::new(Field::new("contains", DataType::Boolean, true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .into_array(rows)?;
                assert_eq!(result.data_type(), &DataType::Boolean);
                assert_eq!(result.len(), rows);
                assert_eq!(result.null_count(), rows);
            }
        }
        Ok(())
    }
}
