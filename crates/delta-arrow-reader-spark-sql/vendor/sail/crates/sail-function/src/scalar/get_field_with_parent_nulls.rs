//! Native field extraction with NULL propagation from the already evaluated parent.
//! Nullable field steps stay separate so native flattening cannot discard intermediate masks.

use datafusion::arrow::array::{MutableArrayData, make_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature,
};
use datafusion_functions::core::getfield::GetFieldFunc;

#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct GetFieldWithParentNulls {
    inner: GetFieldFunc,
}

impl GetFieldWithParentNulls {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ScalarUDFImpl for GetFieldWithParentNulls {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.schema_name(args)
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(types)
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.placement(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let parent = args.args.first().cloned();
        let result = self.inner.invoke_with_args(args)?;
        match (result, parent) {
            (ColumnarValue::Array(child), Some(parent)) if !child.data_type().is_null() => {
                let parent = parent.into_array(child.len())?;
                let Some(parent_nulls) = parent
                    .logical_nulls()
                    .filter(|nulls| nulls.null_count() > 0)
                else {
                    return Ok(ColumnarValue::Array(child));
                };
                if matches!(
                    child.data_type(),
                    DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
                ) {
                    // These layouts encode NULLs in their children, not in a top-level bitmap.
                    let data = child.to_data();
                    let mut masked = MutableArrayData::new(vec![&data], true, child.len());
                    let mut filled = 0;
                    for (start, end) in parent_nulls.inner().set_slices() {
                        masked.extend_nulls(start - filled);
                        masked.extend(0, start, end);
                        filled = end;
                    }
                    masked.extend_nulls(child.len() - filled);
                    return Ok(ColumnarValue::Array(make_array(masked.freeze())));
                }
                let nulls = NullBuffer::union(Some(&parent_nulls), child.nulls());
                Ok(ColumnarValue::Array(make_array(
                    child.to_data().into_builder().nulls(nulls).build()?,
                )))
            }
            (result, _) => Ok(result),
        }
    }
}
