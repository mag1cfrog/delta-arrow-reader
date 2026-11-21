// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray, GenericListArray, OffsetSizeTrait};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::plan_err;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_functions_nested::empty::ArrayEmpty;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Explode {
    signature: Signature,
    kind: ExplodeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplodeKind {
    Explode,
    ExplodeOuter,
    PosExplode,
    PosExplodeOuter,
    Inline,
    InlineOuter,
    StackSingleColumn,
}

impl Explode {
    pub fn new(kind: ExplodeKind) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            kind,
        }
    }

    pub fn kind(&self) -> &ExplodeKind {
        &self.kind
    }
}

impl ScalarUDFImpl for Explode {
    fn name(&self) -> &str {
        match self.kind {
            ExplodeKind::Explode => "explode",
            ExplodeKind::ExplodeOuter => "explode_outer",
            ExplodeKind::PosExplode => "posexplode",
            ExplodeKind::PosExplodeOuter => "posexplode_outer",
            ExplodeKind::Inline => "inline",
            ExplodeKind::InlineOuter => "inline_outer",
            ExplodeKind::StackSingleColumn => "stack",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let field = match &arg_types {
            &[DataType::List(field)]
            | &[DataType::LargeList(field)]
            | &[DataType::FixedSizeList(field, _)]
            | &[DataType::Map(field, _)] => field,
            _ => return plan_err!("{} should only be called with a list or map", self.name()),
        };
        if self.kind == ExplodeKind::StackSingleColumn {
            // Enclosing expressions check the original STRUCT type before generator expansion.
            let column = Field::new("col0", field.data_type().clone(), true)
                .with_metadata(field.metadata().clone());
            Ok(DataType::Struct(vec![Arc::new(column)].into()))
        } else {
            Ok(field.data_type().clone())
        }
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct InlineOuterListEmpty {
    native: ArrayEmpty,
}

impl InlineOuterListEmpty {
    fn empty_by_offsets<O: OffsetSizeTrait>(array: &GenericListArray<O>) -> BooleanArray {
        let offsets = array.value_offsets();
        let empty = BooleanBuffer::collect_bool(array.len(), |row| {
            !array.is_null(row) && offsets[row] == offsets[row + 1]
        });
        BooleanArray::new(empty, array.nulls().cloned())
    }
}

impl ScalarUDFImpl for InlineOuterListEmpty {
    fn name(&self) -> &str {
        "inline_outer_list_empty"
    }

    fn signature(&self) -> &Signature {
        self.native.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.native.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [ColumnarValue::Array(array)] = args.args.as_slice() else {
            return self.native.invoke_with_args(args);
        };
        let empty = match array.data_type() {
            DataType::List(_) => Self::empty_by_offsets(array.as_list::<i32>()),
            DataType::LargeList(_) => Self::empty_by_offsets(array.as_list::<i64>()),
            _ => return self.native.invoke_with_args(args),
        };
        Ok(ColumnarValue::Array(Arc::new(empty)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array, ListArray, StructArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn inline_outer_offset_emptiness_matches_native() -> Result<()> {
        let child = StructArray::try_new(
            vec![Arc::new(Field::new("x", DataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                None,
                Some(3),
                None,
            ]))],
            Some(NullBuffer::from(vec![true, true, true, false, true])),
        )?;
        let item = Arc::new(Field::new_list_field(child.data_type().clone(), true));
        let nullable = Some(NullBuffer::from(vec![true, true, false, true, true]));
        let records = ListArray::try_new(
            item.clone(),
            OffsetBuffer::from_lengths([1, 0, 2, 1, 1]),
            Arc::new(child),
            nullable,
        )?;
        let kernel = InlineOuterListEmpty::default();
        for storage in [DataType::List(item.clone()), DataType::LargeList(item)] {
            let array = datafusion::arrow::compute::cast(&records, &storage)?;
            for input in [array.clone(), array.slice(1, 4), array.slice(0, 0)] {
                let args = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(input.clone())],
                    arg_fields: vec![Arc::new(Field::new("records", storage.clone(), true))],
                    number_rows: input.len(),
                    return_field: Arc::new(Field::new("empty", DataType::Boolean, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                let expected = kernel
                    .native
                    .invoke_with_args(args.clone())?
                    .into_array(input.len())?;
                let actual: ArrayRef = kernel.invoke_with_args(args)?.into_array(input.len())?;
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
        }
        Ok(())
    }
}
