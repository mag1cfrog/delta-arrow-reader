// Added to the vendored Sail planner for the Delta reader experiment.
// See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, GenericListArray, OffsetSizeTrait,
};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct HasNullInnerArray;

impl ScalarUDFImpl for HasNullInnerArray {
    fn name(&self) -> &str {
        "spark_has_null_inner_array"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |arrays| {
                let [outer] = take_function_args(self.name(), arrays)?;
                let has_null = match outer.data_type() {
                    DataType::List(_) => check_inner_array_nulls(outer.as_list::<i32>()),
                    DataType::LargeList(_) => check_inner_array_nulls(outer.as_list::<i64>()),
                    other => return internal_err!("Unexpected nested array storage {other}"),
                };
                Ok(Arc::new(has_null) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

fn check_inner_array_nulls<O: OffsetSizeTrait>(outer: &GenericListArray<O>) -> BooleanArray {
    let Some(inner_nulls) = outer.values().nulls().filter(|mask| mask.null_count() > 0) else {
        return BooleanArray::new(
            BooleanBuffer::new_unset(outer.len()),
            outer.nulls().cloned(),
        );
    };
    let mut result = BooleanBuilder::with_capacity(outer.len());
    for (row, offsets) in outer.value_offsets().windows(2).enumerate() {
        if outer.is_null(row) {
            result.append_null();
        } else {
            let mut range = offsets[0].as_usize()..offsets[1].as_usize();
            result.append_value(range.any(|index| inner_nulls.is_null(index)));
        }
    }
    result.finish()
}
