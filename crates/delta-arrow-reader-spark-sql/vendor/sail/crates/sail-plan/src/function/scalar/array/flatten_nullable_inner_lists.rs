// Added for the Delta reader experiment. See the Spark crate's UPSTREAM.md.
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, NullArray, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ScalarFunctionArgs, ScalarUDFImpl, ValueOrLambda,
    Volatility,
};

use super::FlattenResult;

/// Flatten a full batch of valid inner lists or evaluate the original guarded expression.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct FlattenNullableInnerLists(FlattenResult);

impl HigherOrderUDFImpl for FlattenNullableInnerLists {
    fn name(&self) -> &str {
        "spark_flatten_nullable_inner_lists"
    }

    fn signature(&self) -> &HigherOrderSignature {
        static SIGNATURE: LazyLock<HigherOrderSignature> = LazyLock::new(|| {
            HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            )
        });
        &SIGNATURE
    }

    fn coerce_value_types(&self, argument_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(argument_types.to_vec())
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(vec![vec![dummy]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [ValueOrLambda::Value(_), ValueOrLambda::Lambda(result)] = args.arg_fields else {
            return internal_err!("flatten requires an array and a guarded fallback");
        };
        Ok(Arc::clone(result))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [ValueOrLambda::Value(array), ValueOrLambda::Lambda(fallback)] = args.args.as_slice()
        else {
            return internal_err!("flatten requires an array and a guarded fallback");
        };
        if let ColumnarValue::Array(array_ref) = array
            && match array_ref.data_type() {
                DataType::List(_) => inner_lists_are_valid(array_ref.as_list::<i32>()),
                DataType::LargeList(_) => inner_lists_are_valid(array_ref.as_list::<i64>()),
                _ => false,
            }
            && let [ValueOrLambda::Value(field), ValueOrLambda::Lambda(_)] =
                args.arg_fields.as_slice()
        {
            return self.0.invoke_with_args(ScalarFunctionArgs {
                args: vec![array.clone()],
                arg_fields: vec![Arc::clone(field)],
                number_rows: args.number_rows,
                return_field: args.return_field,
                config_options: args.config_options,
            });
        }
        let dummy = || Ok(Arc::new(NullArray::new(args.number_rows)) as ArrayRef);
        fallback.evaluate(&[&dummy], |captures| Ok(captures.to_vec()))
    }
}

fn inner_lists_are_valid<O: OffsetSizeTrait>(array: &GenericListArray<O>) -> bool {
    let Some(nulls) = array.values().nulls() else {
        return true;
    };
    let offsets = array.value_offsets();
    let start = offsets[0].as_usize();
    let end = offsets[array.len()].as_usize();
    nulls.null_count() == 0 || nulls.slice(start, end - start).null_count() == 0
}
