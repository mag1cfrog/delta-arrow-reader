// Added to the vendored Sail planner for the Delta reader experiment.
// See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, AsArray, GenericListArray, Int64Array, Int64Builder, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, EvaluatedRowPrefix, RowEvaluationFailure, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct BigIntArrayAtLiteralIndex {
    pub(super) zero_based_index: usize,
    pub(super) error_out_of_bounds: bool,
}

impl BigIntArrayAtLiteralIndex {
    fn look_up_list_rows<O: OffsetSizeTrait>(
        &self,
        list: &GenericListArray<O>,
    ) -> (Int64Array, Option<RowEvaluationFailure>) {
        let mut output = Int64Builder::with_capacity(list.len());
        let values = list.values().as_primitive::<Int64Type>();
        for (row, offsets) in list.value_offsets().windows(2).enumerate() {
            if list.is_null(row) {
                output.append_null();
                continue;
            }
            let start = offsets[0].as_usize();
            let end = offsets[1].as_usize();
            if self.zero_based_index < end - start {
                let index = start + self.zero_based_index;
                output.append_option(values.is_valid(index).then(|| values.value(index)));
            } else if self.error_out_of_bounds {
                return (
                    output.finish(),
                    Some(RowEvaluationFailure {
                        row_index: row,
                        error: DataFusionError::Execution(
                            "element_at: the index is out of bounds".to_owned(),
                        ),
                    }),
                );
            } else {
                output.append_null();
            }
        }
        (output.finish(), None)
    }
}

impl ScalarUDFImpl for BigIntArrayAtLiteralIndex {
    fn name(&self) -> &str {
        "spark_bigint_array_at_literal_index"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn supports_constant_folding(&self) -> bool {
        false
    }

    fn requires_row_ordered_errors(&self) -> bool {
        self.error_out_of_bounds
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke_rows_until_error(args)?.into_result()
    }

    fn invoke_rows_until_error(&self, args: ScalarFunctionArgs) -> Result<EvaluatedRowPrefix> {
        let [array] = take_function_args(self.name(), args.args)?;
        let scalar = matches!(array, ColumnarValue::Scalar(_));
        let rows = if scalar {
            args.number_rows.min(1)
        } else {
            args.number_rows
        };
        let array = array.into_array(rows)?;
        let (values, failure) = match array.data_type() {
            DataType::List(_) => self.look_up_list_rows(array.as_list::<i32>()),
            DataType::LargeList(_) => self.look_up_list_rows(array.as_list::<i64>()),
            other => return internal_err!("Unexpected BIGINT array lookup input {other}"),
        };
        let values = if scalar && values.len() == 1 && failure.is_none() {
            ColumnarValue::Scalar(ScalarValue::Int64(values.iter().next().flatten()))
        } else {
            ColumnarValue::Array(Arc::new(values))
        };
        Ok(EvaluatedRowPrefix { values, failure })
    }
}
