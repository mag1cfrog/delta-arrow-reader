// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, AsArray, UInt64Array};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::functions::expr_fn::abs;
use datafusion::functions::math::abs::AbsFunc;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, cast, expr, lit, when,
};
use datafusion_spark::function::bitwise::expr_fn as bitwise_fn;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

#[derive(Debug, PartialEq, Eq, Hash)]
struct UnsignedShiftBits;

impl ScalarUDFImpl for UnsignedShiftBits {
    fn name(&self) -> &str {
        "spark_unsigned_shift_bits"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Int64], Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), args.args.as_slice())?;
        let unsigned = match value {
            ColumnarValue::Array(array) => {
                let array = array.as_primitive::<Int64Type>();
                if array.values().contains(&i64::MIN)
                    && array.iter().flatten().any(|value| value == i64::MIN)
                {
                    None
                } else {
                    let values = ScalarBuffer::from(array.values().inner().clone());
                    Some(ColumnarValue::Array(Arc::new(UInt64Array::new(
                        values,
                        array.nulls().cloned(),
                    ))))
                }
            }
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MIN))) => None,
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Some(ColumnarValue::Scalar(
                ScalarValue::UInt64(value.map(|value| value as u64)),
            )),
            _ => return internal_err!("{} expects BIGINT", self.name()),
        };
        if let Some(unsigned) = unsigned {
            return Ok(unsigned);
        }
        // The original signed-to-unsigned expression rejects BIGINT_MIN via ABS.
        AbsFunc::new().invoke_with_args(args)?;
        internal_err!("ABS did not reject BIGINT_MIN")
    }
}

fn shiftrightunsigned(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (value, shift) = arguments.two()?;

    let input_type = value.clone().get_type(function_context.schema)?;

    let (unsigned_type, max_const) = match input_type.clone() {
        DataType::Int32 => Ok((DataType::UInt32, u32::MAX as u64)),
        DataType::Int64 => Ok((DataType::UInt64, u64::MAX)),
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "`shiftrightunsigned`: unsupported input type {wrong_type:?}"
        ))),
    }?;

    let unsigned = if input_type == DataType::Int64 && matches!(value, expr::Expr::Column(_)) {
        ScalarUDF::from(UnsignedShiftBits).call(vec![value])
    } else {
        let unsigned = when(
            value.clone().lt(lit(0)),
            lit(max_const) - (abs(value.clone()) - lit(1)),
        )
        .otherwise(value)?;
        cast(unsigned, unsigned_type)
    };

    Ok(cast(unsigned >> shift, input_type))
}

pub(super) fn list_built_in_bitwise_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("&", F::binary_op(Operator::BitwiseAnd)),
        ("^", F::binary_op(Operator::BitwiseXor)),
        ("bit_count", F::unary(bitwise_fn::bit_count)),
        ("bitwise_not", F::unary(bitwise_fn::bitwise_not)),
        ("bit_get", F::binary(bitwise_fn::bit_get)),
        ("getbit", F::binary(bitwise_fn::bit_get)),
        ("shiftleft", F::binary_op(Operator::BitwiseShiftLeft)),
        ("<<", F::binary_op(Operator::BitwiseShiftLeft)),
        ("shiftright", F::binary_op(Operator::BitwiseShiftRight)),
        (">>", F::binary_op(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::custom(shiftrightunsigned)),
        (">>>", F::custom(shiftrightunsigned)),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unary(|arg| (-arg) - lit(1))),
    ]
}

#[test]
fn unsigned_shift_bits_preserve_sliced_values_and_validity() -> Result<()> {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Field, UInt64Type};

    let invoke = |value, number_rows| {
        UnsignedShiftBits.invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![Arc::new(Field::new("value", DataType::Int64, true))],
            number_rows,
            return_field: Arc::new(Field::new("bits", DataType::UInt64, true)),
            config_options: Arc::new(Default::default()),
        })
    };
    let input = Int64Array::new(
        vec![7, i64::MIN, -1, i64::MAX, i64::MIN].into(),
        Some(NullBuffer::from(vec![true, false, true, true, true])),
    );
    let sliced = input.slice(1, 3);
    let value = invoke(ColumnarValue::Array(Arc::new(sliced.clone())), 3)?.into_array(3)?;
    let unsigned = value.as_primitive::<UInt64Type>();
    assert!(unsigned.values().inner().ptr_eq(sliced.values().inner()));
    assert_eq!(unsigned.nulls(), sliced.nulls());
    assert_eq!(
        unsigned.iter().collect::<Vec<_>>(),
        [None, Some(u64::MAX), Some(i64::MAX as u64)],
    );
    let error = invoke(ColumnarValue::Array(Arc::new(input)), 5).unwrap_err();
    assert!(error.to_string().contains("Int64Array overflow on abs("));
    let scalar = invoke(ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))), 1)?;
    assert!(matches!(
        scalar,
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(u64::MAX)))
    ));
    Ok(())
}
