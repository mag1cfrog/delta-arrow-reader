use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray};
use datafusion::arrow::compute::kernels::numeric::{add, mul, sub};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::utils::make_scalar_function;
use datafusion::physical_expr_common::datum::apply;
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, exec_err, internal_err};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use super::utils::try_op::try_binary_op_primitive;

/// Checked integer arithmetic. ANSI operators raise on overflow and TRY functions return NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCheckedArithmetic {
    op: Operator,
    signature: Signature,
    null_on_overflow: bool,
}

impl SparkCheckedArithmetic {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(
            op,
            Operator::Plus | Operator::Minus | Operator::Multiply
        ));
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
            null_on_overflow: false,
        }
    }

    pub fn new_try(op: Operator) -> Self {
        Self {
            null_on_overflow: true,
            ..Self::new(op)
        }
    }
}

impl ScalarUDFImpl for SparkCheckedArithmetic {
    fn name(&self) -> &str {
        match (self.op, self.null_on_overflow) {
            (Operator::Plus, false) => "spark_checked_add",
            (Operator::Minus, false) => "spark_checked_subtract",
            (Operator::Multiply, false) => "spark_checked_multiply",
            (Operator::Plus, true) => "spark_try_add",
            (Operator::Minus, true) => "spark_try_subtract",
            (Operator::Multiply, true) => "spark_try_multiply",
            _ => unreachable!("constructor validates the operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
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

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &datafusion_expr::simplify::SimplifyContext,
    ) -> Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::{lit, simplify::ExprSimplifyResult};
        // Match native NULL propagation. Spark's earlier evaluation of local
        // VALUES is preserved by the existing planning-time error precheck.
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            let types = args
                .iter()
                .map(|arg| info.get_data_type(arg))
                .collect::<Result<Vec<_>>>()?;
            return Ok(ExprSimplifyResult::Simplified(lit(
                datafusion_common::ScalarValue::try_new_null(&self.return_type(&types)?)?,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = types else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let (left, right) = BinaryTypeCoercer::new(left, &self.op, right).get_input_types()?;
        if left != right || !left.is_signed_integer() {
            return internal_err!("{} requires signed integers", self.name());
        }
        Ok(vec![left, right])
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(self.coerce_types(types)?.remove(0))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            self.null_on_overflow || args.arg_fields.iter().any(|f| f.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.null_on_overflow {
            let op = self.op;
            let name = self.name();
            return make_scalar_function(
                |arrays| {
                    let [left, right] = take_function_args(name, arrays)?;
                    Ok(match left.data_type() {
                        DataType::Int8 => try_integer_arithmetic::<Int8Type>(left, right, op),
                        DataType::Int16 => try_integer_arithmetic::<Int16Type>(left, right, op),
                        DataType::Int32 => try_integer_arithmetic::<Int32Type>(left, right, op),
                        DataType::Int64 => try_integer_arithmetic::<Int64Type>(left, right, op),
                        _ => return internal_err!("{name} requires signed integers"),
                    })
                },
                vec![],
            )(&args.args);
        }
        let [left, right] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let kernel = match self.op {
            Operator::Plus => add,
            Operator::Minus => sub,
            Operator::Multiply => mul,
            _ => unreachable!("constructor validates the operator"),
        };
        match apply(left, right, kernel) {
            Err(DataFusionError::ArrowError(error, _))
                if matches!(error.as_ref(), ArrowError::ArithmeticOverflow(_)) =>
            {
                let condition = if matches!(left.data_type(), DataType::Int8 | DataType::Int16) {
                    "BINARY_ARITHMETIC_OVERFLOW"
                } else {
                    "ARITHMETIC_OVERFLOW"
                };
                exec_err!("[{condition}] {}: {error}", self.name())
            }
            result => result,
        }
    }
}

fn try_integer_arithmetic<T: ArrowPrimitiveType>(
    left: &ArrayRef,
    right: &ArrayRef,
    op: Operator,
) -> ArrayRef {
    let apply = match op {
        Operator::Plus => T::Native::add_checked,
        Operator::Minus => T::Native::sub_checked,
        Operator::Multiply => T::Native::mul_checked,
        _ => unreachable!("constructor validates the operator"),
    };
    Arc::new(try_binary_op_primitive(
        left.as_primitive::<T>(),
        right.as_primitive::<T>(),
        |left, right| apply(left, right).ok(),
    ))
}
