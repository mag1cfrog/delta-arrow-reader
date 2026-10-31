use std::sync::Arc;

use datafusion::arrow::compute::kernels::numeric::{add, mul, sub};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::physical_expr_common::datum::apply;
use datafusion_common::{DataFusionError, Result, exec_err, internal_err};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// ANSI integer arithmetic. The planner selects this function per expression;
/// legacy arithmetic keeps its native wrapping operator.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCheckedArithmetic {
    op: Operator,
    signature: Signature,
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
        }
    }
}

impl ScalarUDFImpl for SparkCheckedArithmetic {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "spark_checked_add",
            Operator::Minus => "spark_checked_subtract",
            Operator::Multiply => "spark_checked_multiply",
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
            args.arg_fields.iter().any(|f| f.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
