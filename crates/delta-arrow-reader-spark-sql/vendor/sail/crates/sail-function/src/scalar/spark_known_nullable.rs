// Local Spark nullability declaration. See the host crate's UPSTREAM.md.
//! Like Spark's `KnownNullable`: its argument unchanged, declared nullable whatever the argument
//! declares, for a Spark expression that is nullable over a column at every stage, such as
//! TRY_TO_DATE with a format. Unlike a `CASE WHEN true` wrapper, the optimizer cannot simplify
//! it away over a column; over a constant it folds like any immutable function, so the folded
//! value is NOT NULL, as Spark's is. Further arguments are inputs that Spark's expression has
//! and DAR's omits, such as the columns, aggregates and window functions of a format Spark does
//! not evaluate. They are not used: they keep the operators below computing what Spark's do, and
//! keep the declaration from folding over a constant value where Spark's expression is not
//! constant. They read columns only, see `simplify_kept_inputs`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// An input a declaration keeps as it was planned: a column, or an aggregate or window function,
/// which the operators below compute.
fn is_planned_input(input: &Expr) -> bool {
    matches!(
        input,
        Expr::Column(_) | Expr::AggregateFunction(_) | Expr::WindowFunction(_)
    )
}

/// `declaration` over `args` with each input after the value that the optimizer has turned into
/// another expression, by merging a projection into it, replaced by the columns that expression
/// reads. The inputs are never used, and Spark never evaluates them; reading a column cannot
/// fail, where evaluating the merged expression could.
pub(crate) fn simplify_kept_inputs(declaration: ScalarUDF, args: Vec<Expr>) -> ExprSimplifyResult {
    if args.iter().skip(1).all(is_planned_input) {
        return ExprSimplifyResult::Original(args);
    }
    let mut args = args.into_iter();
    let mut kept = args.next().into_iter().collect::<Vec<_>>();
    for input in args {
        let inputs = if is_planned_input(&input) {
            vec![input]
        } else {
            let mut columns = input.column_refs().into_iter().cloned().collect::<Vec<_>>();
            columns.sort();
            columns.into_iter().map(Expr::Column).collect()
        };
        for input in inputs {
            if !kept[1..].contains(&input) {
                kept.push(input);
            }
        }
    }
    ExprSimplifyResult::Simplified(declaration.call(kept))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkKnownNullable {
    signature: Signature,
}

impl Default for SparkKnownNullable {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkKnownNullable {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkKnownNullable {
    fn name(&self) -> &str {
        "spark_known_nullable"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [data_type, ..] => Ok(data_type.clone()),
            [] => internal_err!("{} requires an argument", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [argument, ..] = args.arg_fields else {
            return internal_err!("{} requires an argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            argument.data_type().clone(),
            true,
        )))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        Ok(simplify_kept_inputs(ScalarUDF::from(Self::new()), args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument, ..] = args.args.as_slice() else {
            return internal_err!("{} requires an argument", self.name());
        };
        Ok(argument.clone())
    }
}
