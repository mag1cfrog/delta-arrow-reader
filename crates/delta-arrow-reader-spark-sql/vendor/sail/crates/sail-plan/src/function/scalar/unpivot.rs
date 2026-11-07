use std::sync::Arc;

use datafusion_common::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

// Native UNNEST widens generated Fields beyond UNPIVOT's known declarations.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct UnpivotFieldDeclaration {
    pub(crate) nullable: bool,
    pub(crate) required_at_runtime: bool,
}

impl ScalarUDFImpl for UnpivotFieldDeclaration {
    fn name(&self) -> &str {
        "spark_unpivot_field_declaration"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        let [data_type] = arguments else {
            return plan_err!("expected one UNPIVOT field");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("expected one UNPIVOT field");
        };
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_nullable(self.nullable),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        Ok(value)
    }
}

pub(crate) fn apply_unpivot_runtime_nullability(expression: Expr) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(function) = &expression else {
        return Ok(Transformed::no(expression));
    };
    let Some(field) = function
        .func
        .inner()
        .downcast_ref::<UnpivotFieldDeclaration>()
    else {
        return Ok(Transformed::no(expression));
    };
    if !field.required_at_runtime {
        return Ok(Transformed::no(expression));
    }
    let [value] = function.args.as_slice() else {
        return plan_err!("expected one UNPIVOT field");
    };
    Ok(Transformed::yes(
        ScalarUDF::from(UnpivotFieldDeclaration {
            nullable: false,
            required_at_runtime: false,
        })
        .call(vec![value.clone()]),
    ))
}
