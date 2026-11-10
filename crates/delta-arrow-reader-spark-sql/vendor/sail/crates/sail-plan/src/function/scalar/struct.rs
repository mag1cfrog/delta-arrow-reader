// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput, dictionary_string_values};

fn r#struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| -> PlanResult<_> {
            match expr {
                Expr::Column(_) | Expr::OuterReferenceColumn(_, _) => Ok(name.clone()),
                Expr::Alias(alias) => Ok(alias.name.clone()),
                #[expect(deprecated)]
                Expr::Wildcard { .. } => Err(PlanError::internal(
                    "wildcard should have been expanded before struct",
                )),
                _ => Ok(format!("col{}", i + 1)),
            }
        })
        .collect::<PlanResult<_>>()?;
    // A dictionary-encoded STRING field holds its STRING values, once the field is named.
    let arguments = input
        .arguments
        .into_iter()
        .map(|arg| dictionary_string_values(arg, input.function_context.schema))
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: arguments,
    }))
}

fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::invalid(format!(
            "named_struct requires an even number of arguments, got {} instead",
            input.arguments.len()
        )));
    }
    let field_names = input
        .arguments
        .iter()
        .enumerate()
        .step_by(2)
        .map(|(index, argument)| {
            argument
                .as_literal()
                .and_then(|value| value.try_as_str().flatten())
                .map(str::to_owned)
                .ok_or_else(|| {
                    PlanError::invalid(format!(
                        "named_struct requires {index}-th (0-indexed) field name as constant string"
                    ))
                })
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(ScalarUDF::from(StructFunction::new(field_names))
        .call(input.arguments.into_iter().skip(1).step_by(2).collect()))
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
