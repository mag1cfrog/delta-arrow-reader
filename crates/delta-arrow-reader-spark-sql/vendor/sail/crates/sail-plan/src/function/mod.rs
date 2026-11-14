// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableFunction;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_expr::expr::Expr;
use lazy_static::lazy_static;

pub use sail_function::scalar::datetime::local_datetime_to_ltz::is_spark_compatible_arrow_fixed_offset;

use crate::error::{PlanError, PlanResult};
use crate::function::common::ScalarFunction;

mod aggregate;
mod collection_result;
pub(crate) mod common;
mod comparison;
pub(crate) mod float_extrema;
pub(crate) mod float_keys;
mod generator;
pub(crate) mod numeric_coercion;
pub(crate) mod result_coercion;
pub(crate) mod scalar;
pub(crate) mod sort_keys;
mod table;
pub(crate) mod temporal_coercion;
mod window;

pub(crate) use aggregate::aggregate_argument_needs_validation;
pub(crate) use aggregate::expression_is_nondeterministic;
pub(crate) use aggregate::get_built_in_aggregate_function;
pub(crate) use aggregate::require_deterministic_scalar_expression;
pub(crate) use comparison::SparkStringTemporalComparisonCast;
pub(crate) use scalar::{get_lambda_parameters, is_higher_order_function};
pub(crate) use window::get_built_in_window_function;

lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
    pub static ref BUILT_IN_TABLE_FUNCTIONS: HashMap<&'static str, Arc<TableFunction>> =
        HashMap::from_iter(table::list_built_in_table_functions());
}

pub fn get_built_in_function(name: &str) -> PlanResult<ScalarFunction> {
    Ok(BUILT_IN_SCALAR_FUNCTIONS
        .get(name)
        .or_else(|| BUILT_IN_GENERATOR_FUNCTIONS.get(name))
        .ok_or_else(|| PlanError::unsupported(format!("unknown function: {name}")))?
        .clone())
}

pub fn get_built_in_table_function(name: &str) -> PlanResult<Arc<TableFunction>> {
    Ok(BUILT_IN_TABLE_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown table function: {name}")))?
        .clone())
}

pub fn is_built_in_generator_function(name: &str) -> bool {
    BUILT_IN_GENERATOR_FUNCTIONS.contains_key(name)
}

pub use generator::get_outer_built_in_generator_functions;

/// This function is temporary and should ONLY be used for COUNT(*).
/// [`Expr::Wildcard`]
///
/// Only aware of this being applicable to [`datafusion::functions_aggregate::count`],
/// although it may be applicable elsewhere as well.
/// Similarly, this function may need to be adjusted if there are other possible pattern matches
/// that were not considered.
#[inline(always)]
pub(super) fn transform_count_star_wildcard_expr(arguments: Vec<Expr>) -> Vec<Expr> {
    match arguments.as_slice() {
        #[expect(deprecated)]
        [
            Expr::Wildcard {
                qualifier: None,
                options: _,
            },
        ] => {
            vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
        }
        _ => arguments,
    }
}

#[cfg(test)]
mod tests {
    use super::is_spark_compatible_arrow_fixed_offset;

    #[test]
    fn spark_compatible_arrow_fixed_offset_matrix() {
        for timezone in [
            "+00", "+01", "+0130", "+01:30", "-00:00", "-01", "-0130", "-01:30", "+18", "+1800",
            "+18:00", "-18", "-1800", "-18:00",
        ] {
            assert!(
                is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }

        for timezone in [
            "",
            "Z",
            "UTC",
            "UTC+01:30",
            "GMT+01:30",
            "+1:30",
            "+01:3",
            "+01:30:15",
            "+0160",
            "+18:01",
            "+1801",
            "+23:59",
            "-18:01",
            "America/Los_Angeles",
        ] {
            assert!(
                !is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }
    }
}
