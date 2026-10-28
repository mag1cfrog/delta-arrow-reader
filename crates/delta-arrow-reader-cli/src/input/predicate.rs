//! Row filters accepted by `--predicate-file`.
//!
//! Predicates combine column comparisons and null checks with boolean operations.
//! Each literal includes its data type. This module defines the file format and
//! limits the number and nesting depth of predicate nodes. The reader resolves
//! referenced columns and checks the predicate against the selected table.
//!
//! For a table whose `id` column is `Int32`, this file represents `id >= 10`:
//!
//! ```json
//! {
//!   "op": "ge",
//!   "column": "id",
//!   "value": {"type": "int32", "value": "10"}
//! }
//! ```
//!
//! Pass the file to `--predicate-file`. Integer literals use decimal strings;
//! the `type` field determines their width.

use delta_arrow_reader::{DeltaComparison, DeltaPredicate};
use serde::Deserialize;
use snafu::ensure;

use crate::{
    diagnostics::{Error, InputValidationSnafu},
    input::JsonObject,
};

mod diagnostics;
mod json_schema;
mod scalar;
mod schema_errors;

pub(crate) use diagnostics::predicate_type_mismatch_correction;
use diagnostics::{prepend_validation_path, validation_correction};
pub(super) use json_schema::predicate_schema;
use scalar::ScalarInput;
pub(super) use scalar::TIMEZONE_EXPECTATION;
pub(super) use schema_errors::diagnose_schema_error;

pub(super) const MAX_PREDICATE_NODES: usize = 1024;
pub(super) const MAX_PREDICATE_DEPTH: usize = 32;

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct PredicateInput {
    root: JsonObject<PredicateNode>,
}

impl PredicateInput {
    pub(crate) fn into_predicate(self) -> Result<DeltaPredicate, Error> {
        let mut node_count = 0;
        self.root.0.into_predicate(1, &mut node_count)
    }
}

#[derive(Deserialize, strum::EnumDiscriminants)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[strum_discriminants(name(PredicateOperator))]
#[strum_discriminants(derive(Deserialize, strum::VariantNames))]
#[strum_discriminants(serde(rename_all = "snake_case"), strum(serialize_all = "snake_case"))]
enum PredicateNode {
    Constant(ValueFields<bool>),
    Eq(ComparisonFields),
    Ne(ComparisonFields),
    Lt(ComparisonFields),
    Le(ComparisonFields),
    Gt(ComparisonFields),
    Ge(ComparisonFields),
    IsNull(ColumnReferenceFields),
    IsNotNull(ColumnReferenceFields),
    And(LogicalGroupFields),
    Or(LogicalGroupFields),
    Not(NegationFields),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ComparisonFields<T = JsonObject<ScalarInput>> {
    column: String,
    value: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnReferenceFields {
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalGroupFields<T = JsonObject<PredicateNode>> {
    args: Vec<T>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NegationFields<T = JsonObject<PredicateNode>> {
    arg: Box<T>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValueFields<T> {
    value: T,
}

impl PredicateNode {
    fn into_predicate(self, depth: usize, node_count: &mut usize) -> Result<DeltaPredicate, Error> {
        ensure!(
            depth <= MAX_PREDICATE_DEPTH,
            InputValidationSnafu {
                reason: "predicate_depth_limit_exceeded",
                correction: validation_correction(
                    "",
                    format!(
                        "predicate nesting of at most {MAX_PREDICATE_DEPTH} levels, including the root"
                    ),
                    "Reduce nesting at this predicate node.",
                ),
            }
        );
        ensure!(
            *node_count < MAX_PREDICATE_NODES,
            InputValidationSnafu {
                reason: "predicate_node_limit_exceeded",
                correction: validation_correction(
                    "",
                    format!("at most {MAX_PREDICATE_NODES} predicate nodes in the whole tree"),
                    "Remove or combine predicate nodes; this node exceeds the tree's total limit.",
                ),
            }
        );
        *node_count += 1;
        Ok(match self {
            Self::Constant(ValueFields { value }) => DeltaPredicate::Constant(value),
            Self::Eq(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Eq, value)?
            }
            Self::Ne(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::NotEq, value)?
            }
            Self::Lt(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Lt, value)?
            }
            Self::Le(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::LtEq, value)?
            }
            Self::Gt(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::Gt, value)?
            }
            Self::Ge(ComparisonFields { column, value }) => {
                convert_comparison(column, DeltaComparison::GtEq, value)?
            }
            Self::IsNull(ColumnReferenceFields { column }) => DeltaPredicate::IsNull { column },
            Self::IsNotNull(ColumnReferenceFields { column }) => {
                DeltaPredicate::IsNotNull { column }
            }
            Self::And(LogicalGroupFields { args }) => DeltaPredicate::And(
                args.into_iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| {
                                prepend_validation_path(error, &format!("/args/{index}"))
                            })
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or(LogicalGroupFields { args }) => DeltaPredicate::Or(
                args.into_iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.0
                            .into_predicate(depth + 1, node_count)
                            .map_err(|error| {
                                prepend_validation_path(error, &format!("/args/{index}"))
                            })
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not(NegationFields { arg }) => DeltaPredicate::Not(Box::new(
                arg.0
                    .into_predicate(depth + 1, node_count)
                    .map_err(|error| prepend_validation_path(error, "/arg"))?,
            )),
        })
    }
}

fn convert_comparison(
    column: String,
    op: DeltaComparison,
    value: JsonObject<ScalarInput>,
) -> Result<DeltaPredicate, Error> {
    Ok(DeltaPredicate::Compare {
        column,
        op,
        value: value
            .0
            .into_scalar()
            .map_err(|error| prepend_validation_path(error, "/value"))?,
    })
}

#[cfg(test)]
mod tests;
