//! Preserve an explicit SQL distribution through physical optimization.

use std::{collections::HashSet, fmt::Formatter, sync::Arc};

use datafusion_common::{DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct RequiredDistributionNode {
    input: Arc<LogicalPlan>,
    keys: Vec<Expr>,
}

impl RequiredDistributionNode {
    pub fn new(input: Arc<LogicalPlan>, keys: Vec<Expr>) -> Self {
        Self { input, keys }
    }

    pub fn keys(&self) -> &[Expr] {
        &self.keys
    }
}

impl UserDefinedLogicalNodeCore for RequiredDistributionNode {
    fn name(&self) -> &str {
        "RequiredDistribution"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.keys.clone()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RequiredDistribution: {:?}", self.keys)
    }

    fn with_exprs_and_inputs(&self, keys: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if keys.len() != self.keys.len() {
            return internal_err!(
                "RequiredDistribution: expected {} keys, got {}",
                self.keys.len(),
                keys.len()
            );
        }
        Ok(Self::new(Arc::new(inputs.one()?), keys))
    }
}
