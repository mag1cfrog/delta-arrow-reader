// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{EmptyRelation, LogicalPlan};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a query plan that produces an empty relation.
    /// When `produce_one_row` is true, it can be used for literal projection with no input.
    pub(super) fn resolve_query_empty(&self, produce_one_row: bool) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }
}
