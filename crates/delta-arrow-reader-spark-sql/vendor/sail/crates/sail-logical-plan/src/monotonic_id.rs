use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub enum MonotonicIdPurpose {
    SqlFunction,
    WindowOrdering,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct MonotonicIdNode {
    input: Arc<LogicalPlan>,
    column_name: String,
    purpose: MonotonicIdPurpose,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl MonotonicIdNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        column_name: String,
        purpose: MonotonicIdPurpose,
    ) -> Result<Self> {
        let mut qualified_fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        qualified_fields.push((
            None,
            Arc::new(Field::new(column_name.clone(), DataType::Int64, false)),
        ));
        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())?
                .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            column_name,
            purpose,
            schema: Arc::new(schema),
        })
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn purpose(&self) -> MonotonicIdPurpose {
        self.purpose
    }
}

impl UserDefinedLogicalNodeCore for MonotonicIdNode {
    fn name(&self) -> &str {
        "MonotonicId"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MonotonicId")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        Self::try_new(input, self.column_name.clone(), self.purpose)
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_and_rebuilt_nodes_preserve_purpose_and_hash_identity() -> Result<()> {
        let input = Arc::new(LogicalPlan::default());
        let nodes = [
            MonotonicIdNode::try_new(
                Arc::clone(&input),
                "id".into(),
                MonotonicIdPurpose::SqlFunction,
            )?,
            MonotonicIdNode::try_new(
                Arc::clone(&input),
                "id".into(),
                MonotonicIdPurpose::WindowOrdering,
            )?,
        ];
        let identities = nodes
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(identities.len(), 2);
        for node in nodes {
            let cloned = node.clone();
            let rebuilt = node.with_exprs_and_inputs(vec![], vec![input.as_ref().clone()])?;
            assert_eq!(cloned.purpose(), node.purpose());
            assert_eq!(rebuilt.purpose(), node.purpose());
            assert_eq!(rebuilt, node);
            assert!(identities.contains(&cloned));
            assert!(identities.contains(&rebuilt));
        }
        Ok(())
    }
}
