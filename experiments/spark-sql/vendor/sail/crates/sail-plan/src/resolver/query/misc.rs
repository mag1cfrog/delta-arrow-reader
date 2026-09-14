// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, ParamValues};
use datafusion_expr::{EmptyRelation, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::literal::LiteralEvaluator;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves a query plan that produces an empty relation.
    /// When `produce_one_row` is true, it can be used for literal projection with no input.
    pub(super) fn resolve_query_empty(&self, produce_one_row: bool) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    pub(super) async fn resolve_query_with_parameters(
        &self,
        input: spec::QueryPlan,
        positional: Vec<spec::Expr>,
        named: Vec<(String, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let evaluator = LiteralEvaluator::new();
        let schema = Arc::new(DFSchema::empty());
        // Evaluate named arguments eagerly so that IDENTIFIER(:col) expressions
        // inside the query body can substitute their placeholder values at plan-resolution
        // time (before `with_param_values` is applied to the resolved plan).
        let named_params = {
            let mut params = HashMap::new();
            for (name, arg) in named {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.insert(name, param);
            }
            params
        };
        // Evaluate positional arguments eagerly for the same reason.
        let positional_params = {
            let mut params = vec![];
            for arg in positional {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.push(param);
            }
            params
        };
        // Enter a scope that makes both named and positional parameter values
        // available for IDENTIFIER clause evaluation inside the query body.
        let mut scope =
            state.enter_param_values_scope(named_params.clone(), positional_params.clone());
        let state = scope.state();
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = if !positional_params.is_empty() {
            input.with_param_values(ParamValues::from(positional_params))?
        } else {
            input
        };
        if !named_params.is_empty() {
            Ok(input.with_param_values(ParamValues::from(named_params))?)
        } else {
            Ok(input)
        }
    }
}
