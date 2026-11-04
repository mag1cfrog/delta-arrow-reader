//! Retain original input-sort boundaries and admit computed prefix producers.
//!
//! Admission follows optimized projection output bindings, including CSE and
//! named aliases. A logical input sort blocks it even if physical planning later
//! moves that sort above the requesting expression.

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::sync::Arc;

use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::expression_errors::SparkSqlExpressionSemantics;

/// Remember original input sorts before CSE and physical projection rewrites.
/// Native sessions and Spark expressions without a row-error request are unchanged.
#[derive(Debug)]
pub struct CaptureSparkInputSortBoundaries;

impl AnalyzerRule for CaptureSparkInputSortBoundaries {
    fn name(&self) -> &str {
        "capture_spark_input_sort_boundaries"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        if options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
        {
            return Ok(plan);
        }
        rewrite_requesting_projections(plan, Rewrite::CaptureInputSorts)
    }
}

/// Apply admission after logical optimization, while original sort markers remain.
pub(crate) fn admit_prefix_producers(plan: LogicalPlan) -> Result<LogicalPlan> {
    rewrite_requesting_projections(plan, Rewrite::AdmitPrefixProducers)
}

#[derive(Clone, Copy)]
enum Rewrite {
    CaptureInputSorts,
    AdmitPrefixProducers,
}

fn rewrite_requesting_projections(plan: LogicalPlan, rewrite: Rewrite) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up_with_subqueries(|plan| {
            let LogicalPlan::Projection(mut projection) = plan else {
                return Ok(Transformed::no(plan));
            };
            let requested = requesting_inputs(&projection.expr, projection.input.schema())?;
            if requested.is_empty() {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            projection.input = Arc::new(rewrite_requested_input(
                Arc::unwrap_or_clone(projection.input),
                &requested,
                rewrite,
            )?);
            Ok(Transformed::yes(LogicalPlan::Projection(projection)))
        })?
        .data)
}

fn requesting_inputs(expressions: &[Expr], schema: &DFSchema) -> Result<BTreeSet<usize>> {
    let mut requested = BTreeSet::new();
    for expression in expressions {
        expression.apply(|expr| {
            if let Expr::ScalarFunction(function) = expr
                && function.func.inner().requires_row_ordered_errors()
            {
                add_inputs(expr, schema, &mut requested)?;
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(requested)
}

fn add_inputs(expr: &Expr, schema: &DFSchema, requested: &mut BTreeSet<usize>) -> Result<()> {
    for column in expr.column_refs() {
        requested.insert(schema.index_of_column(column)?);
    }
    Ok(())
}

fn is_computed(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_computed(&alias.expr),
        Expr::Column(_) | Expr::Literal(..) => false,
        _ => true,
    }
}

fn rewrite_requested_input(
    plan: LogicalPlan,
    requested: &BTreeSet<usize>,
    rewrite: Rewrite,
) -> Result<LogicalPlan> {
    if requested.is_empty() {
        return Ok(plan);
    }
    match plan {
        LogicalPlan::Projection(mut projection) => {
            let mut inputs = BTreeSet::new();
            let mut computed = false;
            for index in requested {
                let Some(expr) = projection.expr.get(*index) else {
                    return internal_err!("Requested projection output {index} is out of bounds");
                };
                computed |= is_computed(expr);
                add_inputs(expr, projection.input.schema(), &mut inputs)?;
            }
            projection.input = Arc::new(rewrite_requested_input(
                Arc::unwrap_or_clone(projection.input),
                &inputs,
                rewrite,
            )?);
            let projection = LogicalPlan::Projection(projection);
            Ok(
                if computed && matches!(rewrite, Rewrite::AdmitPrefixProducers) {
                    ProjectionRowErrorNode::PrefixProducer(Arc::new(projection)).into_plan()
                } else {
                    projection
                },
            )
        }
        LogicalPlan::SubqueryAlias(mut alias) => {
            // An alias changes qualifiers, not the positional output binding.
            alias.input = Arc::new(rewrite_requested_input(
                Arc::unwrap_or_clone(alias.input),
                requested,
                rewrite,
            )?);
            Ok(LogicalPlan::SubqueryAlias(alias))
        }
        LogicalPlan::Sort(sort) if matches!(rewrite, Rewrite::CaptureInputSorts) => Ok(
            ProjectionRowErrorNode::InputSortBoundary(Arc::new(LogicalPlan::Sort(sort)))
                .into_plan(),
        ),
        LogicalPlan::Extension(extension) => {
            if let Some(ProjectionRowErrorNode::PrefixProducer(input)) = extension
                .node
                .as_any()
                .downcast_ref::<ProjectionRowErrorNode>(
            ) {
                let input = rewrite_requested_input(input.as_ref().clone(), requested, rewrite)?;
                if matches!(&input, LogicalPlan::Extension(extension)
                    if matches!(extension.node.as_any().downcast_ref::<ProjectionRowErrorNode>(),
                        Some(ProjectionRowErrorNode::PrefixProducer(_))))
                {
                    return Ok(input);
                }
                return Ok(ProjectionRowErrorNode::PrefixProducer(Arc::new(input)).into_plan());
            }
            // No row-domain transport is established through other operators.
            Ok(LogicalPlan::Extension(extension))
        }
        other => Ok(other),
    }
}

/// Plan-only provenance. Both variants preserve their current child's full schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) enum ProjectionRowErrorNode {
    /// An original logical sort stops requests from its output consumers.
    InputSortBoundary(Arc<LogicalPlan>),
    /// An optimized computed projection admitted through actual column lineage.
    PrefixProducer(Arc<LogicalPlan>),
}

impl ProjectionRowErrorNode {
    fn input(&self) -> &LogicalPlan {
        match self {
            Self::InputSortBoundary(input) | Self::PrefixProducer(input) => input,
        }
    }

    fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for ProjectionRowErrorNode {
    fn name(&self) -> &str {
        match self {
            Self::InputSortBoundary(_) => "InputSortRowErrorBoundary",
            Self::PrefixProducer(_) => "ProjectionRowErrorPrefix",
        }
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input().schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!(
                "{} requires no expressions and exactly one input",
                self.name()
            );
        }
        let input = Arc::new(inputs.remove(0));
        Ok(match self {
            Self::InputSortBoundary(_) => Self::InputSortBoundary(input),
            Self::PrefixProducer(_) => Self::PrefixProducer(input),
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}
