// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, create_physical_sort_exprs};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DataFusionError, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Union, UserDefinedLogicalNode};
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use self::input_sort_boundary::InputSortBoundaryExec;
use self::monotonic_id::MonotonicIdExec;
use self::signed_range_window::{
    SparkSignedRangeWindowNode, lower_signed_range_windows, plan_signed_range_window,
};
use self::spark_partition_id::SparkPartitionIdExec;
use crate::declaration_inputs::drop_declaration_inputs;
use crate::expression_errors::SparkSqlExpressionSemantics;
use crate::function::common::{is_arrow_extension_metadata_key, set_result_metadata};
use crate::function::scalar::{apply_unpivot_runtime_nullability, lower_runtime_concat};
use crate::projection_row_errors::{ProjectionRowErrorNode, admit_prefix_producers};

mod input_sort_boundary;
mod monotonic_id;
mod seeded_rand;
mod signed_range_window;
mod spark_partition_id;

/// Plan native DataFusion queries and the supported Spark execution extensions.
#[derive(Debug)]
pub struct SparkQueryPlanner;

#[async_trait]
impl QueryPlanner for SparkQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut state = Cow::Borrowed(session_state);
        logical_plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::Extension(extension) = plan
                && let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>()
                && !sort.preserve_partitioning()
            {
                // ponytail: this plan disables automatic round-robin repartitioning to
                // keep the required global order. Restore parallelism when aggregate
                // ordering keys can be carried through projections that hide them.
                state
                    .to_mut()
                    .config_mut()
                    .options_mut()
                    .optimizer
                    .enable_round_robin_repartition = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let spark_semantics = session_state
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        let runtime_plan = if spark_semantics {
            Cow::Owned(lower_signed_range_windows(admit_prefix_producers(
                logical_plan
                    .clone()
                    .transform_up_with_subqueries(|node| {
                        let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                        node.map_expressions(|expression| {
                            let name = names.save(&expression);
                            expression
                                .transform_up(|expression| {
                                    lower_runtime_concat(expression)
                                        .map_err(|error| {
                                            DataFusionError::External(Box::new(error))
                                        })?
                                        .transform_data(apply_unpivot_runtime_nullability)
                                })
                                .map(|expression| {
                                    expression.update_data(|expression| name.restore(expression))
                                })
                        })
                    })?
                    .data,
            )?)?)
        } else {
            Cow::Borrowed(logical_plan)
        };
        let runtime_plan = drop_declaration_inputs(runtime_plan.into_owned(), session_state)?;
        let runtime_plan = if spark_semantics {
            align_result_metadata_for_execution(runtime_plan)?
        } else {
            runtime_plan
        };
        let physical =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SparkExtensionPlanner)])
                .create_physical_plan(&runtime_plan, &state)
                .await?;
        seeded_rand::bind_seeded_rand_streams(physical)
    }
}

fn align_result_metadata_for_execution(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Annotate after logical optimization so logical field pruning sees the original expressions.
    Ok(plan
        .transform_up_with_subqueries(|node| {
            let node = node
                .map_expressions(|expression| {
                    expression.transform_up(remove_alias_result_metadata)
                })?
                .data;
            let node = if let LogicalPlan::Union(union) = node {
                let union = Union::try_new(union.inputs)?;
                let union_schema = Arc::clone(&union.schema);
                let inputs = union
                    .inputs
                    .into_iter()
                    .map(|input| align_union_input_metadata(input, &union_schema))
                    .collect::<Result<Vec<_>>>()?;
                LogicalPlan::Union(Union::try_new(inputs)?)
            } else {
                node.recompute_schema()?
            };
            Ok(Transformed::yes(node))
        })?
        .data)
}

fn remove_alias_result_metadata(mut expression: Expr) -> Result<Transformed<Expr>> {
    // Nonliteral physical aliases do not apply their copied result metadata.
    if let Expr::Alias(alias) = &mut expression
        && let Some(metadata) = &mut alias.metadata
    {
        let mut retained = metadata.to_hashmap();
        retained.retain(|key, _| is_arrow_extension_metadata_key(key));
        if retained.len() != metadata.len() {
            *metadata = retained.into();
            return Ok(Transformed::yes(expression));
        }
    }
    Ok(Transformed::no(expression))
}

fn align_union_input_metadata(
    input: Arc<LogicalPlan>,
    union_schema: &DFSchema,
) -> Result<Arc<LogicalPlan>> {
    let mut changed = false;
    let columns = input
        .schema()
        .iter()
        .zip(union_schema.fields())
        .map(|((qualifier, field), target)| {
            let column = Expr::from((qualifier, field));
            let mut metadata = field.metadata().clone();
            metadata.retain(|key, value| {
                is_arrow_extension_metadata_key(key) || target.metadata().get(key) == Some(value)
            });
            if metadata == *field.metadata() {
                column
            } else {
                changed = true;
                set_result_metadata(column, metadata.clone().into_iter().collect())
                    .alias_qualified_with_metadata(
                        qualifier.cloned(),
                        field.name(),
                        Some(metadata.into()),
                    )
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(input);
    }
    Ok(Arc::new(
        LogicalPlanBuilder::from(input.as_ref().clone())
            .project(columns)?
            .build()?,
    ))
}

// Keep callers on SparkQueryPlanner so required-order safeguards also apply.
struct SparkExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SparkExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> = if let Some(window) =
            node.as_any().downcast_ref::<SparkSignedRangeWindowNode>()
        {
            let [input] = physical_inputs else {
                return internal_err!("SparkSignedRangeWindow requires exactly one physical input");
            };
            plan_signed_range_window(window, Arc::clone(input), session_state)?
        } else if let Some(provenance) = node.as_any().downcast_ref::<ProjectionRowErrorNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
                    "Projection row-error provenance requires exactly one physical input"
                );
            };
            match provenance {
                ProjectionRowErrorNode::InputSortBoundary(_) => {
                    if let Some(sort) = input.downcast_ref::<SortExec>() {
                        Arc::new(InputSortBoundaryExec::new(sort.clone()))
                    } else {
                        // shortcut: protect retained sorts; restore removed sorts when their live specification is retained.
                        Arc::clone(input)
                    }
                }
                ProjectionRowErrorNode::PrefixProducer(_) => {
                    let Some(projection) = input.downcast_ref::<ProjectionExec>() else {
                        return internal_err!(
                            "A row-error prefix producer must be a computed projection"
                        );
                    };
                    if projection.schema().fields() != node.schema().fields() {
                        return internal_err!(
                            "A row-error prefix producer changed its bound output fields"
                        );
                    }
                    Arc::new(projection.clone().with_row_error_prefix(true))
                }
            }
        } else if let Some(partition_id) = node.as_any().downcast_ref::<SparkPartitionIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SparkPartitionIdExec requires exactly one physical input");
            };
            Arc::new(SparkPartitionIdExec::try_new(
                input.clone(),
                partition_id.column_name(),
                node.schema().inner().clone(),
            )?)
        } else if let Some(monotonic_id) = node.as_any().downcast_ref::<MonotonicIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MonotonicIdExec requires exactly one physical input");
            };
            Arc::new(MonotonicIdExec::try_new(
                input.clone(),
                monotonic_id.column_name(),
                node.schema().inner().clone(),
            )?)
        } else if let Some(sort) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SortExec requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                sort.sort_expr(),
                node.schema(),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("SortExec requires at least one sort expression");
            };
            Arc::new(
                SortExec::new(ordering, input.clone())
                    .with_fetch(sort.fetch())
                    .with_preserve_partitioning(true),
            )
        } else if let Some(sort) = node.as_any().downcast_ref::<RequiredSortNode>() {
            let [input] = physical_inputs else {
                return internal_err!("RequiredSort requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                sort.sort_expr(),
                node.schema(),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("RequiredSort requires at least one sort expression");
            };
            let physical = SortExec::new(ordering, input.clone())
                .with_fetch(sort.fetch())
                .with_preserve_partitioning(sort.preserve_partitioning());
            let requirements = OrderingRequirements::from(physical.expr().clone());
            let distribution = physical.required_input_distribution().swap_remove(0);
            Arc::new(OutputRequirementExec::new(
                Arc::new(physical),
                Some(requirements),
                distribution,
                sort.fetch(),
            ))
        } else {
            return Ok(None);
        };
        Ok(Some(plan))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::EmptyRelation;

    use super::*;

    #[test]
    fn arrow_extension_metadata_keeps_the_input_plan() -> Result<()> {
        let plan_with_metadata = |metadata: HashMap<String, String>| -> Result<Arc<LogicalPlan>> {
            let schema = Schema::new(vec![
                Field::new(
                    "r",
                    DataType::Struct(vec![Field::new("x", DataType::Int32, false)].into()),
                    false,
                )
                .with_metadata(metadata),
            ]);
            Ok(Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::try_from(schema)?),
            })))
        };
        let source = plan_with_metadata(HashMap::from([
            ("ARROW:extension:name".into(), "dar.struct".into()),
            ("ARROW:extension:metadata".into(), "{}".into()),
        ]))?;
        let other = plan_with_metadata(HashMap::from([("owner".into(), "right".into())]))?;
        let union = Union::try_new(vec![source.clone(), other])?;
        assert!(union.schema.field(0).metadata().is_empty());
        let aligned = align_union_input_metadata(source.clone(), &union.schema)?;
        assert_eq!(aligned.as_ref(), source.as_ref());
        Ok(())
    }
}
