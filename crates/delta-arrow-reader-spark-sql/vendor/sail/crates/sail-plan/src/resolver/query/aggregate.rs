// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::functions_aggregate::{average, bit_and_or_xor, bool_and_or, count, min_max, sum};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::{enumerate_grouping_sets, find_aggregate_exprs, powerset};
use datafusion_expr::{
    Aggregate, AggregateUDF, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    Projection, ScalarUDF, SortExpr, Volatility, bitwise_and, bitwise_or, bitwise_shift_left,
    bitwise_shift_right, cast, lit, when,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::scalar::explode::Explode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::error::{PlanError, PlanResult};
use crate::function::common::SparkComparisonFloat;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::{AggregateState, PlanResolverState};
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

/// Projections resolved by index (a `None` marks a projection deferred until the
/// grouping is materialized), paired with the indices of the deferred projections.
type ResolvedProjections = (Vec<Option<NamedExpr>>, Vec<usize>);

/// A map from a grouping generator expression to the column that materializes it.
type GeneratorReplacements = Vec<(Expr, Expr)>;

const GROUPING_KEY_ORDER_METADATA: &str = "sail:grouping_key_order";

/// Returns the name of a volatile (non-deterministic) scalar expression found
/// in an aggregate context. Catches two Spark CheckAnalysis violations:
/// 1. Volatile scalar UDF used directly in aggregate projections (outside any aggregate fn)
/// 2. Volatile scalar UDF nested inside aggregate function arguments
fn find_volatile_in_aggregate_context(expr: &Expr) -> Option<String> {
    let mut found_name: Option<String> = None;
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e
            && f.func.signature().volatility == Volatility::Volatile
        {
            found_name = Some(f.func.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found_name
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_aggregate(
        &self,
        aggregate: spec::Aggregate,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Aggregate {
            input,
            grouping,
            aggregate: projections,
            having,
            with_grouping_expressions,
        } = aggregate;

        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();

        // Resolve the projections, deferring any that reference a grouping output
        // until the grouping is materialized below.
        let (resolved_projections, deferred_projections) = self
            .resolve_projections_deferring(&projections, schema, state)
            .await?;

        let grouping = {
            let projections = resolved_projections.iter().flatten().cloned().collect();
            let mut scope = state.enter_aggregate_scope(AggregateState::Grouping { projections });
            let state = scope.state();
            self.resolve_named_expressions(grouping, schema, state)
                .await?
        };

        // Expand a generator (e.g. window's `explode`) in the grouping into a named
        // column; a no-op for ordinary aggregates.
        let (input, grouping, generator_replacements) =
            self.expand_grouping_generators(input, grouping, state)?;
        let schema = input.schema();

        // Resolve the deferred projections (grouping columns are now in scope) and
        // inline any re-used generator expressions.
        let projections = self
            .finish_projections(
                resolved_projections,
                deferred_projections,
                &projections,
                &generator_replacements,
                schema,
                state,
            )
            .await?;

        // Spark CheckAnalysis: reject non-deterministic expressions in aggregate context
        for proj in &projections {
            if let Some(name) = find_volatile_in_aggregate_context(&proj.expr) {
                return Err(PlanError::AnalysisError(format!(
                    "Non-deterministic expression {name} should not appear in an aggregate query",
                )));
            }
        }

        let having = if let Some(having) = having {
            let mut grouping = Self::named_grouping_expressions(grouping.clone())?;
            // Repeated sets share a name binding, but retain their result rows.
            let mut seen = HashSet::new();
            grouping.retain(|x| {
                seen.insert((
                    x.name[0].to_ascii_lowercase(),
                    Self::unalias_grouping_expr(&x.expr).clone(),
                ))
            });
            let mut scope = state.enter_aggregate_scope(AggregateState::Having {
                projections: projections.clone(),
                grouping,
            });
            let state = scope.state();
            Some(Self::replace_generator_expressions(
                self.resolve_expression(having, schema, state).await?,
                &generator_replacements,
            )?)
        } else {
            None
        };

        self.rewrite_aggregate(
            input,
            projections,
            grouping,
            having,
            with_grouping_expressions,
            state,
        )
    }

    fn resolve_grouping_positions(
        &self,
        exprs: Vec<NamedExpr>,
        projections: &[NamedExpr],
    ) -> PlanResult<Vec<NamedExpr>> {
        let num_projections = projections.len() as i64;
        exprs
            .into_iter()
            .map(|named_expr| {
                let NamedExpr { expr, .. } = &named_expr;
                match expr {
                    Expr::Literal(scalar_value, _metadata) => {
                        let position = match scalar_value {
                            ScalarValue::Int32(Some(position)) => *position as i64,
                            ScalarValue::Int64(Some(position)) => *position,
                            _ => return Ok(named_expr),
                        };
                        if position > 0_i64 && position <= num_projections {
                            Ok(projections[(position - 1) as usize].clone())
                        } else {
                            Err(PlanError::invalid(format!(
                                "Cannot resolve column position {position}. Valid positions are 1 to {num_projections}."
                            )))
                        }
                    }
                    _ => Ok(named_expr),
                }
            })
            .collect()
    }

    pub(super) fn rewrite_aggregate(
        &self,
        input: LogicalPlan,
        projections: Vec<NamedExpr>,
        grouping: Vec<NamedExpr>,
        having: Option<Expr>,
        with_grouping_expressions: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let grouping = self.resolve_grouping_positions(grouping, &projections)?;
        let (group_exprs, grouping_exprs, grouping_order) =
            Self::canonicalize_grouping_keys(grouping.iter().map(|x| x.expr.clone()).collect())?;
        let has_grouping_set = Self::has_grouping_set(&group_exprs);
        let projections = projections
            .into_iter()
            .map(|x| {
                Self::rewrite_grouping_functions(
                    x,
                    &grouping_exprs,
                    has_grouping_set,
                    &grouping_order,
                )
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let having = having
            .map(|having| {
                Self::rewrite_grouping_expr(
                    having,
                    &grouping_exprs,
                    has_grouping_set,
                    &grouping_order,
                )
            })
            .transpose()?;
        let mut aggregate_candidates = projections
            .iter()
            .map(|x| x.expr.clone())
            .collect::<Vec<_>>();
        if let Some(having) = having.as_ref() {
            aggregate_candidates.push(having.clone());
        }
        let aggregate_exprs = find_aggregate_exprs(&aggregate_candidates);
        // Normalize only each complete grouping key. Aggregate arguments and
        // operands inside a computed key must continue to see the original values.
        let group_exprs = group_exprs.into_iter().map(|expr| {
            expr.transform_down(|expr| {
                if matches!(expr, Expr::GroupingSet(_)) {
                    return Ok(Transformed::no(expr));
                }
                let normalize = matches!(expr.get_type(input.schema())?, DataType::Float32 | DataType::Float64)
                    // Ordinary constant grouping expressions also serve as output
                    // constants; Spark preserves their original zero sign. Grouping
                    // sets materialize those keys and must still normalize them.
                    && (has_grouping_set || expr.any_column_refs() || expr.contains_outer()
                        || expr.contains_scalar_subquery() || expr.is_volatile());
                let expr = if normalize {
                    let (qualifier, name) = expr.qualified_name();
                    ScalarUDF::from(SparkComparisonFloat { widen: false })
                        .call(vec![expr.unalias()]).alias_qualified(qualifier, name)
                } else { expr };
                Ok(Transformed::new(expr, normalize, TreeNodeRecursion::Jump))
            }).data()
        }).collect::<DataFusionResult<Vec<_>>>()?;
        let mut plan = LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggregate_exprs.clone())?
            .build()?;
        if !grouping_order.is_empty() {
            // ORDER BY resolves after aggregate construction. Keep the semantic
            // order on the private ID field, without adding an execution node.
            let LogicalPlan::Aggregate(aggregate) = &mut plan else {
                return Err(PlanError::internal("expected aggregate plan"));
            };
            let order = serde_json::to_string(&grouping_order)
                .map_err(|error| PlanError::internal(error.to_string()))?;
            let fields = aggregate
                .schema
                .iter()
                .map(|(qualifier, field)| {
                    let field = if field.name() == Aggregate::INTERNAL_GROUPING_ID {
                        let mut metadata = field.metadata().clone();
                        metadata.insert(GROUPING_KEY_ORDER_METADATA.into(), order.clone());
                        Arc::new(field.as_ref().clone().with_metadata(metadata))
                    } else {
                        Arc::clone(field)
                    };
                    (qualifier.cloned(), field)
                })
                .collect();
            aggregate.schema = Arc::new(
                DFSchema::new_with_metadata(fields, aggregate.schema.metadata().clone())?
                    .with_functional_dependencies(
                        aggregate.schema.functional_dependencies().clone(),
                    )?,
            );
        }
        let mut aggregate_or_grouping_exprs = aggregate_exprs;
        aggregate_or_grouping_exprs.extend(grouping_exprs);
        let grouping_exprs = Self::named_grouping_expressions(grouping)?;
        let projections = if with_grouping_expressions {
            grouping_exprs.into_iter().chain(projections).collect()
        } else {
            projections
        };
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = x;
                let expr = Self::rebase_expression(expr, &aggregate_or_grouping_exprs, &plan)?;
                Ok(NamedExpr {
                    name,
                    expr,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let plan = match having {
            Some(having) => {
                let having =
                    Self::rebase_expression(having.clone(), &aggregate_or_grouping_exprs, &plan)?;
                LogicalPlanBuilder::from(plan).having(having)?.build()?
            }
            None => plan,
        };
        let (plan, projections) =
            self.rewrite_projection::<MonotonicIdRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<ExplodeRewriter>(plan, projections, state)?;
        let (plan, projections) =
            self.rewrite_projection::<WindowRewriter>(plan, projections, state)?;
        let projections = projections
            .into_iter()
            .map(|x| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _,
                } = x;
                Ok(expr.alias(state.register_field_name(name.one()?)))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalPlanBuilder::from(plan)
            .project(projections)?
            .build()?)
    }

    pub(crate) fn input_sort_ordering(input: &LogicalPlan) -> Option<Vec<SortExpr>> {
        Self::find_input_sort_ordering(input)?
            .into_iter()
            .map(|sort| {
                Some(SortExpr {
                    expr: normalize_col(sort.expr, input).ok()?,
                    asc: sort.asc,
                    nulls_first: sort.nulls_first,
                })
            })
            .collect()
    }

    fn find_input_sort_ordering(input: &LogicalPlan) -> Option<Vec<SortExpr>> {
        match input {
            LogicalPlan::Sort(sort) => Some(sort.expr.clone()),
            LogicalPlan::SubqueryAlias(alias) => {
                let ordering = Self::find_input_sort_ordering(alias.input.as_ref())?;
                ordering
                    .into_iter()
                    .map(|sort| {
                        let expr = sort
                            .expr
                            .transform(|expr| match expr {
                                Expr::Column(mut column) => {
                                    column.relation = Some(alias.alias.clone());
                                    Ok(Transformed::yes(Expr::Column(column)))
                                }
                                expr => Ok(Transformed::no(expr)),
                            })
                            .data()
                            .ok()?;
                        Some(SortExpr { expr, ..sort })
                    })
                    .collect()
            }
            LogicalPlan::Filter(filter) => Self::find_input_sort_ordering(filter.input.as_ref()),
            LogicalPlan::Limit(limit) => Self::find_input_sort_ordering(limit.input.as_ref()),
            LogicalPlan::Projection(projection) => {
                let ordering = Self::find_input_sort_ordering(projection.input.as_ref())?;
                ordering
                    .into_iter()
                    .map(|sort| {
                        let expr = Self::remap_through_projection(sort.expr, projection)?;
                        Some(SortExpr { expr, ..sort })
                    })
                    .collect()
            }
            LogicalPlan::Window(window) => {
                let [Expr::WindowFunction(function)] = window.window_expr.as_slice() else {
                    return None;
                };
                let mut required = function
                    .params
                    .partition_by
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: true,
                    })
                    .collect::<Vec<_>>();
                required.extend(function.params.order_by.clone());

                let child = Self::find_input_sort_ordering(window.input.as_ref());
                if required.is_empty() {
                    child
                } else {
                    match child {
                        Some(child) if child.starts_with(&required) => Some(child),
                        _ => Some(required),
                    }
                }
            }
            LogicalPlan::Unnest(unnest) => {
                let ordering = Self::find_input_sort_ordering(unnest.input.as_ref())?;
                let references_unnested_column = ordering.iter().any(|sort| {
                    sort.expr.column_refs().iter().any(|column| {
                        unnest
                            .input
                            .schema()
                            .maybe_index_of_column(column)
                            .is_none_or(|index| {
                                unnest
                                    .list_type_columns
                                    .iter()
                                    .any(|(unnested, _)| *unnested == index)
                                    || unnest.struct_type_columns.contains(&index)
                            })
                    })
                });
                (!references_unnested_column).then_some(ordering)
            }
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                if let Some(sort) = node.downcast_ref::<RequiredSortNode>() {
                    Some(sort.sort_expr().to_vec())
                } else if let Some(sort) = node.downcast_ref::<SortWithinPartitionsNode>() {
                    Some(sort.sort_expr().to_vec())
                } else if let Some(node) = node.downcast_ref::<MonotonicIdNode>() {
                    Self::find_input_sort_ordering(node.input().as_ref())
                } else if let Some(node) = node.downcast_ref::<SparkPartitionIdNode>() {
                    Self::find_input_sort_ordering(node.input().as_ref())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn remap_through_projection(expr: Expr, projection: &Projection) -> Option<Expr> {
        let rewritten = expr
            .transform_down(|expr| {
                match projection
                    .expr
                    .iter()
                    .position(|projection| projection.clone().unalias() == expr)
                {
                    Some(index) => {
                        let (qualifier, field) = projection.schema.qualified_field(index);
                        Ok(Transformed::new(
                            Expr::from(Column::from((qualifier, field))),
                            true,
                            TreeNodeRecursion::Jump,
                        ))
                    }
                    None => Ok(Transformed::no(expr)),
                }
            })
            .data()
            .ok()?;
        let mut derivable = true;
        let _ = rewritten.apply(|expr| {
            if let Expr::Column(column) = expr
                && projection
                    .schema
                    .qualified_field_from_column(column)
                    .is_err()
            {
                derivable = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        derivable.then_some(rewritten)
    }

    pub(crate) fn is_order_irrelevant_udaf(
        udf: &AggregateUDF,
        args: &[Expr],
        schema: &DFSchemaRef,
    ) -> PlanResult<bool> {
        if udf == min_max::min_udaf().as_ref()
            || udf == min_max::max_udaf().as_ref()
            || udf == count::count_udaf().as_ref()
            || udf == bit_and_or_xor::bit_and_udaf().as_ref()
            || udf == bit_and_or_xor::bit_or_udaf().as_ref()
            || udf == bit_and_or_xor::bit_xor_udaf().as_ref()
            || udf == bool_and_or::bool_and_udaf().as_ref()
            || udf == bool_and_or::bool_or_udaf().as_ref()
        {
            return Ok(true);
        }

        if udf == sum::sum_udaf().as_ref()
            || udf == average::avg_udaf().as_ref()
            || udf.inner().is::<SparkTrySum>()
            || udf.inner().is::<TryAvgFunction>()
        {
            let Some(arg) = args.first() else {
                return Ok(false);
            };
            return Ok(!matches!(
                arg.get_type(schema.as_ref())?,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ));
        }

        Ok(false)
    }

    fn requires_input_order(
        aggregates: &[Expr],
        schema: &DFSchemaRef,
        has_grouping: bool,
    ) -> PlanResult<bool> {
        for aggregate in aggregates {
            let Expr::AggregateFunction(function) = aggregate else {
                return Ok(true);
            };
            let udf = function.func.as_ref();
            if !has_grouping
                && (udf == sum::sum_udaf().as_ref() || udf == average::avg_udaf().as_ref())
            {
                continue;
            }
            if !Self::is_order_irrelevant_udaf(udf, &function.params.args, schema)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn preserve_order_sensitive_aggregate_sorts(
        plan: LogicalPlan,
    ) -> PlanResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|plan| {
                let LogicalPlan::Aggregate(mut aggregate) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let aggregate_exprs = find_aggregate_exprs(&aggregate.aggr_expr);
                if !Self::requires_input_order(
                    &aggregate_exprs,
                    aggregate.input.schema(),
                    !aggregate.group_expr.is_empty(),
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                let (input, found, _) = Self::require_input_sort_inner(Arc::unwrap_or_clone(
                    Arc::clone(&aggregate.input),
                ))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
                if !found {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }
                aggregate.input = Arc::new(input);
                Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
            })
            .data()?)
    }

    pub(crate) fn require_input_sort_inner(
        plan: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, bool, bool)> {
        match plan {
            LogicalPlan::Sort(sort) => Ok((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RequiredSortNode::new(
                        sort.input, sort.expr, sort.fetch, false,
                    )),
                }),
                true,
                true,
            )),
            LogicalPlan::Limit(mut limit) => {
                let fetch = match (limit.get_skip_type()?, limit.get_fetch_type()?) {
                    (SkipType::Literal(skip), FetchType::Literal(Some(fetch))) => Some(
                        skip.checked_add(fetch)
                            .ok_or_else(|| PlanError::invalid("LIMIT + OFFSET overflow"))?,
                    ),
                    _ => None,
                };
                let input = Arc::unwrap_or_clone(std::mem::replace(
                    &mut limit.input,
                    Arc::new(LogicalPlan::default()),
                ));
                let (input, found, global) = match fetch {
                    Some(fetch) => Self::require_input_sort_with_fetch(input, fetch)?,
                    None => Self::require_input_sort_inner(input)?,
                };
                limit.input = Arc::new(input);
                Ok((LogicalPlan::Limit(limit), found, global))
            }
            LogicalPlan::Extension(extension) => {
                if let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>() {
                    let global = !sort.preserve_partitioning();
                    return Ok((LogicalPlan::Extension(extension), true, global));
                }
                if let Some(sort) = extension
                    .node
                    .as_any()
                    .downcast_ref::<SortWithinPartitionsNode>()
                {
                    return Ok((
                        LogicalPlan::Extension(Extension {
                            node: Arc::new(RequiredSortNode::new(
                                Arc::clone(sort.input()),
                                sort.sort_expr().to_vec(),
                                sort.fetch(),
                                true,
                            )),
                        }),
                        true,
                        false,
                    ));
                }
                if !extension.node.as_any().is::<MonotonicIdNode>()
                    && !extension.node.as_any().is::<SparkPartitionIdNode>()
                {
                    return Ok((LogicalPlan::Extension(extension), false, false));
                }

                let plan = LogicalPlan::Extension(extension);
                let expressions = plan.expressions();
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_inner(child)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
            plan => {
                let transparent = matches!(
                    plan,
                    LogicalPlan::SubqueryAlias(_)
                        | LogicalPlan::Projection(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Window(_)
                        | LogicalPlan::Unnest(_)
                );
                if !transparent {
                    return Ok((plan, false, false));
                }

                // Unnest's with_new_exprs requires no expressions and rebuilds from its own exec_columns.
                let expressions = if matches!(&plan, LogicalPlan::Unnest(_)) {
                    vec![]
                } else {
                    plan.expressions()
                };
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_inner(child)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
        }
    }

    fn require_input_sort_with_fetch(
        plan: LogicalPlan,
        fetch: usize,
    ) -> PlanResult<(LogicalPlan, bool, bool)> {
        match plan {
            LogicalPlan::Sort(sort) => Ok((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RequiredSortNode::new(
                        sort.input,
                        sort.expr,
                        Some(sort.fetch.map_or(fetch, |old| old.min(fetch))),
                        false,
                    )),
                }),
                true,
                true,
            )),
            plan => {
                if !matches!(
                    plan,
                    LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
                ) {
                    return Self::require_input_sort_inner(plan);
                }

                let expressions = plan.expressions();
                let child = plan.inputs().one()?.clone();
                let (child, found, global) = Self::require_input_sort_with_fetch(child, fetch)?;
                if found {
                    Ok((plan.with_new_exprs(expressions, vec![child])?, true, global))
                } else {
                    Ok((plan, false, false))
                }
            }
        }
    }

    fn canonicalize_grouping_keys(
        grouping: Vec<Expr>,
    ) -> DataFusionResult<(Vec<Expr>, Vec<Expr>, Vec<usize>)> {
        if !Self::has_grouping_set(&grouping) {
            let keys = grouping.clone();
            return Ok((grouping, keys, vec![]));
        }
        // Aliases name outputs, not independent subtotal dimensions. Only strip
        // aliases around complete keys; never rewrite aggregate inputs or operands.
        let grouping = grouping
            .into_iter()
            .map(|expr| {
                expr.transform_down(|mut expr| {
                    if matches!(expr, Expr::GroupingSet(_)) {
                        return Ok(Transformed::no(expr));
                    }
                    let aliased = matches!(expr, Expr::Alias(_));
                    while matches!(expr, Expr::Alias(_)) {
                        expr = expr.unalias();
                    }
                    Ok(Transformed::new(expr, aliased, TreeNodeRecursion::Jump))
                })
                .data()
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let keys = Self::distinct_grouping_expressions_from_exprs(&grouping);
        if keys.len() > 64 {
            return Err(DataFusionError::Plan(
                "[GROUPING_SIZE_LIMIT_EXCEEDED] grouping set size cannot be greater than 64".into(),
            ));
        }
        let grouping = grouping.into_iter().map(|expr| {
            let (set, cube) = match expr {
                Expr::GroupingSet(GroupingSet::Rollup(set)) => (set, false),
                Expr::GroupingSet(GroupingSet::Cube(set)) => (set, true),
                expr => return Ok(expr),
            };
            let mut seen = HashSet::new();
            if set.iter().all(|key| seen.insert(key)) {
                return Ok(Expr::GroupingSet(if cube { GroupingSet::Cube(set) } else { GroupingSet::Rollup(set) }));
            }
            // Apply the existing mixed-grouping-set limit before expansion.
            if (cube && set.len() > 12) || (!cube && set.len() >= 4096) {
                return Err(DataFusionError::Plan(
                    "The number of grouping_set in grouping_sets exceeds the maximum limit 4096".into(),
                ));
            }
            let sets = if cube {
                powerset(&set)?.into_iter().map(|set| set.into_iter().cloned().collect()).collect()
            } else {
                (0..=set.len()).rev().map(|end| set[..end].to_vec()).collect()
            };
            Ok(Expr::GroupingSet(GroupingSet::GroupingSets(sets)))
        }).collect::<DataFusionResult<Vec<_>>>()?;
        let mut grouping = enumerate_grouping_sets(grouping)?;
        if let [Expr::GroupingSet(GroupingSet::GroupingSets(sets))] = grouping.as_mut_slice() {
            // Native logical typing counts equal expression lists, while execution
            // counts equal membership masks. Give both the same canonical lists,
            // retaining every occurrence so duplicate subtotal rows survive.
            for set in sets {
                *set = keys
                    .iter()
                    .filter(|key| set.contains(key))
                    .cloned()
                    .collect();
            }
        }
        let native_keys = Self::distinct_grouping_expressions_from_exprs(&grouping);
        let order = if keys == native_keys {
            vec![]
        } else {
            // Grouping-set order is not SQL row order. A complete set can lead
            // native field discovery in semantic order, avoiding runtime bit
            // permutation for ordinary mixed ROLLUP/CUBE while retaining duplicates.
            if let [Expr::GroupingSet(GroupingSet::GroupingSets(sets))] = grouping.as_mut_slice()
                && let Some(full) = sets.iter().position(|set| set.len() == keys.len())
            {
                sets.swap(0, full);
                return Ok((grouping, keys, vec![]));
            }
            keys.iter()
                .map(|key| {
                    native_keys
                        .iter()
                        .position(|native| native == key)
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing canonical grouping key".into())
                        })
                })
                .collect::<DataFusionResult<Vec<_>>>()?
        };
        Ok((grouping, keys, order))
    }

    pub(super) fn grouping_key_order(
        aggregate: &Aggregate,
        key_count: usize,
    ) -> PlanResult<Vec<usize>> {
        let Some(value) = aggregate
            .schema
            .fields()
            .iter()
            .find(|field| field.name() == Aggregate::INTERNAL_GROUPING_ID)
            .and_then(|field| field.metadata().get(GROUPING_KEY_ORDER_METADATA))
        else {
            return Ok(vec![]);
        };
        let order: Vec<usize> =
            serde_json::from_str(value).map_err(|error| PlanError::internal(error.to_string()))?;
        let mut indices = order.clone();
        indices.sort_unstable();
        if !indices.into_iter().eq(0..key_count) {
            return Err(PlanError::internal("invalid grouping key permutation"));
        }
        Ok(order)
    }

    fn unalias_grouping_expr(mut expr: &Expr) -> &Expr {
        while let Expr::Alias(alias) = expr {
            expr = alias.expr.as_ref();
        }
        expr
    }

    fn named_grouping_expressions(grouping: Vec<NamedExpr>) -> PlanResult<Vec<NamedExpr>> {
        let mut result = Vec::new();
        for NamedExpr {
            name,
            expr,
            metadata,
        } in grouping
        {
            let exprs = match expr {
                Expr::GroupingSet(g) => g.distinct_expr().into_iter().cloned().collect(),
                expr => vec![expr],
            };
            if name.len() != exprs.len() {
                return Err(PlanError::internal(format!(
                    "group-by name count does not match expression count: {name:?} {exprs:?}",
                )));
            }
            result.extend(exprs.into_iter().zip(name).map(|(expr, name)| NamedExpr {
                name: vec![name],
                expr,
                metadata: metadata.clone(),
            }));
        }
        Ok(result)
    }

    pub(super) fn has_grouping_set(grouping: &[Expr]) -> bool {
        grouping.iter().any(|x| matches!(x, Expr::GroupingSet(_)))
    }

    pub(super) fn distinct_grouping_expressions_from_exprs(grouping: &[Expr]) -> Vec<Expr> {
        if !Self::has_grouping_set(grouping) {
            return grouping.to_vec();
        }
        // Mixed grouping sets share one bitmap, with one bit per distinct key.
        let mut seen = HashSet::new();
        grouping
            .iter()
            .flat_map(|x| match x {
                Expr::GroupingSet(g) => g.distinct_expr(),
                expr => vec![expr],
            })
            .filter(|expr| seen.insert(*expr))
            .map(|expr| {
                // Sort rewriting sees the materialized key; GROUPING arguments
                // still refer to the original expression before normalization.
                if let Expr::Alias(alias) = expr
                    && let Expr::ScalarFunction(function) = alias.expr.as_ref()
                    && function
                        .func
                        .inner()
                        .downcast_ref::<SparkComparisonFloat>()
                        .is_some_and(|normalizer| !normalizer.widen)
                    && let [arg] = function.args.as_slice()
                {
                    arg
                } else {
                    expr
                }
            })
            .cloned()
            .collect()
    }

    /// Resolves the projections against the input. If that fails, resolves each one
    /// individually and defers (by index) those that cannot resolve yet because they
    /// reference a grouping output not in scope until the grouping is materialized.
    async fn resolve_projections_deferring(
        &self,
        projections: &[spec::Expr],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<ResolvedProjections> {
        if let Ok(resolved) = self
            .resolve_named_expressions(projections.to_vec(), schema, state)
            .await
        {
            return Ok((resolved.into_iter().map(Some).collect(), vec![]));
        }
        let mut resolved = Vec::with_capacity(projections.len());
        let mut deferred = vec![];
        for (index, projection) in projections.iter().enumerate() {
            match self
                .resolve_named_expression(projection.clone(), schema, state)
                .await
            {
                Ok(named) => resolved.push(Some(named)),
                Err(_) => {
                    resolved.push(None);
                    deferred.push(index);
                }
            }
        }
        Ok((resolved, deferred))
    }

    /// Resolves the deferred projections (the grouping columns are now in scope) and
    /// inlines re-used generator expressions, producing the final projection list.
    async fn finish_projections(
        &self,
        mut resolved: Vec<Option<NamedExpr>>,
        deferred: Vec<usize>,
        projections: &[spec::Expr],
        generator_replacements: &[(Expr, Expr)],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        for index in deferred {
            resolved[index] = Some(
                self.resolve_named_expression(projections[index].clone(), schema, state)
                    .await?,
            );
        }
        resolved
            .into_iter()
            .enumerate()
            .map(|(index, named)| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = named.ok_or_else(|| {
                    PlanError::internal(format!("projection {index} was not resolved"))
                })?;
                Ok(NamedExpr {
                    name,
                    expr: Self::replace_generator_expressions(expr, generator_replacements)?,
                    metadata,
                })
            })
            .collect()
    }

    fn rewrite_grouping_functions(
        named_expr: NamedExpr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        grouping_order: &[usize],
    ) -> PlanResult<NamedExpr> {
        let NamedExpr {
            name,
            expr,
            metadata,
        } = named_expr;
        Ok(NamedExpr {
            name,
            expr: Self::rewrite_grouping_expr(
                expr,
                grouping_exprs,
                has_grouping_set,
                grouping_order,
            )?,
            metadata,
        })
    }

    pub(super) fn rewrite_grouping_expr(
        expr: Expr,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        grouping_order: &[usize],
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|expr| {
                if let Expr::AggregateFunction(function) = expr {
                    match function.func.name() {
                        "grouping" => Ok(Transformed::yes(Self::grouping_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                            grouping_order,
                        )?)),
                        "grouping_id" => Ok(Transformed::yes(Self::grouping_id_on_grouping_id(
                            function,
                            grouping_exprs,
                            has_grouping_set,
                            grouping_order,
                        )?)),
                        _ => Ok(Transformed::no(Expr::AggregateFunction(function))),
                    }
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .data()?)
    }

    fn grouping_id_column() -> Expr {
        Expr::Column(Column::from(Aggregate::INTERNAL_GROUPING_ID))
    }

    fn grouping_bitmask_literal(value: u64, grouping_expr_count: usize) -> Expr {
        let value = if grouping_expr_count <= 8 {
            ScalarValue::UInt8(Some(value as u8))
        } else if grouping_expr_count <= 16 {
            ScalarValue::UInt16(Some(value as u16))
        } else if grouping_expr_count <= 32 {
            ScalarValue::UInt32(Some(value as u32))
        } else {
            ScalarValue::UInt64(Some(value))
        };
        Expr::Literal(value, None)
    }

    fn format_grouping_exprs(exprs: &[Expr]) -> String {
        exprs
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn reject_grouping_clause(
        function: &datafusion_expr::expr::AggregateFunction,
    ) -> DataFusionResult<()> {
        if function.params.distinct
            || function.params.filter.is_some()
            || !function.params.order_by.is_empty()
            || function.params.null_treatment.is_some()
        {
            Err(DataFusionError::Plan(format!(
                "invalid {} function clause",
                function.func.name()
            )))
        } else {
            Ok(())
        }
    }

    fn grouping_on_grouping_id(
        function: datafusion_expr::expr::AggregateFunction,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        grouping_order: &[usize],
    ) -> DataFusionResult<Expr> {
        Self::reject_grouping_clause(&function)?;
        if !has_grouping_set {
            return Err(DataFusionError::Plan(
                "[UNSUPPORTED_GROUPING_EXPRESSION] grouping functions can only be used with grouping sets, cube, or rollup".to_string(),
            ));
        }
        let args = &function.params.args;
        let [arg] = args.as_slice() else {
            return Err(DataFusionError::Plan(
                "grouping requires exactly one argument".to_string(),
            ));
        };
        let position = grouping_exprs
            .iter()
            .position(|expr| Self::unalias_grouping_expr(expr) == Self::unalias_grouping_expr(arg))
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "[GROUPING_COLUMN_MISMATCH] grouping column {} is not in grouping columns ({})",
                    arg,
                    Self::format_grouping_exprs(grouping_exprs)
                ))
            })?;
        let position = grouping_order.get(position).copied().unwrap_or(position);
        let bitmap_index = grouping_exprs.len() - 1 - position;
        let group_bit = bitwise_and(
            Self::grouping_id_column(),
            Self::grouping_bitmask_literal(1_u64 << bitmap_index, grouping_exprs.len()),
        );
        let shifted = if bitmap_index == 0 {
            group_bit
        } else {
            bitwise_shift_right(
                group_bit,
                Self::grouping_bitmask_literal(bitmap_index as u64, grouping_exprs.len()),
            )
        };
        Ok(cast(
            shifted,
            datafusion_common::arrow::datatypes::DataType::Int8,
        ))
    }

    fn grouping_id_on_grouping_id(
        function: datafusion_expr::expr::AggregateFunction,
        grouping_exprs: &[Expr],
        has_grouping_set: bool,
        grouping_order: &[usize],
    ) -> DataFusionResult<Expr> {
        Self::reject_grouping_clause(&function)?;
        if !has_grouping_set {
            return Err(DataFusionError::Plan(
                "[UNSUPPORTED_GROUPING_EXPRESSION] grouping functions can only be used with grouping sets, cube, or rollup".to_string(),
            ));
        }
        if grouping_exprs.len() > 64 {
            return Err(DataFusionError::Plan(
                "[GROUPING_SIZE_LIMIT_EXCEEDED] grouping set size cannot be greater than 64"
                    .to_string(),
            ));
        }
        let args = &function.params.args;
        if !args.is_empty()
            && (args.len() != grouping_exprs.len()
                || !args.iter().zip(grouping_exprs).all(|(arg, key)| {
                    Self::unalias_grouping_expr(arg) == Self::unalias_grouping_expr(key)
                }))
        {
            return Err(DataFusionError::Plan(format!(
                "[GROUPING_ID_COLUMN_MISMATCH] grouping_id columns ({}) do not match grouping columns ({})",
                Self::format_grouping_exprs(args),
                Self::format_grouping_exprs(grouping_exprs)
            )));
        }
        // DataFusion stores duplicate-set ordinals above the semantic bitmap.
        // Keep duplicate rows, but expose only which grouping keys are absent.
        let count = grouping_exprs.len();
        let mask = if count == 64 {
            u64::MAX
        } else {
            (1_u64 << count) - 1
        };
        let id = if grouping_order.is_empty() {
            bitwise_and(
                Self::grouping_id_column(),
                Self::grouping_bitmask_literal(mask, count),
            )
        } else {
            // Combine bits with the same displacement into one native operation.
            // Duplicate ordinals stay internal; only semantic bits are selected.
            let mut masks = BTreeMap::<isize, u64>::new();
            for (semantic, &native) in grouping_order.iter().enumerate() {
                let displacement = native as isize - semantic as isize;
                *masks.entry(displacement).or_default() |= 1_u64 << (count - 1 - native);
            }
            masks
                .into_iter()
                .map(|(shift, mask)| {
                    let bits = bitwise_and(
                        Self::grouping_id_column(),
                        Self::grouping_bitmask_literal(mask, count),
                    );
                    if shift == 0 {
                        bits
                    } else if shift > 0 {
                        bitwise_shift_left(
                            bits,
                            Self::grouping_bitmask_literal(shift as u64, count),
                        )
                    } else {
                        bitwise_shift_right(
                            bits,
                            Self::grouping_bitmask_literal((-shift) as u64, count),
                        )
                    }
                })
                .reduce(bitwise_or)
                .ok_or_else(|| DataFusionError::Internal("empty grouping key permutation".into()))?
        };
        if count == 64 {
            // Spark's BIGINT is signed. Convert the unsigned bit pattern with
            // range-safe native expressions instead of an overflowing cast.
            let high_bit = lit(1_u64 << 63);
            Ok(when(
                id.clone().gt_eq(high_bit.clone()),
                cast(id.clone() - high_bit, DataType::Int64) + lit(i64::MIN),
            )
            .otherwise(cast(id, DataType::Int64))?)
        } else {
            Ok(cast(id, DataType::Int64))
        }
    }

    /// Expands a generator in the grouping into rows, naming the unnested column
    /// after the grouping output. Returns a map from each generator to its column.
    /// A no-op when the grouping has no generator.
    fn expand_grouping_generators(
        &self,
        input: LogicalPlan,
        grouping: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>, GeneratorReplacements)> {
        if !grouping.iter().any(Self::grouping_has_generator) {
            return Ok((input, grouping, vec![]));
        }
        let generators = grouping.iter().map(|x| x.expr.clone()).collect::<Vec<_>>();
        let (input, mut grouping) =
            self.rewrite_projection::<ExplodeRewriter>(input, grouping, state)?;
        let mut replacements = vec![];
        for (group, generator) in grouping.iter_mut().zip(generators) {
            // The rewriter returns the unnested column wrapped in an alias. Rename
            // that column to the grouping's output name and use it directly.
            let column = match (&group.expr, group.name.as_slice()) {
                (Expr::Alias(alias), [name]) => match alias.expr.as_ref() {
                    Expr::Column(column) => {
                        state.set_field_name(column.name(), name);
                        Some(Expr::Column(column.clone()))
                    }
                    _ => None,
                },
                _ => None,
            };
            if let Some(column) = column {
                replacements.push((generator, column.clone()));
                group.expr = column;
            }
        }
        Ok((input, grouping, replacements))
    }

    /// Whether a grouping expression contains a generator (e.g. `explode`) and so
    /// must expand the input rows before grouping.
    fn grouping_has_generator(group: &NamedExpr) -> bool {
        group
            .expr
            .exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<Explode>())))
            .unwrap_or(false)
    }

    /// Replaces each generator expression with a reference to its materialized
    /// grouping column, so a re-used generator resolves to the same column.
    fn replace_generator_expressions(
        expr: Expr,
        replacements: &[(Expr, Expr)],
    ) -> PlanResult<Expr> {
        if replacements.is_empty() {
            return Ok(expr);
        }
        Ok(expr
            .transform_down(|e| match replacements.iter().find(|(from, _)| *from == e) {
                Some((_, to)) => Ok(Transformed::yes(to.clone())),
                None => Ok(Transformed::no(e)),
            })
            .data()?)
    }

    /// Reference: [datafusion_sql::utils::rebase_expr]
    pub(super) fn rebase_expression(
        expr: Expr,
        base: &[Expr],
        plan: &LogicalPlan,
    ) -> PlanResult<Expr> {
        Ok(expr
            .transform_down(|e| {
                if base.contains(&e) {
                    Ok(Transformed::yes(
                        Self::expr_as_column_expr(&e, plan)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    ))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .data()?)
    }

    // Modification of DataFusion's `expr_as_column_expr`
    fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> PlanResult<Expr> {
        match expr {
            Expr::Column(column) => {
                let result = plan
                    .schema()
                    .qualified_field_from_column(column)
                    .or_else(|_| {
                        let column = Column::new_unqualified(column.name.clone());
                        plan.schema().qualified_field_from_column(&column)
                    })?;
                let (qualifier, field) = result;
                Ok(Expr::from(Column::from((qualifier, field))))
            }
            _ => Ok(Expr::Column(Column::from_name(
                expr.schema_name().to_string(),
            ))),
        }
    }
}

#[cfg(test)]
mod float_grouping_tests {
    use super::*;
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn grouping_normalizes_keys_without_changing_aggregate_inputs() -> PlanResult<()> {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f32::from_bits(0x7fc00001)),
                Some(f32::from_bits(0x7fc00002)),
                Some(f32::from_bits(0xffc00003)),
                None,
                Some(1.0),
                Some(-1.0),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::from_bits(0x7ff8000000000001)),
                Some(f64::from_bits(0x7ff8000000000002)),
                Some(f64::from_bits(0xfff8000000000003)),
                None,
                Some(1.0),
                Some(-1.0),
            ])),
        ];
        for input in inputs {
            for ansi in [true, false] {
                let ctx = SessionContext::new_with_state(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(
                            SessionConfig::new()
                                .with_target_partitions(2)
                                .with_batch_size(2),
                        )
                        .with_query_planner(Arc::new(SparkQueryPlanner))
                        .build(),
                );
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "v",
                    input.data_type().clone(),
                    true,
                )]));
                let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()])?;
                ctx.register_table(
                    "t",
                    Arc::new(MemTable::try_new(
                        schema,
                        vec![vec![batch.slice(0, 4)], vec![batch.slice(4, 4)]],
                    )?),
                )?;
                let sql = "SELECT v,count(*) AS n,sum(CASE WHEN CAST(v AS STRING) LIKE '-%' THEN 1 ELSE 0 END) AS negative_inputs FROM t GROUP BY v";
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let mut config = PlanConfig::default();
                config.ansi_mode = ansi;
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                assert_eq!(named.fields, vec!["v", "n", "negative_inputs"]);
                assert_eq!(named.plan.schema().field(0).data_type(), input.data_type());
                let frame = ctx.execute_logical_plan(named.plan).await?;
                let physical = frame.create_physical_plan().await?;
                let plan = datafusion::physical_plan::displayable(physical.as_ref())
                    .indent(true)
                    .to_string();
                assert!(plan.contains("AggregateExec"), "{plan}");
                let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
                let mut rows = vec![];
                for batch in batches {
                    let counts = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let negatives = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        let value = if batch.column(0).is_null(i) {
                            None
                        } else if input.data_type() == &DataType::Float32 {
                            Some(f64::from(
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Float32Array>()
                                    .unwrap()
                                    .value(i),
                            ))
                        } else {
                            Some(
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap()
                                    .value(i),
                            )
                        };
                        let category = match value {
                            None => 0,
                            Some(v) if v.is_nan() => 1,
                            Some(v) if v == 0.0 => 2,
                            Some(v) if v > 0.0 => 3,
                            _ => 4,
                        };
                        if matches!(category, 1 | 2) {
                            assert!(!value.unwrap().is_sign_negative());
                        }
                        rows.push((category, counts.value(i), negatives.value(i)));
                    }
                }
                rows.sort_unstable();
                assert_eq!(
                    rows,
                    vec![(0, 1, 0), (1, 3, 0), (2, 2, 1), (3, 1, 0), (4, 1, 1)],
                    "{} ansi={ansi}",
                    input.data_type()
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[tokio::test]
async fn grouping_ids_preserve_duplicate_rows_and_signed_bits() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::{Array, Int64Array};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let mut cases = Vec::new();
    for count in [1, 8, 16, 32, 63, 64] {
        let columns = (0..count)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let values = vec!["1"; count].join(",");
        let groups = if count == 64 {
            format!("({columns}),()")
        } else {
            format!("({columns}),({columns}),()")
        };
        let ids = if count == 64 {
            vec![-1, 0]
        } else {
            vec![0, 0, ((1_u64 << count) - 1) as i64]
        };
        cases.push((format!("SELECT grouping_id() AS gid,count(*) AS n FROM (VALUES({values}),({values})) t({columns}) GROUP BY GROUPING SETS ({groups}) ORDER BY gid"), ids, 2));
    }
    cases.push(("SELECT grouping_id() AS gid,count(*) AS n FROM (VALUES(1),(2)) t(v) GROUP BY GROUPING SETS ((),(),()) ORDER BY gid".into(), vec![0,0,0], 2));
    // Empty grouping sets still emit grand-total rows on empty input. Spark 4.2
    // drops them in this shape; retain the SQL aggregate semantics instead.
    cases.push(("SELECT grouping_id(v) AS gid,count(*) AS n FROM (SELECT 1 AS v WHERE FALSE) t GROUP BY GROUPING SETS ((v),(v),(),()) ORDER BY gid".into(), vec![1,1], 0));
    cases.push(("SELECT grouping_id(v,w) AS gid,count(*) AS n FROM (VALUES(1,2),(1,2)) t(v,w) GROUP BY ROLLUP(v,w),ROLLUP(v,w) ORDER BY grouping_id(v,w)".into(), vec![0,0,0,0,0,1,1,1,3], 2));
    for ansi in [true, false] {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (sql, expected, count) in &cases {
            let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            assert_eq!(named.fields, vec!["gid", "n"]);
            assert_eq!(named.plan.schema().field(0).data_type(), &DataType::Int64);
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let mut ids = Vec::new();
            for batch in batches {
                let groups = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(groups.null_count(), 0);
                assert_eq!(counts.null_count(), 0);
                for i in 0..batch.num_rows() {
                    ids.push(groups.value(i));
                    assert_eq!(counts.value(i), *count);
                }
            }
            assert_eq!(&ids, expected, "ansi={ansi}: {sql}");
        }
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn grouping_functions_resolve_original_keys_in_having_and_order() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::Int64Array;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let source = "(VALUES(CAST('-0' AS FLOAT),1),(CAST('0' AS FLOAT),2),(CAST('1' AS FLOAT),1),(CAST(NULL AS FLOAT),2)) t(v,w)";
    let ordinary = vec![(0, 1), (0, 1), (0, 2), (1, 4)];
    let cases = [
        ("ROLLUP(v)", "", "grouping_id(v),n", ordinary.clone()),
        ("CUBE(v)", "", "grouping(v),n", ordinary),
        (
            "ROLLUP(v)",
            "HAVING grouping_id(v)=1",
            "gid,n",
            vec![(1, 4)],
        ),
        ("ROLLUP(v)", "HAVING grouping(v)=1", "gid,n", vec![(1, 4)]),
        (
            "ROLLUP(v)",
            "HAVING v IS NULL",
            "gid,n",
            vec![(0, 1), (1, 4)],
        ),
        (
            "ROLLUP(v)",
            "HAVING count(*)>1",
            "grouping_id(v),n",
            vec![(0, 2), (1, 4)],
        ),
        (
            "ROLLUP(v),ROLLUP(v)",
            "HAVING grouping_id(v)=1",
            "gid,n",
            vec![(1, 4)],
        ),
        (
            "ROLLUP(v),ROLLUP(V)",
            "HAVING grouping_id(v)=1",
            "gid,n",
            vec![(1, 4)],
        ),
        (
            "GROUPING SETS((v),(v),())",
            "HAVING grouping_id(v)=0",
            "gid,n",
            vec![(0, 1), (0, 1), (0, 1), (0, 1), (0, 2), (0, 2)],
        ),
    ];
    for ansi in [true, false] {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (grouping, having, order, expected) in &cases {
            let sql = format!(
                "SELECT grouping_id(v) AS gid,count(*) AS n FROM {source} GROUP BY {grouping} {having} ORDER BY {order}"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            assert_eq!(named.fields, vec!["gid", "n"]);
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let mut actual = Vec::new();
            for batch in batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                actual.extend((0..batch.num_rows()).map(|i| (ids.value(i), counts.value(i))));
            }
            assert_eq!(&actual, expected, "ansi={ansi}: {sql}");
        }
        let sql = format!(
            "SELECT grouping_id(v) AS gid,count(*) AS n FROM {source} GROUP BY ROLLUP(v) ORDER BY grouping_id(w)"
        );
        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        assert!(resolver.resolve_named_plan(spec).await.is_err());
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn grouping_aliases_share_semantic_keys_and_keep_duplicate_rows() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::{Array, Int64Array};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    let mut cases = vec![
        ("SELECT v AS k1,v AS k2,grouping_id() AS gid,count(*) AS n FROM (VALUES(CAST('-0' AS FLOAT)),(CAST('0' AS FLOAT)),(CAST(NULL AS FLOAT))) t(v) GROUP BY ROLLUP(k1,k2) ORDER BY gid,n".to_string(), 2, vec![(0,1),(0,1),(0,2),(0,2),(1,3)]),
        ("SELECT v AS k1,v AS k2,grouping_id() AS gid,count(*) AS n FROM (VALUES(1),(NULL)) t(v) GROUP BY CUBE(k1,k2) ORDER BY gid,n".to_string(), 2, vec![(0,1),(0,1),(0,1),(0,1),(0,1),(0,1),(1,2)]),
        ("SELECT grouping_id(v,w) AS gid,count(*) AS n FROM (VALUES(1,2),(1,2)) t(v,w) GROUP BY ROLLUP(v,v,w) ORDER BY gid,n".to_string(), 0, vec![(0,2),(1,2),(1,2),(3,2)]),
        ("SELECT v AS k1,v AS k2,grouping_id() AS gid,count(*) AS n FROM (SELECT 1 AS v WHERE FALSE) t GROUP BY ROLLUP(k1,k2) ORDER BY gid,n".to_string(), 2, vec![(1,0)]),
    ];
    for count in [8, 16, 32, 63] {
        let columns = (0..count)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let reversed = (0..count)
            .rev()
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let values = vec!["1"; count].join(",");
        let (sets, ids) = if count == 63 {
            (
                format!("({columns}),({reversed}),()"),
                vec![(0, 2), (0, 2), (((1_u64 << count) - 1) as i64, 2)],
            )
        } else {
            (
                format!("({columns}),({reversed}),({columns},c0),()"),
                vec![(0, 2), (0, 2), (0, 2), (((1_u64 << count) - 1) as i64, 2)],
            )
        };
        cases.push((format!("SELECT grouping_id() AS gid,count(*) AS n FROM (VALUES({values}),({values})) t({columns}) GROUP BY GROUPING SETS ({sets}) ORDER BY gid,n"),0,ids));
    }
    for ansi in [true, false] {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(2)
                        .with_batch_size(2),
                )
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (sql, index, expected) in &cases {
            let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let mut rows = Vec::new();
            for batch in batches {
                let ids = batch
                    .column(*index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(index + 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ids.null_count(), 0);
                assert_eq!(counts.null_count(), 0);
                for row in 0..batch.num_rows() {
                    rows.push((ids.value(row), counts.value(row)));
                }
            }
            assert_eq!(&rows, expected, "ansi={ansi}: {sql}");
        }
        let columns = (0..63)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let values = vec!["1"; 63].join(",");
        let sql = format!(
            "SELECT grouping_id() FROM (VALUES({values})) t({columns}) GROUP BY GROUPING SETS (({columns}),({columns}),({columns}))"
        );
        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let named = resolver.resolve_named_plan(spec).await?;
        let error = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("require 65 bits"), "{error}");
        let columns = (0..65)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let values = vec!["1"; 65].join(",");
        for function in ["grouping_id()", "grouping(c0)"] {
            let sql = format!(
                "SELECT {function} FROM (VALUES({values})) t({columns}) GROUP BY ROLLUP({columns})"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            assert!(
                resolver
                    .resolve_named_plan(spec)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("GROUPING_SIZE_LIMIT_EXCEEDED")
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn mixed_grouping_functions_use_semantic_bit_order() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::{Array, Int64Array};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    for ansi in [true, false] {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(2)
                        .with_batch_size(1),
                )
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        let sql = "SELECT grouping_id(v,w) AS gid FROM (VALUES(1,2)) t(v,w) GROUP BY ROLLUP(v),ROLLUP(w) ORDER BY gid";
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let named = resolver.resolve_named_plan(spec).await?;
        named.plan.apply(|plan| {
            if let LogicalPlan::Aggregate(aggregate) = plan {
                assert!(
                    PlanResolver::grouping_key_order(aggregate, 2)
                        .unwrap()
                        .is_empty()
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        for count in [8, 16, 32, 63, 64] {
            let columns = (0..count)
                .map(|i| format!("c{i}"))
                .collect::<Vec<_>>()
                .join(",");
            let tail = (2..count)
                .map(|i| format!("c{i}"))
                .collect::<Vec<_>>()
                .join(",");
            let values = vec!["1"; count].join(",");
            let last = count - 1;
            let base = format!(
                "SELECT grouping_id() AS gid,grouping_id({columns}) AS explicit_gid,CAST(grouping(c0) AS BIGINT) AS first_missing,CAST(grouping(c{last}) AS BIGINT) AS last_missing,count(*) AS n FROM (VALUES({values}),({values})) t({columns}) GROUP BY GROUPING SETS ((c0),(c1)),GROUPING SETS (({tail}))"
            );
            let first = (1_u64 << (count - 1)) as i64;
            let second = (1_u64 << (count - 2)) as i64;
            let mut expected = vec![vec![first, first, 1, 0, 2], vec![second, second, 0, 0, 2]];
            expected.sort();
            for (suffix, expected) in [
                ("ORDER BY gid".to_string(), expected.clone()),
                (format!("ORDER BY grouping_id({columns})"), expected),
                (
                    "HAVING grouping(c0)=1 ORDER BY grouping_id()".to_string(),
                    vec![vec![first, first, 1, 0, 2]],
                ),
            ] {
                let sql = format!("{base} {suffix}");
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let named = resolver.resolve_named_plan(spec).await?;
                let batches = ctx
                    .execute_logical_plan(named.plan)
                    .await?
                    .collect()
                    .await?;
                let mut actual = Vec::new();
                for batch in batches {
                    assert!(
                        !batch
                            .schema()
                            .metadata()
                            .contains_key(GROUPING_KEY_ORDER_METADATA)
                    );
                    assert!(
                        batch.schema().fields().iter().all(|field| !field
                            .metadata()
                            .contains_key(GROUPING_KEY_ORDER_METADATA))
                    );
                    for row in 0..batch.num_rows() {
                        actual.push(
                            batch
                                .columns()
                                .iter()
                                .map(|column| {
                                    let column =
                                        column.as_any().downcast_ref::<Int64Array>().unwrap();
                                    assert!(!column.is_null(row));
                                    column.value(row)
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                assert_eq!(actual, expected, "ansi={ansi}: {sql}");
            }
        }
        let sql = "SELECT grouping_id(w,v) FROM (VALUES(1,2)) t(v,w) GROUP BY ROLLUP(v),ROLLUP(w)";
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        assert!(
            resolver
                .resolve_named_plan(spec)
                .await
                .unwrap_err()
                .to_string()
                .contains("GROUPING_ID_COLUMN_MISMATCH")
        );
    }
    Ok(())
}
