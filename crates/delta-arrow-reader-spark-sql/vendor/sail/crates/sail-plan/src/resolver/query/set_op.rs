use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::{Column, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::type_coercion::binary::type_union_resolution;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    Expr, ExprSchemable, Join, JoinConstraint, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
    WindowFrame, WindowFunctionDefinition, expr,
};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, SparkComparisonFloat};
use crate::function::result_coercion::coerce_result_values;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

mod constant_columns;
mod pending;
mod union;
use constant_columns::PreserveSetConstant;
mod filter_context;
use pending::PendingSetOperation;

impl PlanResolver<'_> {
    pub(in crate::resolver) fn lower_resolved_set_operations(
        &self,
        plan: LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Ok(pending::lower_query_sets(&plan, self.ctx.state(), state)?)
    }

    pub(super) async fn resolve_query_set_operation(
        &self,
        op: spec::SetOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::SetOpType;

        let spec::SetOperation {
            left,
            right,
            set_op_type,
            is_all,
            by_name,
            allow_missing_columns,
        } = op;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        let (left, right) = if matches!(set_op_type, SetOpType::Union) {
            (left, right)
        } else {
            self.coerce_set_operation_inputs(left, right)?
        };
        match set_op_type {
            SetOpType::Union => {
                let (left, right) = if by_name {
                    let left_names = Self::get_field_names(left.schema(), state)?;
                    let right_names = Self::get_field_names(right.schema(), state)?;
                    let (mut left_reordered_columns, mut right_reordered_columns): (
                        Vec<Expr>,
                        Vec<Expr>,
                    ) = left_names
                        .iter()
                        .enumerate()
                        .map(|(left_idx, left_name)| {
                            match right_names
                                .iter()
                                .position(|right_name| left_name.eq_ignore_ascii_case(right_name))
                            {
                                Some(right_idx) => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Column(Column::from(
                                        right.schema().qualified_field(right_idx),
                                    )),
                                )),
                                None if allow_missing_columns => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Literal(ScalarValue::Null, None)
                                        .alias(state.register_field_name(left_name)),
                                )),
                                None => Err(PlanError::invalid(format!(
                                    "right column not found: {left_name}"
                                ))),
                            }
                        })
                        .collect::<PlanResult<Vec<(Expr, Expr)>>>()?
                        .into_iter()
                        .unzip();
                    if allow_missing_columns {
                        let (left_extra_columns, right_extra_columns): (Vec<Expr>, Vec<Expr>) =
                            right_names
                                .into_iter()
                                .enumerate()
                                .filter(|(_, right_name)| {
                                    !left_names
                                        .iter()
                                        .any(|left_name| left_name.eq_ignore_ascii_case(right_name))
                                })
                                .map(|(right_idx, right_name)| {
                                    (
                                        Expr::Literal(ScalarValue::Null, None)
                                            .alias(state.register_field_name(right_name)),
                                        Expr::Column(Column::from(
                                            right.schema().qualified_field(right_idx),
                                        )),
                                    )
                                })
                                .collect::<Vec<(Expr, Expr)>>()
                                .into_iter()
                                .unzip();
                        right_reordered_columns.extend(right_extra_columns);
                        left_reordered_columns.extend(left_extra_columns);
                        (
                            project(left, left_reordered_columns)?,
                            project(right, right_reordered_columns)?,
                        )
                    } else {
                        (left, project(right, right_reordered_columns)?)
                    }
                } else {
                    (left, right)
                };
                let (left, right) = self.coerce_set_operation_inputs(left, right)?;
                Ok(LogicalPlan::Extension(datafusion_expr::Extension {
                    node: Arc::new(PendingSetOperation::new(left, right, set_op_type, is_all)?),
                }))
            }
            SetOpType::Intersect | SetOpType::Except => {
                Ok(LogicalPlan::Extension(datafusion_expr::Extension {
                    node: Arc::new(PendingSetOperation::new(left, right, set_op_type, is_all)?),
                }))
            }
        }
    }

    fn coerce_set_operation_inputs(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        let left_len = left.schema().fields().len();
        let right_len = right.schema().fields().len();
        if left_len != right_len {
            return Err(PlanError::invalid(format!(
                "Set operation inputs must have the same number of columns: left has {left_len}, right has {right_len}"
            )));
        }
        let schema = Arc::new(merge_schema(&[&left, &right]));
        let context = FunctionContextInput {
            argument_display_names: &[],
            plan_config: &self.config,
            session_context: self.ctx,
            schema: &schema,
        };
        let mut left_columns = Vec::with_capacity(left_len);
        let mut right_columns = Vec::with_capacity(right_len);
        for index in 0..left_len {
            let columns = [&left, &right]
                .into_iter()
                .map(|input| Expr::from(input.schema().qualified_field(index)))
                .collect();
            let columns = coerce_result_values(columns, &context)?;
            let types = columns
                .iter()
                .map(|column| column.get_type(&schema))
                .collect::<Result<Vec<_>, _>>()?;
            let target = type_union_resolution(&types).ok_or_else(|| {
                PlanError::invalid(format!(
                    "Incompatible set-operation column types: {types:?}"
                ))
            })?;
            for (column, (input, output)) in columns
                .into_iter()
                .zip([(&left, &mut left_columns), (&right, &mut right_columns)])
            {
                let (qualifier, field) = input.schema().qualified_field(index);
                let column = column.cast_to(&target, &schema)?;
                output.push(if column == Expr::from((qualifier, field)) {
                    column
                } else {
                    column.alias_qualified_with_metadata(
                        qualifier.cloned(),
                        field.name(),
                        Some(field.metadata().clone().into()),
                    )
                });
            }
        }
        let [left, right] =
            [(left, left_columns), (right, right_columns)].map(|(input, columns)| {
                if columns.iter().all(|column| column.try_as_col().is_some()) {
                    Ok(input)
                } else {
                    project(input, columns).map_err(PlanError::from)
                }
            });
        Ok((left?, right?))
    }
}

fn lower_set_operation(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: JoinType,
    is_all: bool,
    constant_columns: &[Option<PreserveSetConstant>],
    under_limit_one: bool,
    state: &mut PlanResolverState,
) -> datafusion_common::Result<LogicalPlan> {
    let result_exprs = set_result_expressions(&left, constant_columns, under_limit_one)?;
    let mut join_keys = set_operation_comparison_keys(&left)
        .into_iter()
        .zip(set_operation_comparison_keys(&right))
        .collect::<Vec<_>>();
    let (left, right) = if is_all {
        let left_row_number_alias = state.register_field_name("row_num");
        let right_row_number_alias = state.register_field_name("row_num");
        let left = add_duplicate_row_number(left, &left_row_number_alias)?;
        let right = add_duplicate_row_number(right, &right_row_number_alias)?;
        join_keys.push((
            Expr::Column(Column::from_name(left_row_number_alias)),
            Expr::Column(Column::from_name(right_row_number_alias)),
        ));
        (left, right)
    } else if under_limit_one {
        (left, right)
    } else {
        (distinct_set_operation_rows(left)?, right)
    };
    let join = Join::try_new(
        Arc::new(left),
        Arc::new(right),
        join_keys,
        None,
        join_type,
        JoinConstraint::On,
        NullEquality::NullEqualsNull,
        false,
    )?;
    Ok(LogicalPlanBuilder::from(LogicalPlan::Join(join))
        .project(result_exprs)?
        .build()?)
}

fn set_result_expressions(
    input: &LogicalPlan,
    constant_columns: &[Option<PreserveSetConstant>],
    under_limit_one: bool,
) -> datafusion_common::Result<Vec<Expr>> {
    input
        .schema()
        .iter()
        .enumerate()
        .map(|(index, (qualifier, field))| {
            let column = Expr::from((qualifier, field));
            if under_limit_one
                || !matches!(field.data_type(), DataType::Float32 | DataType::Float64)
            {
                return Ok(column);
            }
            let result = if let Some(constant) = &constant_columns[index] {
                ScalarUDF::from(constant.clone()).call(vec![column])
            } else {
                ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![column])
            };
            Ok(result.alias_qualified_with_metadata(
                qualifier.cloned(),
                field.name(),
                Some(field.metadata().clone().into()),
            ))
        })
        .collect()
}

// Normalize comparisons after coercion, without rewriting the returned values.
fn set_operation_comparison_keys(input: &LogicalPlan) -> Vec<Expr> {
    input
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::from((qualifier, field));
            if matches!(field.data_type(), DataType::Float32 | DataType::Float64) {
                ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![column])
            } else {
                column
            }
        })
        .collect()
}

fn distinct_set_operation_rows(input: LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    let keys = set_operation_comparison_keys(&input);
    if keys.iter().all(|key| matches!(key, Expr::Column(_))) {
        return Ok(LogicalPlanBuilder::from(input).distinct()?.build()?);
    }
    let fields = input.schema().clone();
    let representatives = fields
        .columns()
        .into_iter()
        .map(|column| first_value(Expr::Column(column), vec![]));
    // Keep original representatives; building the aggregate also preserves its nullable result schema.
    let aggregate = LogicalPlanBuilder::from(input)
        .aggregate(keys, representatives)?
        .build()?;
    let output = aggregate
        .schema()
        .iter()
        .skip(fields.fields().len())
        .zip(fields.iter())
        .map(|((qualifier, field), (output_qualifier, output_field))| {
            Expr::from((qualifier, field)).alias_qualified_with_metadata(
                output_qualifier.cloned(),
                output_field.name(),
                Some(output_field.metadata().clone().into()),
            )
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(aggregate)
        .project(output)?
        .build()?)
}

// Pair equal rows by occurrence so each right row matches at most one left row.
fn add_duplicate_row_number(
    input: LogicalPlan,
    column_name: &str,
) -> datafusion_common::Result<LogicalPlan> {
    let row_number = Expr::WindowFunction(Box::new(expr::WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
        params: WindowFunctionParams {
            args: vec![],
            partition_by: set_operation_comparison_keys(&input),
            order_by: vec![],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: Some(NullTreatment::RespectNulls),
            distinct: false,
        },
    }))
    .alias(column_name);
    Ok(LogicalPlanBuilder::from(input)
        .window(vec![row_number])?
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::DataType;
    use arrow::util::display::array_value_to_string;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn union_by_name_aligns_columns_before_coercion() -> PlanResult<()> {
        let session = SessionContext::new();
        for missing in [false, true] {
            let right = if missing {
                "SELECT CAST(2 AS BIGINT) AS id,CAST(3 AS SMALLINT) AS extra"
            } else {
                "SELECT 'second' AS label,CAST(2 AS BIGINT) AS id"
            };
            let ast = sail_sql_analyzer::parser::parse_one_statement(&format!(
                "SELECT 1 AS id,'first' AS label UNION ALL {right}"
            ))?;
            let mut query = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let spec::QueryNode::SetOperation(operation) = &mut query.node else {
                return Err(PlanError::invalid("expected UNION query"));
            };
            operation.by_name = true;
            operation.allow_missing_columns = missing;
            let named = PlanResolver::new(&session, Arc::new(Default::default()))
                .resolve_named_plan(query)
                .await?;
            assert_eq!(named.plan.schema().field(0).data_type(), &DataType::Int64);
            assert_eq!(named.plan.schema().field(1).data_type(), &DataType::Utf8);
            let mut expected_names = vec!["id", "label"];
            let mut expected_rows = vec![
                vec![Some("1"), Some("first")],
                vec![Some("2"), Some("second")],
            ];
            if missing {
                assert_eq!(named.plan.schema().field(2).data_type(), &DataType::Int16);
                expected_names.push("extra");
                expected_rows = vec![
                    vec![Some("1"), Some("first"), None],
                    vec![Some("2"), None, Some("3")],
                ];
            }
            assert_eq!(named.fields, expected_names);
            let physical = session.state().create_physical_plan(&named.plan).await?;
            let mut rows = Vec::new();
            for batch in collect(physical, session.task_ctx()).await? {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| {
                                if column
                                    .logical_nulls()
                                    .is_some_and(|nulls| nulls.is_null(row))
                                {
                                    Ok(None)
                                } else {
                                    array_value_to_string(column.as_ref(), row).map(Some)
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
            rows.sort();
            let rows = rows
                .iter()
                .map(|row| row.iter().map(|value| value.as_deref()).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            assert_eq!(rows, expected_rows);
        }
        Ok(())
    }
}
