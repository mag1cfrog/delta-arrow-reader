// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::{Column, JoinType, NullEquality};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, build_join_schema};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunctionInput, spark_binary_expr};
use crate::function::get_built_in_function;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

const IMPLICIT_CARTESIAN_PRODUCT_MSG: &str = "Detected implicit cartesian product for INNER join between logical plans. \
    Join condition is missing or trivial. \
    Either: use the CROSS JOIN syntax to allow cartesian products between \
    these relations, or: enable implicit cartesian products by setting the \
    configuration variable spark.sql.crossJoin.enabled=true;";

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_join(
        &self,
        join: spec::Join,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Join {
            left,
            right,
            join_type,
            join_criteria,
            join_data_type,
        } = join;
        let left = self
            .resolve_query_plan_with_hidden_fields(*left, state)
            .await?;
        let right = self
            .resolve_query_plan_with_hidden_fields(*right, state)
            .await?;
        let join_type = match join_type {
            spec::JoinType::Inner => Some(JoinType::Inner),
            spec::JoinType::LeftOuter => Some(JoinType::Left),
            spec::JoinType::RightOuter => Some(JoinType::Right),
            spec::JoinType::FullOuter => Some(JoinType::Full),
            spec::JoinType::LeftSemi => Some(JoinType::LeftSemi),
            spec::JoinType::RightSemi => Some(JoinType::RightSemi),
            spec::JoinType::LeftAnti => Some(JoinType::LeftAnti),
            spec::JoinType::RightAnti => Some(JoinType::RightAnti),
            spec::JoinType::Cross => None,
        };

        match (join_type, join_criteria) {
            (None, Some(_)) => Err(PlanError::invalid("cross join with join criteria")),
            // When the join criteria are not specified, any join type has the semantics of a cross join.
            (Some(_), None) | (None, None) => {
                if join_data_type.is_some() {
                    return Err(PlanError::invalid("cross join with join data type"));
                }
                // When the join type is not an explicit cross join and no join criteria are given,
                // we need to check whether implicit cartesian products are allowed.
                if join_type.is_some() && !self.config.cross_join_enabled {
                    return Err(PlanError::AnalysisError(
                        IMPLICIT_CARTESIAN_PRODUCT_MSG.to_string(),
                    ));
                }
                Ok(LogicalPlanBuilder::from(left).cross_join(right)?.build()?)
            }
            (Some(join_type), Some(spec::JoinCriteria::On(condition))) => {
                // Use Inner to build the schema for resolving the ON condition,
                // because the condition may reference columns from both sides.
                // Semi/anti/mark joins restrict the output schema, but the ON
                // condition still needs access to both sides.
                let join_schema = Arc::new(build_join_schema(
                    left.schema(),
                    right.schema(),
                    &JoinType::Inner,
                )?);
                let condition = self
                    .resolve_expression(condition, &join_schema, state)
                    .await?
                    .unalias_nested()
                    .data;

                let plan = LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(condition))?
                    .build()?;
                Ok(plan)
            }
            (Some(join_type), Some(spec::JoinCriteria::Natural)) => {
                let left_names = Self::get_field_names(left.schema(), state)?;
                let right_names = Self::get_field_names(right.schema(), state)?;
                let using = left_names
                    .iter()
                    .filter(|name| right_names.contains(name))
                    .map(|x| x.clone().into())
                    .collect::<Vec<_>>();
                // We let the column resolver return errors when either plan contains
                // duplicated columns for the natural join key.
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
            (Some(join_type), Some(spec::JoinCriteria::Using(using))) => {
                let join_columns =
                    self.resolve_query_join_using_columns(&left, &right, using, state)?;
                self.resolve_query_join_using(left, right, join_type, join_columns, state)
            }
        }
    }

    fn resolve_query_join_using_columns(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        using: Vec<spec::Identifier>,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<(String, (Column, Column))>> {
        using
            .into_iter()
            .map(|name| {
                let left_column = self.resolve_one_column(left.schema(), name.as_ref(), state)?;
                let right_column = self.resolve_one_column(right.schema(), name.as_ref(), state)?;
                Ok((name.into(), (left_column, right_column)))
            })
            .collect()
    }

    fn resolve_query_join_using(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        join_columns: Vec<(String, (Column, Column))>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let (left_columns, right_columns) = join_columns
            .iter()
            .map(|(_, (left, right))| (left.clone(), right.clone()))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        // USING and NATURAL need the same equality semantics as an ON predicate.
        // Use both inputs even when a semi/anti join outputs only one side.
        let comparison_schema = Arc::new(build_join_schema(
            left.schema(),
            right.schema(),
            &JoinType::Inner,
        )?);
        let (left_keys, right_keys): (Vec<_>, Vec<_>) = join_columns
            .iter()
            .map(|(_, (left, right))| {
                let Expr::BinaryExpr(comparison) = spark_binary_expr(
                    Expr::Column(left.clone()),
                    Operator::Eq,
                    Expr::Column(right.clone()),
                    &comparison_schema,
                    &self.config,
                )?
                else {
                    unreachable!("spark_binary_expr returns a binary expression")
                };
                Ok((*comparison.left, *comparison.right))
            })
            .collect::<PlanResult<Vec<_>>>()?
            .into_iter()
            .unzip();
        let unchanged = left_keys
            .iter()
            .zip(&left_columns)
            .chain(right_keys.iter().zip(&right_columns))
            .all(|(expr, column)| matches!(expr, Expr::Column(key) if key == column));
        let builder = if unchanged {
            LogicalPlanBuilder::from(left).join_detailed(
                right,
                join_type,
                (left_columns.clone(), right_columns.clone()),
                None,
                NullEquality::NullEqualsNothing,
            )?
        } else {
            LogicalPlanBuilder::from(left).join_with_expr_keys(
                right,
                join_type,
                (left_keys, right_keys),
                None,
            )?
        };
        // Re-register join key columns as hidden fields so that subsequent
        // attribute resolution can still find them.
        let mut columns = builder.schema().columns();
        // Qualified wildcards expose USING keys before the remaining columns,
        // in USING-list order, including keys retained from earlier joins.
        columns.sort_by_key(|col| {
            left_columns
                .iter()
                .position(|key| key == col)
                .or_else(|| right_columns.iter().position(|key| key == col))
                .unwrap_or(left_columns.len())
        });
        let hidden_columns = columns
            .into_iter()
            .map(|col| {
                if left_columns.iter().any(|x| x.name == col.name)
                    || right_columns.iter().any(|x| x.name == col.name)
                {
                    let info = state.get_field_info(col.name())?.clone();
                    let field_id = state.register_hidden_field_name(info.name());
                    let qualifier = col.relation.clone();
                    Ok(Expr::Column(col).alias_qualified(qualifier, field_id))
                } else {
                    Ok(Expr::Column(col))
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let builder = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right => {
                let uses_right = matches!(join_type, JoinType::Right);
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        let column = if uses_right { right } else { left };
                        Expr::Column(column).alias(state.register_field_name(name))
                    })
                    .chain(hidden_columns);
                builder.project(projections)?
            }
            JoinType::Full => {
                let join_schema = builder.schema().clone();
                let coalesce_function = get_built_in_function("coalesce")?;
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        let expression = coalesce_function(ScalarFunctionInput {
                            arguments: vec![Expr::Column(left), Expr::Column(right)],
                            function_context: FunctionContextInput {
                                argument_display_names: &[],
                                plan_config: &self.config,
                                session_context: self.ctx,
                                schema: &join_schema,
                            },
                        })?;
                        Ok(expression.alias(state.register_field_name(name)))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                builder.project(projections.into_iter().chain(hidden_columns))?
            }
            JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftMark
            | JoinType::RightMark => {
                let uses_right = matches!(
                    join_type,
                    JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
                );
                let projections = join_columns
                    .into_iter()
                    .map(|(name, (left, right))| {
                        let col = if uses_right { right } else { left };
                        Expr::Column(col).alias(state.register_field_name(name))
                    })
                    .chain(hidden_columns);
                builder.project(projections)?
            }
        };
        Ok(builder.build()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;
    use arrow::array::Int64Array;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn using_float_keys_match_zero_signs_but_not_nulls() -> PlanResult<()> {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        for dtype in ["FLOAT", "DOUBLE"] {
            for (join, using, expected) in [
                ("INNER JOIN", "USING(v)", vec![Some(0), Some(1)]),
                ("NATURAL INNER JOIN", "", vec![Some(0), Some(1)]),
                ("LEFT JOIN", "USING(v)", vec![Some(0), Some(1), Some(2)]),
                ("RIGHT JOIN", "USING(v)", vec![None, Some(0), Some(1)]),
                (
                    "FULL JOIN",
                    "USING(v)",
                    vec![None, Some(0), Some(1), Some(2)],
                ),
                ("LEFT SEMI JOIN", "USING(v)", vec![Some(0), Some(1)]),
                ("LEFT ANTI JOIN", "USING(v)", vec![Some(2)]),
            ] {
                let sql = format!(
                    "SELECT CAST(id AS BIGINT) AS id FROM \
                    (VALUES (0,CAST('-0.0' AS {dtype})),(1,CAST('0.0' AS {dtype})),(2,CAST(NULL AS {dtype}))) l(id,v) \
                    {join} (VALUES (9,CAST('0.0' AS {dtype})),(10,CAST(NULL AS {dtype}))) r(rid,v) {using} ORDER BY id"
                );
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let named = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()))
                    .resolve_named_plan(spec)
                    .await?;
                let frame = ctx.execute_logical_plan(named.plan).await?;
                let physical = frame.create_physical_plan().await?;
                assert!(
                    datafusion::physical_plan::displayable(physical.as_ref())
                        .indent(true)
                        .to_string()
                        .contains("HashJoinExec"),
                    "{sql}"
                );
                let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
                let mut rows = Vec::new();
                for batch in batches {
                    rows.extend(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter(),
                    );
                }
                assert_eq!(rows, expected, "{sql}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn qualified_using_keys_remain_scoped() -> PlanResult<()> {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let source =
            "(VALUES (1,10),(2,20)) l(id,v) FULL JOIN (VALUES (11,10),(12,30)) r(rid,v) USING(v)";
        let cases = [
            (
                format!("SELECT l.v,r.v FROM {source} ORDER BY l.v,r.v"),
                vec!["v", "v"],
                vec![
                    vec![None, Some(30)],
                    vec![Some(10), Some(10)],
                    vec![Some(20), None],
                ],
            ),
            (
                format!("SELECT l.* FROM {source} ORDER BY id,v"),
                vec!["v", "id"],
                vec![
                    vec![None, None],
                    vec![Some(10), Some(1)],
                    vec![Some(20), Some(2)],
                ],
            ),
            (
                format!("SELECT * FROM {source} ORDER BY v"),
                vec!["v", "id", "rid"],
                vec![
                    vec![Some(10), Some(1), Some(11)],
                    vec![Some(20), Some(2), None],
                    vec![Some(30), None, Some(12)],
                ],
            ),
            (
                format!(
                    "SELECT l.v,r.v FROM {source} NATURAL INNER JOIN (VALUES (10),(20),(30)) t(v) ORDER BY l.v,r.v"
                ),
                vec!["v", "v"],
                vec![
                    vec![None, Some(30)],
                    vec![Some(10), Some(10)],
                    vec![Some(20), None],
                ],
            ),
            (
                format!("SELECT s.v FROM (SELECT struct(l.*) AS s FROM {source}) q ORDER BY s.v"),
                vec!["v"],
                vec![vec![None], vec![Some(10)], vec![Some(20)]],
            ),
        ];
        for (sql, fields, expected) in cases {
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()))
                .resolve_named_plan(spec)
                .await?;
            assert_eq!(named.fields, fields, "{sql}");
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| {
                                column
                                    .as_any()
                                    .downcast_ref::<arrow::array::Int32Array>()
                                    .unwrap()
                                    .iter()
                                    .nth(row)
                                    .unwrap()
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            assert_eq!(rows, expected, "{sql}");
        }
        // Verify name resolution here. The SQL corpus separately records the
        // remaining physical-planning gap for this correlated scalar shape.
        let sql =
            format!("SELECT id,(SELECT s.v FROM (SELECT struct(l.*) AS s) q) AS v FROM {source}");
        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let named = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()))
            .resolve_named_plan(spec)
            .await?;
        assert_eq!(named.fields, vec!["id", "v"]);
        assert_eq!(
            named.plan.schema().field(1).data_type(),
            &arrow::datatypes::DataType::Int32
        );
        Ok(())
    }
}
