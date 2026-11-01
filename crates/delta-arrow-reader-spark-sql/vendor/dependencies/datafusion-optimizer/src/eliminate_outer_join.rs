// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`EliminateOuterJoin`] converts `LEFT/RIGHT/FULL` joins to `INNER` joins
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::{Join, JoinType, LogicalPlan};
use datafusion_expr::{Expr, Filter, Operator};

use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr::{BinaryExpr, Cast, InList, Like, TryCast};
use std::sync::Arc;

///
/// Attempt to replace outer joins with inner joins.
///
/// Outer joins are typically more expensive to compute at runtime
/// than inner joins and prevent various forms of predicate pushdown
/// and other optimizations, so removing them if possible is beneficial.
///
/// Inner joins filter out rows that do match. Outer joins pass rows
/// that do not match padded with nulls. If there is a filter in the
/// query that would filter any such null rows after the join the rows
/// introduced by the outer join are filtered.
///
/// For example, in the `select ... from a left join b on ... where b.xx = 100;`
///
/// For rows when `b.xx` is null (as it would be after an outer join),
/// the `b.xx = 100` predicate filters them out and there is no
/// need to produce null rows for output.
///
/// Generally, an outer join can be rewritten to inner join if the
/// filters from the WHERE clause return false while any inputs are
/// null and columns of those quals are come from nullable side of
/// outer join.
#[derive(Default, Debug)]
pub struct EliminateOuterJoin;

impl EliminateOuterJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to eliminate outer joins.
impl OptimizerRule for EliminateOuterJoin {
    fn name(&self) -> &str {
        "eliminate_outer_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(mut filter) => match Arc::unwrap_or_clone(filter.input) {
                LogicalPlan::Join(join) => {
                    let new_join_type = if join.join_type.is_outer() {
                        let left_non_nullable = rejects_null_input(
                            &filter.predicate,
                            NullRejectionContext::Predicate,
                            &mut |column, _| Ok(join.left.schema().has_column(column)),
                        )?;
                        let right_non_nullable = rejects_null_input(
                            &filter.predicate,
                            NullRejectionContext::Predicate,
                            &mut |column, _| Ok(join.right.schema().has_column(column)),
                        )?;
                        eliminate_outer(join.join_type, left_non_nullable, right_non_nullable)
                    } else {
                        join.join_type
                    };

                    let new_join = Arc::new(LogicalPlan::Join(Join {
                        left: join.left,
                        right: join.right,
                        join_type: new_join_type,
                        join_constraint: join.join_constraint,
                        on: join.on.clone(),
                        filter: join.filter.clone(),
                        schema: Arc::clone(&join.schema),
                        null_equality: join.null_equality,
                        null_aware: join.null_aware,
                    }));
                    Filter::try_new(filter.predicate, new_join)
                        .map(|f| Transformed::yes(LogicalPlan::Filter(f)))
                }
                filter_input => {
                    filter.input = Arc::new(filter_input);
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

pub fn eliminate_outer(
    join_type: JoinType,
    left_non_nullable: bool,
    right_non_nullable: bool,
) -> JoinType {
    let mut new_join_type = join_type;
    match join_type {
        JoinType::Left if right_non_nullable => {
            new_join_type = JoinType::Inner;
        }
        JoinType::Left => {}
        JoinType::Right if left_non_nullable => {
            new_join_type = JoinType::Inner;
        }
        JoinType::Right => {}
        JoinType::Full => {
            if left_non_nullable && right_non_nullable {
                new_join_type = JoinType::Inner;
            } else if left_non_nullable {
                new_join_type = JoinType::Left;
            } else if right_non_nullable {
                new_join_type = JoinType::Right;
            }
        }
        _ => {}
    }
    new_join_type
}

/// Whether NULL input must make a value NULL or prevent a predicate being true.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullRejectionContext {
    /// A WHERE predicate rejects both false and NULL.
    Predicate,
    /// An enclosing expression requires a NULL value, not merely false.
    Value,
}

/// Prove that NULL padding of one input is rejected without evaluating values.
///
/// `column_rejects_null` resolves each column in the supplied context. For a
/// direct join input it tests schema membership; a caller following projected
/// columns can resolve their definitions without copying expression trees.
/// Unknown expressions return false. OR requires both operands to reject the
/// same input, even when they reference different columns from that input.
pub fn rejects_null_input<F>(
    expr: &Expr,
    context: NullRejectionContext,
    column_rejects_null: &mut F,
) -> Result<bool>
where
    F: FnMut(&Column, NullRejectionContext) -> Result<bool>,
{
    use NullRejectionContext::{Predicate, Value};
    Ok(match expr {
        Expr::Column(column) => return column_rejects_null(column, context),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And if context == Predicate => {
                rejects_null_input(left, Predicate, column_rejects_null)?
                    || rejects_null_input(right, Predicate, column_rejects_null)?
            }
            Operator::And | Operator::Or => {
                rejects_null_input(left, context, column_rejects_null)?
                    && rejects_null_input(right, context, column_rejects_null)?
            }
            op if op.returns_null_on_null() => {
                rejects_null_input(left, Value, column_rejects_null)?
                    || rejects_null_input(right, Value, column_rejects_null)?
            }
            _ => false,
        },
        Expr::Not(arg) | Expr::Negative(arg) => {
            rejects_null_input(arg, Value, column_rejects_null)?
        }
        Expr::IsNotNull(arg) | Expr::IsTrue(arg) | Expr::IsFalse(arg) | Expr::IsNotUnknown(arg) => {
            context == Predicate && rejects_null_input(arg, Value, column_rejects_null)?
        }
        Expr::Cast(Cast { expr, field: _ }) | Expr::TryCast(TryCast { expr, field: _ }) => {
            rejects_null_input(expr, Value, column_rejects_null)?
        }
        Expr::InList(InList { expr, .. }) => rejects_null_input(expr, Value, column_rejects_null)?,
        Expr::Between(between) => rejects_null_input(&between.expr, Value, column_rejects_null)?,
        Expr::Like(Like { expr, pattern, .. }) => {
            rejects_null_input(expr, Value, column_rejects_null)?
                || rejects_null_input(pattern, Value, column_rejects_null)?
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{
        Operator::{And, Or},
        binary_expr, cast, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        not, try_cast,
    };

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateOuterJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn eliminate_left_with_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_null())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NULL
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_not_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_not_null())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NOT NULL
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_right_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t1.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) OR t1.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) AND t2.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // t2.b IN (1, 2, 3) rejects nulls — if t2.b is NULL the IN returns
        // NULL which is filtered out. So Left Join should become Inner Join.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32), lit(3u32)], false))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2), UInt32(3)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_in_list_containing_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IN list with NULL still rejects null input columns:
        // if t2.b is NULL, NULL IN (1, NULL) evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(ScalarValue::UInt32(None))], false))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(NULL)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_not_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // NOT IN also rejects nulls: if t2.b is NULL, NOT (NULL IN (...))
        // evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32)], true))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b NOT IN ([UInt32(1), UInt32(2)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // BETWEEN rejects nulls: if t2.b is NULL, NULL BETWEEN 1 AND 10
        // evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").between(lit(1u32), lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_right_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Right join: filter on left (nullable) side with BETWEEN should convert to Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").between(lit(1u32), lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Full join with BETWEEN on both sides should become Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").between(lit(1u32), lit(10u32)),
                And,
                col("t2.b").between(lit(5u32), lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b BETWEEN UInt32(1) AND UInt32(10) AND t2.b BETWEEN UInt32(5) AND UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Full join with IN filters on both sides should become Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").in_list(vec![lit(1u32), lit(2u32)], false),
                And,
                col("t2.b").in_list(vec![lit(3u32), lit(4u32)], false),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b IN ([UInt32(1), UInt32(2)]) AND t2.b IN ([UInt32(3), UInt32(4)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_in_list_or_is_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // WHERE (t2.b IN (1, 2)) OR (t2.b IS NULL)
        // The OR with IS NULL makes the predicate null-tolerant:
        // when t2.b is NULL, IS NULL returns true, so the whole OR is true.
        // The outer join must be preserved.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false),
                Or,
                col("t2.b").is_null(),
            ))?
            .build()?;

        // Should NOT be converted to Inner — OR with IS NULL preserves null rows
        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)]) OR t2.b IS NULL
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_like() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE rejects nulls: if t2.b is NULL, the result is NULL (filtered out)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").like(lit("%pattern%")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t2.b LIKE Utf8("%pattern%")
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_left_with_like_pattern_column() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE with nullable column on the pattern side:
        // 'x' LIKE t2.b → if t2.b is NULL, result is NULL (filtered out)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(lit("x").like(col("t2.b")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: Utf8("x") LIKE t2.b
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_full_with_like_cross_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE with columns from both sides: t1.c LIKE t2.b
        // If t1 is NULL → NULL LIKE t2.b → NULL → filtered out (left non-nullable)
        // If t2 is NULL → t1.c LIKE NULL → NULL → filtered out (right non-nullable)
        // Both sides are non-nullable → FULL → INNER
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.c").like(col("t2.b")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.c LIKE t2.b
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS TRUE rejects nulls: if the expression is NULL, IS TRUE returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS TRUE
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_false() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS FALSE rejects nulls: if the expression is NULL, IS FALSE returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_false())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS FALSE
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_not_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT UNKNOWN rejects nulls: if the expression is NULL, IS NOT UNKNOWN returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_not_unknown())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS NOT UNKNOWN
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_not_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT TRUE is NOT null-rejecting: if the expression is NULL,
        // IS NOT TRUE returns true, so null rows pass through
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_not_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS NOT TRUE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS UNKNOWN is NOT null-rejecting: if the expression is NULL,
        // IS UNKNOWN returns true, so null rows pass through
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_unknown())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS UNKNOWN
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // NOT(<x> IS TRUE) is equivalent to (<x> IS NOT TRUE): TRUE when
        // <x> is FALSE OR NULL. So `WHERE NOT((t2.b > 5) IS TRUE)` accepts
        // rows where t2.b is NULL (because t2.b > 5 is NULL → IS TRUE is
        // FALSE → NOT FALSE = TRUE). The LEFT JOIN must NOT be converted.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_true()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS TRUE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_false() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Same shape, IS FALSE: NOT(<x> IS FALSE) accepts NULL on the
        // inner column.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_false()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS FALSE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_not_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Same shape, IS NOT UNKNOWN: NOT(<x> IS NOT UNKNOWN) is
        // equivalent to (<x> IS UNKNOWN), which is TRUE when <x> is NULL.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_not_unknown()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS NOT UNKNOWN
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_type_cast() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                cast(col("t1.b"), DataType::Int64).gt(lit(10u32)),
                And,
                try_cast(col("t2.c"), DataType::Int64).lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: CAST(t1.b AS Int64) > UInt32(10) AND TRY_CAST(t2.c AS Int64) < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- FULL JOIN → LEFT / RIGHT tests -----
    #[test]
    fn eliminate_full_to_left_with_left_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with null-rejecting filter only on left side → LEFT JOIN
        // (left side becomes non-nullable, right side stays nullable)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").gt(lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_to_right_with_right_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with null-rejecting filter only on right side → RIGHT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)])
          Right Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_to_left_with_like() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with LIKE on left side only → LEFT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").like(lit("%val%")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t1.b LIKE Utf8("%val%")
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_full_to_right_with_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with IS TRUE on right side only → RIGHT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS TRUE
          Right Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- Nested AND / OR tests -----

    #[test]
    fn eliminate_left_with_and_multiple_null_rejecting() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Multiple null-rejecting predicates combined with AND on nullable side
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false),
                And,
                col("t2.c").between(lit(5u32), lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)]) AND t2.c BETWEEN UInt32(5) AND UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_or_same_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // OR of two null-rejecting predicates on different columns of the same
        // nullable side. If t2 rows are NULL (from LEFT JOIN), both t2.b and
        // t2.c are NULL, so the entire OR evaluates to NULL → filtered out.
        // This IS null-rejecting, so join should be eliminated.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").gt(lit(10u32)),
                Or,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) OR t2.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_or_cross_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // OR with columns from different sides — t1.b (preserved) OR t2.b
        // (nullable). When t2 is NULL, t1.b > 10 can still be true, so the
        // OR is NOT null-rejecting. Join must be preserved.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t2.b").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) OR t2.b < UInt32(20)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- Mixed predicate tests -----

    #[test]
    fn eliminate_full_with_mixed_predicates() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with different null-rejecting expr types on each side:
        // LIKE on left, BETWEEN on right → INNER JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").like(lit("%pattern%")),
                And,
                col("t2.b").between(lit(1u32), lit(10u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t1.b LIKE Utf8("%pattern%") AND t2.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_left_with_is_true_and_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // AND of IS TRUE and IN on nullable side — both null-rejecting
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").gt(lit(5u32)).is_true(),
                And,
                col("t2.c").in_list(vec![lit(1u32), lit(2u32)], false),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(5) IS TRUE AND t2.c IN ([UInt32(1), UInt32(2)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_arithmetic_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // t2.b * 2 + 1 > 10 is null-rejecting on t2.b: arithmetic
        // operators propagate NULL, so the whole expression is NULL when
        // t2.b is NULL, and NULL > 10 is filtered out by WHERE.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(
                binary_expr(
                    binary_expr(col("t2.b"), Operator::Multiply, lit(2u32)),
                    Operator::Plus,
                    lit(1u32),
                )
                .gt(lit(10u32)),
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b * UInt32(2) + UInt32(1) > UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_negative_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Unary minus propagates NULL: -NULL is NULL, so `WHERE -t2.b > 0`
        // is null-rejecting on t2.b.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(Expr::Negative(Box::new(col("t2.b"))).gt(lit(0u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: (- t2.b) > UInt32(0)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_distinct_from() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS DISTINCT FROM is NOT null-rejecting: t2.b IS DISTINCT FROM 5 is
        // true when t2.b is NULL (NULL is distinct from 5). Padding rows from
        // a LEFT JOIN would survive the filter, so the LEFT JOIN must stay.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b"),
                Operator::IsDistinctFrom,
                lit(5u32),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS DISTINCT FROM UInt32(5)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_not_distinct_from() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT DISTINCT FROM is also not null-rejecting: t2.b IS NOT
        // DISTINCT FROM NULL is true when t2.b is NULL. The LEFT JOIN must
        // stay.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b"),
                Operator::IsNotDistinctFrom,
                lit(ScalarValue::UInt32(None)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NOT DISTINCT FROM UInt32(NULL)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }
}
