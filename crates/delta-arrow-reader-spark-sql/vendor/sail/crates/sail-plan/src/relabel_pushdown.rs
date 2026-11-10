// Local Spark session-zone relabel pushdown. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
//! A TIMESTAMP column whose type names another zone than the session's is read through a cast
//! that relabels it with the session zone (#1012). The relabel keeps every instant, so a NULL
//! test, a comparison of instants or an IN list of instants means the same over the stored
//! column. A scan that prunes
//! only with bare columns, as the Delta provider does, needs the predicate on the stored column
//! (#1096).
//!
//! DataFusion's filter pushdown moves a filter below the relabel, replacing the column with the
//! cast. On the next pass its simplifier removes the cast from a comparison or IN list with
//! literals, but not from `IS NULL` or `IS NOT NULL`, and common subexpression elimination first
//! moves a cast
//! the filter reads twice, as in `BETWEEN` or an IN list beside HOUR, into a projection where
//! neither rule sees it. This rule runs right after filter pushdown and removes the relabel from
//! NULL tests and from comparisons and IN lists with TIMESTAMP literals, in filters and in the
//! filters pushed to a scan, retagging the literals with the stored zone. Functions of the relabeled value, such as HOUR, keep it.
//! Native DataFusion SQL, planned without Spark's expression semantics, is unchanged.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::optimizer::{ApplyOrder, Optimizer};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::logical_plan::Filter;
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};

use crate::expression_errors::SparkSqlExpressionSemantics;

#[derive(Debug, Default)]
pub struct UnwrapSessionZoneRelabel;

impl UnwrapSessionZoneRelabel {
    /// Add the rule to `optimizer` right after filter pushdown, or last without it.
    /// `configure_spark_sql` adds it to DataFusion's default optimizer.
    pub fn install(optimizer: &mut Optimizer) {
        let position = optimizer
            .rules
            .iter()
            .position(|rule| rule.name() == "push_down_filter")
            .map_or(optimizer.rules.len(), |position| position + 1);
        optimizer.rules.insert(position, Arc::new(Self));
    }
}

impl OptimizerRule for UnwrapSessionZoneRelabel {
    fn name(&self) -> &str {
        "unwrap_session_zone_relabel"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if config
            .options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
        {
            return Ok(Transformed::no(plan));
        }
        match plan {
            LogicalPlan::Filter(filter) => {
                let schema = Arc::clone(filter.input.schema());
                let predicate = unwrap_relabels(filter.predicate.clone(), &schema)?;
                if !predicate.transformed {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    predicate.data,
                    filter.input,
                )?)))
            }
            LogicalPlan::TableScan(mut scan) if !scan.filters.is_empty() => {
                let schema = Arc::new(DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(),
                    &scan.source.schema(),
                )?);
                let mut transformed = false;
                scan.filters = std::mem::take(&mut scan.filters)
                    .into_iter()
                    .map(|filter| {
                        let filter = unwrap_relabels(filter, &schema)?;
                        transformed |= filter.transformed;
                        Ok(filter.data)
                    })
                    .collect::<Result<_>>()?;
                Ok(Transformed::new_transformed(
                    LogicalPlan::TableScan(scan),
                    transformed,
                ))
            }
            plan => Ok(Transformed::no(plan)),
        }
    }
}

/// `expr` with the relabel removed from each NULL test over a relabeled column and each
/// comparison or IN list of one with TIMESTAMP literals, and the literals retagged with the
/// column's zone.
fn unwrap_relabels(expr: Expr, schema: &DFSchemaRef) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        Ok(match expr {
            Expr::IsNull(value) => match stored_column(&value, schema) {
                Some(column) => Transformed::yes(Expr::IsNull(Box::new(column))),
                None => Transformed::no(Expr::IsNull(value)),
            },
            Expr::IsNotNull(value) => match stored_column(&value, schema) {
                Some(column) => Transformed::yes(Expr::IsNotNull(Box::new(column))),
                None => Transformed::no(Expr::IsNotNull(value)),
            },
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_instant_comparison(op) => {
                match (stored_column(&left, schema), stored_column(&right, schema)) {
                    (Some(column), None) => match retagged(&right, &column, schema)? {
                        Some(literal) => Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(column),
                            op,
                            Box::new(literal),
                        ))),
                        None => Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })),
                    },
                    (None, Some(column)) => match retagged(&left, &column, schema)? {
                        Some(literal) => Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(literal),
                            op,
                            Box::new(column),
                        ))),
                        None => Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })),
                    },
                    _ => Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })),
                }
            }
            Expr::InList(in_list) => match stored_column(&in_list.expr, schema) {
                Some(column) => {
                    let list = in_list
                        .list
                        .iter()
                        .map(|item| retagged(item, &column, schema))
                        .collect::<Result<Option<Vec<_>>>>()?;
                    match list {
                        Some(list) => Transformed::yes(Expr::InList(InList::new(
                            Box::new(column),
                            list,
                            in_list.negated,
                        ))),
                        None => Transformed::no(Expr::InList(in_list)),
                    }
                }
                None => Transformed::no(Expr::InList(in_list)),
            },
            expr => Transformed::no(expr),
        })
    })
}

/// Comparisons whose result depends only on the instants compared.
fn is_instant_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// The stored column under a relabel: a cast of a zoned TIMESTAMP column to the same unit in
/// another zone, named or a fixed offset. Arrow's cast keeps the stored values.
fn stored_column(expr: &Expr, schema: &DFSchemaRef) -> Option<Expr> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    let column @ Expr::Column(_) = cast.expr.as_ref() else {
        return None;
    };
    match (column.get_type(schema).ok()?, cast.field.data_type()) {
        (DataType::Timestamp(stored, Some(_)), DataType::Timestamp(relabeled, Some(_)))
            if stored == *relabeled =>
        {
            Some(column.clone())
        }
        _ => None,
    }
}

/// A TIMESTAMP literal of the stored column's unit as a literal of its type: the same instant
/// with the stored zone.
fn retagged(expr: &Expr, column: &Expr, schema: &DFSchemaRef) -> Result<Option<Expr>> {
    let Expr::Literal(value, metadata) = expr else {
        return Ok(None);
    };
    let target = column.get_type(schema)?;
    let retagged = match (value, &target) {
        (ScalarValue::TimestampSecond(v, Some(_)), DataType::Timestamp(_, zone)) => {
            ScalarValue::TimestampSecond(*v, zone.clone())
        }
        (ScalarValue::TimestampMillisecond(v, Some(_)), DataType::Timestamp(_, zone)) => {
            ScalarValue::TimestampMillisecond(*v, zone.clone())
        }
        (ScalarValue::TimestampMicrosecond(v, Some(_)), DataType::Timestamp(_, zone)) => {
            ScalarValue::TimestampMicrosecond(*v, zone.clone())
        }
        (ScalarValue::TimestampNanosecond(v, Some(_)), DataType::Timestamp(_, zone)) => {
            ScalarValue::TimestampNanosecond(*v, zone.clone())
        }
        _ => return Ok(None),
    };
    if retagged.data_type() != target {
        return Ok(None);
    }
    Ok(Some(Expr::Literal(retagged, metadata.clone())))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::optimizer::optimizer::Optimizer;
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr::{Expr, cast, col, lit};

    use super::{UnwrapSessionZoneRelabel, unwrap_relabels};

    fn zoned(unit: TimeUnit, zone: Option<&str>) -> DataType {
        DataType::Timestamp(unit, zone.map(Into::into))
    }

    fn rewritten(expr: Expr) -> Result<(bool, Expr)> {
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("t", zoned(TimeUnit::Microsecond, Some("UTC")), true),
            Field::new("ms", zoned(TimeUnit::Millisecond, Some("UTC")), true),
            Field::new("tn", zoned(TimeUnit::Microsecond, None), true),
            Field::new("k", DataType::Int64, true),
        ]))?);
        let rewritten = unwrap_relabels(expr, &schema)?;
        Ok((rewritten.transformed, rewritten.data))
    }

    fn la() -> DataType {
        zoned(TimeUnit::Microsecond, Some("America/Los_Angeles"))
    }

    #[test]
    fn relabels_unwrap_to_the_stored_column_and_zone() -> Result<()> {
        let instant = |zone: &str| {
            lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_094_200_000_000),
                Some(zone.into()),
            ))
        };
        assert_eq!(
            rewritten(cast(col("t"), la()).is_null())?,
            (true, col("t").is_null())
        );
        assert_eq!(
            rewritten(instant("America/Los_Angeles").lt(cast(col("t"), la())))?,
            (true, instant("UTC").lt(col("t")))
        );
        // A NULL test or comparison nested in a larger predicate, and a NULL TIMESTAMP.
        let null_instant =
            |zone: &str| lit(ScalarValue::TimestampMicrosecond(None, Some(zone.into())));
        assert_eq!(
            rewritten(
                cast(col("t"), la())
                    .is_not_null()
                    .and(cast(col("t"), la()).not_eq(null_instant("America/Los_Angeles")))
                    .or(col("k").is_null())
            )?,
            (
                true,
                col("t")
                    .is_not_null()
                    .and(col("t").not_eq(null_instant("UTC")))
                    .or(col("k").is_null())
            )
        );
        assert_eq!(
            rewritten(cast(col("t"), la()).in_list(
                vec![
                    instant("America/Los_Angeles"),
                    null_instant("America/Los_Angeles")
                ],
                true,
            ))?,
            (
                true,
                col("t").in_list(vec![instant("UTC"), null_instant("UTC")], true)
            )
        );
        Ok(())
    }

    #[test]
    fn the_rule_follows_filter_pushdown_or_comes_last() {
        let names = |optimizer: &Optimizer| {
            optimizer
                .rules
                .iter()
                .map(|rule| rule.name().to_owned())
                .collect::<Vec<_>>()
        };
        let mut optimizer = Optimizer::new();
        let mut expected = names(&optimizer);
        let position = expected
            .iter()
            .position(|name| name == "push_down_filter")
            .map(|position| position + 1);
        assert!(position.is_some_and(|position| position < expected.len()));
        expected.insert(
            position.unwrap_or_default(),
            "unwrap_session_zone_relabel".into(),
        );
        UnwrapSessionZoneRelabel::install(&mut optimizer);
        assert_eq!(names(&optimizer), expected);

        let mut optimizer = Optimizer::with_rules(vec![Arc::new(SimplifyExpressions::new())]);
        UnwrapSessionZoneRelabel::install(&mut optimizer);
        assert_eq!(
            names(&optimizer),
            ["simplify_expressions", "unwrap_session_zone_relabel"]
        );
    }

    #[test]
    fn casts_that_change_values_or_operands_that_are_not_literals_keep_the_cast() -> Result<()> {
        for expr in [
            // A change of unit, a local timestamp read as an instant and an instant read as a
            // local timestamp change the value.
            cast(col("ms"), la()).is_null(),
            cast(col("tn"), la()).is_null(),
            cast(col("t"), zoned(TimeUnit::Microsecond, None)).is_null(),
            cast(col("k"), la()).is_null(),
            // The other operand is not a TIMESTAMP literal of the column's unit.
            cast(col("t"), la()).eq(cast(col("tn"), la())),
            cast(col("t"), la()).eq(lit(ScalarValue::TimestampMillisecond(
                Some(1),
                Some("America/Los_Angeles".into()),
            ))),
            cast(col("t"), la()).eq(lit(ScalarValue::Null)),
            cast(col("t"), la()).eq(lit(1_i64)),
            cast(col("t"), la()).in_list(
                vec![
                    lit(ScalarValue::TimestampMicrosecond(
                        Some(1),
                        Some("America/Los_Angeles".into()),
                    )),
                    cast(col("tn"), la()),
                ],
                false,
            ),
        ] {
            assert_eq!(rewritten(expr.clone())?, (false, expr));
        }
        Ok(())
    }
}
