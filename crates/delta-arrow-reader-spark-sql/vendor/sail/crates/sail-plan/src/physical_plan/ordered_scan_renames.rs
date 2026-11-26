// Added for DAR Spark SQL. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
//! Lower wide scan renames whose input columns already match scan order.

use std::collections::HashSet;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::{Column, DFSchemaRef, Result, internal_datafusion_err, internal_err};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, Projection, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;

pub(super) fn lower_ordered_scan_renames(plan: LogicalPlan, spark_semantics: bool) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(projection)
            if spark_semantics && renames_in_scan_order(&projection) =>
        {
            LogicalPlan::Extension(Extension {
                node: Arc::new(OrderedScanRenames { projection }),
            })
        }
        plan => plan,
    }
}

fn column_rename(expression: &Expr) -> Option<(&Column, &str)> {
    let Expr::Alias(alias) = expression else {
        return None;
    };
    let Expr::Column(column) = alias.expr.as_ref() else {
        return None;
    };
    Some((column, &alias.name))
}

fn renames_in_scan_order(projection: &Projection) -> bool {
    let width = projection.expr.len();
    if width < 64 || projection.schema.fields().len() != width {
        return false;
    }
    let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        return false;
    };
    let schema = &scan.projected_schema;
    if !scan.filters.is_empty() || schema.fields().len() != width {
        return false;
    }
    // Unique, fully qualified names make the existing name lookup select this ordinal.
    let mut names = HashSet::with_capacity(width);
    schema
        .iter()
        .zip(projection.expr.iter().zip(projection.schema.fields()))
        .all(|((qualifier, input), (expression, output))| {
            column_rename(expression).is_some_and(|(column, alias)| {
                qualifier == Some(&scan.table_name)
                    && qualifier == column.relation.as_ref()
                    && input.name() == &column.name
                    && output.name() == alias
                    && names.insert(input.name().as_str())
            })
        })
        && names.len() == width
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(super) struct OrderedScanRenames {
    projection: Projection,
}

impl fmt::Debug for OrderedScanRenames {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(formatter)
    }
}

impl UserDefinedLogicalNodeCore for OrderedScanRenames {
    fn name(&self) -> &str {
        "OrderedScanRenames"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.projection.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.projection.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.projection.expr.clone()
    }

    fn fmt_for_explain(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {} columns",
            self.name(),
            self.projection.expr.len()
        )
    }

    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let [input] = inputs.try_into().map_err(|_| {
            internal_datafusion_err!("Ordered scan renames require exactly one logical input")
        })?;
        let projection = Projection::try_new(expressions, Arc::new(input))?;
        if !renames_in_scan_order(&projection) {
            return internal_err!("Ordered scan renames require the original column order");
        }
        Ok(Self { projection })
    }
}

impl OrderedScanRenames {
    pub(super) fn create_physical_projection(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let expressions = self
            .projection
            .expr
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                let (column, alias) = column_rename(expression).ok_or_else(|| {
                    internal_datafusion_err!("Ordered scan rename lost its column")
                })?;
                Ok(ProjectionExpr {
                    expr: Arc::new(PhysicalColumn::new(&column.name, index)),
                    alias: alias.to_owned(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::TableScan;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::physical_plan::SparkExtensionPlanner;

    #[tokio::test]
    async fn wide_scan_renames_match_default_lowering() -> Result<()> {
        let session = SessionContext::new();
        let batch = RecordBatch::try_from_iter((0..66).map(|index| {
            let array: ArrayRef = if index % 2 == 0 {
                Arc::new(Int32Array::from(vec![Some(index), None]))
            } else {
                Arc::new(Int64Array::from(vec![Some(i64::from(index)), None]))
            };
            (format!("c{index}"), array)
        }))?;
        session.register_batch("wide", batch)?;
        let LogicalPlan::TableScan(scan) = session.table("wide").await?.into_unoptimized_plan()
        else {
            return internal_err!("Expected a table scan");
        };
        let scan = Arc::new(LogicalPlan::TableScan(TableScan::try_new(
            scan.table_name,
            scan.source,
            Some((1..66).rev().collect()),
            scan.filters,
            scan.fetch,
        )?));
        let expr = scan
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(index, column)| Expr::Column(column).alias(format!("result{index}")))
            .collect::<Vec<_>>();
        let projection =
            LogicalPlan::Projection(Projection::try_new(expr.clone(), Arc::clone(&scan))?);
        let ordinary = lower_ordered_scan_renames(projection.clone(), false);
        assert_eq!(ordinary, projection);
        let lowered = lower_ordered_scan_renames(projection, true);
        assert!(matches!(lowered, LogicalPlan::Extension(_)));
        let state = session.state();
        let before = DefaultPhysicalPlanner::default()
            .create_physical_plan(&ordinary, &state)
            .await?;
        let after =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SparkExtensionPlanner)])
                .create_physical_plan(&lowered, &state)
                .await?;
        assert_eq!(before.schema(), after.schema());
        assert_eq!(
            displayable(before.as_ref()).indent(true).to_string(),
            displayable(after.as_ref()).indent(true).to_string()
        );
        assert_eq!(
            collect(before, session.task_ctx()).await?,
            collect(after, session.task_ctx()).await?
        );

        let mut reordered = expr;
        reordered.swap(0, 1);
        let fallback = LogicalPlan::Projection(Projection::try_new(reordered, scan)?);
        assert_eq!(lower_ordered_scan_renames(fallback.clone(), true), fallback);
        Ok(())
    }
}
