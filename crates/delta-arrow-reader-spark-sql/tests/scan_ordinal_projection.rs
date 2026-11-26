//! Scan ordinal comparison against the original ordered column lookup.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, TableScan, lit};
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::SessionContext;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(OptimizeProjections::new()
        .rewrite(plan, &OptimizerContext::new())?
        .data)
}

#[tokio::test]
async fn scan_renames_keep_source_ordinals_and_field_order() -> TestResult {
    let session = SessionContext::new();
    let input = RecordBatch::try_from_iter((0..401).map(|index| {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(index), None]));
        (format!("c{index}"), values)
    }))?;
    session.register_batch("wide_inputs", input)?;
    let LogicalPlan::TableScan(source) =
        session.table("wide_inputs").await?.into_unoptimized_plan()
    else {
        return Err("missing source scan".into());
    };
    let original_projection = (1..401).rev().collect::<Vec<_>>();
    let scan = Arc::new(LogicalPlan::TableScan(TableScan::try_new(
        source.table_name,
        source.source,
        Some(original_projection.clone()),
        source.filters,
        source.fetch,
    )?));
    let columns = scan.schema().columns();
    for (source_indices, selected_indices) in [
        ((0..400).collect::<Vec<_>>(), (0..400).collect::<Vec<_>>()),
        ((0..400).rev().collect(), (0..400).collect()),
        ((1..400).collect(), (1..400).collect()),
    ] {
        let exprs = source_indices
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                Expr::Column(columns[source].clone()).alias(format!("out{index}"))
            })
            .collect::<Vec<_>>();
        let projection = Projection::try_new(exprs.clone(), Arc::clone(&scan))?;
        let plan = rewrite(LogicalPlan::Projection(projection.clone()))?;
        let LogicalPlan::Projection(actual) = plan else {
            return Err("missing scan renames".into());
        };
        let LogicalPlan::TableScan(input) = actual.input.as_ref() else {
            return Err("missing scan".into());
        };
        // Resolve the columns through the original ordered lookup.
        let mut expected = exprs
            .into_iter()
            .map(|expr| match expr.unalias() {
                Expr::Column(column) => scan.schema().index_of_column(&column),
                _ => unreachable!("constructed a column rename"),
            })
            .collect::<Result<Vec<_>>>()?;
        expected.sort_unstable();
        assert_eq!(expected, selected_indices);
        assert_eq!(
            input.projection,
            Some(
                expected
                    .into_iter()
                    .map(|index| original_projection[index])
                    .collect()
            ),
        );
        assert_eq!(actual.expr, projection.expr);
        assert_eq!(actual.schema, projection.schema);
    }

    let LogicalPlan::TableScan(mut input) = scan.as_ref().clone() else {
        return Err("missing source scan".into());
    };
    let prefix_len = columns.len() - 1;
    input.projected_schema = Arc::new(DFSchema::from_field_specific_qualified_schema(
        vec![Some(input.table_name.clone()); prefix_len],
        input.projected_schema.inner(),
    )?);
    let exprs = columns[..prefix_len]
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, column)| Expr::Column(column).alias(format!("out{index}")))
        .chain([lit(-1).alias("tail")])
        .collect::<Vec<_>>();
    let plan = rewrite(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(LogicalPlan::TableScan(input)),
    )?))?;
    let LogicalPlan::Projection(out) = plan else {
        return Err("missing scan renames".into());
    };
    let LogicalPlan::TableScan(input) = out.input.as_ref() else {
        return Err("missing narrowed scan".into());
    };
    assert_eq!(
        input.projection.as_deref(),
        Some(&original_projection[..prefix_len])
    );
    Ok(())
}
