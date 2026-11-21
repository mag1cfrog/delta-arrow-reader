// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::{DFSchema, exec_err};
use datafusion_expr::{Expr, LogicalPlan, Projection};

/// Wraps a logical plan in a projection that aliases each column to a new name.
pub fn rename_logical_plan(
    plan: LogicalPlan,
    names: &[String],
) -> datafusion_common::Result<LogicalPlan> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for logical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let expr = plan
        .schema()
        .columns()
        .into_iter()
        .zip(names.iter())
        .map(|(column, name)| {
            let relation = column.relation.clone();
            Expr::Column(column).alias_qualified(relation, name)
        })
        .collect();
    // Each qualified lookup would find this same field in a wide, unique scan.
    let input = plan.schema();
    if names.len() >= 64
        && input.functional_dependencies().is_empty()
        && let LogicalPlan::TableScan(scan) = &plan
    {
        let mut input_names = HashSet::with_capacity(names.len());
        if input.iter().all(|(qualifier, field)| {
            qualifier == Some(&scan.table_name) && input_names.insert(field.name())
        }) {
            let renamed = input
                .iter()
                .zip(names)
                .map(|((qualifier, field), name)| {
                    (
                        qualifier.cloned(),
                        Arc::new(field.as_ref().clone().with_name(name)),
                    )
                })
                .collect();
            let schema = DFSchema::new_with_metadata(renamed, input.metadata().clone())?;
            return Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                expr,
                Arc::new(plan),
                Arc::new(schema),
            )?));
        }
    }
    // The logical plan schema requires field names to be unique.
    // To support duplicate field names, construct the physical plan directly.
    Ok(LogicalPlan::Projection(Projection::try_new(
        expr,
        Arc::new(plan),
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion_common::{Constraint, Constraints, Result, TableReference};
    use datafusion_expr::TableScan;

    use super::*;

    #[test]
    fn wide_scan_rename_matches_normal_projection_fields_and_errors() -> Result<()> {
        let fields = (0..64)
            .map(|index| {
                Field::new(format!("c{index}"), DataType::Int32, index != 0).with_metadata(
                    HashMap::from([("note".to_owned(), format!("field {index}"))]),
                )
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("note".to_owned(), "input".to_owned())]),
        ));
        for constraints in [
            Constraints::default(),
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        ] {
            let table =
                MemTable::try_new(Arc::clone(&schema), vec![vec![]])?.with_constraints(constraints);
            let scan = LogicalPlan::TableScan(TableScan::try_new(
                TableReference::full("catalog", "schema", "table"),
                provider_as_source(Arc::new(table)),
                Some((0..64).rev().collect()),
                vec![],
                None,
            )?);
            let names = (0..64).map(|i| format!("new_{i}")).collect::<Vec<_>>();
            let reference = |names: &[String]| {
                let fields = scan
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(names)
                    .map(|(column, name)| {
                        let relation = column.relation.clone();
                        Expr::Column(column).alias_qualified(relation, name)
                    })
                    .collect();
                Projection::try_new(fields, Arc::new(scan.clone())).map(LogicalPlan::Projection)
            };
            let actual = rename_logical_plan(scan.clone(), &names)?;
            let expected = reference(&names)?;
            assert_eq!(actual, expected);
            assert_eq!(actual.schema(), expected.schema());

            let duplicate_names = vec!["duplicate".to_owned(); 64];
            assert_eq!(
                rename_logical_plan(scan.clone(), &duplicate_names)
                    .unwrap_err()
                    .to_string(),
                reference(&duplicate_names).unwrap_err().to_string(),
            );
        }
        Ok(())
    }
}
