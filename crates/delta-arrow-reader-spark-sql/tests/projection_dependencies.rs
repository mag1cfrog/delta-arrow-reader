//! Projection fields and constraints survive DataFusion schema recomputation.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Constraint, Constraints, DFSchema, FunctionalDependencies, Result};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, Projection, col};

#[test]
fn projection_preserves_fields_and_remaps_existing_dependencies() -> Result<()> {
    let fields = Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("id", DataType::Int32, false),
    ]);
    for constraints in [
        None,
        Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            vec![1],
        )])),
    ] {
        let dependencies = FunctionalDependencies::new_from_constraints(constraints.as_ref(), 2);
        let input = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::try_from(fields.clone())?
                    .with_functional_dependencies(dependencies.clone())?,
            ),
        }));
        let expressions = vec![col("id").alias("key"), col("value").alias("payload")];
        let output = Projection::try_new(expressions, input)?;
        assert_eq!(
            output.schema.as_arrow().fields(),
            Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("payload", DataType::Int64, true),
            ])
            .fields(),
        );
        assert_eq!(
            output.schema.functional_dependencies(),
            &dependencies.project_functional_dependencies(&[1, 0], 2),
        );
    }
    Ok(())
}
