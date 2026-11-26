//! Compare wide projection properties with the original mapping path.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion::prelude::SessionContext;

fn projection_with_expected_properties(
    expressions: Vec<ProjectionExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let old = ProjectionExprs::from_expressions(expressions.clone());
    let mapping = old.projection_mapping(&input.schema())?;
    let schema = Arc::new(old.project_schema(&input.schema())?);
    let input_properties = input.equivalence_properties();
    let properties = input_properties.project(&mapping, Arc::clone(&schema));
    let partitions = input
        .output_partitioning()
        .project(&mapping, input_properties);

    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, Arc::clone(&input))?);
    let actual = projection.equivalence_properties();
    assert_eq!(projection.schema(), schema);
    assert_eq!(projection.output_partitioning(), &partitions);
    assert_eq!(
        projection.output_ordering(),
        properties.output_ordering().as_ref()
    );
    assert_eq!(projection.pipeline_behavior(), input.pipeline_behavior());
    assert_eq!(projection.boundedness(), input.boundedness());
    assert_eq!(actual.constraints(), properties.constraints());
    assert_eq!(
        format!("{:?}", actual.eq_group()),
        format!("{:?}", properties.eq_group()),
    );
    Ok(projection)
}

#[tokio::test]
async fn ordered_columns_retain_values_and_projected_properties() -> Result<()> {
    let fields = (0..80)
        .map(|i| Field::new(format!("column_{i}"), DataType::Int32, true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        (0..80)
            .map(|i| Arc::new(Int32Array::from(vec![Some(i), None])) as ArrayRef)
            .collect(),
    )?;
    let source = MemorySourceConfig::try_new(&[vec![batch.clone()]], Arc::clone(&schema), None)?;
    let unordered: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source.clone());
    let expressions = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), i)),
            alias: format!("renamed_{i}"),
        })
        .collect::<Vec<_>>();

    let projection =
        projection_with_expected_properties(expressions.clone(), Arc::clone(&unordered))?;
    let rows = collect(projection, SessionContext::new().task_ctx()).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns(), batch.columns());

    let mut reordered = expressions.clone();
    reordered.swap(0, 1);
    projection_with_expected_properties(reordered, Arc::clone(&unordered))?;

    let mut duplicate = expressions.clone();
    duplicate[1].expr = Arc::clone(&duplicate[0].expr);
    projection_with_expected_properties(duplicate, Arc::clone(&unordered))?;

    let order = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new(
        schema.field(0).name(),
        0,
    )))])
    .expect("nonempty ordering");
    let ordered: Arc<dyn ExecutionPlan> =
        DataSourceExec::from_data_source(source.try_with_sort_information(vec![order])?);
    assert!(ordered.output_ordering().is_some());
    projection_with_expected_properties(expressions.clone(), ordered)?;

    let mut invalid = expressions;
    invalid[0].expr = Arc::new(Column::new("different_name", 0));
    let expected = ProjectionExprs::from_expressions(invalid.clone())
        .projection_mapping(&schema)
        .expect_err("mismatched input field name");
    let actual =
        ProjectionExec::try_new(invalid, unordered).expect_err("mismatched input field name");
    assert_eq!(actual.to_string(), expected.to_string());
    Ok(())
}
