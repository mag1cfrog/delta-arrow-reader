//! Compare ordered scan aliases with the original projection field resolver.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::logical_expr::logical_plan::projection_schema;
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::prelude::SessionContext;

fn original_schema(input: &LogicalPlan, expressions: &[Expr]) -> Result<DFSchema> {
    assert!(input.schema().functional_dependencies().is_empty());
    DFSchema::new_with_metadata(
        exprlist_to_fields(expressions, input)?,
        input.schema().metadata().clone(),
    )
}

fn assert_same_schema(input: &LogicalPlan, expressions: &[Expr]) {
    let expected = original_schema(input, expressions);
    let actual = projection_schema(input, expressions);
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(&expected, actual.as_ref()),
        (Err(expected), Err(actual)) => assert_eq!(expected.to_string(), actual.to_string()),
        (expected, actual) => {
            panic!("schema outcomes differ: {expected:?} compared with {actual:?}")
        }
    }
}

#[tokio::test]
async fn ordered_scan_aliases_keep_schema_values_and_fallbacks() -> Result<()> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for index in 0..80 {
        let (kind, array): (_, ArrayRef) = if index % 2 == 0 {
            (
                DataType::Int32,
                Arc::new(Int32Array::from(vec![Some(index), None, Some(-index)])),
            )
        } else {
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![
                    Some(i64::from(index)),
                    None,
                    Some(0),
                ])),
            )
        };
        fields.push(
            Field::new(format!("column_{index}"), kind, true)
                .with_metadata(HashMap::from([("input".to_string(), index.to_string())])),
        );
        columns.push(array);
    }
    let schema = Schema::new_with_metadata(
        fields,
        HashMap::from([("dataset".to_string(), "ordered_renames".to_string())]),
    );
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    let session = SessionContext::new();
    session.register_batch("wide_source", batch)?;
    let input = Arc::new(session.table("wide_source").await?.into_unoptimized_plan());
    assert!(matches!(input.as_ref(), LogicalPlan::TableScan(_)));

    let expressions = input
        .schema()
        .columns()
        .into_iter()
        .enumerate()
        .map(|(index, column)| {
            let qualifier = (index % 2 == 0).then(|| TableReference::bare("renamed"));
            Expr::Column(column).alias_qualified(qualifier, format!("alias_{index}"))
        })
        .collect::<Vec<_>>();
    assert_same_schema(&input, &expressions);

    let expected = Projection::try_new_with_schema(
        expressions.clone(),
        Arc::clone(&input),
        Arc::new(original_schema(&input, &expressions)?),
    )?;
    let actual = Projection::try_new(expressions.clone(), Arc::clone(&input))?;
    assert_eq!(expected.schema, actual.schema);
    let expected = session
        .execute_logical_plan(LogicalPlan::Projection(expected))
        .await?
        .collect()
        .await?;
    let actual = session
        .execute_logical_plan(LogicalPlan::Projection(actual))
        .await?
        .collect()
        .await?;
    assert_eq!(expected, actual);

    assert_same_schema(&input, &expressions[..63]);

    let mut reordered = expressions.clone();
    reordered.swap(0, 1);
    assert_same_schema(&input, &reordered);

    let mut with_metadata = expressions.clone();
    let column = input.schema().columns().remove(0);
    let metadata = FieldMetadata::from(&HashMap::from([(
        "annotation".to_string(),
        "alias value".to_string(),
    )]));
    with_metadata[0] = Expr::Column(column).alias_qualified_with_metadata(
        None::<TableReference>,
        "alias_0",
        Some(metadata),
    );
    assert_same_schema(&input, &with_metadata);

    let mut missing = expressions;
    missing[0] = Expr::Column(Column::new(
        Some(TableReference::bare("wide_source")),
        "missing_column",
    ))
    .alias("alias_0");
    assert_same_schema(&input, &missing);
    Ok(())
}
