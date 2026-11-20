//! Wide projection merges retain DataFusion's ordered column lookup and native results.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Column, DFSchema, Result, TableReference};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, Projection, lit};
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::configure_spark_sql;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(OptimizeProjections::new()
        .rewrite(plan, &OptimizerContext::new())?
        .data)
}

fn resolution_schemas(width: usize) -> Result<Vec<(&'static str, DFSchema)>> {
    let qualifiers = [
        None,
        Some(TableReference::bare("t")),
        Some(TableReference::partial("s", "t")),
        Some(TableReference::full("cat", "s", "t")),
    ];
    let mixed = DFSchema::new_with_metadata(
        (0..width)
            .map(|index| {
                (
                    qualifiers[index % qualifiers.len()].clone(),
                    Arc::new(Field::new(format!("c{index}"), DataType::Int32, true)),
                )
            })
            .collect(),
        HashMap::new(),
    )?;
    let qualified_duplicates = DFSchema::new_with_metadata(
        (0..width)
            .map(|index| {
                let (qualifier, name) = if index < qualifiers.len() {
                    (
                        [
                            TableReference::full("cat", "s", "t"),
                            TableReference::bare("t"),
                            TableReference::partial("s", "t"),
                            TableReference::bare("other"),
                        ][index]
                            .clone(),
                        "shared".to_owned(),
                    )
                } else {
                    (TableReference::bare("t"), format!("c{index}"))
                };
                (
                    Some(qualifier),
                    Arc::new(Field::new(name, DataType::Int32, true)),
                )
            })
            .collect(),
        HashMap::new(),
    )?;
    // Arrow-to-DFSchema conversion admits duplicate fields used by aggregate states.
    let unqualified_duplicates = DFSchema::try_from(Schema::new(
        (0..width)
            .map(|index| {
                Field::new(
                    if index < 2 {
                        "shared".to_owned()
                    } else {
                        format!("c{index}")
                    },
                    DataType::Int32,
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ))?;
    Ok(vec![
        ("mixed qualifiers", mixed),
        ("qualified duplicates", qualified_duplicates),
        ("unqualified duplicates", unqualified_duplicates),
    ])
}

#[test]
fn projection_merge_matches_original_column_lookup() -> TestResult {
    let qualifiers = [
        None,
        Some(TableReference::bare("t")),
        Some(TableReference::partial("s", "t")),
        Some(TableReference::full("cat", "s", "t")),
        Some(TableReference::full("other_cat", "s", "t")),
        Some(TableReference::full("cat", "other_schema", "t")),
        Some(TableReference::bare("other")),
    ];
    for (input_width, output_width) in
        [(63, 63), (64, 63), (63, 64), (64, 64), (65, 65), (400, 400)]
    {
        for (label, schema) in resolution_schemas(input_width)? {
            let schema = Arc::new(schema);
            let leaf = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(DFSchema::empty()),
            }));
            let prior_exprs = schema
                .iter()
                .enumerate()
                .map(|(index, (qualifier, field))| {
                    lit(index as i32).alias_qualified(qualifier.cloned(), field.name())
                })
                .collect::<Vec<_>>();
            let prior = Arc::new(LogicalPlan::Projection(Projection::try_new_with_schema(
                prior_exprs.clone(),
                Arc::clone(&leaf),
                Arc::clone(&schema),
            )?));
            let output_schema = Arc::new(DFSchema::try_from(Schema::new(
                (0..output_width)
                    .map(|index| Field::new(format!("out{index}"), DataType::Int32, true))
                    .collect::<Vec<_>>(),
            ))?);
            for name in ["c0", "c1", "c2", "c3", "shared", "missing", "C0"] {
                for qualifier in &qualifiers {
                    let requests = (0..output_width)
                        .map(|index| {
                            if index == 0 {
                                Column::new(qualifier.clone(), name)
                            } else {
                                Column::from(schema.qualified_field(index % input_width))
                            }
                        })
                        .collect::<Vec<_>>();
                    let plan = LogicalPlan::Projection(Projection::try_new_with_schema(
                        requests
                            .iter()
                            .enumerate()
                            .map(|(index, column)| {
                                Expr::Column(column.clone()).alias(format!("out{index}"))
                            })
                            .collect(),
                        Arc::clone(&prior),
                        Arc::clone(&output_schema),
                    )?);
                    // The oracle uses the original ordered DFSchema lookup for every request.
                    let expected = requests
                        .iter()
                        .enumerate()
                        .map(|(index, column)| {
                            let old_index = schema.index_of_column(column)?;
                            Ok(prior_exprs[old_index]
                                .clone()
                                .unalias_nested()
                                .data
                                .alias(format!("out{index}")))
                        })
                        .collect::<Result<Vec<_>>>()
                        .and_then(|exprs| {
                            Projection::try_new(exprs, Arc::clone(&leaf))
                                .map(LogicalPlan::Projection)
                        })
                        .and_then(rewrite);
                    let actual = rewrite(plan);
                    let context =
                        format!("{label}, {input_width}/{output_width}, {qualifier:?}, {name}");
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected, "{context}"),
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.to_string(), expected.to_string(), "{context}")
                        }
                        (actual, expected) => {
                            return Err(format!("{context}: {actual:?} != {expected:?}").into());
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn projection_merge_keeps_repeated_computations() -> TestResult {
    let prior = Arc::new(LogicalPlan::Projection(Projection::try_new(
        (0..64)
            .map(|index| (lit(index) + lit(1)).alias(format!("c{index}")))
            .collect(),
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        })),
    )?));
    let plan = LogicalPlan::Projection(Projection::try_new(
        (0..64)
            .map(|index| {
                Expr::Column(Column::new_unqualified(format!("c{}", index / 2)))
                    .alias(format!("out{index}"))
            })
            .collect(),
        prior,
    )?);
    let LogicalPlan::Projection(top) = rewrite(plan)? else {
        return Err("missing outer projection".into());
    };
    let LogicalPlan::Projection(prior) = top.input.as_ref() else {
        return Err("repeated computation was inlined".into());
    };
    assert_eq!(prior.expr.len(), 32);
    assert!(
        prior
            .expr
            .iter()
            .all(|expr| { matches!(expr.clone().unalias(), Expr::BinaryExpr(_)) })
    );
    Ok(())
}

#[tokio::test]
async fn native_projection_merges_preserve_values_nulls_and_executed_types() -> TestResult {
    let width = 400;
    let input = RecordBatch::try_from_iter((0..width).map(|index| {
        let values: ArrayRef = match index % 3 {
            0 => Arc::new(Int32Array::from(vec![Some(index as i32), None, Some(-7)])),
            1 => Arc::new(Int64Array::from(vec![Some(index as i64), None, Some(-9)])),
            _ => Arc::new(StringArray::from(vec![Some("value"), None, Some("")])),
        };
        (format!("c{index}"), values)
    }))?;
    let expected = RecordBatch::try_from_iter(
        (0..width)
            .rev()
            .map(|index| (format!("out{index}"), Arc::clone(input.column(index)))),
    )?;
    for configured in [false, true] {
        let builder = SessionStateBuilder::new().with_default_features();
        let session = SessionContext::new_with_state(if configured {
            configure_spark_sql(builder).build()
        } else {
            builder.build()
        });
        session.register_batch("wide_inputs", input.clone())?;
        let qualifier = TableReference::full("datafusion", "public", "wide_inputs");
        let first = session.table("wide_inputs").await?.select(
            (0..width)
                .map(|index| {
                    Expr::Column(Column::new(Some("wide_inputs"), format!("c{index}")))
                        .alias_qualified(Some(qualifier.clone()), format!("c{index}"))
                })
                .collect::<Vec<_>>(),
        )?;
        let frame = first.select(
            (0..width)
                .rev()
                .map(|index| {
                    Expr::Column(Column::new(Some(qualifier.clone()), format!("c{index}")))
                        .alias(format!("out{index}"))
                })
                .collect::<Vec<_>>(),
        )?;
        let plan = frame.clone().into_unoptimized_plan();
        let LogicalPlan::Projection(top) = &plan else {
            return Err("missing outer projection".into());
        };
        assert!(matches!(top.input.as_ref(), LogicalPlan::Projection(_)));
        let LogicalPlan::Projection(merged) = rewrite(plan)? else {
            return Err("missing merged projection".into());
        };
        assert!(matches!(merged.input.as_ref(), LogicalPlan::TableScan(_)));
        assert_eq!(
            merged.expr,
            (0..width)
                .rev()
                .map(|index| {
                    Expr::Column(Column::new(Some("wide_inputs"), format!("c{index}")))
                        .alias(format!("out{index}"))
                })
                .collect::<Vec<_>>(),
        );
        let physical = frame.create_physical_plan().await?;
        let executed_schema = physical.schema();
        assert_eq!(
            executed_schema,
            expected.schema(),
            "configured={configured}"
        );
        let batches = collect(physical, session.task_ctx()).await?;
        let actual = concat_batches(&executed_schema, &batches)?;
        assert_eq!(actual, expected, "configured={configured}");
    }
    Ok(())
}
