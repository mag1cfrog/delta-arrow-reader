//! STRUCT validation preserves native dictionary fields through public SQL lambdas.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, SparkSqlQuery, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
#[expect(dead_code, reason = "Only the shared schema and batch validators encode this Arrow contract.")]
mod spark_results;
use spark_results::TestResult;

fn provider(nested: bool, names: &[&str], batch_size: usize) -> TestResult<MemTable> {
    let fields: Vec<_> = names.iter().map(|name| {
        Arc::new(Field::new(*name, DataType::Int32, true).with_metadata(
            HashMap::from([("source".into(), "dictionary_item".into())]),
        ))
    }).collect();
    let columns = names.iter().enumerate().map(|(index, _)| {
        Arc::new(Int32Array::from(if index == 0 {
            vec![Some(7), None]
        } else {
            vec![Some(70), Some(8)]
        })) as ArrayRef
    }).collect();
    let values: ArrayRef = Arc::new(StructArray::try_new(fields.into(), columns, None)?);
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
        values,
    )?);
    let values = if nested {
        let field = Arc::new(Field::new("nested", dictionary.data_type().clone(), true));
        Arc::new(StructArray::try_new(vec![field].into(), vec![dictionary], None)?) as ArrayRef
    } else {
        dictionary
    };
    let list: ArrayRef = Arc::new(ListArray::try_new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([2, 1, 0, 0, 1]),
        values,
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    )?);
    let schema = Arc::new(Schema::new(vec![Field::new("items", list.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![list])?;
    let batches = (0..batch.num_rows()).step_by(batch_size)
        .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
        .collect();
    Ok(MemTable::try_new(schema, vec![batches])?)
}

async fn check_result(session: &SessionContext, query: SparkSqlQuery) -> TestResult {
    let value_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
    let expected = Field::new(
        "result",
        DataType::List(Arc::new(Field::new_list_field(value_type, true))),
        true,
    );
    assert_eq!(query.column_names, ["result"]);
    assert_eq!(query.logical_plan.schema().fields().len(), 1);
    assert_eq!(query.logical_plan.schema().field(0).clone().with_name("result"), expected);
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    assert_eq!(schema.as_ref(), &Schema::new(vec![expected]));
    spark_results::validate_physical_schema(
        query.logical_plan.schema().fields(), &query.column_names, schema.fields(),
    )?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        let list = batch.column(0).as_any().downcast_ref::<ListArray>()
            .ok_or("expected List array")?;
        for row in 0..list.len() {
            if list.is_null(row) {
                rows.push(Value::Null);
            } else {
                let values = list.value(row);
                let decoded = arrow::compute::cast(values.as_ref(), &DataType::Int32)?;
                let integers = decoded.as_any().downcast_ref::<Int32Array>()
                    .ok_or("expected decoded INT values")?;
                rows.push(json!(integers.iter().collect::<Vec<_>>()));
            }
        }
    }
    rows.sort_by_key(Value::to_string);
    // These are the provider's dictionary keys and values, including NULL and empty lists.
    assert_eq!(rows, vec![json!([7,null]), json!([7]), json!([]), json!([null]), Value::Null]);
    Ok(())
}

#[tokio::test]
async fn dictionary_struct_fields_keep_native_types_and_shared_validation() -> TestResult {
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new().with_default_features().with_config(
                    SessionConfig::new().with_batch_size(batch_size).with_target_partitions(1),
                ),
            ).build(),
        );
        for (layout, nested) in [("direct", false), ("nested", true)] {
            for (case, names, child, condition) in [
                ("original", vec!["item"], "item", None),
                ("case", vec!["Item"], "item", None),
                ("missing", vec!["item"], "missing", Some("FIELD_NOT_FOUND")),
                ("duplicate", vec!["item", "ITEM"], "item", Some("AMBIGUOUS_REFERENCE_TO_FIELDS")),
            ] {
                let table = format!("dictionary_{layout}_{case}");
                session.register_table(table.as_str(), Arc::new(provider(nested, &names, batch_size)?))?;
                for ansi in [true, false] {
                    comparisons += 1;
                    let field = if nested { format!("x.nested.{child}") } else { format!("x.{child}") };
                    let sql = format!("SELECT TRANSFORM(items,x -> {field}) AS result FROM {table}");
                    let planned = plan_query(
                        &session,
                        &sql,
                        &SparkSqlOptions { ansi_mode: ansi, session_timezone: "UTC".into(),
                            ..SparkSqlOptions::default() },
                    ).await;
                    let comparison = if let Some(condition) = condition {
                        match planned {
                            Err(DataFusionError::External(source))
                                if matches!(source.downcast_ref::<PlanError>(),
                                    Some(PlanError::AnalysisError(message)) if message.starts_with(
                                        &format!("cannot resolve attribute: [{condition}] field {child:?} ")
                                    )) => Ok(()),
                            Err(error) => Err(format!("expected planning {condition}, got {error}").into()),
                            Ok(_) => Err(format!("expected planning {condition}, got a logical plan").into()),
                        }
                    } else {
                        match planned {
                            Ok(query) => check_result(&session, query).await,
                            Err(error) => Err(error.into()),
                        }
                    };
                    if let Err(error) = comparison {
                        differences.push(format!("{table} ANSI={ansi} batch={batch_size}: {error}"));
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 32);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
