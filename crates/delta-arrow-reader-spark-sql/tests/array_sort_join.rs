//! Sorting and joining arrays preserve Spark's NULL values and schema guarantees.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type InputRow = (
    i32,
    Option<Vec<Option<String>>>,
    Option<String>,
    Option<String>,
);

fn references() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/array-sort-join-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn register_inputs(reference: &Value, batch_size: usize) -> TestResult<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    for table in reference["tables"].as_array().ok_or("missing tables")? {
        let rows: Vec<InputRow> = serde_json::from_value(table["rows"].clone())?;
        let fields = table["schema"]["fields"]
            .as_array()
            .ok_or("missing fields")?
            .iter()
            .map(|field| {
                let data_type = match field["type"].as_str() {
                    Some("integer") => DataType::Int32,
                    Some("string") => DataType::Utf8,
                    None if field["type"]["type"] == "array" => {
                        assert_eq!(field["type"]["elementType"], "string");
                        DataType::new_list(
                            DataType::Utf8,
                            field["type"]["containsNull"]
                                .as_bool()
                                .ok_or("missing containsNull")?,
                        )
                    }
                    _ => return Err("unexpected input type".into()),
                };
                Ok(Field::new(
                    field["name"].as_str().ok_or("missing field name")?,
                    data_type,
                    field["nullable"].as_bool().ok_or("missing nullable")?,
                ))
            })
            .collect::<TestResult<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        let DataType::List(element) = schema.field(1).data_type() else {
            return Err("expected array input".into());
        };
        let mut arrays = ListBuilder::new(StringBuilder::new()).with_field(element.clone());
        for (_, values, _, _) in &rows {
            if let Some(values) = values {
                for value in values {
                    arrays.values().append_option(value.as_deref());
                }
            }
            arrays.append(values.is_some());
        }
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(arrays.finish()),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|row| row.2.as_deref()),
                )),
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|row| row.3.as_deref()),
                )),
            ],
        )?;
        let batches = (0..input.num_rows())
            .step_by(batch_size)
            .map(|offset| input.slice(offset, batch_size.min(input.num_rows() - offset)))
            .collect();
        session.register_table(
            table["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
    }
    Ok(session)
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Null => json!("void"),
        DataType::Int32 => json!("integer"),
        DataType::Utf8 => json!("string"),
        DataType::List(field) => {
            json!({"type":"array", "elementType":spark_schema_type(field.data_type())?, "containsNull":field.is_nullable()})
        }
        other => return Err(format!("unexpected result type {other:?}").into()),
    })
}

fn encode_arrow_cell(array: &ArrayRef, row: usize) -> TestResult<Value> {
    if array.data_type() == &DataType::Null || array.is_null(row) {
        Ok(Value::Null)
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = list.value(row);
        Ok(Value::Array(
            (0..values.len())
                .map(|i| encode_arrow_cell(&values, i))
                .collect::<TestResult<_>>()?,
        ))
    } else {
        Ok(array_value_to_string(array.as_ref(), row)?.into())
    }
}

async fn execute_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let fields = physical.schema().fields().iter().enumerate().map(|(i, field)| {
            let logical = query.logical_plan.schema().field(i);
            assert_eq!(logical.data_type(), field.data_type());
            assert_eq!(logical.is_nullable(), field.is_nullable());
            assert_eq!(logical.metadata(), field.metadata());
        assert_eq!(query.column_names[i], *field.name());
        Ok(json!({"name":field.name(), "type":spark_schema_type(field.data_type())?, "nullable":field.is_nullable(), "metadata":field.metadata()}))
    }).collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| encode_arrow_cell(array, row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}

#[tokio::test]
async fn array_sort_and_join_match_spark_values_and_complete_schemas() -> TestResult {
    let records = references()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len(), 113);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 112);
    let mut failures = Vec::new();
    for batch_size in [1, 32] {
        let session = register_inputs(&records[0], batch_size)?;
        for case in &records[1..] {
            let actual = execute_query(&session, case).await;
            let expected = json!({"schema":case["schema"],"rows":case["rows"]});
            if actual.as_ref().ok() != Some(&expected) {
                failures.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_array_functions_keep_their_original_null_contract() -> TestResult {
    let records = references()?;
    let session = register_inputs(&records[0], 32)?;
    let query = session.sql("SELECT array_to_string(a,d,CAST(NULL AS VARCHAR)) AS joined, array_sort(a) AS sorted FROM nonnull ORDER BY id").await?;
    assert!(query.schema().field(0).is_nullable());
    assert!(query.schema().field(1).is_nullable());
    let batches = query.collect().await?;
    let actual = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| encode_arrow_cell(batch.column(0), row)))
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(
        actual,
        json!(["b,a,b", "", "", "c", "", "a", "b", ""])
            .as_array()
            .ok_or("expected array")?
            .clone()
    );
    Ok(())
}
