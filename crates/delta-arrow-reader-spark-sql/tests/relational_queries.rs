//! Relational queries preserve duplicate rows, SQL NULLs and named result types.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn relational_queries_match_independent_references() -> TestResult {
    let records = include_str!("fixtures/relational-queries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 128);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
        assert_eq!(
            case.get("sql_null_rows"),
            approved_in_null_rows(case).as_ref()
        );
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
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
        register_relational_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = execute_relational_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] == "ok" => {
                    let fields = expected["schema"]["fields"]
                        .as_array()
                        .ok_or("missing reference fields")?
                        .iter()
                        .map(
                            |f| json!({"name":f["name"],"type":f["type"],"metadata":f["metadata"]}),
                        )
                        .collect::<Vec<_>>();
                    let rows = case.get("sql_null_rows").unwrap_or(&expected["rows"]);
                    actual["rows"] == *rows && actual["fields"] == json!(fields)
                }
                Err(error) if expected["status"] == "error" => {
                    expected["condition"] == "SCALAR_SUBQUERY_TOO_MANY_ROWS"
                        && error
                            .to_string()
                            .contains("Scalar subquery returned more than one row")
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

// The approved projected-IN policy retains SQL three-valued results, including empty inputs.
fn approved_in_null_rows(case: &Value) -> Option<Value> {
    let values = match case["id"].as_str()? {
        "in" => [None, None, Some("true"), Some("true"), None],
        "not_in" => [None, None, Some("false"), Some("false"), None],
        "in_empty" => [Some("false"); 5],
        "not_in_empty" => [Some("true"); 5],
        _ => return None,
    };
    Some(json!(
        [None, Some("1"), Some("2"), Some("2"), Some("4")]
            .into_iter()
            .zip(values)
            .map(|(key, found)| json!([key, found]))
            .collect::<Vec<_>>()
    ))
}

fn register_relational_inputs(
    session: &SessionContext,
    reference: &Value,
    batch_size: usize,
) -> TestResult {
    for source in reference["inputs"].as_array().ok_or("missing inputs")? {
        let names: Vec<String> = serde_json::from_value(source["columns"].clone())?;
        let types: Vec<String> = serde_json::from_value(source["types"].clone())?;
        let rows: Vec<Vec<Value>> = serde_json::from_value(source["rows"].clone())?;
        assert_eq!(names.len(), types.len());
        assert!(rows.iter().all(|row| row.len() == names.len()));
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, (name, data_type)) in names.iter().zip(&types).enumerate() {
            let values = json!(
                rows.iter()
                    .map(|row| row[index].clone())
                    .collect::<Vec<_>>()
            );
            let array: ArrayRef = match data_type.as_str() {
                "integer" => Arc::new(Int32Array::from(
                    serde_json::from_value::<Vec<Option<i32>>>(values)?,
                )),
                "long" => Arc::new(Int64Array::from(
                    serde_json::from_value::<Vec<Option<i64>>>(values)?,
                )),
                "string" => Arc::new(StringArray::from_iter(serde_json::from_value::<
                    Vec<Option<String>>,
                >(values)?)),
                "double" => {
                    let values: Vec<Option<String>> = serde_json::from_value(values)?;
                    let values = values
                        .iter()
                        .map(|v| v.as_deref().map(str::parse::<f64>).transpose())
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(Float64Array::from(values))
                }
                other => return Err(format!("unexpected input type: {other}").into()),
            };
            fields.push(Field::new(name, array.data_type().clone(), true));
            columns.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let input = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let batches = if input.num_rows() == 0 {
            vec![input]
        } else {
            (0..input.num_rows())
                .step_by(batch_size)
                .map(|start| input.slice(start, batch_size.min(input.num_rows() - start)))
                .collect()
        };
        session.register_table(
            source["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;
    }
    Ok(())
}

async fn execute_relational_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    assert_eq!(schema.fields().len(), query.column_names.len());
    let fields = schema
        .fields()
        .iter()
        .zip(&query.column_names)
        .enumerate()
        .map(|(index, (field, name))| {
            assert_eq!(field.name(), name);
            let logical_field = query.logical_plan.schema().field(index);
            assert_eq!(field.data_type(), logical_field.data_type());
            assert_eq!(field.metadata(), logical_field.metadata());
            let data_type = match field.data_type() {
                DataType::Int32 => "integer".into(),
                DataType::Int64 => "long".into(),
                DataType::Boolean => "boolean".into(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string".into(),
                DataType::Float64 => "double".into(),
                DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
                other => return Err(format!("unexpected output type: {other}").into()),
            };
            Ok(json!({"name":name,"type":data_type,"metadata":field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                let value = if column.is_null(row) {
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                    assert!(schema.field(index).is_nullable());
                    Value::Null
                } else if column.data_type() == &DataType::Float64 {
                    let value = column.as_primitive::<Float64Type>().value(row);
                    json!(if value.is_nan() {
                        "nan".into()
                    } else if value == 0.0 {
                        "0".into()
                    } else {
                        value.to_string()
                    })
                } else {
                    json!(array_value_to_string(column.as_ref(), row)?)
                };
                values.push(value);
            }
            rows.push(values);
        }
    }
    Ok(json!({"rows":rows,"fields":fields}))
}
