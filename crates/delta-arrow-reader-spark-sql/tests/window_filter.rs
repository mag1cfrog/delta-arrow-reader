//! Aggregate window FILTER selects rows within each frame without removing output rows.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, StringArray,
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

#[derive(serde::Deserialize)]
struct WindowFilterInputRow {
    id: i32,
    group: i32,
    predicate: Option<bool>,
    keep: Option<bool>,
    raw: String,
    value: Option<i32>,
}

#[tokio::test]
async fn window_filters_match_spark_frames_results_and_errors() -> TestResult {
    let records = include_str!("fixtures/window-filter-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 220);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
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
        register_window_filter_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = execute_window_filter_query(&session, case).await;
            let expected = filter_preserving_expected(case);
            let matches = match &actual {
                Ok(actual) => actual == expected,
                Err(error) => {
                    let message = error.to_string();
                    match expected["condition"].as_str() {
                        Some("CAST_INVALID_INPUT") => {
                            message.contains("Cannot cast string 'bad'")
                                || message
                                    .contains("[CAST_INVALID_INPUT] Cannot cast STRING to BOOLEAN")
                        }
                        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => {
                            message.contains("COUNT_IF requires a BOOLEAN argument")
                        }
                        Some("INVALID_AGGREGATE_FILTER.NOT_BOOLEAN") => {
                            message.contains("window FILTER requires BOOLEAN")
                        }
                        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
                            message.contains("`count_if` requires 1 argument")
                        }
                        Some("INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC") => {
                            message.contains("window FILTER requires a deterministic expression")
                        }
                        Some("INVALID_AGGREGATE_FILTER.CONTAINS_AGGREGATE") => {
                            message.contains("window FILTER cannot contain an aggregate function")
                        }
                        Some("INVALID_AGGREGATE_FILTER.CONTAINS_WINDOW_FUNCTION") => {
                            message.contains("window FILTER cannot contain a window function")
                        }
                        Some("INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX") => message
                            .contains("FILTER is only supported for aggregate window functions"),
                        Some("DISTINCT_WINDOW_FUNCTION_UNSUPPORTED") => {
                            message.contains("DISTINCT window functions are unsupported")
                        }
                        _ => false,
                    }
                }
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

fn filter_preserving_expected(case: &Value) -> &Value {
    let excluded_rule = match case["id"].as_str() {
        Some("count_if_false" | "count_false" | "sum_false") => {
            assert_eq!(case["expected"]["condition"], "INTERNAL_ERROR");
            Some("org.apache.spark.sql.catalyst.optimizer.EliminateAggregateFilter")
        }
        Some("first_value_prefix") => {
            // Spark's FIRST_VALUE-to-NTH_VALUE rewrite drops FILTER in prefix frames.
            assert_eq!(case["expected"]["rows"][1][1], "1");
            Some("org.apache.spark.sql.catalyst.optimizer.OptimizeWindowFunctions")
        }
        _ => None,
    };
    let reference = case.get("filter_preserving_reference");
    assert_eq!(reference.is_some(), excluded_rule.is_some());
    if let Some(rule) = excluded_rule {
        let reference = &case["filter_preserving_reference"];
        assert_eq!(reference["excluded_rule"], rule);
        assert_eq!(reference["expected"]["status"], "ok");
        if case["id"] == "first_value_prefix" {
            assert_eq!(reference["expected"]["rows"][1][1], Value::Null);
            assert_eq!(reference["expected"]["rows"][3][1], "3");
        }
        &reference["expected"]
    } else {
        &case["expected"]
    }
}

fn register_window_filter_inputs(
    session: &SessionContext,
    reference: &Value,
    batch_size: usize,
) -> TestResult {
    assert_eq!(
        reference["fields"],
        json!([
            ["id", "integer"],
            ["g", "integer"],
            ["predicate", "boolean"],
            ["keep", "boolean"],
            ["raw", "string"],
            ["value", "integer"]
        ])
    );
    let rows: Vec<WindowFilterInputRow> = serde_json::from_value(reference["rows"].clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("g", DataType::Int32, true),
        Field::new("predicate", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, true),
        Field::new("raw", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.group).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.predicate).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.keep).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.raw.as_str()),
        )),
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.value).collect::<Vec<_>>(),
        )),
    ];
    let input = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    for (name, batch) in [
        ("inputs", input.clone()),
        ("empty_inputs", input.slice(0, 0)),
    ] {
        let batches = if batch.num_rows() == 0 {
            vec![batch]
        } else {
            (0..batch.num_rows())
                .step_by(batch_size)
                .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
                .collect()
        };
        session.register_table(
            name,
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?),
        )?;
    }
    Ok(())
}

async fn execute_window_filter_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
    let fields = schema.fields().iter().enumerate().map(|(index, field)| {
        assert_eq!(field.name(), &query.column_names[index]);
        let logical = query.logical_plan.schema().field(index);
        assert_eq!(logical.data_type(), field.data_type());
        assert_eq!(logical.metadata(), field.metadata());
        assert_eq!(logical.is_nullable(), field.is_nullable());
        Ok(json!({"name":field.name(),"type":spark_schema_type(field.data_type())?,"nullable":field.is_nullable(),"metadata":field.metadata()}))
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
    Ok(json!({"status":"ok","schema":{"type":"struct","fields":fields},"rows":rows}))
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::Float64 => json!("double"),
        DataType::Utf8 => json!("string"),
        DataType::List(field) => {
            json!({"type":"array", "elementType":spark_schema_type(field.data_type())?, "containsNull":field.is_nullable()})
        }
        other => return Err(format!("unexpected output type: {other:?}").into()),
    })
}

fn encode_arrow_cell(array: &ArrayRef, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
        Ok(Value::Null)
    } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        Ok(json!(values.value(row)))
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
