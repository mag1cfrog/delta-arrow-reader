//! COUNT_IF counts true predicates only among rows admitted by its SQL FILTER.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray};
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
struct CountIfInputRow {
    id: i32,
    group: i32,
    predicate: Option<bool>,
    keep: Option<bool>,
    raw: String,
}

#[tokio::test]
async fn count_if_matches_spark_predicate_and_filter_results() -> TestResult {
    let records = include_str!("fixtures/count-if-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 164);
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
        register_count_if_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = execute_count_if_query(&session, case).await;
            let expected = &case["expected"];
            let matches =
                match &actual {
                    Ok(actual) => actual == expected,
                    Err(error) => {
                        let message = error.to_string();
                        match expected["condition"].as_str() {
                            Some("CAST_INVALID_INPUT") => {
                                message.contains("Cannot cast string 'bad'")
                                    || message.contains(
                                        "[CAST_INVALID_INPUT] Cannot cast STRING to BOOLEAN",
                                    )
                            }
                            Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => {
                                message.contains("COUNT_IF requires a BOOLEAN argument")
                            }
                            Some("INVALID_AGGREGATE_FILTER.NOT_BOOLEAN") => {
                                message.contains("COUNT_IF FILTER requires BOOLEAN")
                            }
                            Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
                                message.contains("`count_if` requires 1 argument")
                            }
                            Some("INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC") => message
                                .contains("COUNT_IF FILTER requires a deterministic expression"),
                            Some("AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION") => message
                                .contains("COUNT_IF argument requires a deterministic expression"),
                            Some("INVALID_AGGREGATE_FILTER.CONTAINS_AGGREGATE") => message
                                .contains("COUNT_IF FILTER cannot contain an aggregate function"),
                            Some("NESTED_AGGREGATE_FUNCTION") => message
                                .contains("COUNT_IF argument cannot contain an aggregate function"),
                            Some("INVALID_AGGREGATE_FILTER.CONTAINS_WINDOW_FUNCTION") => {
                                message.contains("COUNT_IF FILTER cannot contain a window function")
                            }
                            Some("_LEGACY_ERROR_TEMP_1031") => message
                                .contains("COUNT_IF argument cannot contain a window function"),
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

fn register_count_if_inputs(
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
            ["raw", "string"]
        ])
    );
    let rows: Vec<CountIfInputRow> = serde_json::from_value(reference["rows"].clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("g", DataType::Int32, true),
        Field::new("predicate", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, true),
        Field::new("raw", DataType::Utf8, true),
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

async fn execute_count_if_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
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
        let data_type = match field.data_type() {
            DataType::Int32 => "integer",
            DataType::Int64 => "long",
            other => return Err(format!("unexpected output type: {other:?}").into()),
        };
        Ok(json!({"name":field.name(),"type":data_type,"nullable":field.is_nullable(),"metadata":field.metadata()}))
    }).collect::<TestResult<Vec<_>>>()?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if array.is_null(row) {
                            Ok(Value::Null)
                        } else {
                            Ok(array_value_to_string(array.as_ref(), row)?.into())
                        }
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(json!({"status":"ok","schema":{"type":"struct","fields":fields},"rows":rows}))
}
