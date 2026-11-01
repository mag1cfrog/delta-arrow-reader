//! Sliding extrema become NULL when their last contributing value leaves the frame.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int32Array, StringArray,
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
struct ExtremaInputRow {
    id: i32,
    group: i32,
    keep: Option<bool>,
    value: Option<i32>,
}

#[tokio::test]
async fn sliding_extrema_match_spark_after_values_leave_the_frame() -> TestResult {
    let records = include_str!("fixtures/window-extrema-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(
        records[0]["types"],
        json!(["integer", "double", "string", "decimal(8,2)"])
    );
    assert_eq!(records.len() - 1, 96);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut comparisons = 0;
    let mut differences = Vec::new();
    for batch_size in [1, 64] {
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
        register_extrema_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            for spark_frontend in [false, true] {
                // FILTER admission in the Spark frontend is tracked separately by #697.
                if spark_frontend && case["filtered"].as_bool().ok_or("missing FILTER marker")? {
                    continue;
                }
                comparisons += 1;
                let actual = execute_extrema_query(&session, case, spark_frontend).await;
                let expected = json!({"schema":case["schema"],"rows":case["rows"]});
                if actual.as_ref().ok() != Some(&expected) {
                    differences.push(format!(
                        "{} ANSI={} batch={batch_size} Spark frontend={spark_frontend}: expected {expected}, got {actual:?}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert_eq!(comparisons, 288);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn register_extrema_inputs(
    session: &SessionContext,
    reference: &Value,
    batch_size: usize,
) -> TestResult {
    let rows: Vec<ExtremaInputRow> = serde_json::from_value(reference["inputs"].clone())?;
    let values = rows.iter().map(|row| row.value).collect::<Vec<_>>();
    let value_arrays: Vec<(&str, ArrayRef)> = vec![
        ("input_integer", Arc::new(Int32Array::from(values.clone()))),
        (
            "input_double",
            Arc::new(Float64Array::from(
                values.iter().map(|v| v.map(f64::from)).collect::<Vec<_>>(),
            )),
        ),
        (
            "input_string",
            Arc::new(StringArray::from_iter(
                values.iter().map(|v| v.map(|v| v.to_string())),
            )),
        ),
        (
            "input_decimal",
            Arc::new(
                Decimal128Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| i128::from(v) * 100))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(8, 2)?,
            ),
        ),
    ];
    for (name, values) in value_arrays {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("g", DataType::Int32, true),
            Field::new("keep", DataType::Boolean, true),
            Field::new("value", values.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.id).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.group).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.keep).collect::<Vec<_>>(),
                )),
                values,
            ],
        )?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect();
        session.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    }
    Ok(())
}

async fn execute_extrema_query(
    session: &SessionContext,
    case: &Value,
    spark_frontend: bool,
) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let (physical, logical) = if spark_frontend {
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        (
            query.create_physical_plan(session).await?,
            query.logical_plan,
        )
    } else {
        let query = session.sql(sql).await?;
        (
            query.clone().create_physical_plan().await?,
            query.into_unoptimized_plan(),
        )
    };
    let fields = physical.schema().fields().iter().enumerate().map(|(index, field)| {
        let logical = logical.schema().field(index);
        assert_eq!(logical.data_type(), field.data_type());
        assert_eq!(logical.metadata(), field.metadata());
        assert_eq!(logical.is_nullable(), field.is_nullable());
        let data_type = match field.data_type() {
            DataType::Int32 => "integer",
            DataType::Float64 => "double",
            DataType::Utf8 => "string",
            DataType::Decimal128(8, 2) => "decimal(8,2)",
            other => return Err(format!("unexpected result type: {other:?}").into()),
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
                        } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                            Ok(json!(values.value(row)))
                        } else {
                            Ok(array_value_to_string(array.as_ref(), row)?.into())
                        }
                    })
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(json!({"schema":{"type":"struct","fields":fields},"rows":rows}))
}
