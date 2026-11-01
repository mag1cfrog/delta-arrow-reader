//! Boolean predicates skip unneeded operands without changing NULL results.

use std::error::Error;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, BooleanArray, Int32Array, StringArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Operator, Volatility, col, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn boolean_results_and_required_errors_match_spark_at_both_batch_sizes() -> TestResult {
    let records = include_str!("fixtures/boolean-evaluation-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
            let rows = input["rows"].as_array().ok_or("missing input rows")?;
            let batch = RecordBatch::try_from_iter([
                (
                    "k",
                    Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)) as ArrayRef,
                ),
                (
                    "v",
                    Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r[1].as_bool())))
                        as ArrayRef,
                ),
                (
                    "s",
                    Arc::new(StringArray::from_iter(rows.iter().map(|r| r[2].as_str())))
                        as ArrayRef,
                ),
            ])?;
            register_input(
                &session,
                input["table"].as_str().ok_or("missing table")?,
                batch,
                batch_size,
            )?;
        }
        let rows = records[0]["truth_rows"]
            .as_array()
            .ok_or("missing truth rows")?;
        let batch = RecordBatch::try_from_iter([
            (
                "k",
                Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)) as ArrayRef,
            ),
            (
                "a",
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r[1].as_bool()))) as ArrayRef,
            ),
            (
                "b",
                Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r[2].as_bool()))) as ArrayRef,
            ),
        ])?;
        register_input(&session, "truth_inputs", batch, batch_size)?;
        for case in &records[1..] {
            let outcome: TestResult<Value> = async {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                assert_eq!(query.column_names, ["r"]);
                assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
                let mut rows = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    for value in batch.column(0).as_boolean() {
                        if value.is_none() {
                            assert!(query.logical_plan.schema().field(0).is_nullable());
                            assert!(schema.field(0).is_nullable());
                        }
                        rows.push(vec![value]);
                    }
                }
                Ok(json!(rows))
            }
            .await;
            let label = format!("{} ANSI={} batch={batch_size}", case["id"], case["ansi"]);
            match case["expected"]["status"].as_str() {
                Some("ok") => assert_eq!(
                    outcome.map_err(|e| format!("{label}: {e}"))?,
                    case["expected"]["rows"],
                    "{label}"
                ),
                Some("error") => {
                    assert_eq!(case["expected"]["condition"], "CAST_INVALID_INPUT");
                    let error = outcome
                        .expect_err("required invalid cast must fail")
                        .to_string();
                    assert!(
                        error.contains("Cannot cast string 'bad'"),
                        "{label}: {error}"
                    );
                }
                _ => return Err("unknown reference outcome".into()),
            }
        }
    }
    Ok(())
}

fn register_input(
    session: &SessionContext,
    name: &str,
    batch: RecordBatch,
    batch_size: usize,
) -> TestResult {
    let batches = (0..batch.num_rows())
        .step_by(batch_size)
        .map(|start| batch.slice(start, batch_size.min(batch.num_rows() - start)))
        .collect();
    session.register_table(
        name,
        Arc::new(MemTable::try_new(batch.schema(), vec![batches])?),
    )?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum BooleanOperand {
    Column,
    Scalar(Option<bool>),
}

#[test]
fn native_boolean_evaluation_visits_only_needed_rows() -> TestResult {
    // Rows are the complete truth table, in true/false/NULL order on each side.
    let left = [
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(false),
        None,
        None,
        None,
    ];
    let right = [
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ];
    let operands = [
        BooleanOperand::Column,
        BooleanOperand::Scalar(Some(true)),
        BooleanOperand::Scalar(Some(false)),
        BooleanOperand::Scalar(None),
    ];
    for offset in [1, 63] {
        for values in [
            left,
            [Some(true); 9],
            [Some(false); 9],
            [None; 9],
            [
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ],
        ] {
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from_iter_values(0..(offset + 9) as i32)) as ArrayRef,
                ),
                (
                    "a",
                    Arc::new(BooleanArray::new(
                        BooleanBuffer::from_iter(
                            std::iter::repeat_n(true, offset)
                                .chain(values.map(|v| v.unwrap_or(true))),
                        ),
                        Some(NullBuffer::from_iter(
                            std::iter::repeat_n(true, offset).chain(values.map(|v| v.is_some())),
                        )),
                    )) as ArrayRef,
                ),
                (
                    "b",
                    Arc::new(BooleanArray::from_iter(
                        std::iter::repeat_n(Some(false), offset).chain(right),
                    )) as ArrayRef,
                ),
            ])?
            .slice(offset, 9);
            for op in [Operator::And, Operator::Or] {
                for left_operand in operands {
                    for right_operand in operands {
                        assert_operand_visits(&batch, op, left_operand, right_operand)?;
                        assert_operand_visits(&batch.slice(0, 0), op, left_operand, right_operand)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn assert_operand_visits(
    batch: &RecordBatch,
    op: Operator,
    left_operand: BooleanOperand,
    right_operand: BooleanOperand,
) -> TestResult {
    let session = SessionContext::new();
    let visits = [
        Arc::new(Mutex::new(Vec::<Vec<i32>>::new())),
        Arc::new(Mutex::new(Vec::<Vec<i32>>::new())),
    ];
    let mut operands = Vec::new();
    for (index, operand) in [left_operand, right_operand].into_iter().enumerate() {
        let visits = visits[index].clone();
        let observed = create_udf(
            &format!("observed_{index}"),
            vec![DataType::Boolean, DataType::Int32],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |arguments| {
                let arrays = ColumnarValue::values_to_arrays(arguments)?;
                visits
                    .lock()
                    .map_err(|_| {
                        datafusion::error::DataFusionError::Execution(
                            "poisoned test recorder".into(),
                        )
                    })?
                    .push(arrays[1].as_primitive::<Int32Type>().values().to_vec());
                Ok(match operand {
                    BooleanOperand::Scalar(value) => {
                        ColumnarValue::Scalar(ScalarValue::Boolean(value))
                    }
                    BooleanOperand::Column => arguments[0].clone(),
                })
            }),
        );
        operands.push(observed.call(vec![col(if index == 0 { "a" } else { "b" }), col("id")]));
    }
    let expression = if op == Operator::And {
        operands[0].clone().and(operands[1].clone())
    } else {
        operands[0].clone().or(operands[1].clone())
    };
    let physical =
        session.create_physical_expr(expression, &DFSchema::try_from(batch.schema())?)?;
    let result = physical.evaluate(batch)?.into_array(batch.num_rows())?;
    let mut expected = Vec::new();
    let mut required_ids = Vec::new();
    for row in 0..batch.num_rows() {
        let left = match left_operand {
            BooleanOperand::Column => batch.column(1).as_boolean().iter().nth(row).flatten(),
            BooleanOperand::Scalar(value) => value,
        };
        let right = match right_operand {
            BooleanOperand::Column => batch.column(2).as_boolean().iter().nth(row).flatten(),
            BooleanOperand::Scalar(value) => value,
        };
        let decided = if op == Operator::And {
            Some(false)
        } else {
            Some(true)
        };
        if left != decided {
            required_ids.push(batch.column(0).as_primitive::<Int32Type>().value(row));
        }
        expected.push(match (op, left, right) {
            (Operator::And, Some(false), _) | (Operator::And, _, Some(false)) => Some(false),
            (Operator::And, Some(true), Some(true)) => Some(true),
            (Operator::Or, Some(true), _) | (Operator::Or, _, Some(true)) => Some(true),
            (Operator::Or, Some(false), Some(false)) => Some(false),
            _ => None,
        });
    }
    assert_eq!(
        result.as_boolean().iter().collect::<Vec<_>>(),
        expected,
        "{op:?}, {left_operand:?}, {right_operand:?}"
    );
    let left_visits = if batch.num_rows() == 0 {
        vec![]
    } else {
        vec![
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec(),
        ]
    };
    assert_eq!(
        *visits[0].lock().map_err(|_| "poisoned left recorder")?,
        left_visits
    );
    let expected_visits = if required_ids.is_empty() {
        vec![]
    } else {
        vec![required_ids]
    };
    assert_eq!(
        *visits[1].lock().map_err(|_| "poisoned right recorder")?,
        expected_visits,
        "{op:?}, {left_operand:?}, {right_operand:?}"
    );
    Ok(())
}
