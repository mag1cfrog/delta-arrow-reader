//! Bounded aggregate frames use native accumulators, including those without retraction.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawWindowExpr};
use datafusion::logical_expr::{Accumulator, Volatility, WindowFunctionDefinition, create_udaf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(serde::Deserialize)]
struct WindowInputRow {
    id: i32,
    group: i32,
    order_key: i32,
    keep: Option<bool>,
    value: Option<i32>,
}

#[derive(Debug)]
struct AggregateFirstLastPlanner;

impl ExprPlanner for AggregateFirstLastPlanner {
    fn plan_window(
        &self,
        mut expr: RawWindowExpr,
    ) -> DataFusionResult<PlannerResult<RawWindowExpr>> {
        // Select the aggregate implementation before window validation. The
        // separate Spark FILTER frontend change makes this selection in production.
        let aggregate = match expr.func_def.name() {
            "first_value" => Some(first_value_udaf()),
            "last_value" => Some(last_value_udaf()),
            _ => None,
        };
        if let Some(aggregate) = aggregate {
            expr.func_def = WindowFunctionDefinition::AggregateUDF(aggregate);
        }
        Ok(PlannerResult::Original(expr))
    }
}

fn create_window_test_session(reference: &Value, batch_size: usize) -> TestResult<SessionContext> {
    let mut session = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2),
    );
    session.register_expr_planner(Arc::new(AggregateFirstLastPlanner))?;
    let rows: Vec<WindowInputRow> = serde_json::from_value(reference["inputs"].clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("g", DataType::Int32, true),
        Field::new("k", DataType::Int32, true),
        Field::new("keep", DataType::Boolean, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.id))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.group))),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|r| r.order_key),
            )),
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.keep))),
            Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.value))),
        ],
    )?;
    let batches = (0..batch.num_rows())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
        .collect();
    session.register_table(
        "inputs",
        Arc::new(MemTable::try_new(schema, vec![batches])?),
    )?;
    Ok(session)
}

fn spark_schema_type(data_type: &DataType) -> TestResult<Value> {
    Ok(match data_type {
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        DataType::List(field) => {
            json!({"type":"array", "elementType":spark_schema_type(field.data_type())?, "containsNull":field.is_nullable()})
        }
        other => return Err(format!("unexpected result type {other:?}").into()),
    })
}

fn encode_arrow_cell(array: &ArrayRef, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
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

async fn execute_window_query(session: &SessionContext, sql: &str) -> TestResult<Value> {
    let query = session.sql(sql).await?;
    let logical = query.schema().clone();
    let physical = query.create_physical_plan().await?;
    let fields = physical
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            assert_eq!(logical.field(i), field);
            Ok(
                json!({"name":field.name(), "type":spark_schema_type(field.data_type())?,
                  "nullable":field.is_nullable(), "metadata":field.metadata()}),
            )
        })
        .collect::<TestResult<Vec<_>>>()?;
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
async fn bounded_aggregate_results_and_schemas_match_spark() -> TestResult {
    let records = include_str!("fixtures/bounded-window-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len(), 29);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ))
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 28);
    let mut failures = Vec::new();
    for batch_size in [1, 4, 64] {
        let session = create_window_test_session(&records[0], batch_size)?;
        for case in &records[1..] {
            let filtered_reference = case.get("filter_preserving_reference");
            assert_eq!(
                filtered_reference.is_some(),
                case["id"] == "cumulative_True"
            );
            if let Some(reference) = filtered_reference {
                assert_eq!(
                    reference["excluded_rule"],
                    "org.apache.spark.sql.catalyst.optimizer.OptimizeWindowFunctions"
                );
                assert_eq!(reference["schema"], case["schema"]);
                // Spark's default FIRST_VALUE-to-NTH_VALUE rewrite drops FILTER.
                assert_eq!(case["rows"][2][2], "2");
                assert_eq!(reference["rows"][2][2], Value::Null);
                assert_eq!(reference["rows"][3][2], "3");
            }
            let actual =
                execute_window_query(&session, case["native_sql"].as_str().ok_or("missing SQL")?)
                    .await;
            let reference = filtered_reference.unwrap_or(case);
            let expected = json!({"schema":reference["schema"],"rows":reference["rows"]});
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
async fn empty_range_frames_discard_previous_contributions() -> TestResult {
    let records = include_str!("fixtures/bounded-window-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let case = records
        .iter()
        .find(|case| case["id"] == "range_gaps_True")
        .ok_or("missing gap reference")?;
    let expected = case["rows"]
        .as_array()
        .ok_or("missing rows")?
        .iter()
        .map(|row| json!([row[0], row[7], row[8], row[9], row[10]]))
        .collect::<Vec<_>>();
    for batch_size in [1, 64] {
        let session = create_window_test_session(&records[0], batch_size)?;
        let columns = ["sum", "count", "min", "max"].map(|function| format!(
            "{function}(value) FILTER (WHERE keep) OVER (ORDER BY k RANGE BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS {function}_value"
        ));
        let actual = execute_window_query(
            &session,
            &format!("SELECT id,{} FROM inputs ORDER BY id", columns.join(",")),
        )
        .await?;
        assert_eq!(actual["rows"], json!(expected), "batch={batch_size}");
    }
    Ok(())
}

#[derive(Debug)]
struct CountedSumAccumulator {
    sum: i64,
    updated_rows: Arc<AtomicUsize>,
    retractable: bool,
}

impl Accumulator for CountedSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        self.updated_rows
            .fetch_add(values[0].len(), Ordering::Relaxed);
        self.sum +=
            arrow::compute::sum(datafusion::common::cast::as_int64_array(&values[0])?).unwrap_or(0);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        assert!(
            self.retractable,
            "non-retracting accumulator must be rebuilt"
        );
        self.sum -=
            arrow::compute::sum(datafusion::common::cast::as_int64_array(&values[0])?).unwrap_or(0);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        self.retractable
    }
    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.sum)))
    }
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        self.update_batch(states)
    }
    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[tokio::test]
async fn frame_recomputation_preserves_incremental_and_peer_work_bounds() -> TestResult {
    const ROWS: usize = 64;
    for retractable in [false, true] {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef,
                ),
                (
                    "peer",
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS as i64).map(|i| i / 8),
                    )),
                ),
                ("value", Arc::new(Int64Array::from(vec![1_i64; ROWS]))),
            ])?,
        )?;
        let updated_rows = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&updated_rows);
        session.register_udaf(create_udaf(
            "counted_sum",
            vec![DataType::Int64],
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            Arc::new(move |_| {
                Ok(Box::new(CountedSumAccumulator {
                    sum: 0,
                    updated_rows: Arc::clone(&counter),
                    retractable,
                }))
            }),
            Arc::new(vec![DataType::Int64]),
        ));
        for (frame, expected_values, max_updates) in [
            (
                "ORDER BY id ROWS BETWEEN 7 PRECEDING AND CURRENT ROW",
                (1..=ROWS).map(|i| i.min(8) as i64).collect::<Vec<_>>(),
                if retractable { ROWS } else { ROWS * 8 },
            ),
            (
                "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                (1..=ROWS as i64).collect(),
                ROWS,
            ),
            (
                "ORDER BY peer RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
                vec![8; ROWS],
                ROWS,
            ),
        ] {
            updated_rows.store(0, Ordering::Relaxed);
            let batches = session
                .sql(&format!(
                    "SELECT counted_sum(value) OVER ({frame}) AS total FROM inputs ORDER BY id"
                ))
                .await?
                .collect()
                .await?;
            let actual = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .into_iter()
                        .flat_map(|a| a.iter())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected_values.into_iter().map(Some).collect::<Vec<_>>()
            );
            let updates = updated_rows.load(Ordering::Relaxed);
            assert!(
                updates <= max_updates,
                "{frame}, retractable={retractable}: {updates} > {max_updates}"
            );
            println!("{frame}, retractable={retractable}: {updates} updated rows");
        }
    }
    Ok(())
}

#[tokio::test]
async fn bounded_windows_preserve_argument_and_filter_errors() -> TestResult {
    let reference: Value = serde_json::from_str(
        include_str!("fixtures/bounded-window-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing reference")?,
    )?;
    let session = create_window_test_session(&reference, 1)?;
    for (expression, expected_message) in [
        (
            "first_value(CAST(CASE WHEN keep IS FALSE THEN 'bad-value' ELSE '1' END AS INT)) FILTER (WHERE keep)",
            "bad-value",
        ),
        (
            "first_value(value) FILTER (WHERE CAST(CASE WHEN id=3 THEN 'bad-filter' ELSE 'true' END AS BOOLEAN))",
            "bad-filter",
        ),
        (
            "approx_distinct(CAST(CASE WHEN keep IS FALSE THEN 'bad-value' ELSE '1' END AS INT)) FILTER (WHERE keep)",
            "bad-value",
        ),
        (
            "ARRAY_AGG(DISTINCT CAST(CASE WHEN keep IS FALSE THEN 'bad-value' ELSE '1' END AS INT)) FILTER (WHERE keep)",
            "bad-value",
        ),
    ] {
        let sql = format!(
            "SELECT {expression} OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM inputs"
        );
        let result = async { session.sql(&sql).await?.collect().await }.await;
        assert!(
            result.is_err(),
            "invalid expression succeeded: {expression}"
        );
        let error = result.err().ok_or("missing error")?.to_string();
        assert!(error.contains(expected_message), "{expression}: {error}");
    }
    Ok(())
}
