//! Grouping-set spill preserves materialized keys, duplicate IDs and aggregate counts.

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{Column, lit};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    ExecutionPlan, InputOrderMode, PhysicalExpr, collect, displayable,
};
use datafusion::prelude::SessionConfig;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, encode_value, validate_batch};

#[tokio::test]
async fn raw_multiple_grouping_sets_preserve_full_ids_and_counts_when_spilling() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let key_count = 2048;
    let keys = (0..key_count)
        .flat_map(|key| [key, key])
        .collect::<Vec<i32>>();
    let batches = keys
        .chunks(16)
        .map(|keys| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(keys.to_vec())),
                    Arc::new(Int32Array::from(vec![10; keys.len()])),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let source = TestMemoryExec::try_new(&[batches], schema.clone(), None)?
        .try_with_sort_information(vec![
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))])
                .ok_or("missing source ordering")?,
        ])?;
    let input: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::update_cache(&Arc::new(source)));
    let groups = PhysicalGroupBy::new(
        vec![
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                "a".into(),
            ),
            (
                Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
                "b".into(),
            ),
        ],
        vec![
            (lit(ScalarValue::Int32(None)), "a".into()),
            (lit(ScalarValue::Int32(None)), "b".into()),
        ],
        vec![vec![false, false], vec![false, true], vec![false, true]],
        true,
    );
    let count = Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![lit(1_i64)])
            .schema(schema.clone())
            .alias("n")
            .build()?,
    );
    let mut expected = Vec::new();
    for key in 0..key_count {
        expected.push(json!([key.to_string(), "10", "0", "2"]));
        expected.push(json!([key.to_string(), null, "1", "2"]));
        expected.push(json!([key.to_string(), null, "5", "2"]));
    }
    expected.sort_by_key(Value::to_string);
    for (memory_limit, expect_spill) in [(64 * 1024 * 1024, false), (64 * 1024, true)] {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        assert!(runtime.disk_manager.tmp_files_enabled());
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(16))
                .with_runtime(runtime),
        );
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            groups.clone(),
            vec![count.clone()],
            vec![None],
            input.clone(),
            schema.clone(),
        )?);
        assert_eq!(aggregate.input_order_mode(), &InputOrderMode::Linear);
        eprintln!(
            "GROUPING_SET_SPILL_START {}",
            json!({
                "memory_limit":memory_limit,"expect_spill":expect_spill,
                "expected_rows":expected.len(),"source_rows":keys.len(),
                "plan":displayable(aggregate.as_ref()).indent(true).to_string(),
            })
        );
        let result = collect(aggregate.clone(), context).await;
        let metrics = aggregate.metrics().ok_or("missing aggregate metrics")?;
        let spill_count = metrics.spill_count().ok_or("missing spill-count metric")?;
        let spilled_bytes = metrics
            .spilled_bytes()
            .ok_or("missing spilled-bytes metric")?;
        let spilled_rows = metrics
            .spilled_rows()
            .ok_or("missing spilled-rows metric")?;
        eprintln!(
            "GROUPING_SET_SPILL_RESULT {}",
            json!({
                "memory_limit":memory_limit,"expect_spill":expect_spill,
                "spill_count":spill_count,"spilled_bytes":spilled_bytes,"spilled_rows":spilled_rows,
                "error":result.as_ref().err().map(ToString::to_string),
            })
        );
        let mut actual = Vec::new();
        for batch in result? {
            validate_batch(aggregate.schema().as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                actual.push(Value::Array(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                ));
            }
        }
        actual.sort_by_key(Value::to_string);
        eprintln!(
            "GROUPING_SET_SPILL_ROWS {}",
            json!({
                "memory_limit":memory_limit,"expect_spill":expect_spill,"actual":actual,
            })
        );
        assert_eq!(actual, expected);
        if expect_spill {
            assert!(
                spill_count > 0 && spilled_bytes > 0 && spilled_rows > 0,
                "resource control did not exercise disk spilling"
            );
        } else {
            assert_eq!((spill_count, spilled_bytes, spilled_rows), (0, 0, 0));
        }
    }
    Ok(())
}
