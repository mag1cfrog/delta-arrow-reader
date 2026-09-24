//! Bounded planning/execution comparison for the optional integer-overflow patch.
use std::{error::Error, fs, sync::Arc, time::Instant};

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
    util::display::array_value_to_string,
};
use datafusion::{
    datasource::MemTable,
    execution::SessionStateBuilder,
    physical_plan::{ExecutionPlan, collect, displayable},
    prelude::{SessionConfig, SessionContext},
};
use sail_plan::decimal_null::SparkDecimalNullPropagation;
use sail_plan::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const ROWS: usize = 1_048_576;
const WARMUPS: usize = 4;
const SAMPLES: usize = 15;

async fn plan(ctx: &SessionContext, ansi: bool, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
    let config = PlanConfig {
        ansi_mode: ansi,
        ..Default::default()
    };
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    let named = PlanResolver::new(ctx, Arc::new(config))
        .resolve_named_plan(spec)
        .await?;
    Ok(ctx
        .execute_logical_plan(named.plan)
        .await?
        .create_physical_plan()
        .await?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let batch_size: usize = args.get(2).ok_or("expected batch size")?.parse()?;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_target_partitions(1),
            )
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .with_analyzer_rule(Arc::new(SparkDecimalNullPropagation))
            .build(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("a32", DataType::Int32, false),
        Field::new("b32", DataType::Int32, false),
        Field::new("a64", DataType::Int64, false),
        Field::new("b64", DataType::Int64, false),
        Field::new("n32", DataType::Int32, true),
        Field::new("n64", DataType::Int64, true),
        Field::new("f64", DataType::Float64, false),
    ]));
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(batch_size) {
        let end = (start + batch_size).min(ROWS);
        let a32 = Int32Array::from_iter_values((start..end).map(|i| (i % 1024) as i32));
        let b32 = Int32Array::from(vec![2; end - start]);
        let a64 = Int64Array::from_iter_values((start..end).map(|i| (i % 1024) as i64));
        let b64 = Int64Array::from(vec![2; end - start]);
        let n32 =
            Int32Array::from_iter((start..end).map(|i| (i % 7 != 0).then_some((i % 1024) as i32)));
        let n64 =
            Int64Array::from_iter((start..end).map(|i| (i % 7 != 0).then_some((i % 1024) as i64)));
        let f64 = Float64Array::from_iter_values((start..end).map(|i| (i % 1024) as f64));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(a32),
            Arc::new(b32),
            Arc::new(a64),
            Arc::new(b64),
            Arc::new(n32),
            Arc::new(n64),
            Arc::new(f64),
        ];
        batches.push(RecordBatch::try_new(schema.clone(), arrays)?);
    }
    ctx.register_table("input", Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    let mut cases = Vec::new();
    for width in [32, 64] {
        for (name, op) in [("add", "+"), ("sub", "-"), ("mul", "*")] {
            for (shape, left, right) in [
                ("arrays", format!("a{width}"), format!("b{width}")),
                (
                    "scalar",
                    format!("a{width}"),
                    format!("CAST(2 AS {})", if width == 32 { "INT" } else { "BIGINT" }),
                ),
                ("nulls", format!("n{width}"), format!("b{width}")),
            ] {
                cases.push((
                    format!("i{width}_{name}_{shape}"),
                    format!("{left} {op} {right}"),
                ));
            }
        }
        cases.push((
            format!("i{width}_add_nested_nulls"),
            format!(
                "n{width} + (b{width} + CAST(2 AS {}))",
                if width == 32 { "INT" } else { "BIGINT" }
            ),
        ));
    }
    cases.extend([
        ("read_control".into(), "a64".into()),
        ("udf_control".into(), "abs(a64)".into()),
        ("double_control".into(), "f64 + f64".into()),
        (
            "decimal_control".into(),
            "CAST(a32 AS DECIMAL(12,2)) + CAST(b32 AS DECIMAL(12,2))".into(),
        ),
    ]);
    let mut results = Vec::<Value>::new();
    for (id, expr) in cases {
        for ansi in [true, false] {
            let sql = format!("SELECT {expr} AS r FROM input");
            let physical = plan(&ctx, ansi, &sql).await?;
            let output = collect(physical.clone(), ctx.task_ctx()).await?;
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                ROWS
            );
            let mut row = 0;
            // Verify every output value outside the measurement window.
            for batch in &output {
                for index in 0..batch.num_rows() {
                    let column = batch.column(0);
                    let null = id.ends_with("nulls") && row % 7 == 0;
                    assert_eq!(column.is_null(index), null, "{id} row {row}");
                    if !null {
                        let value = (row % 1024) as f64;
                        let expected = if id.contains("nested_nulls") {
                            value + 4.0
                        } else if id.contains("_add_") || id == "decimal_control" {
                            value + 2.0
                        } else if id.contains("_sub_") {
                            value - 2.0
                        } else if id.contains("_mul_") || id == "double_control" {
                            value * 2.0
                        } else {
                            value
                        };
                        assert_eq!(
                            array_value_to_string(column.as_ref(), index)?.parse::<f64>()?,
                            expected,
                            "{id} row {row}"
                        );
                    }
                    row += 1;
                }
            }
            let output_bytes: usize = output.iter().map(RecordBatch::get_array_memory_size).sum();
            drop(output);
            let mut execution = Vec::new();
            let mut planning = Vec::new();
            for iteration in 0..WARMUPS + SAMPLES {
                let start = Instant::now();
                let output = collect(physical.clone(), ctx.task_ctx()).await?;
                drop(output);
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                if iteration >= WARMUPS {
                    execution.push(elapsed);
                }
            }
            for iteration in 0..WARMUPS + SAMPLES {
                let start = Instant::now();
                let physical = plan(&ctx, ansi, &sql).await?;
                std::hint::black_box(physical);
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                if iteration >= WARMUPS {
                    planning.push(elapsed);
                }
            }
            results.push(json!({"id": id, "ansi": ansi, "sql": sql,
                "types": format!("{:?}", physical.schema().field(0).data_type()),
                "physical_plan": displayable(physical.as_ref()).indent(true).to_string(),
                "output_bytes": output_bytes, "execution_ms": execution, "planning_ms": planning}));
        }
    }
    fs::write(
        args.get(1).ok_or("expected output path")?,
        serde_json::to_vec_pretty(&json!({
            "rows": ROWS, "batch_size": batch_size, "warmups": WARMUPS, "samples": SAMPLES,
            "results": results,
        }))?,
    )?;
    Ok(())
}
