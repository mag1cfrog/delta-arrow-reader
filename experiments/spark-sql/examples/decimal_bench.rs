use std::{error::Error, fs, hint::black_box, sync::Arc, time::Instant};

use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use datafusion::{
    datasource::MemTable,
    execution::SessionStateBuilder,
    physical_plan::{ExecutionPlan, displayable, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use futures_util::StreamExt;
use sail_plan::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ROWS: usize = 1_048_576;
const BATCH_SIZE: usize = 8192;
const WARMUPS: usize = 2;
const SAMPLES: usize = 9;

// In-memory projection only. Measure Delta I/O and concurrent queries separately.
fn input(precision: u8, scale: i8, nulls: bool) -> Result<MemTable> {
    let data_type = if precision == 0 {
        DataType::Float64
    } else {
        DataType::Decimal128(precision, scale)
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", data_type.clone(), nulls),
        Field::new("b", data_type, nulls),
    ]));
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(BATCH_SIZE) {
        let columns = [false, true]
            .into_iter()
            .map(|divisor| -> Result<ArrayRef> {
                let values = (start..(start + BATCH_SIZE).min(ROWS)).map(|i| {
                    let period = if divisor { 13 } else { 10 };
                    if nulls && i % period == period - 1 {
                        None
                    } else if divisor {
                        Some((i % 97 + 3) as i128)
                    } else {
                        Some((i % 10000 + 2) as i128)
                    }
                });
                Ok(if precision == 0 {
                    Arc::new(Float64Array::from_iter(
                        values.map(|v| v.map(|v| v as f64 / 100.0)),
                    ))
                } else {
                    let unit = 10_i128.pow((scale - 2) as u32);
                    Arc::new(
                        Decimal128Array::from_iter(values.map(|v| v.map(|v| v * unit)))
                            .with_precision_and_scale(precision, scale)?,
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        batches.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    Ok(MemTable::try_new(schema, vec![batches])?)
}

async fn consume(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Result<(usize, usize)> {
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let (mut rows, mut nulls) = (0, 0);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows();
        nulls += batch.column(0).null_count();
        black_box(batch);
    }
    Ok((rows, nulls))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("use cargo build --release for timing".into());
    }
    let args = std::env::args().collect::<Vec<_>>();
    let output = args.get(1).ok_or("usage: decimal_bench OUTPUT_JSON")?;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_batch_size(BATCH_SIZE)
                    .with_target_partitions(1),
            )
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    let mut results = Vec::new();
    for (precision, scale, nulls) in [
        (10, 2, false),
        (18, 4, false),
        (38, 6, false),
        (10, 2, true),
        (0, 2, false),
    ] {
        ctx.deregister_table("bench_input")?;
        ctx.register_table("bench_input", Arc::new(input(precision, scale, nulls)?))?;
        for divisor_kind in ["column", "typed_literal", "integer_literal"] {
            if divisor_kind == "integer_literal" && (nulls || precision == 0) {
                continue;
            }
            let scalar = divisor_kind != "column";
            let denominator = if !scalar {
                "b".to_owned()
            } else if divisor_kind == "integer_literal" {
                "3".to_owned()
            } else if precision == 0 {
                "CAST(3 AS DOUBLE)".to_owned()
            } else {
                format!("CAST(3 AS DECIMAL({precision},{scale}))")
            };
            let sql = format!("SELECT a / {denominator} AS quotient FROM bench_input");
            let expected_nulls = (0..ROWS)
                .filter(|i| nulls && (i % 10 == 9 || (!scalar && i % 13 == 12)))
                .count();
            for ansi in [true, false] {
                let mut config = PlanConfig::default();
                config.ansi_mode = ansi;
                config.session_timezone = "UTC".into();
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                let frame = ctx.execute_logical_plan(named.plan).await?;
                let plan = frame.create_physical_plan().await?;
                let physical_plan = displayable(plan.as_ref()).indent(true).to_string();
                let mut stream = execute_stream(plan.clone(), ctx.task_ctx())?;
                let first = stream.next().await.ok_or("missing first batch")??;
                let first_values = (0..3)
                    .map(|i| array_value_to_string(first.column(0).as_ref(), i))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                drop(stream);
                for _ in 0..WARMUPS {
                    assert_eq!(consume(&ctx, plan.clone()).await?, (ROWS, expected_nulls));
                }
                let mut elapsed_ms = Vec::new();
                for _ in 0..SAMPLES {
                    let start = Instant::now();
                    let counts = consume(&ctx, plan.clone()).await?;
                    elapsed_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    assert_eq!(counts, (ROWS, expected_nulls));
                }
                let mut sorted = elapsed_ms.clone();
                sorted.sort_by(f64::total_cmp);
                let median_ms = sorted[SAMPLES / 2];
                let id = format!("p{precision}_s{scale}_nulls{nulls}_{divisor_kind}_ansi{ansi}");
                eprintln!("{id}: {median_ms:.3} ms");
                results.push(json!({
                    "id": id, "sql": sql, "ansi": ansi,
                    "precision": precision, "scale": scale,
                    "nullable_input": nulls, "divisor_kind": divisor_kind,
                    "output_type": format!("{:?}", plan.schema().field(0).data_type()),
                    "output_nulls": expected_nulls, "first_values": first_values,
                    "physical_plan": physical_plan,
                    "samples_ms": elapsed_ms, "median_ms": median_ms,
                }));
            }
        }
    }
    let capture: Value = json!({
        "rows": ROWS, "batch_size": BATCH_SIZE, "partitions": 1,
        "warmups": WARMUPS, "samples": SAMPLES, "results": results,
    });
    fs::write(output, serde_json::to_vec_pretty(&capture)?)?;
    Ok(())
}
