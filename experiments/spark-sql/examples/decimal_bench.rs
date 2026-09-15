use std::{
    error::Error,
    fs,
    hint::black_box,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array},
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
fn input(precision: u8, scale: i8, divisor_scale: i8, nulls: bool) -> Result<MemTable> {
    let data_type = |scale| {
        if precision == 0 {
            DataType::Float64
        } else {
            DataType::Decimal128(precision, scale)
        }
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", data_type(scale), nulls),
        Field::new("b", data_type(divisor_scale), nulls),
    ]));
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(BATCH_SIZE) {
        let columns = [false, true]
            .into_iter()
            .map(|divisor| -> Result<ArrayRef> {
                let scale = if divisor { divisor_scale } else { scale };
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
                    let unit = 10_i128.pow((scale - if scale >= 35 { 5 } else { 2 }) as u32);
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

// perf stat --delay=-1 --control=fifo:DIR/control,DIR/ack
// Counting covers the enabled phase and the control handshake.
fn perf_command(command: &[u8]) -> Result<()> {
    if let Some(dir) = std::env::var_os("DECIMAL_BENCH_PERF_DIR") {
        let dir = PathBuf::from(dir);
        fs::OpenOptions::new()
            .write(true)
            .open(dir.join("control"))?
            .write_all(command)?;
        let mut ack = String::new();
        BufReader::new(fs::File::open(dir.join("ack"))?).read_line(&mut ack)?;
        if ack != "ack\n" {
            return Err("unexpected perf control acknowledgement".into());
        }
    }
    Ok(())
}

async fn plan_query(ctx: &SessionContext, sql: &str, ansi: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = PlanConfig::default();
    config.ansi_mode = ansi;
    config.session_timezone = "UTC".into();
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    let named = PlanResolver::new(ctx, Arc::new(config))
        .resolve_named_plan(spec)
        .await?;
    let frame = ctx.execute_logical_plan(named.plan).await?;
    Ok(frame.create_physical_plan().await?)
}

fn subquery_input_id(position: usize) -> usize {
    position.wrapping_mul(4099).wrapping_add(17) % ROWS
}

// Permuted outer rows make a missing ORDER BY observable. Half the keys have no
// inner group; every eighth inner group has only NULL x values.
fn register_subquery_inputs(ctx: &SessionContext) -> Result<usize> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Decimal128(18, 4), false),
    ]));
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(BATCH_SIZE) {
        let ids = (start..(start + BATCH_SIZE).min(ROWS)).map(|i| subquery_input_id(i) as i64);
        batches.push(RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(ids.clone())),
                Arc::new(Int64Array::from_iter_values(ids.clone().map(|i| i % 512))),
                Arc::new(
                    Decimal128Array::from_iter_values(ids.map(|i| i128::from(i % 97 + 2) * 100))
                        .with_precision_and_scale(18, 4)?,
                ),
            ],
        )?);
    }
    ctx.register_table(
        "bench_outer",
        Arc::new(MemTable::try_new(schema, vec![batches])?),
    )?;
    let keys = (0_i64..256)
        .flat_map(|k| std::iter::repeat_n(k, (1 + k % 3) as usize))
        .collect::<Vec<_>>();
    let inner_rows = keys.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("x", DataType::Decimal128(18, 4), true),
        Field::new("d", DataType::Decimal128(18, 4), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(keys.iter().copied())),
            Arc::new(
                Decimal128Array::from_iter(
                    keys.iter()
                        .map(|k| (k % 8 != 0).then_some(i128::from(k + 1) * 10_000)),
                )
                .with_precision_and_scale(18, 4)?,
            ),
            Arc::new(
                Decimal128Array::from_iter_values(std::iter::repeat_n(40_000, inner_rows))
                    .with_precision_and_scale(18, 4)?,
            ),
        ],
    )?;
    ctx.register_table(
        "bench_inner",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(inner_rows)
}

fn expected_subquery_value(case: &str, id: usize) -> Option<i128> {
    let key = id % 512;
    let count = if key < 256 { 1 + key % 3 } else { 0 };
    match case {
        "no_division" | "round_wide" => Some(id as i128 * 1_000_000),
        "round_decimal" => Some((id % 97 + 2) as i128 * 10_000),
        "div_integer" | "div_decimal" => Some((id / 4) as i128 * 1_000_000),
        "div_column" => Some((id / (key + 1)) as i128 * 1_000_000),
        "negative_literal" => Some((id % 97 + 2) as i128 * -2_500),
        "plain_projection" | "plain_sorted" | "scalar_projection" | "scalar_sorted" => {
            Some((id % 97 + 2) as i128 * 2_500)
        }
        "correlated_max" => (key < 256 && key % 8 != 0).then_some((key + 1) as i128 * 250_000),
        "correlated_count" | "nested_lateral" => Some(count as i128 * 250_000),
        "chained_lateral" => Some((count + 1 + count % 3) as i128 * 250_000),
        "left_lateral" => (count > 1).then_some(count as i128 * 250_000),
        "null_divide_left" | "null_divide_right" => None,
        _ => unreachable!("unknown benchmark case"),
    }
}

async fn validate_subquery(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    case: &str,
    sorted: bool,
) -> Result<(usize, usize, i128)> {
    if plan.schema().fields().len() != 1
        || plan.schema().field(0).data_type() != &DataType::Decimal128(38, 6)
    {
        return Err(format!("unexpected result schema: {:?}", plan.schema()).into());
    }
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let (mut rows, mut nulls, mut sum) = (0, 0, 0);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for actual in values.iter() {
            if rows >= ROWS {
                return Err("too many output rows".into());
            }
            let id = if sorted {
                rows
            } else {
                subquery_input_id(rows)
            };
            let expected = expected_subquery_value(case, id);
            if actual != expected {
                return Err(format!("row {rows}: expected {expected:?}, got {actual:?}").into());
            }
            rows += 1;
            nulls += usize::from(actual.is_none());
            sum += actual.unwrap_or(0);
        }
    }
    if rows != ROWS {
        return Err(format!("expected {ROWS} rows, got {rows}").into());
    }
    Ok((rows, nulls, sum))
}

async fn subquery_bench(
    ctx: &SessionContext,
    output: &str,
    selected: Option<&str>,
    check_only: bool,
) -> Result<()> {
    let inner_rows = register_subquery_inputs(ctx)?;
    let phase = std::env::var("DECIMAL_BENCH_PERF_PHASE").unwrap_or_else(|_| "execution".into());
    if phase != "planning" && phase != "execution" {
        return Err("DECIMAL_BENCH_PERF_PHASE must be planning or execution".into());
    }
    let divisor = "(SELECT min(d) FROM bench_inner)";
    let count = "SELECT count(*) AS n FROM bench_inner i WHERE i.k=o.k";
    let cases = [
        (
            "no_division",
            "CAST(o.id AS DECIMAL(38,6))".to_owned(),
            "".to_owned(),
            false,
        ),
        (
            "plain_projection",
            "o.a / CAST(4 AS DECIMAL(18,4))".into(),
            "".into(),
            false,
        ),
        (
            "negative_literal",
            "o.a / CAST(-4 AS DECIMAL(18,4))".into(),
            "".into(),
            false,
        ),
        ("round_decimal", "ROUND(o.a, 2)".into(), "".into(), false),
        ("div_integer", "o.id DIV 4".into(), "".into(), false),
        (
            "div_decimal",
            "CAST(o.id AS DECIMAL(18,2)) DIV CAST(4 AS DECIMAL(18,2))".into(),
            "".into(),
            false,
        ),
        ("div_column", "o.id DIV (o.k + 1)".into(), "".into(), false),
        (
            "round_wide",
            "ROUND(CAST(o.id AS DECIMAL(38,4)), 2)".into(),
            "".into(),
            false,
        ),
        (
            "plain_sorted",
            "o.a / CAST(4 AS DECIMAL(18,4))".into(),
            "".into(),
            true,
        ),
        (
            "scalar_projection",
            format!("o.a / {divisor}"),
            "".into(),
            false,
        ),
        ("scalar_sorted", format!("o.a / {divisor}"), "".into(), true),
        (
            "null_divide_left",
            format!("CAST(NULL AS DECIMAL(18,4)) / {divisor}"),
            "".into(),
            false,
        ),
        (
            "null_divide_right",
            format!("{divisor} / CAST(NULL AS DECIMAL(18,4))"),
            "".into(),
            false,
        ),
        (
            "correlated_max",
            format!("(SELECT max(i.x) / {divisor} FROM bench_inner i WHERE i.k=o.k)"),
            "".into(),
            true,
        ),
        (
            "correlated_count",
            format!(
                "(SELECT CAST(count(*) AS DECIMAL(18,4)) / {divisor} FROM bench_inner i WHERE i.k=o.k)"
            ),
            "".into(),
            true,
        ),
        (
            "nested_lateral",
            format!("CAST(t.n AS DECIMAL(18,4)) / {divisor}"),
            format!("CROSS JOIN LATERAL (SELECT z.n FROM ({count}) z) t"),
            true,
        ),
        (
            "chained_lateral",
            format!("CAST(t.n + u.m AS DECIMAL(18,4)) / {divisor}"),
            format!(
                "CROSS JOIN LATERAL ({count}) t CROSS JOIN LATERAL (SELECT count(*) AS m FROM bench_inner j WHERE j.k=t.n) u"
            ),
            true,
        ),
        (
            "left_lateral",
            format!("CAST(t.n AS DECIMAL(18,4)) / {divisor}"),
            format!("LEFT JOIN LATERAL ({count}) t ON t.n>1"),
            true,
        ),
    ];
    let mut results = Vec::new();
    for (case, expression, join, sorted) in cases {
        let order = if sorted { "ORDER BY o.id" } else { "" };
        let sql = format!(
            "SELECT CAST({expression} AS DECIMAL(38,6)) AS r FROM bench_outer o {join} {order}"
        );
        for ansi in [true, false] {
            let id = format!("{case}_ansi{ansi}");
            if selected.is_some_and(|s| s != id) {
                continue;
            }
            let mut result = json!({"id": id, "sql": sql, "ansi": ansi, "sorted": sorted});
            let plan = match plan_query(ctx, &sql, ansi).await {
                Ok(plan) => plan,
                Err(error) if check_only => {
                    result["status"] = json!("planning_error");
                    result["error"] = json!(error.to_string());
                    results.push(result);
                    continue;
                }
                Err(error) => return Err(error),
            };
            result["physical_plan"] = json!(displayable(plan.as_ref()).indent(true).to_string());
            let (rows, nulls, sum) = match validate_subquery(ctx, plan, case, sorted).await {
                Ok(counts) => counts,
                Err(error) if check_only => {
                    result["status"] = json!("validation_error");
                    result["error"] = json!(error.to_string());
                    results.push(result);
                    continue;
                }
                Err(error) => return Err(error),
            };
            result["status"] = json!("ok");
            result["validated_rows"] = json!(rows);
            result["output_nulls"] = json!(nulls);
            result["coefficient_sum"] = json!(sum.to_string());
            if !check_only {
                for _ in 0..WARMUPS {
                    let plan = plan_query(ctx, &sql, ansi).await?;
                    assert_eq!(consume(ctx, plan).await?, (rows, nulls));
                }
                // Every execution gets a fresh physical plan. ScalarSubqueryExec
                // and hash joins otherwise retain results from their first run.
                let mut plans = Vec::new();
                let mut planning_ms = Vec::new();
                if phase == "planning" {
                    perf_command(b"enable\n")?;
                }
                for _ in 0..SAMPLES {
                    let start = Instant::now();
                    let plan = plan_query(ctx, &sql, ansi).await?;
                    planning_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    plans.push(plan);
                }
                if phase == "planning" {
                    perf_command(b"disable\n")?;
                }
                let mut execution_ms = Vec::new();
                if phase == "execution" {
                    perf_command(b"enable\n")?;
                }
                for plan in &plans {
                    let start = Instant::now();
                    let counts = consume(ctx, plan.clone()).await?;
                    execution_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    assert_eq!(counts, (rows, nulls));
                }
                if phase == "execution" {
                    perf_command(b"disable\n")?;
                }
                result["planning_samples_ms"] = json!(planning_ms);
                result["samples_ms"] = json!(execution_ms);
            }
            eprintln!("{id}: validated {rows} rows, {nulls} NULLs");
            results.push(result);
        }
    }
    if results.is_empty() {
        return Err("no benchmark case matched CASE_ID".into());
    }
    fs::write(
        output,
        serde_json::to_vec_pretty(&json!({
            "rows": ROWS, "inner_rows": inner_rows, "batch_size": BATCH_SIZE, "partitions": 1,
            "input_permutation": "(position * 4099 + 17) % rows",
            "warmups": WARMUPS, "samples": SAMPLES, "check_only": check_only,
            "perf_phase": phase, "results": results,
        }))?,
    )?;
    Ok(())
}

async fn cast_bench(ctx: &SessionContext, output: &str, selected: Option<&str>) -> Result<()> {
    let mut results = Vec::new();
    for nulls in [false, true] {
        ctx.deregister_table("bench_input")?;
        ctx.register_table("bench_input", Arc::new(input(18, 4, 4, nulls)?))?;
        // Prepare converted input arrays outside the measured cast.
        for (table, data_type) in [
            ("bench_strings", "VARCHAR"),
            ("bench_float32", "REAL"),
            ("bench_float64", "DOUBLE"),
        ] {
            ctx.deregister_table(table)?;
            let frame = ctx
                .sql(&format!(
                    "SELECT CAST(a AS {data_type}) AS a FROM bench_input"
                ))
                .await?;
            let schema = Arc::new(frame.schema().as_arrow().clone());
            ctx.register_table(
                table,
                Arc::new(MemTable::try_new(schema, vec![frame.collect().await?])?),
            )?;
        }
        for (kind, table, precision, scale) in [
            ("narrow", "bench_input", 10, 2),
            ("widen", "bench_input", 38, 4),
            ("string", "bench_strings", 18, 4),
            ("float32", "bench_float32", 18, 4),
            ("float64", "bench_float64", 18, 4),
        ] {
            let sql = format!("SELECT CAST(a AS DECIMAL({precision},{scale})) AS r FROM {table}");
            for ansi in [true, false] {
                let id = format!("cast_{kind}_nulls{nulls}_ansi{ansi}");
                if selected.is_some_and(|s| s != id) {
                    continue;
                }
                let plan = plan_query(ctx, &sql, ansi).await?;
                let mut stream = execute_stream(plan.clone(), ctx.task_ctx())?;
                let (mut rows, mut output_nulls, mut coefficient_sum) = (0, 0, 0_i128);
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or("expected Decimal128 output")?;
                    assert_eq!(values.data_type(), &DataType::Decimal128(precision, scale));
                    for value in values.iter() {
                        let expected = if nulls && rows % 10 == 9 {
                            None
                        } else {
                            Some((rows % 10000 + 2) as i128 * 10_i128.pow((scale - 2) as u32))
                        };
                        assert_eq!(value, expected, "{id}, row {rows}");
                        rows += 1;
                        output_nulls += usize::from(value.is_none());
                        coefficient_sum += value.unwrap_or(0);
                    }
                }
                assert_eq!(rows, ROWS);
                for _ in 0..WARMUPS {
                    assert_eq!(consume(ctx, plan.clone()).await?, (rows, output_nulls));
                }
                let mut samples_ms = Vec::new();
                perf_command(b"enable\n")?;
                for _ in 0..SAMPLES {
                    let start = Instant::now();
                    let counts = consume(ctx, plan.clone()).await?;
                    samples_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    assert_eq!(counts, (rows, output_nulls));
                }
                perf_command(b"disable\n")?;
                eprintln!("{id}: validated {rows} rows, {output_nulls} NULLs");
                results.push(json!({
                    "id": id, "sql": sql, "validated_rows": rows,
                    "output_nulls": output_nulls, "coefficient_sum": coefficient_sum.to_string(),
                    "output_nullable": plan.schema().field(0).is_nullable(),
                    "output_type": format!("{:?}", plan.schema().field(0).data_type()),
                    "physical_plan": displayable(plan.as_ref()).indent(true).to_string(),
                    "samples_ms": samples_ms,
                }));
            }
        }
    }
    if results.is_empty() {
        return Err("no benchmark case matched CASE_ID".into());
    }
    fs::write(
        output,
        serde_json::to_vec_pretty(&json!({
            "rows": ROWS, "batch_size": BATCH_SIZE, "partitions": 1,
            "warmups": WARMUPS, "samples": SAMPLES, "results": results,
        }))?,
    )?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("use cargo build --release for timing".into());
    }
    let args = std::env::args().collect::<Vec<_>>();
    let output = args
        .get(1)
        .ok_or("usage: decimal_bench OUTPUT_JSON [SUITE [CASE_ID]]")?;
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
    let inputs = match args.get(2).map(String::as_str) {
        Some("casts") => return cast_bench(&ctx, output, args.get(3).map(String::as_str)).await,
        Some(mode @ ("subqueries" | "subquery-check")) => {
            return subquery_bench(
                &ctx,
                output,
                args.get(3).map(String::as_str),
                mode == "subquery-check",
            )
            .await;
        }
        None | Some("normal") => vec![
            (10, 2, 2, false),
            (18, 4, 4, false),
            (38, 6, 6, false),
            (10, 2, 2, true),
            (0, 2, 2, false),
        ],
        Some("float") => vec![(0, 2, 2, false), (0, 2, 2, true)],
        Some("high-scale") => vec![(38, 35, 35, false), (38, 38, 38, false), (38, 38, 38, true)],
        Some("high-scale-35") => vec![(38, 35, 35, false)],
        // Scale 6 still needs the fallback after shifting; scale 7 just fits i256.
        Some("high-scale-mixed") => vec![(38, 6, 38, false), (38, 7, 38, false), (38, 6, 38, true)],
        Some(_) => {
            return Err(
                "expected normal, float, high-scale, high-scale-35, high-scale-mixed, subqueries, subquery-check or casts"
                    .into(),
            );
        }
    };
    let mut results = Vec::new();
    for (precision, scale, divisor_scale, nulls) in inputs {
        ctx.deregister_table("bench_input")?;
        ctx.register_table(
            "bench_input",
            Arc::new(input(precision, scale, divisor_scale, nulls)?),
        )?;
        for divisor_kind in [
            "column",
            "typed_literal",
            "integer_literal",
            "numerator_literal",
        ] {
            if divisor_kind == "numerator_literal" && precision != 0 {
                continue;
            }
            if divisor_kind == "integer_literal" && (nulls || precision == 0 || divisor_scale >= 35)
            {
                continue;
            }
            let numerator_scalar = divisor_kind == "numerator_literal";
            let scalar = !matches!(divisor_kind, "column" | "numerator_literal");
            let denominator = if !scalar {
                "b".to_owned()
            } else if divisor_kind == "integer_literal" {
                "3".to_owned()
            } else if precision == 0 {
                "CAST(3 AS DOUBLE)".to_owned()
            } else if divisor_scale >= 35 {
                format!("CAST('0.3' AS DECIMAL({precision},{divisor_scale}))")
            } else {
                format!("CAST(3 AS DECIMAL({precision},{divisor_scale}))")
            };
            let numerator = if numerator_scalar {
                "CAST(3 AS DOUBLE)"
            } else {
                "a"
            };
            let sql = format!("SELECT {numerator} / {denominator} AS quotient FROM bench_input");
            let expected_nulls = (0..ROWS)
                .filter(|i| {
                    nulls && ((!numerator_scalar && i % 10 == 9) || (!scalar && i % 13 == 12))
                })
                .count();
            for ansi in [true, false] {
                let divisor_id = if scale == divisor_scale {
                    String::new()
                } else {
                    format!("_divisor_s{divisor_scale}")
                };
                let id = format!(
                    "p{precision}_s{scale}{divisor_id}_nulls{nulls}_{divisor_kind}_ansi{ansi}"
                );
                if args.get(3).is_some_and(|selected| selected != &id) {
                    continue;
                }
                let plan = plan_query(&ctx, &sql, ansi).await?;
                let physical_plan = displayable(plan.as_ref()).indent(true).to_string();
                let mut stream = execute_stream(plan.clone(), ctx.task_ctx())?;
                let first = stream.next().await.ok_or("missing first batch")??;
                let first_values = (0..3)
                    .map(|i| array_value_to_string(first.column(0).as_ref(), i))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                drop(stream);
                if precision == 0 {
                    // Validate every float output outside the timed section.
                    let mut stream = execute_stream(plan.clone(), ctx.task_ctx())?;
                    let mut offset = 0;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        let output = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or("expected Float64 quotient")?;
                        for (row, actual) in output.iter().enumerate() {
                            let i = offset + row;
                            let a = if numerator_scalar {
                                Some(3.0)
                            } else if nulls && i % 10 == 9 {
                                None
                            } else {
                                Some((i % 10000 + 2) as f64 / 100.0)
                            };
                            let b = if scalar {
                                Some(3.0)
                            } else if nulls && i % 13 == 12 {
                                None
                            } else {
                                Some((i % 97 + 3) as f64 / 100.0)
                            };
                            assert_eq!(actual, a.zip(b).map(|(a, b)| a / b), "{id}, row {i}");
                        }
                        offset += batch.num_rows();
                    }
                    assert_eq!(offset, ROWS);
                }
                for _ in 0..WARMUPS {
                    assert_eq!(consume(&ctx, plan.clone()).await?, (ROWS, expected_nulls));
                }
                let mut elapsed_ms = Vec::new();
                perf_command(b"enable\n")?;
                for _ in 0..SAMPLES {
                    let start = Instant::now();
                    let counts = consume(&ctx, plan.clone()).await?;
                    elapsed_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    assert_eq!(counts, (ROWS, expected_nulls));
                }
                perf_command(b"disable\n")?;
                let mut sorted = elapsed_ms.clone();
                sorted.sort_by(f64::total_cmp);
                let median_ms = sorted[SAMPLES / 2];
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
    if results.is_empty() {
        return Err("no benchmark case matched CASE_ID".into());
    }
    let capture: Value = json!({
        "rows": ROWS, "batch_size": BATCH_SIZE, "partitions": 1,
        "warmups": WARMUPS, "samples": SAMPLES, "results": results,
    });
    fs::write(output, serde_json::to_vec_pretty(&capture)?)?;
    Ok(())
}
