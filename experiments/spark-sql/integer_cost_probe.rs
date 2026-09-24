//! Diagnostic physical-expression comparison; native eager evaluation is not a Spark replacement.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    error::Error,
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    },
    time::Instant,
};

use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_physical_expr::{
    PhysicalExpr, ScalarFunctionExpr,
    expressions::{BinaryExpr, Column, Literal},
};
use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;
use serde_json::json;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Expr = Arc<dyn PhysicalExpr>;
const ROWS: usize = 1_048_576;
const WARMUPS: usize = 4;
const SAMPLES: usize = 15;

// Count a separate, untimed execution. Timing keeps counting disabled. All
// expression outputs are dropped before stopping, while input/plan storage lives on.
struct CountingAllocator;
static COUNT: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocation(size: usize) {
    CALLS.fetch_add(1, Relaxed);
    BYTES.fetch_add(size, Relaxed);
    let live = LIVE.fetch_add(size, Relaxed) + size;
    PEAK.fetch_max(live, Relaxed);
}

// SAFETY: Forward the allocator contract and original layout/pointers to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && COUNT.load(Relaxed) {
            allocation(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && COUNT.load(Relaxed) {
            allocation(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if COUNT.load(Relaxed) {
            LIVE.fetch_sub(layout.size(), Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(ptr, layout, size) };
        if !out.is_null() && COUNT.load(Relaxed) {
            LIVE.fetch_sub(layout.size(), Relaxed);
            allocation(size);
        }
        out
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn expression(
    schema: &Schema,
    width: usize,
    op: Operator,
    shape: &str,
    mode: &str,
) -> Result<Expr> {
    let left: Expr = Arc::new(Column::new("a", 0));
    let column: Expr = Arc::new(Column::new("b", 1));
    let scalar: Expr = Arc::new(Literal::new(if width == 32 {
        ScalarValue::Int32(Some(2))
    } else {
        ScalarValue::Int64(Some(2))
    }));
    let operation = |left: Expr, right: Expr| -> Result<Expr> {
        if mode == "spark" {
            Ok(Arc::new(ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::new_from_impl(SparkCheckedArithmetic::new(op))),
                vec![left, right],
                schema,
                Arc::new(ConfigOptions::default()),
            )?))
        } else {
            Ok(Arc::new(
                BinaryExpr::new(left, op, right).with_fail_on_overflow(mode != "wrap"),
            ))
        }
    };
    let right = match shape {
        "scalar" => scalar,
        "nested_nulls" => operation(column, scalar)?,
        _ => column,
    };
    operation(left, right)
}

fn execute(expr: &Expr, batches: &[RecordBatch]) -> Result<()> {
    for batch in batches {
        std::hint::black_box(expr.evaluate(batch)?);
    }
    Ok(())
}

fn validate(expr: &Expr, batches: &[RecordBatch], op: Operator, shape: &str) -> Result<()> {
    let mut row = 0;
    for batch in batches {
        let result = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        assert_eq!(result.data_type(), batch.column(0).data_type());
        for index in 0..result.len() {
            let null = shape.ends_with("nulls") && row % 7 == 0;
            assert_eq!(result.is_null(index), null);
            if !null {
                let left = (row % 1024) as i64;
                let right = if shape == "nested_nulls" { 4 } else { 2 };
                let expected = match op {
                    Operator::Plus => left + right,
                    Operator::Minus => left - right,
                    Operator::Multiply => left * right,
                    _ => unreachable!(),
                };
                let actual = if let Some(array) = result.as_any().downcast_ref::<Int32Array>() {
                    i64::from(array.value(index))
                } else {
                    result
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(index)
                };
                assert_eq!(actual, expected);
            }
            row += 1;
        }
    }
    assert_eq!(row, ROWS);
    Ok(())
}

fn check_diagnostic_boundary() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![i32::MAX])) as ArrayRef,
        ],
    )?;
    let native = expression(&schema, 32, Operator::Plus, "nested_nulls", "native")?;
    let spark = expression(&schema, 32, Operator::Plus, "nested_nulls", "spark")?;
    assert!(native.evaluate(&batch).is_err());
    let result = spark.evaluate(&batch)?.into_array(1)?;
    assert!(result.is_null(0));
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let out = args.get(1).ok_or("expected output path")?;
    let batch_size: usize = args.get(2).ok_or("expected batch size")?.parse()?;
    assert!(matches!(batch_size, 256 | 8192));
    let reverse = args.get(3).is_some_and(|value| value == "reverse");
    check_diagnostic_boundary()?;
    let mut results = Vec::new();
    for width in [32, 64] {
        for (name, op) in [
            ("add", Operator::Plus),
            ("sub", Operator::Minus),
            ("mul", Operator::Multiply),
        ] {
            for shape in ["arrays", "scalar", "nulls", "nested_nulls"] {
                if shape == "nested_nulls" && op != Operator::Plus {
                    continue;
                }
                let nullable = shape.ends_with("nulls");
                let ty = if width == 32 {
                    DataType::Int32
                } else {
                    DataType::Int64
                };
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", ty.clone(), nullable),
                    Field::new("b", ty, false),
                ]));
                let batches = (0..ROWS)
                    .step_by(batch_size)
                    .map(|start| {
                        let end = (start + batch_size).min(ROWS);
                        let arrays: Vec<ArrayRef> = if width == 32 {
                            vec![
                                Arc::new(Int32Array::from_iter((start..end).map(|row| {
                                    (!nullable || row % 7 != 0).then_some((row % 1024) as i32)
                                }))),
                                Arc::new(Int32Array::from(vec![2; end - start])),
                            ]
                        } else {
                            vec![
                                Arc::new(Int64Array::from_iter((start..end).map(|row| {
                                    (!nullable || row % 7 != 0).then_some((row % 1024) as i64)
                                }))),
                                Arc::new(Int64Array::from(vec![2; end - start])),
                            ]
                        };
                        RecordBatch::try_new(schema.clone(), arrays).unwrap()
                    })
                    .collect::<Vec<_>>();
                let native = expression(&schema, width, op, shape, "native")?;
                let spark = expression(&schema, width, op, shape, "spark")?;
                let wrapping = expression(&schema, width, op, shape, "wrap")?;
                let mut modes = vec![
                    ("wrapping", wrapping),
                    ("native_a", native.clone()),
                    ("spark_a", spark.clone()),
                    ("spark_b", spark),
                    ("native_b", native),
                ];
                if reverse {
                    modes.reverse();
                }
                for (mode, expr) in modes {
                    validate(&expr, &batches, op, shape)?;
                    let mut samples = Vec::new();
                    for i in 0..WARMUPS + SAMPLES {
                        let start = Instant::now();
                        execute(&expr, &batches)?;
                        let ms = start.elapsed().as_secs_f64() * 1000.0;
                        if i >= WARMUPS {
                            samples.push(ms);
                        }
                    }
                    for counter in [&CALLS, &BYTES, &LIVE, &PEAK] {
                        counter.store(0, Relaxed);
                    }
                    COUNT.store(true, Relaxed);
                    let status = execute(&expr, &batches);
                    COUNT.store(false, Relaxed);
                    status?;
                    let live = LIVE.load(Relaxed);
                    assert_eq!(
                        live, 0,
                        "all measured outputs and temporary allocations must be dropped"
                    );
                    results.push(json!({
                        "id": format!("i{width}_{name}_{shape}"), "mode": mode,
                        "expression": expr.to_string(), "execution_ms": samples,
                        "allocation_calls": CALLS.load(Relaxed), "allocated_bytes": BYTES.load(Relaxed),
                        "peak_live_bytes": PEAK.load(Relaxed), "live_bytes_after": live,
                    }));
                }
            }
        }
    }
    fs::write(
        out,
        serde_json::to_vec_pretty(&json!({
            "rows": ROWS, "batch_size": batch_size, "reverse": reverse,
            "warmups": WARMUPS, "samples": SAMPLES, "boundary_check": "passed",
            "measurement": "physical expressions only; allocation counters use a separate untimed pass",
            "results": results,
        }))?,
    )?;
    Ok(())
}
