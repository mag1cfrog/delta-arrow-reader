//! Isolate the native Float64 IN expression used by the SQL benchmark.
//! Input generation matches generic_in_double_zero_long_list in decimal_bench.rs.

use std::fs;
use std::hint::black_box;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array};
use arrow::compute::kernels::numeric::add_wrapping;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, InListExpr};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const ROWS: usize = 1_048_576;
const BATCH_SIZE: usize = 8192;
const MEMBERS: usize = 128;

fn input_id(position: usize) -> usize {
    position.wrapping_mul(4099).wrapping_add(17) % ROWS
}

fn input_value(position: usize) -> f64 {
    match input_id(position) % 97 {
        0 => 0.0,
        1 => -0.0,
        2 => f64::NAN,
        3 => -f64::NAN,
        4 => f64::from_bits(0x7ff0_0000_0000_0123),
        5 => f64::from_bits(0xfff0_0000_0000_0123),
        6 => -1.25,
        value => value as f64,
    }
}

// Same FIFO handshake as decimal_bench.rs; the controller gates the phase.
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

fn positive_setting(name: &str, default: usize) -> Result<usize> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}

fn execute(expr: &dyn PhysicalExpr, batches: &[RecordBatch]) -> Result<()> {
    for batch in batches {
        black_box(expr.evaluate(black_box(batch))?);
    }
    Ok(())
}

fn validate(expr: &dyn PhysicalExpr, batches: &[RecordBatch]) -> Result<usize> {
    let mut matching = 0;
    let mut position = 0;
    for batch in batches {
        let array = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let actual = array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or("expected Boolean IN output")?;
        assert_eq!(actual.null_count(), 0);
        for value in actual.values() {
            let member = input_id(position) % 97;
            let expected = member <= 1 || member >= 7;
            assert_eq!(value, expected, "row {position}");
            matching += usize::from(value);
            position += 1;
        }
    }
    assert_eq!(position, ROWS);
    assert_eq!(matching, 994_522);
    Ok(matching)
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 || !matches!(args[2].as_str(), "check" | "measure") {
        return Err("usage: native_in_bench OUTPUT.json check|measure".into());
    }
    let warmups = positive_setting("DECIMAL_BENCH_IN_WARMUPS", 32)?;
    let quartets = positive_setting("DECIMAL_BENCH_IN_SAMPLES", 261)?;
    let executions = quartets.checked_mul(4).ok_or("sample count overflow")?;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
    let zero = Float64Array::new_scalar(0.0);
    let mut fingerprint = 0xcbf2_9ce4_8422_2325_u64;
    let mut normalized_special_bits = [None; 7];
    let mut batches = Vec::with_capacity(ROWS / BATCH_SIZE);
    for start in (0..ROWS).step_by(BATCH_SIZE) {
        let raw = Float64Array::from_iter_values((start..start + BATCH_SIZE).map(input_value));
        // Match v + 0 using Arrow's actual arithmetic kernel, outside measurement.
        let normalized = add_wrapping(&raw, &zero)?;
        let values = normalized.as_any().downcast_ref::<Float64Array>().unwrap();
        for (offset, value) in values.values().iter().enumerate() {
            let bits = value.to_bits();
            fingerprint = (fingerprint ^ bits).wrapping_mul(0x100_0000_01b3);
            let member = input_id(start + offset) % 97;
            if member < normalized_special_bits.len() {
                assert!(normalized_special_bits[member].is_none_or(|old| old == bits));
                normalized_special_bits[member] = Some(bits);
            }
        }
        batches.push(RecordBatch::try_new(Arc::clone(&schema), vec![normalized])?);
    }
    assert_eq!(normalized_special_bits[0], Some(0));
    assert_eq!(normalized_special_bits[1], Some(0));
    let members = Arc::new(Float64Array::from_iter_values(
        (0..MEMBERS).map(|n| n as f64),
    ));
    let expr =
        InListExpr::try_new_from_array(Arc::new(Column::new("v", 0)), members, false, &schema)?;
    let expression = expr.to_string();
    assert!(expression.contains("IN (SET)"));
    let matching = validate(&expr, &batches)?;
    let mut samples_ms = Vec::with_capacity(if args[2] == "measure" { executions } else { 0 });
    if args[2] == "measure" {
        for _ in 0..warmups {
            execute(&expr, &batches)?;
        }
        perf_command(b"enable\n")?;
        for _ in 0..executions {
            let start = Instant::now();
            execute(&expr, &batches)?;
            samples_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        perf_command(b"disable\n")?;
        assert_eq!(validate(&expr, &batches)?, matching);
    }
    fs::write(
        &args[1],
        serde_json::to_vec_pretty(&serde_json::json!({
            "mode": args[2], "rows": ROWS, "batch_size": BATCH_SIZE,
            "members": MEMBERS, "input_domain": 97, "matching_inputs": matching,
            "input_permutation": "(position * 4099 + 17) % rows",
            "normalized_input_fingerprint": format!("{fingerprint:016x}"),
            "normalized_special_bits": normalized_special_bits.map(|bits| format!("{:016x}", bits.unwrap())),
            "normalization": "Arrow add_wrapping with scalar Float64 zero, before measurement",
            "retained_tables": 1, "expression": expression, "warmups": warmups,
            "quartets": quartets, "samples_ms": samples_ms,
            "labeling": "post-collection ABBA/BAAB, alternating quartet index and process slot"
        }))?,
    )?;
    Ok(())
}
