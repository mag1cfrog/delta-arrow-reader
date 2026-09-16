// Standalone Arrow kernel benchmark. Link against the candidate arrow-cast,
// arrow-array and arrow-schema release artifacts (see the evaluation report).
use arrow_array::{Array, ArrayRef, Decimal128Array, Float64Array, types::Float64Type};
use arrow_schema::DataType;
use std::{hint::black_box, sync::Arc, time::Instant};

fn convert(input: &Decimal128Array, scale: i8, variant: &str) -> ArrayRef {
    match variant {
        "before" => {
            // Exact formula from arrow-cast 58.4.0's Decimal128 -> Float64 arm.
            Arc::new(input.unary::<_, Float64Type>(|v| v as f64 / 10_f64.powi(scale as i32)))
        }
        "after" => arrow_cast::cast(input, &DataType::Float64).unwrap(),
        _ => panic!("expected before or after"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let case = args.get(1).expect("case name").as_str();
    let variant = args.get(2).expect("before or after").as_str();
    let (scale, multiplier, nulls) = match case {
        "small" => (4, 100_i128, false),
        "small_nulls" => (4, 100, true),
        "exact_wide" => (18, 10_000_000_000_000_000, false),
        "exact_128" => (18, 1_i128 << 100, false),
        "exact_wide_nulls" => (18, 10_000_000_000_000_000, true),
        "negative_exact" => (-18, 10_000_000_000_000_000, false),
        "wide" => (18, 987_654_321_098_765_432_101, false),
        "precision38" => (18, 987_654_321_098_765_432_101_987_654_321_001_111, false),
        "wide_nulls" => (18, 987_654_321_098_765_432_101, true),
        "scale22" => (22, 987_654_321_098_765_432_101, false),
        "scale23" => (23, 987_654_321_098_765_432_101, false),
        "scale27" => (27, 987_654_321_098_765_432_101, false),
        "scale28" => (28, 987_654_321_098_765_432_101, false),
        "scale31" => (31, 987_654_321_098_765_432_101, false),
        "scale31_small" => (31, 1, false),
        "scale31_precision38" => (31, 987_654_321_098_765_432_101_987_654_321_001_111, false),
        "scale31_nulls" => (31, 987_654_321_098_765_432_101, true),
        "scale32" => (32, 987_654_321_098_765_432_101, false),
        "scale38_small" => (38, 1, false),
        "scale38_wide" => (38, 987_654_321_098_765_432_101, false),
        "scale0" => (0, 987_654_321_098_765_432_101, false),
        "negative_scale" => (-4, 100, false),
        "negative_wide" => (-22, 987_654_321_098_765_432_101, false),
        _ => panic!("unknown case"),
    };
    const ROWS: usize = 1_048_576;
    const BATCH: usize = 8192;
    let coefficient = |id: usize| {
        let value = (id % 97 + 2) as i128 * multiplier;
        if id % 2 == 0 { value } else { -value }
    };
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(BATCH) {
        // Slicing preserves a nonzero buffer/null-bit offset.
        let array = Decimal128Array::from_iter(std::iter::once(Some(0)).chain(
            (start..start + BATCH).map(|id| {
                if nulls && id % 4 != 0 {
                    None
                } else {
                    Some(coefficient(id))
                }
            }),
        ))
        .with_precision_and_scale(38, scale)
        .unwrap();
        batches.push(array.slice(1, BATCH));
    }
    let mut checked = 0;
    let mut oracle_differences = 0;
    for input in &batches {
        let result = convert(input, scale, variant);
        let actual = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(actual.nulls(), input.nulls());
        for (v, actual) in input.iter().zip(actual.iter()) {
            let oracle = v.map(|v| format!("{v}e{}", -(scale as i16)).parse::<f64>().unwrap());
            let expected = if variant == "before" {
                v.map(|v| v as f64 / 10_f64.powi(scale as i32))
            } else {
                oracle
            };
            assert_eq!(actual.map(f64::to_bits), expected.map(f64::to_bits));
            oracle_differences += usize::from(actual.map(f64::to_bits) != oracle.map(f64::to_bits));
            checked += 1;
        }
    }
    let mut samples = Vec::new();
    for run in 0..11 {
        let start = Instant::now();
        for input in &batches {
            black_box(convert(black_box(input), black_box(scale), variant));
        }
        if run >= 2 {
            samples.push(start.elapsed().as_secs_f64() * 1000.);
        }
    }
    assert_eq!(checked, ROWS);
    println!(
        "{{\"case\":\"{case}\",\"variant\":\"{variant}\",\"rows\":{ROWS},\"batch_size\":{BATCH},\"checked\":{checked},\"oracle_differences\":{oracle_differences},\"samples_ms\":{samples:?}}}"
    );
}
