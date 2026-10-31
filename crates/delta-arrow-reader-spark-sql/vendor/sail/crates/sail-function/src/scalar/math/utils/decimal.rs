// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::cmp::{max, min};

use datafusion::arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};

// https://github.com/apache/spark/blob/50a328ba98577ea12bbae50f2cbf406438b01a2f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1491-L1508
#[inline]
pub fn round_decimal_base(
    precision: i32,
    scale: i32,
    target_scale: i32,
    decimal_128: bool,
) -> (u8, i8) {
    let integral_least_num_digits = precision - scale + 1;
    if target_scale < 0 {
        // Match Spark's Int arithmetic, and cap precision before narrowing to u8.
        let new_precision = max(
            integral_least_num_digits,
            target_scale.wrapping_neg().wrapping_add(1),
        );
        if decimal_128 {
            (
                min(new_precision, i32::from(DECIMAL128_MAX_PRECISION)) as u8,
                0,
            )
        } else {
            (
                min(new_precision, i32::from(DECIMAL256_MAX_PRECISION)) as u8,
                0,
            )
        }
    } else {
        let new_scale = min(scale, target_scale);
        let max_precision = if decimal_128 {
            DECIMAL128_MAX_PRECISION
        } else {
            DECIMAL256_MAX_PRECISION
        } as i32;
        (
            min(integral_least_num_digits + new_scale, max_precision) as u8,
            new_scale as i8,
        )
    }
}
